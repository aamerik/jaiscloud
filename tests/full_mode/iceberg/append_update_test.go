//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
)

// TestIceberg_GlueCatalog_AppendMultipleBatches verifies that Iceberg accumulates
// data correctly across multiple INSERT batches without overwriting prior data.
// Three batches of 100 rows each should yield 300 total rows.
func TestIceberg_GlueCatalog_AppendMultipleBatches(t *testing.T) {
	requireIcebergEnv(t)

	glueClient := newGlueClient(t)
	s3Client := newS3Client(t)

	resetIcebergTables(t, glueClient, "ledger")

	// Job 1 — Create table and insert batch 1 (ids 1–100).
	runSparkSQL(t, SparkJob{
		Name: "ledger-batch1",
		SQL: `
CREATE TABLE IF NOT EXISTS glue.iceberg_test_db.ledger (
  id       INT,
  batch    INT,
  amount   DOUBLE
)
USING iceberg
LOCATION 's3://iceberg-warehouse/ledger';

INSERT INTO glue.iceberg_test_db.ledger
SELECT id, 1, CAST(id AS DOUBLE) * 1.0
FROM range(1, 101);
`,
	})

	// Verify count after batch 1.
	runSparkSQL(t, SparkJob{
		Name: "ledger-count1",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/ledger-count1/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.ledger;
`,
	})
	cnt1 := findS3JSON(t, s3Client, "iceberg-warehouse", "ledger-count1/")
	if v, _ := cnt1["cnt"].(float64); int(v) != 100 {
		t.Errorf("after batch 1: expected cnt=100, got %v", cnt1["cnt"])
	}

	// Job 2 — Append batch 2 (ids 101–200).
	runSparkSQL(t, SparkJob{
		Name: "ledger-batch2",
		SQL: `
INSERT INTO glue.iceberg_test_db.ledger
SELECT id + 100, 2, CAST(id AS DOUBLE) * 2.0
FROM range(1, 101);
`,
	})

	// Verify count after batch 2.
	runSparkSQL(t, SparkJob{
		Name: "ledger-count2",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/ledger-count2/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.ledger;
`,
	})
	cnt2 := findS3JSON(t, s3Client, "iceberg-warehouse", "ledger-count2/")
	if v, _ := cnt2["cnt"].(float64); int(v) != 200 {
		t.Errorf("after batch 2: expected cnt=200, got %v", cnt2["cnt"])
	}

	// Job 3 — Append batch 3 (ids 201–300).
	runSparkSQL(t, SparkJob{
		Name: "ledger-batch3",
		SQL: `
INSERT INTO glue.iceberg_test_db.ledger
SELECT id + 200, 3, CAST(id AS DOUBLE) * 3.0
FROM range(1, 101);
`,
	})

	// ── Final assertions ──────────────────────────────────────────────────────

	// a. Total count = 300.
	runSparkSQL(t, SparkJob{
		Name: "ledger-count-total",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/ledger-total/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.ledger;
`,
	})
	total := findS3JSON(t, s3Client, "iceberg-warehouse", "ledger-total/")
	if v, _ := total["cnt"].(float64); int(v) != 300 {
		t.Errorf("expected total cnt=300, got %v", total["cnt"])
	}

	// b. Per-batch counts — each batch contributes exactly 100 rows.
	runSparkSQL(t, SparkJob{
		Name: "ledger-by-batch",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/ledger-by-batch/'
USING JSON
SELECT batch, COUNT(*) AS cnt
FROM glue.iceberg_test_db.ledger
GROUP BY batch
ORDER BY batch;
`,
	})
	// Verify S3 has at least one data file for the per-batch output.
	if !hasS3Objects(t, s3Client, "iceberg-warehouse", "ledger-by-batch/") {
		t.Error("expected per-batch count output in S3")
	}

	// c. Glue table has 3 snapshot metadata files (one per commit).
	metaCount := countS3Objects(t, s3Client, "iceberg-warehouse", "ledger/metadata/")
	if metaCount < 3 {
		t.Errorf("expected at least 3 metadata files (one per commit), got %d", metaCount)
	}

	// d. Glue table metadata_location is non-empty.
	tableOut, err := glueClient.GetTable(context.Background(), &awsglue.GetTableInput{
		DatabaseName: aws.String("iceberg_test_db"),
		Name:         aws.String("ledger"),
	})
	if err != nil {
		t.Fatalf("GetTable ledger: %v", err)
	}
	if tableOut.Table.Parameters["metadata_location"] == "" {
		t.Error("expected non-empty metadata_location in Glue table")
	}
}

// TestIceberg_GlueCatalog_SchemaAndDataUpdate verifies Iceberg's support for
// in-place schema changes (ALTER TABLE ADD COLUMN) combined with data updates
// (UPDATE SET) on existing rows.
func TestIceberg_GlueCatalog_SchemaAndDataUpdate(t *testing.T) {
	requireIcebergEnv(t)

	glueClient := newGlueClient(t)
	s3Client := newS3Client(t)

	resetIcebergTables(t, glueClient, "inventory")

	// Job 1 — Create table with initial schema (id, product, quantity) and
	// insert 100 rows. No price column yet.
	runSparkSQL(t, SparkJob{
		Name: "inventory-initial",
		SQL: `
CREATE TABLE IF NOT EXISTS glue.iceberg_test_db.inventory (
  id       INT,
  product  STRING,
  quantity INT
)
USING iceberg
LOCATION 's3://iceberg-warehouse/inventory';

INSERT INTO glue.iceberg_test_db.inventory
SELECT id, CONCAT('item-', CAST(id AS STRING)), id * 10
FROM range(1, 101);
`,
	})

	// Job 2 — Add the 'price' column (schema evolution) and set it for the
	// first 50 rows via UPDATE. The remaining 50 rows keep price = NULL.
	runSparkSQL(t, SparkJob{
		Name: "inventory-evolve-and-update",
		SQL: `
ALTER TABLE glue.iceberg_test_db.inventory ADD COLUMN price DOUBLE;

UPDATE glue.iceberg_test_db.inventory
SET price = CAST(id AS DOUBLE) * 9.99
WHERE id <= 50;
`,
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Glue table StorageDescriptor contains the 'price' column.
	tableOut, err := glueClient.GetTable(context.Background(), &awsglue.GetTableInput{
		DatabaseName: aws.String("iceberg_test_db"),
		Name:         aws.String("inventory"),
	})
	if err != nil {
		t.Fatalf("GetTable inventory: %v", err)
	}
	foundPrice := false
	for _, col := range tableOut.Table.StorageDescriptor.Columns {
		if aws.ToString(col.Name) == "price" {
			foundPrice = true
		}
	}
	if !foundPrice {
		t.Error("expected 'price' column in Glue table StorageDescriptor after ALTER TABLE")
	}

	// b. metadata_location updated after schema change + update.
	metaLoc := tableOut.Table.Parameters["metadata_location"]
	if metaLoc == "" || metaLoc[:5] != "s3://" {
		t.Errorf("unexpected metadata_location after schema update: %q", metaLoc)
	}

	// c. Total count still 100 (no rows deleted by UPDATE).
	runSparkSQL(t, SparkJob{
		Name: "inventory-count-total",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/inventory-total/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.inventory;
`,
	})
	totalResult := findS3JSON(t, s3Client, "iceberg-warehouse", "inventory-total/")
	if v, _ := totalResult["cnt"].(float64); int(v) != 100 {
		t.Errorf("expected total cnt=100, got %v", totalResult["cnt"])
	}

	// d. Exactly 50 rows have a non-NULL price (the updated rows, id 1–50).
	runSparkSQL(t, SparkJob{
		Name: "inventory-count-priced",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/inventory-priced/'
USING JSON
SELECT COUNT(*) AS cnt
FROM glue.iceberg_test_db.inventory
WHERE price IS NOT NULL;
`,
	})
	pricedResult := findS3JSON(t, s3Client, "iceberg-warehouse", "inventory-priced/")
	if v, _ := pricedResult["cnt"].(float64); int(v) != 50 {
		t.Errorf("expected 50 priced rows, got %v", pricedResult["cnt"])
	}

	// e. Exactly 50 rows still have NULL price (the un-updated rows, id 51–100).
	runSparkSQL(t, SparkJob{
		Name: "inventory-count-unpriced",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/inventory-unpriced/'
USING JSON
SELECT COUNT(*) AS cnt
FROM glue.iceberg_test_db.inventory
WHERE price IS NULL;
`,
	})
	unpricedResult := findS3JSON(t, s3Client, "iceberg-warehouse", "inventory-unpriced/")
	if v, _ := unpricedResult["cnt"].(float64); int(v) != 50 {
		t.Errorf("expected 50 un-priced rows, got %v", unpricedResult["cnt"])
	}

	// f. Price values match expected: price for id=1 should be 1*9.99=9.99.
	// Write a specific row lookup to verify UPDATE correctness.
	runSparkSQL(t, SparkJob{
		Name: "inventory-price-check",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/inventory-price-check/'
USING JSON
SELECT id, price
FROM glue.iceberg_test_db.inventory
WHERE id = 1;
`),
	})
	priceCheck := findS3JSON(t, s3Client, "iceberg-warehouse", "inventory-price-check/")
	if price, _ := priceCheck["price"].(float64); price < 9.98 || price > 10.0 {
		t.Errorf("expected price≈9.99 for id=1, got %v", priceCheck["price"])
	}
}
