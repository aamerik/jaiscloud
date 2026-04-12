//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
)

// TestIceberg_GlueCatalog_SchemaEvolution adds a column to an existing table and
// verifies old rows return NULL for the new column while new rows have values.
func TestIceberg_GlueCatalog_SchemaEvolution(t *testing.T) {
	requireIcebergEnv(t)

	glueClient := newGlueClient(t)
	s3Client := newS3Client(t)

	resetIcebergTables(t, glueClient, "products")

	// Job 1 — Create table with initial schema and insert 50 rows
	runSparkSQL(t, SparkJob{
		Name: "products-initial",
		SQL: `
CREATE TABLE IF NOT EXISTS glue.iceberg_test_db.products (
  id   INT,
  name STRING
)
USING iceberg
LOCATION 's3://iceberg-warehouse/products';

INSERT INTO glue.iceberg_test_db.products
SELECT id, CONCAT('product-', CAST(id AS STRING))
FROM range(1, 51);
`,
	})

	// Job 2 — Add column and insert 50 more rows with price values
	runSparkSQL(t, SparkJob{
		Name: "products-evolve",
		SQL: `
ALTER TABLE glue.iceberg_test_db.products ADD COLUMN price DOUBLE;

INSERT INTO glue.iceberg_test_db.products (id, name, price)
SELECT id + 50, CONCAT('evolved-', CAST(id AS STRING)), CAST(id AS DOUBLE) * 9.99
FROM range(1, 51);
`,
	})

	// Job 3 — Total count
	runSparkSQL(t, SparkJob{
		Name: "products-count",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/products-count/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.products;
`,
	})

	// Job 4 — Rows without price (old rows)
	runSparkSQL(t, SparkJob{
		Name: "products-null-price-count",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/products-nullprice/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.products WHERE price IS NULL;
`,
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Glue GetTable returns "price" column in StorageDescriptor
	tableOut, err := glueClient.GetTable(context.Background(), &awsglue.GetTableInput{
		DatabaseName: aws.String("iceberg_test_db"),
		Name:         aws.String("products"),
	})
	if err != nil {
		t.Fatalf("GetTable products: %v", err)
	}
	foundPrice := false
	for _, col := range tableOut.Table.StorageDescriptor.Columns {
		if aws.ToString(col.Name) == "price" {
			foundPrice = true
		}
	}
	if !foundPrice {
		t.Error("expected 'price' column in Glue table StorageDescriptor")
	}

	// b. metadata_location updated (CAS passed) — non-empty and valid S3 URI
	metaLoc := tableOut.Table.Parameters["metadata_location"]
	if metaLoc == "" || metaLoc[:5] != "s3://" {
		t.Errorf("unexpected metadata_location: %q", metaLoc)
	}

	// c. Total count = 100
	countResult := findS3JSON(t, s3Client, "iceberg-warehouse", "products-count/")
	cnt, _ := countResult["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected total cnt=100, got %v", cnt)
	}

	// d. Exactly 50 rows have NULL price (the original rows)
	nullResult := findS3JSON(t, s3Client, "iceberg-warehouse", "products-nullprice/")
	nullCnt, _ := nullResult["cnt"].(float64)
	if int(nullCnt) != 50 {
		t.Errorf("expected 50 null-price rows, got %v", nullCnt)
	}
}
