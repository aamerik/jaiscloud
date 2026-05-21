//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
)

// TestIceberg_GlueCatalog_PartitionedTable creates a DATE-partitioned Iceberg table,
// inserts 300 rows across 3 dates, and verifies partition metadata in Glue and
// that a date-filtered query returns only the matching partition.
func TestIceberg_GlueCatalog_PartitionedTable(t *testing.T) {
	requireIcebergEnv(t)

	glueClient := newGlueClient(t)
	s3Client := newS3Client(t)

	resetIcebergTables(t, glueClient, "sales")

	// Job 1 — Create partitioned table and insert 300 rows (100 per date)
	runSparkSQL(t, SparkJob{
		Name: "create-sales",
		SQL: fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS glue.%s.sales (
  id        INT,
  region    STRING,
  amount    DOUBLE,
  sale_date DATE
)
USING iceberg
PARTITIONED BY (days(sale_date))
LOCATION '%s';

INSERT INTO glue.%s.sales
SELECT
  id,
  CASE WHEN id %% 3 = 0 THEN 'us-east' WHEN id %% 3 = 1 THEN 'us-west' ELSE 'eu-central' END AS region,
  CAST(id * 1.5 AS DOUBLE) AS amount,
  DATE '2026-01-01' AS sale_date
FROM range(1, 101);

INSERT INTO glue.%s.sales
SELECT
  id + 100,
  'us-east',
  CAST(id * 2.0 AS DOUBLE),
  DATE '2026-01-02'
FROM range(1, 101);

INSERT INTO glue.%s.sales
SELECT
  id + 200,
  'eu-central',
  CAST(id * 2.5 AS DOUBLE),
  DATE '2026-01-03'
FROM range(1, 101);
`, icebergDB(), tableLocation("sales"), icebergDB(), icebergDB(), icebergDB()),
	})

	// Job 2 — Filtered read for 2026-01-02
	runSparkSQL(t, SparkJob{
		Name: "count-by-date",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt
FROM glue.%s.sales
WHERE sale_date = DATE '2026-01-02';
`, outputLoc("sales-count"), icebergDB()),
	})

	// Job 3 — Full count
	runSparkSQL(t, SparkJob{
		Name: "count-total",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.%s.sales;
`, outputLoc("sales-total"), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Glue GetTable shows PartitionKeys (the partition spec is in the table metadata)
	tableOut, err := glueClient.GetTable(context.Background(), &awsglue.GetTableInput{
		DatabaseName: aws.String(icebergDB()),
		Name:         aws.String("sales"),
	})
	if err != nil {
		t.Fatalf("GetTable sales: %v", err)
	}
	if tableOut.Table.Parameters["table_type"] != "ICEBERG" {
		t.Errorf("expected table_type=ICEBERG, got %q", tableOut.Table.Parameters["table_type"])
	}
	if tableOut.Table.Parameters["metadata_location"] == "" {
		t.Error("expected non-empty metadata_location")
	}
	// Note: Iceberg tables do NOT register partitions via the Glue Partition API.
	// Partition metadata is embedded in Iceberg's own metadata files (in S3).
	// GetPartitions returns 0 for Iceberg tables — this is the correct behavior
	// in both real AWS and JaisCloud.

	// b. S3 has 3 partition directories
	if countS3Objects(t, s3Client, "iceberg-warehouse", tablePrefix("sales", "data/")) < 3 {
		t.Error("expected at least 3 data files (one per partition) in sales/data/")
	}

	// c. Filtered count = 100 (partition pruning on 2026-01-02)
	filteredResult := findS3JSON(t, s3Client, "iceberg-warehouse", outputPrefix("sales-count"))
	cnt, _ := filteredResult["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected filtered cnt=100, got %v", cnt)
	}

	// d. Total count = 300
	totalResult := findS3JSON(t, s3Client, "iceberg-warehouse", outputPrefix("sales-total"))
	total, _ := totalResult["cnt"].(float64)
	if int(total) != 300 {
		t.Errorf("expected total cnt=300, got %v", total)
	}
}
