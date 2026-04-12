//go:build iceberg_e2e

package iceberg_test

import (
	"context"
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
		SQL: `
CREATE TABLE IF NOT EXISTS glue.iceberg_test_db.sales (
  id        INT,
  region    STRING,
  amount    DOUBLE,
  sale_date DATE
)
USING iceberg
PARTITIONED BY (days(sale_date))
LOCATION 's3://iceberg-warehouse/sales';

INSERT INTO glue.iceberg_test_db.sales
SELECT
  id,
  CASE WHEN id % 3 = 0 THEN 'us-east' WHEN id % 3 = 1 THEN 'us-west' ELSE 'eu-central' END AS region,
  CAST(id * 1.5 AS DOUBLE) AS amount,
  DATE '2026-01-01' AS sale_date
FROM range(1, 101);

INSERT INTO glue.iceberg_test_db.sales
SELECT
  id + 100,
  'us-east',
  CAST(id * 2.0 AS DOUBLE),
  DATE '2026-01-02'
FROM range(1, 101);

INSERT INTO glue.iceberg_test_db.sales
SELECT
  id + 200,
  'eu-central',
  CAST(id * 2.5 AS DOUBLE),
  DATE '2026-01-03'
FROM range(1, 101);
`,
	})

	// Job 2 — Filtered read for 2026-01-02
	runSparkSQL(t, SparkJob{
		Name: "count-by-date",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/sales-count/'
USING JSON
SELECT COUNT(*) AS cnt
FROM glue.iceberg_test_db.sales
WHERE sale_date = DATE '2026-01-02';
`,
	})

	// Job 3 — Full count
	runSparkSQL(t, SparkJob{
		Name: "count-total",
		SQL: `
INSERT OVERWRITE DIRECTORY 's3a://iceberg-warehouse/sales-total/'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.iceberg_test_db.sales;
`,
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Glue GetPartitions returns 3 partitions
	partOut, err := glueClient.GetPartitions(context.Background(), &awsglue.GetPartitionsInput{
		DatabaseName: aws.String("iceberg_test_db"),
		TableName:    aws.String("sales"),
	})
	if err != nil {
		t.Fatalf("GetPartitions: %v", err)
	}
	if len(partOut.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(partOut.Partitions))
	}

	// b. Partition values include all 3 dates
	dates := map[string]bool{}
	for _, p := range partOut.Partitions {
		if len(p.Values) > 0 {
			dates[p.Values[0]] = true
		}
	}
	for _, expected := range []string{"2026-01-01", "2026-01-02", "2026-01-03"} {
		if !dates[expected] {
			t.Errorf("expected partition for %s, got partitions: %v", expected, dates)
		}
	}

	// c. S3 has 3 partition directories
	if countS3Objects(t, s3Client, "iceberg-warehouse", "sales/data/") < 3 {
		t.Error("expected at least 3 data files (one per partition) in s3://iceberg-warehouse/sales/data/")
	}

	// d. Filtered count = 100
	filteredResult := readS3JSON(t, s3Client, "iceberg-warehouse", "sales-count/result.json")
	cnt, _ := filteredResult["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected filtered cnt=100, got %v", cnt)
	}

	// e. Total count = 300
	totalResult := readS3JSON(t, s3Client, "iceberg-warehouse", "sales-total/result.json")
	total, _ := totalResult["cnt"].(float64)
	if int(total) != 300 {
		t.Errorf("expected total cnt=300, got %v", total)
	}
}
