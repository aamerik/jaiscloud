//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
)

// TestIceberg_GlueCatalog_WriteAndRead creates an Iceberg table, inserts 100 rows,
// reads them back via a COUNT(*) job, and verifies the result via S3 + Glue APIs.
func TestIceberg_GlueCatalog_WriteAndRead(t *testing.T) {
	requireIcebergEnv(t)

	glueClient := newGlueClient(t)
	s3Client := newS3Client(t)

	resetIcebergTables(t, glueClient, "events")

	// Job 1 — Write: create table and insert 100 rows
	runSparkSQL(t, SparkJob{
		Name: "write-events",
		SQL: fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS glue.%s.events (
  id         INT,
  name       STRING,
  amount     DOUBLE,
  created_at TIMESTAMP
)
USING iceberg
LOCATION '%s';

%s
`, icebergDB(), tableLocation("events"), buildInsertSQL(icebergDB(), "events", 100)),
	})

	// Job 2 — Read: COUNT(*) and write result to S3
	runSparkSQL(t, SparkJob{
		Name: "read-events-count",
		SQL: fmt.Sprintf(`
SELECT COUNT(*) AS cnt FROM glue.%s.events;
-- Write result to S3 as JSON
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.%s.events;
`, icebergDB(), outputLoc("events-count"), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Glue GetTable returns ICEBERG table with metadata_location
	tableOut, err := glueClient.GetTable(context.Background(), &awsglue.GetTableInput{
		DatabaseName: aws.String(icebergDB()),
		Name:         aws.String("events"),
	})
	if err != nil {
		t.Fatalf("GetTable events: %v", err)
	}
	params := tableOut.Table.Parameters
	if params["table_type"] != "ICEBERG" {
		t.Errorf("expected table_type=ICEBERG, got %q", params["table_type"])
	}
	if params["metadata_location"] == "" {
		t.Error("expected non-empty metadata_location parameter")
	}

	// b. S3 metadata directory contains at least 1 JSON file
	if !hasS3Objects(t, s3Client, "iceberg-warehouse", tablePrefix("events", "metadata/")) {
		t.Error("expected metadata files in s3://iceberg-warehouse/.../events/metadata/")
	}

	// c. S3 data directory contains at least 1 Parquet file
	if !hasS3Objects(t, s3Client, "iceberg-warehouse", tablePrefix("events", "data/")) {
		t.Error("expected data files in s3://iceberg-warehouse/.../events/data/")
	}

	// d. COUNT result = 100
	result := findS3JSON(t, s3Client, "iceberg-warehouse", outputPrefix("events-count"))
	cnt, _ := result["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected cnt=100, got %v", cnt)
	}
}

// buildInsertSQL generates a bulk INSERT of n rows into the given db.table.
func buildInsertSQL(db, table string, n int) string {
	sql := fmt.Sprintf("INSERT INTO glue.%s.%s VALUES\n", db, table)
	for i := 1; i <= n; i++ {
		comma := ","
		if i == n {
			comma = ";"
		}
		sql += fmt.Sprintf("  (%d, 'user%d', %.2f, current_timestamp())%s\n", i, i, float64(i)*0.99, comma)
	}
	return sql
}
