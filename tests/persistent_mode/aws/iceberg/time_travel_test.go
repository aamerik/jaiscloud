//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
)

// TestIceberg_GlueCatalog_TimeTravel inserts data in two batches, captures the
// snapshot ID after the first batch, then queries AS OF that snapshot and verifies
// only the first batch is visible.
func TestIceberg_GlueCatalog_TimeTravel(t *testing.T) {
	requireIcebergEnv(t)

	glueClient := newGlueClient(t)
	s3Client := newS3Client(t)

	resetIcebergTables(t, glueClient, "orders")

	// Job 1 — First batch (100 rows) + write snapshot ID to S3
	runSparkSQL(t, SparkJob{
		Name: "orders-batch1",
		SQL: fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS glue.%s.orders (
  id     INT,
  status STRING
)
USING iceberg
LOCATION '%s';

INSERT INTO glue.%s.orders
SELECT id, 'pending' FROM range(1, 101);

-- Write the current snapshot_id to S3 for the Go test to read back.
INSERT OVERWRITE DIRECTORY '%s'
USING TEXT
SELECT CAST(snapshot_id AS STRING)
FROM glue.%s.orders.snapshots
ORDER BY committed_at DESC
LIMIT 1;
`, icebergDB(), tableLocation("orders"), icebergDB(), outputLoc("orders-meta"), icebergDB()),
	})

	// Job 2 — Second batch (100 more rows)
	runSparkSQL(t, SparkJob{
		Name: "orders-batch2",
		SQL: fmt.Sprintf(`
INSERT INTO glue.%s.orders
SELECT id + 100, 'shipped' FROM range(1, 101);
`, icebergDB()),
	})

	// Read snapshot ID from S3 before running time-travel queries.
	// Spark's FileOutputCommitter writes UUID-named files (e.g. part-00000-<uuid>.c000.txt)
	// so we use findS3Text to locate the first data object under the prefix.
	snapshotIDStr := findS3Text(t, s3Client, "iceberg-warehouse", outputPrefix("orders-meta"))
	snapshotID, err := strconv.ParseInt(snapshotIDStr, 10, 64)
	if err != nil {
		t.Fatalf("parse snapshot ID %q: %v", snapshotIDStr, err)
	}
	if snapshotID <= 0 {
		t.Fatalf("invalid snapshot ID: %d", snapshotID)
	}
	t.Logf("snapshot ID after batch 1: %d", snapshotID)

	// Job 3a — Time-travel query: COUNT(*) AS OF snapshot 1
	runSparkSQL(t, SparkJob{
		Name: "orders-timetravel",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt
FROM glue.%s.orders
VERSION AS OF %s;
`, outputLoc("orders-snap1"), icebergDB(), snapshotIDStr),
	})

	// Job 3b — Current snapshot query: COUNT(*) (both batches)
	runSparkSQL(t, SparkJob{
		Name: "orders-current",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM glue.%s.orders;
`, outputLoc("orders-current"), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. S3 metadata has at least 2 snapshot files
	metaCount := countS3Objects(t, s3Client, "iceberg-warehouse", tablePrefix("orders", "metadata/"))
	if metaCount < 2 {
		t.Errorf("expected at least 2 metadata files, got %d", metaCount)
	}

	// b. Glue GetTable metadata_location is non-empty
	tableOut, err := glueClient.GetTable(context.Background(), &awsglue.GetTableInput{
		DatabaseName: aws.String(icebergDB()),
		Name:         aws.String("orders"),
	})
	if err != nil {
		t.Fatalf("GetTable orders: %v", err)
	}
	if tableOut.Table.Parameters["metadata_location"] == "" {
		t.Error("expected non-empty metadata_location in Glue table")
	}

	// c. Time-travel result = 100 (only batch 1)
	snap1Result := findS3JSON(t, s3Client, "iceberg-warehouse", outputPrefix("orders-snap1"))
	snap1Cnt, _ := snap1Result["cnt"].(float64)
	if int(snap1Cnt) != 100 {
		t.Errorf("expected time-travel cnt=100, got %v", snap1Cnt)
	}

	// d. Current result = 200 (both batches)
	currentResult := findS3JSON(t, s3Client, "iceberg-warehouse", outputPrefix("orders-current"))
	currentCnt, _ := currentResult["cnt"].(float64)
	if int(currentCnt) != 200 {
		t.Errorf("expected current cnt=200, got %v", currentCnt)
	}
}
