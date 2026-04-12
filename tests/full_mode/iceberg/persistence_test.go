//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestIceberg_Persistence_AcrossRestart verifies that Iceberg table data and
// metadata survive a JaisCloud process restart.
//
// The test manages its own JaisCloud server instance on a separate port so it
// can control the lifecycle independently of the shared test server.
//
// Required env vars:
//
//	SPARK_E2E_ICEBERG_IMAGE — Docker image with Spark + Iceberg (same as other tests)
//	JAISCLOUD_DSN           — PostgreSQL DSN for full-mode persistence
//
// Optional env vars:
//
//	JAISCLOUD_BIN          — path to jaiscloud binary (default: ../../../jaiscloud)
//	JAISCLOUD_PERSIST_PORT — port for the managed server (default: 4567)
func TestIceberg_Persistence_AcrossRestart(t *testing.T) {
	requireIcebergEnv(t)

	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)

	// Blob storage must survive across restarts — use t.TempDir() which persists
	// for the duration of the test function (cleanup runs after the function returns).
	blobDir := t.TempDir()

	const (
		bucket    = "persist-warehouse"
		glueDB    = "persist_db"
		tableName = "checkpoints"
		rowCount  = 200
	)

	// ── Phase 1: start server, create infrastructure, insert data ────────────

	proc1 := startJaisCloudProcess(t, port, dsn, blobDir)
	defer proc1.Process.Kill() // safety net; explicit kill happens below
	waitForHealth(t, host)

	ctx := context.Background()
	s3Client := newS3ClientAt(t, host)
	glueClient := newGlueClientAt(t, host)

	// Create shared infrastructure for this test's server instance.
	if _, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("CreateBucket %s: %v", bucket, err)
	}
	if _, err := glueClient.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(glueDB)},
	}); err != nil {
		t.Fatalf("CreateDatabase %s: %v", glueDB, err)
	}

	overrideConf := []string{
		fmt.Sprintf("spark.sql.catalog.glue.warehouse=s3://%s/", bucket),
	}

	// Create table and insert rowCount rows.
	runSparkSQLOnHost(t, host, SparkJob{
		Name:      "persist-write",
		ExtraConf: overrideConf,
		SQL: fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS glue.%s.%s (
  id    INT,
  value STRING
)
USING iceberg
LOCATION 's3://%s/%s';

%s
`, glueDB, tableName, bucket, tableName, buildPersistInsertSQL(glueDB, tableName, rowCount)),
	})

	// Read summary stats before restart: MIN(id), MAX(id), SUM(id), COUNT(*).
	// SUM of 1..N = N*(N+1)/2 — used to detect data loss or duplication.
	runSparkSQLOnHost(t, host, SparkJob{
		Name:      "persist-stats-before",
		ExtraConf: overrideConf,
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY 's3a://%s/stats-before/'
USING JSON
SELECT
  MIN(id)               AS min_id,
  MAX(id)               AS max_id,
  SUM(CAST(id AS LONG)) AS sum_id,
  COUNT(*)              AS cnt
FROM glue.%s.%s;
`, bucket, glueDB, tableName),
	})

	statsBefore := findS3JSON(t, s3Client, bucket, "stats-before/")
	assertPersistStats(t, "before restart", statsBefore, rowCount)

	// ── Kill server and restart ───────────────────────────────────────────────

	t.Log("killing server for restart test")
	if err := proc1.Process.Kill(); err != nil {
		t.Fatalf("kill server: %v", err)
	}
	proc1.Wait() //nolint:errcheck

	t.Log("restarting server")
	proc2 := startJaisCloudProcess(t, port, dsn, blobDir)
	defer proc2.Process.Kill()
	waitForHealth(t, host)

	// ── Phase 2: verify data survived the restart ─────────────────────────────

	// Use a fresh S3 client (same endpoint; connection pool resets automatically).
	s3Client2 := newS3ClientAt(t, host)

	runSparkSQLOnHost(t, host, SparkJob{
		Name:      "persist-stats-after",
		ExtraConf: overrideConf,
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY 's3a://%s/stats-after/'
USING JSON
SELECT
  MIN(id)               AS min_id,
  MAX(id)               AS max_id,
  SUM(CAST(id AS LONG)) AS sum_id,
  COUNT(*)              AS cnt
FROM glue.%s.%s;
`, bucket, glueDB, tableName),
	})

	statsAfter := findS3JSON(t, s3Client2, bucket, "stats-after/")
	assertPersistStats(t, "after restart", statsAfter, rowCount)

	// Compare before/after to confirm exact match (no data loss or duplication).
	if statsBefore["sum_id"] != statsAfter["sum_id"] {
		t.Errorf("sum_id mismatch: before=%v after=%v", statsBefore["sum_id"], statsAfter["sum_id"])
	}
}

// buildPersistInsertSQL generates an INSERT of n rows with id 1..n.
func buildPersistInsertSQL(glueDB, tableName string, n int) string {
	return fmt.Sprintf(
		"INSERT INTO glue.%s.%s SELECT id, CONCAT('row-', CAST(id AS STRING)) FROM range(1, %d);",
		glueDB, tableName, n+1,
	)
}

// assertPersistStats verifies that a summary-stats JSON row matches expectations
// for a table with sequential ids 1..n.
func assertPersistStats(t *testing.T, label string, stats map[string]any, n int) {
	t.Helper()

	cnt, _ := stats["cnt"].(float64)
	if int(cnt) != n {
		t.Errorf("[%s] expected cnt=%d, got %v", label, n, cnt)
	}

	minID, _ := stats["min_id"].(float64)
	if int(minID) != 1 {
		t.Errorf("[%s] expected min_id=1, got %v", label, minID)
	}

	maxID, _ := stats["max_id"].(float64)
	if int(maxID) != n {
		t.Errorf("[%s] expected max_id=%d, got %v", label, n, maxID)
	}

	expectedSum := float64(n) * float64(n+1) / 2
	sumID, _ := stats["sum_id"].(float64)
	if math.Abs(sumID-expectedSum) > 0.5 {
		t.Errorf("[%s] expected sum_id=%.0f, got %v", label, expectedSum, sumID)
	}
}
