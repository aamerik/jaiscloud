//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func icebergImage() string {
	return os.Getenv("SPARK_E2E_ICEBERG_IMAGE")
}

func requireIcebergEnv(t *testing.T) {
	t.Helper()
	if icebergImage() == "" {
		t.Skip("SPARK_E2E_ICEBERG_IMAGE not set — skipping Iceberg e2e test")
	}
}

func pollInterval() time.Duration {
	if v := os.Getenv("SPARK_E2E_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 5 * time.Second
}

func jobTimeout() time.Duration {
	if v := os.Getenv("SPARK_E2E_JOB_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 10 * time.Minute
}

// ─── AWS client factories ─────────────────────────────────────────────────────

func awsCfg(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return cfg
}

func newS3Client(t *testing.T) *awss3.Client {
	t.Helper()
	return newS3ClientAt(t, jaiscloudHost())
}

func newGlueClient(t *testing.T) *awsglue.Client {
	t.Helper()
	return newGlueClientAt(t, jaiscloudHost())
}

func newDynamoClient(t *testing.T) *awsdynamo.Client {
	t.Helper()
	return newDynamoClientAt(t, jaiscloudHost())
}

func newS3ClientAt(t *testing.T, host string) *awss3.Client {
	t.Helper()
	return awss3.NewFromConfig(awsCfg(t), func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(host)
		o.UsePathStyle = true
	})
}

func newGlueClientAt(t *testing.T, host string) *awsglue.Client {
	t.Helper()
	return awsglue.NewFromConfig(awsCfg(t), func(o *awsglue.Options) {
		o.BaseEndpoint = aws.String(host)
	})
}

func newDynamoClientAt(t *testing.T, host string) *awsdynamo.Client {
	t.Helper()
	return awsdynamo.NewFromConfig(awsCfg(t), func(o *awsdynamo.Options) {
		o.BaseEndpoint = aws.String(host)
	})
}

// ─── run-scoped naming helpers ────────────────────────────────────────────────

// icebergDB returns the Glue database name for this test run.
// Each invocation of `go test` gets a unique 6-hex-char suffix so that
// concurrent or back-to-back runs never conflict on the same database name.
func icebergDB() string { return "iceberg_test_" + testRunID }

// tableLocation returns the S3 LOCATION clause value for an Iceberg table in
// this run (using the s3:// scheme consumed by Iceberg's S3FileIO).
func tableLocation(table string) string {
	return fmt.Sprintf("s3://iceberg-warehouse/%s/%s", testRunID, table)
}

// outputLoc returns the s3a:// URI used in INSERT OVERWRITE DIRECTORY for this run.
func outputLoc(dir string) string {
	return fmt.Sprintf("s3a://iceberg-warehouse/%s/%s/", testRunID, dir)
}

// outputPrefix returns the S3 key prefix for assertions on output directories
// (bucket is always "iceberg-warehouse").
func outputPrefix(dir string) string {
	return fmt.Sprintf("%s/%s/", testRunID, dir)
}

// tablePrefix returns the S3 key prefix for a table's sub-directory (e.g.
// "metadata/" or "data/") — used with countS3Objects / hasS3Objects.
func tablePrefix(table, subdir string) string {
	return fmt.Sprintf("%s/%s/%s", testRunID, table, subdir)
}

// ─── reset helpers ────────────────────────────────────────────────────────────

// resetIcebergTables drops named Glue tables so each test starts clean.
func resetIcebergTables(t *testing.T, glueClient *awsglue.Client, tableNames ...string) {
	t.Helper()
	for _, name := range tableNames {
		_, _ = glueClient.DeleteTable(context.Background(), &awsglue.DeleteTableInput{
			DatabaseName: aws.String(icebergDB()),
			Name:         aws.String(name),
		})
	}
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudHost()+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

// ─── Spark SQL job runner ─────────────────────────────────────────────────────

// SparkJob holds parameters for a Docker-based spark-sql job.
type SparkJob struct {
	// Name is used for logging.
	Name string
	// SQL is the Spark SQL script to execute.
	SQL string
	// ExtraConf holds additional --conf flags (e.g. "spark.driver.memory=1g").
	ExtraConf []string
}

// runSparkSQL submits a Spark SQL job via Docker using the default JaisCloud host.
func runSparkSQL(t *testing.T, job SparkJob) {
	t.Helper()
	runSparkSQLOnHost(t, jaiscloudHost(), job)
}

// runSparkSQLOnHost submits a Spark SQL job via Docker pointing at a specific JaisCloud host.
// The SQL is written to a temp file and mounted read-only into the container.
func runSparkSQLOnHost(t *testing.T, host string, job SparkJob) {
	t.Helper()
	requireIcebergEnv(t)

	sqlFile, err := os.CreateTemp("", fmt.Sprintf("iceberg-%s-*.sql", job.Name))
	if err != nil {
		t.Fatalf("create temp SQL file: %v", err)
	}
	defer os.Remove(sqlFile.Name())
	if _, err := sqlFile.WriteString(job.SQL); err != nil {
		t.Fatalf("write SQL: %v", err)
	}
	sqlFile.Close()

	args := []string{
		"docker", "run", "--rm",
		"--add-host=host.docker.internal:host-gateway",
		// AWS credentials and region so the SDK inside the container resolves them.
		"-e", "AWS_REGION=us-east-1",
		"-e", "AWS_DEFAULT_REGION=us-east-1",
		"-e", "AWS_ACCESS_KEY_ID=test",
		"-e", "AWS_SECRET_ACCESS_KEY=test",
		"-v", sqlFile.Name() + ":/tmp/job.sql:ro",
		icebergImage(),
		"/opt/spark/bin/spark-sql",
		"--master", "local[2]",
	}
	for _, c := range icebergSparkConfForHost(host) {
		args = append(args, "--conf", c)
	}
	for _, c := range job.ExtraConf {
		args = append(args, "--conf", c)
	}
	args = append(args, "-f", "/tmp/job.sql")

	t.Logf("running spark-sql job %q on %s", job.Name, host)
	ctx, cancel := context.WithTimeout(context.Background(), jobTimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = &testWriter{t: t, prefix: job.Name + ": "}
	cmd.Stderr = &testWriter{t: t, prefix: job.Name + " ERR: "}
	if err := cmd.Run(); err != nil {
		t.Fatalf("spark-sql job %q: %v", job.Name, err)
	}
}

// testWriter streams command output to t.Log line by line.
type testWriter struct {
	t      *testing.T
	prefix string
	buf    string
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.buf += string(p)
	for {
		idx := strings.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.t.Log(w.prefix + w.buf[:idx])
		w.buf = w.buf[idx+1:]
	}
	return len(p), nil
}

// icebergSparkConf returns the standard Spark conf for Iceberg on the default JaisCloud host.
func icebergSparkConf() []string {
	return icebergSparkConfForHost(jaiscloudHost())
}

// icebergSparkConfForHost returns Spark conf for Iceberg pointing at a specific JaisCloud host.
// Replaces localhost with host.docker.internal so the Docker container can reach the host.
//
// Two separate AWS clients are configured:
//   - Iceberg's S3FileIO (iceberg-aws-bundle, AWS SDK v2) — uses spark.sql.catalog.glue.s3.*
//   - Hadoop S3A connector (aws-java-sdk-bundle, AWS SDK v1) — uses spark.hadoop.fs.s3a.*
//     (used by Spark's INSERT OVERWRITE DIRECTORY queries)
func icebergSparkConfForHost(host string) []string {
	dockerHost := strings.Replace(host, "localhost", "host.docker.internal", 1)
	return []string{
		"spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
		fmt.Sprintf("spark.sql.catalog.glue.warehouse=s3://iceberg-warehouse/%s/", testRunID),
		"spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
		// No distributed lock manager — each Spark job runs in its own JVM so the
		// default InMemoryLockManager is sufficient for single-writer tests.

		// Glue catalog endpoint (Iceberg AWS SDK v2 client)
		fmt.Sprintf("spark.sql.catalog.glue.glue.endpoint=%s", dockerHost),

		// Iceberg S3FileIO endpoint (iceberg-aws-bundle uses its own S3 client, not S3A)
		fmt.Sprintf("spark.sql.catalog.glue.s3.endpoint=%s", dockerHost),
		"spark.sql.catalog.glue.s3.path-style-access=true",

		// Hadoop S3A connector — used by Spark's INSERT OVERWRITE DIRECTORY ... USING JSON
		fmt.Sprintf("spark.hadoop.fs.s3a.endpoint=%s", dockerHost),
		"spark.hadoop.fs.s3a.path.style.access=true",
		"spark.hadoop.fs.s3a.access.key=test",
		"spark.hadoop.fs.s3a.secret.key=test",
		"spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
		"spark.hadoop.fs.s3a.connection.ssl.enabled=false",
		"spark.hadoop.fs.s3a.endpoint.region=us-east-1",
	}
}

// ─── S3 helpers ───────────────────────────────────────────────────────────────

// readS3JSON fetches an S3 object and decodes it as JSON.
func readS3JSON(t *testing.T, s3Client *awss3.Client, bucket, key string) map[string]any {
	t.Helper()
	out, err := s3Client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject s3://%s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read S3 body: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse JSON from s3://%s/%s: %v\nbody: %s", bucket, key, err, body)
	}
	return result
}

// readS3Text reads an S3 object and returns it as a trimmed string.
func readS3Text(t *testing.T, s3Client *awss3.Client, bucket, key string) string {
	t.Helper()
	out, err := s3Client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject s3://%s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read S3 body: %v", err)
	}
	return strings.TrimSpace(string(body))
}

// countS3Objects counts objects under a prefix.
func countS3Objects(t *testing.T, s3Client *awss3.Client, bucket, prefix string) int {
	t.Helper()
	out, err := s3Client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 s3://%s/%s: %v", bucket, prefix, err)
	}
	if out.KeyCount == nil {
		return 0
	}
	return int(*out.KeyCount)
}

// hasS3Objects returns true if any object exists under the given prefix.
func hasS3Objects(t *testing.T, s3Client *awss3.Client, bucket, prefix string) bool {
	return countS3Objects(t, s3Client, bucket, prefix) > 0
}

// findS3Text lists objects under prefix, reads the first data file (skipping
// _SUCCESS and hidden files), and returns its content as a trimmed string.
// Spark's INSERT OVERWRITE DIRECTORY creates files like part-00000-<uuid>.c000.txt
// rather than a fixed name, so this helper is more robust than a hardcoded key.
func findS3Text(t *testing.T, s3Client *awss3.Client, bucket, prefix string) string {
	t.Helper()
	out, err := s3Client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 s3://%s/%s: %v", bucket, prefix, err)
	}
	for _, obj := range out.Contents {
		key := aws.ToString(obj.Key)
		lastSlash := strings.LastIndex(key, "/")
		base := key[lastSlash+1:]
		if strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".") || base == "" {
			continue
		}
		return readS3Text(t, s3Client, bucket, key)
	}
	t.Fatalf("no data objects found at s3://%s/%s (found %d total)", bucket, prefix, len(out.Contents))
	return ""
}

// findS3JSON lists objects under prefix, reads the first data file (skipping
// _SUCCESS and hidden files), and decodes it as a single JSON object.
// Spark's INSERT OVERWRITE DIRECTORY creates files like part-00000-<uuid>.c000.json
// rather than a fixed name, so this helper is more robust than a hardcoded key.
func findS3JSON(t *testing.T, s3Client *awss3.Client, bucket, prefix string) map[string]any {
	t.Helper()
	out, err := s3Client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 s3://%s/%s: %v", bucket, prefix, err)
	}
	for _, obj := range out.Contents {
		key := aws.ToString(obj.Key)
		// Extract the last path segment to check for hidden/marker files.
		lastSlash := strings.LastIndex(key, "/")
		base := key[lastSlash+1:]
		if strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".") || base == "" {
			continue
		}
		return readS3JSON(t, s3Client, bucket, key)
	}
	t.Fatalf("no data objects found at s3://%s/%s (found %d total)", bucket, prefix, len(out.Contents))
	return nil
}

// ─── persistence server helpers ──────────────────────────────────────────────

// jaiscloudBin returns the path to the jaiscloud binary.
// Tests run with working directory = tests/persistent_mode/iceberg/, so the project
// root binary is three levels up.
func jaiscloudBin() string {
	if b := os.Getenv("JAISCLOUD_BIN"); b != "" {
		return b
	}
	const rel = "../../../jaiscloud"
	if _, err := os.Stat(rel); err == nil {
		return rel
	}
	return "jaiscloud"
}

// persistPort returns the port used by the managed JaisCloud process in persistence tests.
func persistPort() int {
	if v := os.Getenv("JAISCLOUD_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 4567
}

// startJaisCloudProcess starts a managed JaisCloud server subprocess and returns
// the exec.Cmd. Callers are responsible for calling cmd.Process.Kill() when done.
func startJaisCloudProcess(t *testing.T, port int, dsn, blobDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(jaiscloudBin(),
		"start",
		"--mode", "full",
		"--port", strconv.Itoa(port),
		"--dsn", dsn,
		"--blob-dir", blobDir,
		"--log-level", "warn",
	)
	cmd.Stdout = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud[%d]: ", port)}
	cmd.Stderr = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud[%d] ERR: ", port)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start jaiscloud on port %d: %v", port, err)
	}
	return cmd
}

// waitForHealth polls /_jaiscloud/health until the server responds 200 or times out.
func waitForHealth(t *testing.T, host string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(host + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud at %s did not become healthy within 30s", host)
}

// ─── unused import guard ─────────────────────────────────────────────────────

var _ = pollInterval // used in scenario tests via helpers
