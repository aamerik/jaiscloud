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
	return awss3.NewFromConfig(awsCfg(t), func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
		o.UsePathStyle = true
	})
}

func newGlueClient(t *testing.T) *awsglue.Client {
	t.Helper()
	return awsglue.NewFromConfig(awsCfg(t), func(o *awsglue.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func newDynamoClient(t *testing.T) *awsdynamo.Client {
	t.Helper()
	return awsdynamo.NewFromConfig(awsCfg(t), func(o *awsdynamo.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

// ─── reset helpers ────────────────────────────────────────────────────────────

// resetIcebergTables drops named Glue tables so each test starts clean.
func resetIcebergTables(t *testing.T, glueClient *awsglue.Client, tableNames ...string) {
	t.Helper()
	for _, name := range tableNames {
		_, _ = glueClient.DeleteTable(context.Background(), &awsglue.DeleteTableInput{
			DatabaseName: aws.String("iceberg_test_db"),
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

// runSparkSQL submits a Spark SQL job via Docker and waits for completion.
// The SQL is written to a temp file and mounted read-only into the container.
func runSparkSQL(t *testing.T, job SparkJob) {
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
		"-v", sqlFile.Name() + ":/tmp/job.sql:ro",
		icebergImage(),
		"/opt/spark/bin/spark-sql",
		"--master", "local[2]",
	}
	for _, c := range icebergSparkConf() {
		args = append(args, "--conf", c)
	}
	for _, c := range job.ExtraConf {
		args = append(args, "--conf", c)
	}
	args = append(args, "-f", "/tmp/job.sql")

	t.Logf("running spark-sql job %q", job.Name)
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

// icebergSparkConf returns the standard Spark conf for Iceberg on JaisCloud.
func icebergSparkConf() []string {
	// Replace localhost with host.docker.internal so the container can reach the host.
	host := strings.Replace(jaiscloudHost(), "localhost", "host.docker.internal", 1)
	return []string{
		"spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
		"spark.sql.catalog.glue.warehouse=s3://iceberg-warehouse/",
		"spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
		"spark.sql.catalog.glue.lock-impl=org.apache.iceberg.aws.dynamodb.DynamoDbLockManager",
		"spark.sql.catalog.glue.lock.table=iceberg_lock",
		fmt.Sprintf("spark.sql.catalog.glue.glue.endpoint=%s", host),
		fmt.Sprintf("spark.sql.catalog.glue.dynamodb.endpoint=%s", host),
		fmt.Sprintf("spark.hadoop.fs.s3a.endpoint=%s", host),
		"spark.hadoop.fs.s3a.path.style.access=true",
		"spark.hadoop.fs.s3a.access.key=test",
		"spark.hadoop.fs.s3a.secret.key=test",
		"spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
		"spark.hadoop.fs.s3a.connection.ssl.enabled=false",
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

// ─── unused import guard ─────────────────────────────────────────────────────

var _ = pollInterval // used in scenario tests via helpers
