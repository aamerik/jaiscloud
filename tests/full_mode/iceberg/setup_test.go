//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// testRunID is a random 6-hex-char string unique to this test binary invocation.
// All Glue DB names and S3 paths are scoped under this ID so that concurrent
// or back-to-back test runs never share state.
var testRunID string

// TestMain creates the shared Iceberg infrastructure once before all tests.
// Shared resources (scoped to this run):
//   - S3 bucket "iceberg-warehouse"
//   - Glue database "iceberg_test_<testRunID>"
//
// Individual tests call resetIcebergTables(t, ...) to drop their own Glue tables
// between runs without touching the database.
//
// No distributed lock manager is configured for Spark — each Docker container
// runs its own JVM and uses the default InMemoryLockManager, which is correct
// for single-writer tests.
func TestMain(m *testing.M) {
	testRunID = fmt.Sprintf("%06x", rand.Uint32())

	ctx := context.Background()
	cfg := mustAWSConfig()

	s3Client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
		o.UsePathStyle = true
	})
	glueClient := awsglue.NewFromConfig(cfg, func(o *awsglue.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})

	// 1. Create S3 warehouse bucket (idempotent — error ignored if already exists)
	_, _ = s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("iceberg-warehouse"),
	})

	// 2. Create run-scoped Glue database (idempotent)
	_, _ = glueClient.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{
			Name: aws.String(icebergDB()),
		},
	})

	code := m.Run()

	// Teardown: remove the run-scoped Glue database.
	// S3 objects are left intact for debugging.
	_, _ = glueClient.DeleteDatabase(ctx, &awsglue.DeleteDatabaseInput{
		Name: aws.String(icebergDB()),
	})

	os.Exit(code)
}

// mustAWSConfig loads an AWS config for TestMain (panics on failure).
func mustAWSConfig() aws.Config {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		panic("load aws config: " + err.Error())
	}
	return cfg
}
