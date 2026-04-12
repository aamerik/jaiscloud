//go:build iceberg_e2e

package iceberg_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestMain creates the shared Iceberg infrastructure once before all tests.
// Shared resources:
//   - S3 bucket "iceberg-warehouse"
//   - Glue database "iceberg_test_db"
//   - DynamoDB table "iceberg_lock" (Iceberg commit-lock table)
//
// Individual tests call resetIcebergTables(t, ...) to drop their own Glue tables
// between runs without touching the database or lock table.
func TestMain(m *testing.M) {
	ctx := context.Background()
	cfg := mustAWSConfig()

	s3Client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
		o.UsePathStyle = true
	})
	glueClient := awsglue.NewFromConfig(cfg, func(o *awsglue.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
	dynamoClient := awsdynamo.NewFromConfig(cfg, func(o *awsdynamo.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})

	// 1. Create S3 warehouse bucket (idempotent — error ignored if already exists)
	_, _ = s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("iceberg-warehouse"),
	})

	// 2. Create Glue database
	_, _ = glueClient.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{
			Name: aws.String("iceberg_test_db"),
		},
	})

	// 3. Create DynamoDB lock table
	// Schema: op (S, HASH), partition (S, RANGE)
	// This is exactly what Iceberg's DynamoDbLockManager expects.
	_, _ = dynamoClient.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:   aws.String("iceberg_lock"),
		BillingMode: dynamotypes.BillingModePayPerRequest,
		KeySchema: []dynamotypes.KeySchemaElement{
			{AttributeName: aws.String("op"), KeyType: dynamotypes.KeyTypeHash},
			{AttributeName: aws.String("partition"), KeyType: dynamotypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamotypes.AttributeDefinition{
			{AttributeName: aws.String("op"), AttributeType: dynamotypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("partition"), AttributeType: dynamotypes.ScalarAttributeTypeS},
		},
	})

	code := m.Run()

	// Teardown: remove shared resources.
	// S3 objects are left intact for debugging unless ICEBERG_CLEAN_S3=true.
	_, _ = glueClient.DeleteDatabase(ctx, &awsglue.DeleteDatabaseInput{
		Name: aws.String("iceberg_test_db"),
	})
	_, _ = dynamoClient.DeleteTable(ctx, &awsdynamo.DeleteTableInput{
		TableName: aws.String("iceberg_lock"),
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
