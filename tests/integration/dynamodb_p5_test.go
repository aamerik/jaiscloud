package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyntype "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P5.2: DynamoDB PartiQL stubs ────────────────────────────────────────────

func TestDynamoDB_PartiQL_ExecuteStatement_ReturnsError(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	_, err := c.ExecuteStatement(ctx, &awsdynamo.ExecuteStatementInput{
		Statement: aws.String(`SELECT * FROM "nonexistent-table"`),
	})
	require.Error(t, err, "ExecuteStatement must return an error (PartiQL not supported)")
}

func TestDynamoDB_PartiQL_ExecuteTransaction_ReturnsError(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	_, err := c.ExecuteTransaction(ctx, &awsdynamo.ExecuteTransactionInput{
		TransactStatements: []dyntype.ParameterizedStatement{
			{Statement: aws.String(`SELECT * FROM "t"`)},
		},
	})
	require.Error(t, err, "ExecuteTransaction must return an error (PartiQL not supported)")
}

// ─── P5.5: DynamoDB Global Tables ────────────────────────────────────────────

func TestDynamoDB_GlobalTable_CreateAndDescribe(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	_, err := c.CreateGlobalTable(ctx, &awsdynamo.CreateGlobalTableInput{
		GlobalTableName: aws.String("global-table-1"),
		ReplicationGroup: []dyntype.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("eu-west-1")},
		},
	})
	require.NoError(t, err)

	out, err := c.DescribeGlobalTable(ctx, &awsdynamo.DescribeGlobalTableInput{
		GlobalTableName: aws.String("global-table-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "global-table-1", aws.ToString(out.GlobalTableDescription.GlobalTableName))
	assert.Equal(t, dyntype.GlobalTableStatusActive, out.GlobalTableDescription.GlobalTableStatus)
	assert.Len(t, out.GlobalTableDescription.ReplicationGroup, 2)
}

func TestDynamoDB_GlobalTable_DuplicateCreateFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	_, err := c.CreateGlobalTable(ctx, &awsdynamo.CreateGlobalTableInput{
		GlobalTableName:  aws.String("dup-global"),
		ReplicationGroup: []dyntype.Replica{{RegionName: aws.String("us-east-1")}},
	})
	require.NoError(t, err)

	_, err = c.CreateGlobalTable(ctx, &awsdynamo.CreateGlobalTableInput{
		GlobalTableName:  aws.String("dup-global"),
		ReplicationGroup: []dyntype.Replica{{RegionName: aws.String("us-west-2")}},
	})
	require.Error(t, err, "duplicate GlobalTable name must fail")
}

func TestDynamoDB_GlobalTable_List(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	for _, name := range []string{"gt-list-a", "gt-list-b"} {
		_, err := c.CreateGlobalTable(ctx, &awsdynamo.CreateGlobalTableInput{
			GlobalTableName:  aws.String(name),
			ReplicationGroup: []dyntype.Replica{{RegionName: aws.String("us-east-1")}},
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListGlobalTables(ctx, &awsdynamo.ListGlobalTablesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.GlobalTables), 2)
}

func TestDynamoDB_GlobalTable_DescribeNotFoundFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	_, err := c.DescribeGlobalTable(ctx, &awsdynamo.DescribeGlobalTableInput{
		GlobalTableName: aws.String("nonexistent-gt"),
	})
	require.Error(t, err, "describing non-existent global table must fail")
}

// ─── P5.6: DynamoDB Kinesis Destinations ─────────────────────────────────────

func TestDynamoDB_KinesisDestination_EnableAndDescribe(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	// Create a table first
	_, err := c.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String("kinesis-tbl"),
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dyntype.KeyTypeHash},
		},
		BillingMode: dyntype.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	streamArn := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"
	_, err = c.EnableKinesisStreamingDestination(ctx, &awsdynamo.EnableKinesisStreamingDestinationInput{
		TableName: aws.String("kinesis-tbl"),
		StreamArn: aws.String(streamArn),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeKinesisStreamingDestination(ctx, &awsdynamo.DescribeKinesisStreamingDestinationInput{
		TableName: aws.String("kinesis-tbl"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.KinesisDataStreamDestinations, 1)
	assert.Equal(t, streamArn, aws.ToString(descOut.KinesisDataStreamDestinations[0].StreamArn))
	assert.Equal(t, dyntype.DestinationStatusActive, descOut.KinesisDataStreamDestinations[0].DestinationStatus)
}

func TestDynamoDB_KinesisDestination_Disable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	_, err := c.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String("kinesis-dis-tbl"),
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dyntype.KeyTypeHash},
		},
		BillingMode: dyntype.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	streamArn := "arn:aws:kinesis:us-east-1:000000000000:stream/dis-stream"
	_, err = c.EnableKinesisStreamingDestination(ctx, &awsdynamo.EnableKinesisStreamingDestinationInput{
		TableName: aws.String("kinesis-dis-tbl"),
		StreamArn: aws.String(streamArn),
	})
	require.NoError(t, err)

	_, err = c.DisableKinesisStreamingDestination(ctx, &awsdynamo.DisableKinesisStreamingDestinationInput{
		TableName: aws.String("kinesis-dis-tbl"),
		StreamArn: aws.String(streamArn),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeKinesisStreamingDestination(ctx, &awsdynamo.DescribeKinesisStreamingDestinationInput{
		TableName: aws.String("kinesis-dis-tbl"),
	})
	require.NoError(t, err)
	assert.Equal(t, dyntype.DestinationStatusDisabled, descOut.KinesisDataStreamDestinations[0].DestinationStatus)
}
