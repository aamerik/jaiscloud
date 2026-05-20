package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyntype "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsstreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBStreams_EnableAndListStreams(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ddb := newDynamoClient(t)
	streams := newDynamoStreamsClient(t)

	// Create table with streams enabled
	_, err := ddb.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:   aws.String("StreamTable"),
		BillingMode: dyntype.BillingModePayPerRequest,
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: dyntype.KeyTypeHash},
		},
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
		StreamSpecification: &dyntype.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dyntype.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	// Enable via UpdateTable (for tables created without streams)
	_, err = ddb.UpdateTable(ctx, &awsdynamo.UpdateTableInput{
		TableName: aws.String("StreamTable"),
		StreamSpecification: &dyntype.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dyntype.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	listOut, err := streams.ListStreams(ctx, &awsstreams.ListStreamsInput{
		TableName: aws.String("StreamTable"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Streams, 1)
	assert.NotEmpty(t, aws.ToString(listOut.Streams[0].StreamArn))
}

func TestDynamoDBStreams_ReadWriteRecords(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ddb := newDynamoClient(t)
	streams := newDynamoStreamsClient(t)

	// Create + enable stream
	_, err := ddb.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:   aws.String("Events"),
		BillingMode: dyntype.BillingModePayPerRequest,
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dyntype.KeyTypeHash},
		},
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	_, err = ddb.UpdateTable(ctx, &awsdynamo.UpdateTableInput{
		TableName: aws.String("Events"),
		StreamSpecification: &dyntype.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dyntype.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	// Get stream ARN
	listOut, err := streams.ListStreams(ctx, &awsstreams.ListStreamsInput{
		TableName: aws.String("Events"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Streams, 1)
	streamArn := aws.ToString(listOut.Streams[0].StreamArn)

	// Describe stream → get shard
	descOut, err := streams.DescribeStream(ctx, &awsstreams.DescribeStreamInput{
		StreamArn: aws.String(streamArn),
	})
	require.NoError(t, err)
	require.Len(t, descOut.StreamDescription.Shards, 1)
	shardId := aws.ToString(descOut.StreamDescription.Shards[0].ShardId)

	// Get shard iterator at TRIM_HORIZON
	iterOut, err := streams.GetShardIterator(ctx, &awsstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamArn),
		ShardId:           aws.String(shardId),
		ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	iter := aws.ToString(iterOut.ShardIterator)
	assert.NotEmpty(t, iter)

	// Write items
	for _, id := range []string{"a", "b", "c"} {
		_, err = ddb.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("Events"),
			Item: map[string]dyntype.AttributeValue{
				"id":    &dyntype.AttributeValueMemberS{Value: id},
				"value": &dyntype.AttributeValueMemberS{Value: "v-" + id},
			},
		})
		require.NoError(t, err)
	}

	// Delete one
	_, err = ddb.DeleteItem(ctx, &awsdynamo.DeleteItemInput{
		TableName: aws.String("Events"),
		Key: map[string]dyntype.AttributeValue{
			"id": &dyntype.AttributeValueMemberS{Value: "b"},
		},
	})
	require.NoError(t, err)

	// GetRecords — should see 3 INSERTs + 1 REMOVE = 4 records
	recOut, err := streams.GetRecords(ctx, &awsstreams.GetRecordsInput{
		ShardIterator: aws.String(iter),
	})
	require.NoError(t, err)
	assert.Len(t, recOut.Records, 4)

	eventNames := make([]string, 0, len(recOut.Records))
	for _, r := range recOut.Records {
		eventNames = append(eventNames, string(r.EventName))
	}
	assert.Contains(t, eventNames, "INSERT")
	assert.Contains(t, eventNames, "REMOVE")
}

func TestDynamoDBStreams_ModifyEvent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ddb := newDynamoClient(t)
	streams := newDynamoStreamsClient(t)

	_, err := ddb.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:   aws.String("Modify"),
		BillingMode: dyntype.BillingModePayPerRequest,
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dyntype.KeyTypeHash},
		},
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
		StreamSpecification: &dyntype.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dyntype.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	listOut, err := streams.ListStreams(ctx, &awsstreams.ListStreamsInput{TableName: aws.String("Modify")})
	require.NoError(t, err)
	streamArn := aws.ToString(listOut.Streams[0].StreamArn)

	descOut, _ := streams.DescribeStream(ctx, &awsstreams.DescribeStreamInput{StreamArn: aws.String(streamArn)})
	shardId := aws.ToString(descOut.StreamDescription.Shards[0].ShardId)
	iterOut, _ := streams.GetShardIterator(ctx, &awsstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamArn),
		ShardId:           aws.String(shardId),
		ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
	})
	iter := aws.ToString(iterOut.ShardIterator)

	// INSERT
	_, err = ddb.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("Modify"),
		Item: map[string]dyntype.AttributeValue{
			"pk":    &dyntype.AttributeValueMemberS{Value: "x"},
			"value": &dyntype.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	// MODIFY via UpdateItem
	_, err = ddb.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName: aws.String("Modify"),
		Key: map[string]dyntype.AttributeValue{
			"pk": &dyntype.AttributeValueMemberS{Value: "x"},
		},
		UpdateExpression:         aws.String("SET #v = :two"),
		ExpressionAttributeNames: map[string]string{"#v": "value"},
		ExpressionAttributeValues: map[string]dyntype.AttributeValue{
			":two": &dyntype.AttributeValueMemberN{Value: "2"},
		},
	})
	require.NoError(t, err)

	recOut, err := streams.GetRecords(ctx, &awsstreams.GetRecordsInput{ShardIterator: aws.String(iter)})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 2)

	eventNames := make([]string, 0, 2)
	for _, r := range recOut.Records {
		eventNames = append(eventNames, string(r.EventName))
	}
	assert.Contains(t, eventNames, "INSERT")
	assert.Contains(t, eventNames, "MODIFY")
}

func TestDynamoDBStreams_NextShardIterator(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ddb := newDynamoClient(t)
	streams := newDynamoStreamsClient(t)

	_, err := ddb.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:   aws.String("T"),
		BillingMode: dyntype.BillingModePayPerRequest,
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dyntype.KeyTypeHash},
		},
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	_, err = ddb.UpdateTable(ctx, &awsdynamo.UpdateTableInput{
		TableName: aws.String("T"),
		StreamSpecification: &dyntype.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dyntype.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	listOut, _ := streams.ListStreams(ctx, &awsstreams.ListStreamsInput{TableName: aws.String("T")})
	streamArn := aws.ToString(listOut.Streams[0].StreamArn)
	descOut, _ := streams.DescribeStream(ctx, &awsstreams.DescribeStreamInput{StreamArn: aws.String(streamArn)})
	shardId := aws.ToString(descOut.StreamDescription.Shards[0].ShardId)
	iterOut, _ := streams.GetShardIterator(ctx, &awsstreams.GetShardIteratorInput{
		StreamArn: aws.String(streamArn), ShardId: aws.String(shardId),
		ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
	})
	iter := aws.ToString(iterOut.ShardIterator)

	// Write item 1
	_, err = ddb.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("T"),
		Item:      map[string]dyntype.AttributeValue{"pk": &dyntype.AttributeValueMemberS{Value: "x"}},
	})
	require.NoError(t, err)

	// Read — gets 1 record, advances iterator
	r1, err := streams.GetRecords(ctx, &awsstreams.GetRecordsInput{ShardIterator: aws.String(iter)})
	require.NoError(t, err)
	assert.Len(t, r1.Records, 1)
	nextIter := aws.ToString(r1.NextShardIterator)
	assert.NotEmpty(t, nextIter)

	// Write item 2
	_, err = ddb.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("T"),
		Item:      map[string]dyntype.AttributeValue{"pk": &dyntype.AttributeValueMemberS{Value: "y"}},
	})
	require.NoError(t, err)

	// Read with next iterator — should get only item 2
	r2, err := streams.GetRecords(ctx, &awsstreams.GetRecordsInput{ShardIterator: aws.String(nextIter)})
	require.NoError(t, err)
	assert.Len(t, r2.Records, 1)
}

func TestDynamoDB_Streams_ViewTypes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)
	streams := newDynamoStreamsClient(t)

	for _, tc := range []struct {
		viewType string
		wantNew  bool
		wantOld  bool
	}{
		{"NEW_IMAGE", true, false},
		{"OLD_IMAGE", false, true},
		{"KEYS_ONLY", false, false},
		{"NEW_AND_OLD_IMAGES", true, true},
	} {
		tc := tc
		t.Run(tc.viewType, func(t *testing.T) {
			tableName := "stream-view-" + tc.viewType
			_, err := client.CreateTable(ctx, &awsdynamo.CreateTableInput{
				TableName:            aws.String(tableName),
				KeySchema:            []dyntype.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: dyntype.KeyTypeHash}},
				AttributeDefinitions: []dyntype.AttributeDefinition{{AttributeName: aws.String("PK"), AttributeType: dyntype.ScalarAttributeTypeS}},
				BillingMode:          dyntype.BillingModePayPerRequest,
				StreamSpecification: &dyntype.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: dyntype.StreamViewType(tc.viewType),
				},
			})
			require.NoError(t, err)

			// Write an item to generate an INSERT record.
			_, err = client.PutItem(ctx, &awsdynamo.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]dyntype.AttributeValue{
					"PK":   &dyntype.AttributeValueMemberS{Value: "k1"},
					"data": &dyntype.AttributeValueMemberS{Value: "hello"},
				},
			})
			require.NoError(t, err)

			// Get stream ARN from table description.
			descOut, err := client.DescribeTable(ctx, &awsdynamo.DescribeTableInput{TableName: aws.String(tableName)})
			require.NoError(t, err)
			streamArn := aws.ToString(descOut.Table.LatestStreamArn)
			require.NotEmpty(t, streamArn)

			dsOut, err := streams.DescribeStream(ctx, &awsstreams.DescribeStreamInput{StreamArn: aws.String(streamArn)})
			require.NoError(t, err)
			require.NotEmpty(t, dsOut.StreamDescription.Shards)

			itOut, err := streams.GetShardIterator(ctx, &awsstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamArn),
				ShardId:           dsOut.StreamDescription.Shards[0].ShardId,
				ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
			})
			require.NoError(t, err)

			recOut, err := streams.GetRecords(ctx, &awsstreams.GetRecordsInput{
				ShardIterator: itOut.ShardIterator,
			})
			require.NoError(t, err)
			require.NotEmpty(t, recOut.Records)

			rec := recOut.Records[0]
			if tc.wantNew {
				assert.NotNil(t, rec.Dynamodb.NewImage, "expected NewImage for %s", tc.viewType)
			} else {
				assert.Nil(t, rec.Dynamodb.NewImage, "expected no NewImage for %s", tc.viewType)
			}
			// INSERT records never have OldImage (no prior version) — check it's absent
			// regardless of view type (that is always correct for INSERT).
			assert.Nil(t, rec.Dynamodb.OldImage, "INSERT records must not have OldImage")
		})
	}
}

func TestDynamoStreamsEventIDUnique(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ddb := newDynamoClient(t)
	streams := newDynamoStreamsClient(t)

	tbl := "EventIDTable"
	_, err := ddb.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:   aws.String(tbl),
		BillingMode: dyntype.BillingModePayPerRequest,
		KeySchema: []dyntype.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: dyntype.KeyTypeHash},
		},
		AttributeDefinitions: []dyntype.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: dyntype.ScalarAttributeTypeS},
		},
		StreamSpecification: &dyntype.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dyntype.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	// Write the same key 3 times to produce 3 stream records.
	for i := 0; i < 3; i++ {
		_, err = ddb.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String(tbl),
			Item: map[string]dyntype.AttributeValue{
				"PK": &dyntype.AttributeValueMemberS{Value: "same-key"},
				"V":  &dyntype.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
			},
		})
		require.NoError(t, err)
	}

	listOut, err := streams.ListStreams(ctx, &awsstreams.ListStreamsInput{
		TableName: aws.String(tbl),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.Streams)

	descOut, err := streams.DescribeStream(ctx, &awsstreams.DescribeStreamInput{
		StreamArn: listOut.Streams[0].StreamArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.StreamDescription.Shards)

	iterOut, err := streams.GetShardIterator(ctx, &awsstreams.GetShardIteratorInput{
		StreamArn:         listOut.Streams[0].StreamArn,
		ShardId:           descOut.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := streams.GetRecords(ctx, &awsstreams.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(recOut.Records), 3, "expected at least 3 stream records")

	eventIDs := map[string]bool{}
	for _, rec := range recOut.Records {
		id := aws.ToString(rec.EventID)
		assert.False(t, eventIDs[id], "duplicate eventID: %s", id)
		eventIDs[id] = true
		// SequenceNumber must be 21-digit zero-padded.
		seq := aws.ToString(rec.Dynamodb.SequenceNumber)
		assert.Regexp(t, `^\d{21}$`, seq, "SequenceNumber must be 21-digit zero-padded")
	}
}
