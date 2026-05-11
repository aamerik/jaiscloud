package integration_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newKinesisClient(t *testing.T) *awskinesis.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awskinesis.NewFromConfig(cfg, func(o *awskinesis.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func TestKinesis_CreateDescribeDeleteStream(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("test-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("test-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.StreamStatusActive, desc.StreamDescription.StreamStatus)
	assert.Len(t, desc.StreamDescription.Shards, 2)
	assert.Contains(t, *desc.StreamDescription.StreamARN, "arn:aws:kinesis:")

	_, err = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{
		StreamName: aws.String("test-stream"),
	})
	require.NoError(t, err)

	_, err = client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("test-stream"),
	})
	require.Error(t, err)
}

func TestKinesis_CreateStream_AlreadyExists(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("dup-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("dup-stream"),
		ShardCount: aws.Int32(1),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceInUseException")
}

func TestKinesis_ListStreams(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	for _, name := range []string{"stream-a", "stream-b", "stream-c"} {
		_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
			StreamName: aws.String(name),
			ShardCount: aws.Int32(1),
		})
		require.NoError(t, err)
	}

	out, err := client.ListStreams(ctx, &awskinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Len(t, out.StreamNames, 3)
}

func TestKinesis_PutAndGetRecord(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("data-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	payload := []byte("hello kinesis")
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("data-stream"),
		PartitionKey: aws.String("pk1"),
		Data:         payload,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *putOut.SequenceNumber)
	assert.NotEmpty(t, *putOut.ShardId)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("data-stream"),
		ShardId:           putOut.ShardId,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 1)
	assert.Equal(t, payload, recOut.Records[0].Data)
	assert.Equal(t, "pk1", *recOut.Records[0].PartitionKey)
}

func TestKinesis_PutRecords_Batch(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("batch-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	records := make([]types.PutRecordsRequestEntry, 5)
	for i := range records {
		records[i] = types.PutRecordsRequestEntry{
			Data:         []byte("record-data"),
			PartitionKey: aws.String(strings.Repeat("k", i+1)),
		}
	}

	out, err := client.PutRecords(ctx, &awskinesis.PutRecordsInput{
		StreamName: aws.String("batch-stream"),
		Records:    records,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), *out.FailedRecordCount)
	assert.Len(t, out.Records, 5)
}

func TestKinesis_GetShardIterator_Latest(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("latest-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// put 3 records
	var lastShardID string
	for i := 0; i < 3; i++ {
		out, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
			StreamName:   aws.String("latest-stream"),
			PartitionKey: aws.String("pk"),
			Data:         []byte("data"),
		})
		require.NoError(t, err)
		lastShardID = *out.ShardId
	}

	// LATEST iterator → should get 0 records
	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("latest-stream"),
		ShardId:           aws.String(lastShardID),
		ShardIteratorType: types.ShardIteratorTypeLatest,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Len(t, recOut.Records, 0)
}

func TestKinesis_MultipleShards_RecordRouting(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("multi-shard"),
		ShardCount: aws.Int32(4),
	})
	require.NoError(t, err)

	// put 20 records with different partition keys
	shardsSeen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		out, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
			StreamName:   aws.String("multi-shard"),
			PartitionKey: aws.String(strings.Repeat("x", i+1)),
			Data:         []byte("d"),
		})
		require.NoError(t, err)
		shardsSeen[*out.ShardId] = true
	}
	// with 20 records and 4 shards, expect at least 2 shards used
	assert.GreaterOrEqual(t, len(shardsSeen), 2)
}

func TestKinesis_Tags(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("tag-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.AddTagsToStream(ctx, &awskinesis.AddTagsToStreamInput{
		StreamName: aws.String("tag-stream"),
		Tags:       map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForStream(ctx, &awskinesis.ListTagsForStreamInput{
		StreamName: aws.String("tag-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	_, err = client.RemoveTagsFromStream(ctx, &awskinesis.RemoveTagsFromStreamInput{
		StreamName: aws.String("tag-stream"),
		TagKeys:    []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForStream(ctx, &awskinesis.ListTagsForStreamInput{
		StreamName: aws.String("tag-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
}

func TestKinesis_RetentionPeriod(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ret-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.IncreaseStreamRetentionPeriod(ctx, &awskinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           aws.String("ret-stream"),
		RetentionPeriodHours: aws.Int32(48),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("ret-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(48), desc.StreamDescription.RetentionPeriodHours)
}

func TestKinesis_Consumer_Lifecycle(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("consumer-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("consumer-stream"),
	})
	require.NoError(t, err)
	streamARN := *desc.StreamDescription.StreamARN

	regOut, err := client.RegisterStreamConsumer(ctx, &awskinesis.RegisterStreamConsumerInput{
		StreamARN:    aws.String(streamARN),
		ConsumerName: aws.String("my-consumer"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-consumer", *regOut.Consumer.ConsumerName)

	listOut, err := client.ListStreamConsumers(ctx, &awskinesis.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Consumers, 1)

	_, err = client.DeregisterStreamConsumer(ctx, &awskinesis.DeregisterStreamConsumerInput{
		ConsumerARN: regOut.Consumer.ConsumerARN,
	})
	require.NoError(t, err)

	listOut2, err := client.ListStreamConsumers(ctx, &awskinesis.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Consumers, 0)
}

func TestKinesis_SplitShard(t *testing.T) {
	resetState(t)
	client := newKinesisClient(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("split-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("split-stream"),
	})
	require.NoError(t, err)
	shardID := *desc.StreamDescription.Shards[0].ShardId

	_, err = client.SplitShard(ctx, &awskinesis.SplitShardInput{
		StreamName:           aws.String("split-stream"),
		ShardToSplit:         aws.String(shardID),
		NewStartingHashKey:   aws.String("170141183460469231731687303715884105728"), // 2^127
	})
	require.NoError(t, err)

	desc2, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("split-stream"),
	})
	require.NoError(t, err)
	// 1 closed parent + 2 open children
	assert.Len(t, desc2.StreamDescription.Shards, 3)
}

// base64 encoded data helper
func b64(s string) []byte {
	d, _ := base64.StdEncoding.DecodeString(s)
	return d
}
