//go:build kinesis_fullmode

package kinesis_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func fullModeJaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func newFullModeKinesisClient(t *testing.T) *awskinesis.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	return awskinesis.NewFromConfig(cfg, func(o *awskinesis.Options) {
		o.BaseEndpoint = aws.String(fullModeJaiscloudHost())
	})
}

func waitForStreamReady(t *testing.T, client *awskinesis.Client, name string) {
	t.Helper()
	ctx := context.Background()
	deadline := clock.RealNow().Add(30 * time.Second)
	for clock.RealNow().Before(deadline) {
		desc, err := client.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(name),
		})
		if err == nil && desc.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("stream %q did not become ACTIVE within 30s", name)
}

// ─── Persistence tests ────────────────────────────────────────────────────────

// TestKinesisGetRecordsPersistence verifies that a record put to a stream is
// readable via GetShardIterator at TRIM_HORIZON + GetRecords, and that the
// data content matches what was put.
func TestKinesisGetRecordsPersistence(t *testing.T) {
	ctx := context.Background()
	client := newFullModeKinesisClient(t)

	name := "test-persistence-stream"

	// CreateStream
	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	waitForStreamReady(t, client, name)

	// Describe to get shard ID
	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String(name),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards)
	shardID := aws.ToString(desc.StreamDescription.Shards[0].ShardId)

	// PutRecord
	payload := []byte("kinesis-persistence-test-data")
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String(name),
		PartitionKey: aws.String("persistence-pk"),
		Data:         payload,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.SequenceNumber))

	// GetShardIterator at TRIM_HORIZON
	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String(name),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	require.NotNil(t, iterOut.ShardIterator)

	// GetRecords — assert the record is there
	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	require.NotEmpty(t, recOut.Records, "expected at least one record after PutRecord")

	// Verify data content matches
	found := false
	for _, r := range recOut.Records {
		if string(r.Data) == string(payload) {
			found = true
			assert.Equal(t, "persistence-pk", aws.ToString(r.PartitionKey))
			break
		}
	}
	assert.True(t, found, "expected record data %q in GetRecords response", string(payload))
}

// TestKinesisPersistence verifies that records written to a stream are
// readable via GetRecords after being put. This exercises the full read-back
// path and serves as a persistence smoke test.
func TestKinesisPersistence(t *testing.T) {
	ctx := context.Background()
	client := newFullModeKinesisClient(t)

	streamName := "persistence-test-stream"

	// Create stream
	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Wait for stream to become ACTIVE
	waitForStreamReady(t, client, streamName)

	// Describe the stream to get shard ID
	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards)
	shardID := aws.ToString(desc.StreamDescription.Shards[0].ShardId)

	// Put a record
	payload := []byte("persistence-record-data")
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("test-partition-key"),
		Data:         payload,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.SequenceNumber))

	// Get a TRIM_HORIZON shard iterator
	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	require.NotNil(t, iterOut.ShardIterator)

	// Get records — should contain the record we put
	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	require.NotEmpty(t, recOut.Records, "expected at least one record")

	// Find our record
	found := false
	for _, r := range recOut.Records {
		if string(r.Data) == string(payload) {
			found = true
			assert.Equal(t, "test-partition-key", aws.ToString(r.PartitionKey))
			break
		}
	}
	assert.True(t, found, "expected to find the put record in GetRecords response")
}

// TestKinesisPersistence_MultipleRecords verifies that multiple records put to
// a stream are all readable.
func TestKinesisPersistence_MultipleRecords(t *testing.T) {
	ctx := context.Background()
	client := newFullModeKinesisClient(t)

	streamName := "persistence-multi-stream"

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	waitForStreamReady(t, client, streamName)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards)
	shardID := aws.ToString(desc.StreamDescription.Shards[0].ShardId)

	// Put 5 records
	const numRecords = 5
	for i := 0; i < numRecords; i++ {
		_, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
			StreamName:   aws.String(streamName),
			PartitionKey: aws.String("pk"),
			Data:         []byte("record"),
		})
		require.NoError(t, err)
	}

	// Small pause to allow the emulator to settle
	time.Sleep(50 * time.Millisecond)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(100),
	})
	require.NoError(t, err)
	assert.Equal(t, numRecords, len(recOut.Records), "expected all %d records to be readable", numRecords)
}
