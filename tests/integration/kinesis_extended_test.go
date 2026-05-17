package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKinesis_PutRecords_Batch creates a stream, puts 5 records as a batch, and
// asserts FailedRecordCount=0 with all SequenceNumbers set.
func TestKinesis_PutRecords_BatchMultiple(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ext-batch-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	entries := make([]kinesistypes.PutRecordsRequestEntry, 5)
	for i := range entries {
		entries[i] = kinesistypes.PutRecordsRequestEntry{
			Data:         []byte("record-" + strings.Repeat("x", i+1)),
			PartitionKey: aws.String("pk-" + strings.Repeat("k", i+1)),
		}
	}

	out, err := client.PutRecords(ctx, &awskinesis.PutRecordsInput{
		StreamName: aws.String("ext-batch-stream"),
		Records:    entries,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), aws.ToInt32(out.FailedRecordCount))
	require.Len(t, out.Records, 5)
	for i, rec := range out.Records {
		assert.NotEmpty(t, aws.ToString(rec.SequenceNumber), "record %d missing SequenceNumber", i)
	}
}

// TestKinesis_GetRecords_Pagination puts 10 records, reads 5 via TRIM_HORIZON, then
// uses NextShardIterator to read the remaining records.
func TestKinesis_GetRecords_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("paginate-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Put 10 records
	var shardID string
	for i := 0; i < 10; i++ {
		putOut, perr := client.PutRecord(ctx, &awskinesis.PutRecordInput{
			StreamName:   aws.String("paginate-stream"),
			PartitionKey: aws.String("pk"),
			Data:         []byte("data"),
		})
		require.NoError(t, perr)
		shardID = aws.ToString(putOut.ShardId)
	}

	// Get shard iterator at TRIM_HORIZON
	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("paginate-stream"),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	// First page: Limit=5
	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(5),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(recOut.Records), 5)
	require.NotEmpty(t, aws.ToString(recOut.NextShardIterator), "expected NextShardIterator for second page")

	// Second page using NextShardIterator
	recOut2, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: recOut.NextShardIterator,
	})
	require.NoError(t, err)
	// Together first + second page should cover all 10 records
	total := len(recOut.Records) + len(recOut2.Records)
	assert.Equal(t, 10, total)
}

// TestKinesis_AddListTags adds two tags to a stream and asserts both are present
// via ListTagsForStream.
func TestKinesis_AddListTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ext-tag-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.AddTagsToStream(ctx, &awskinesis.AddTagsToStreamInput{
		StreamName: aws.String("ext-tag-stream"),
		Tags:       map[string]string{"project": "jaiscloud", "stage": "integration"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForStream(ctx, &awskinesis.ListTagsForStreamInput{
		StreamName: aws.String("ext-tag-stream"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range listOut.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "jaiscloud", tagMap["project"])
	assert.Equal(t, "integration", tagMap["stage"])
}

// TestKinesis_DescribeStream_Shards creates a stream and asserts DescribeStream
// returns at least one shard with a ShardId.
func TestKinesis_DescribeStream_Shards(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("shards-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("shards-stream"),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(descOut.StreamDescription.Shards), 1)
	for _, shard := range descOut.StreamDescription.Shards {
		assert.NotEmpty(t, aws.ToString(shard.ShardId))
	}
}

// TestKinesis_ShardIteratorType_LATEST puts a record, then gets a LATEST shard iterator.
// GetRecords should return 0 records since no data was written after the iterator was created.
func TestKinesis_ShardIteratorType_LATEST(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("latest-iter-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Put a record so the shard is known
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("latest-iter-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte("pre-data"),
	})
	require.NoError(t, err)

	// LATEST iterator — positioned after all existing records
	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("latest-iter-stream"),
		ShardId:           putOut.ShardId,
		ShardIteratorType: kinesistypes.ShardIteratorTypeLatest,
	})
	require.NoError(t, err)

	// No new records were written after LATEST — expect 0 records
	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Len(t, recOut.Records, 0)
}

// TestKinesis_ShardIteratorType_AT_SEQUENCE puts a record, creates an AT_SEQUENCE_NUMBER
// iterator for that record's sequence, and asserts 1 record is returned.
func TestKinesis_ShardIteratorType_AT_SEQUENCE(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("at-seq-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("at-seq-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte("hello"),
	})
	require.NoError(t, err)
	seqNum := aws.ToString(putOut.SequenceNumber)
	shardID := aws.ToString(putOut.ShardId)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:             aws.String("at-seq-stream"),
		ShardId:                aws.String(shardID),
		ShardIteratorType:      kinesistypes.ShardIteratorTypeAtSequenceNumber,
		StartingSequenceNumber: aws.String(seqNum),
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 1)
	assert.Equal(t, seqNum, aws.ToString(recOut.Records[0].SequenceNumber))
	assert.Equal(t, []byte("hello"), recOut.Records[0].Data)
}

// TestKinesis_ShardIteratorType_AFTER_SEQUENCE puts one record, creates an
// AFTER_SEQUENCE_NUMBER iterator for that record's sequence, and asserts 0 records
// are returned (since no records exist after that specific sequence).
func TestKinesis_ShardIteratorType_AFTER_SEQUENCE(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("after-seq-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("after-seq-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte("only-record"),
	})
	require.NoError(t, err)
	seqNum := aws.ToString(putOut.SequenceNumber)
	shardID := aws.ToString(putOut.ShardId)

	// AFTER_SEQUENCE_NUMBER of the only record → position points past the end
	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:             aws.String("after-seq-stream"),
		ShardId:                aws.String(shardID),
		ShardIteratorType:      kinesistypes.ShardIteratorTypeAfterSequenceNumber,
		StartingSequenceNumber: aws.String(seqNum),
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Len(t, recOut.Records, 0)
}

// TestKinesis_ListStreams creates two streams and asserts both names are present
// in ListStreams.
func TestKinesis_ListStreams_Extended(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newKinesisClient(t)

	for _, name := range []string{"ext-stream-alpha", "ext-stream-beta"} {
		_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
			StreamName: aws.String(name),
			ShardCount: aws.Int32(1),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListStreams(ctx, &awskinesis.ListStreamsInput{})
	require.NoError(t, err)

	found := make(map[string]bool)
	for _, n := range listOut.StreamNames {
		found[n] = true
	}
	assert.True(t, found["ext-stream-alpha"], "expected ext-stream-alpha in ListStreams")
	assert.True(t, found["ext-stream-beta"], "expected ext-stream-beta in ListStreams")
}
