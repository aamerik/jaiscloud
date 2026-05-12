package integration_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newKinesisClientCov(t *testing.T) *awskinesis.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	return awskinesis.NewFromConfig(cfg, func(o *awskinesis.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
	})
}

// ─── Stream CRUD ──────────────────────────────────────────────────────────────

// TestKinesisCov_CreateStream_ProvisionedMode verifies that a provisioned-mode
// stream is created and immediately ACTIVE.
func TestKinesisCov_CreateStream_ProvisionedMode(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("prov-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{
		StreamName: aws.String("prov-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, kinesistypes.StreamStatusActive, out.StreamDescriptionSummary.StreamStatus)
}

// TestKinesisCov_CreateStream_OnDemandMode verifies that an ON_DEMAND stream
// can be created.
func TestKinesisCov_CreateStream_OnDemandMode(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ondemand-stream"),
		StreamModeDetails: &kinesistypes.StreamModeDetails{
			StreamMode: kinesistypes.StreamModeOnDemand,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{
		StreamName: aws.String("ondemand-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, kinesistypes.StreamStatusActive, out.StreamDescriptionSummary.StreamStatus)
}

// TestKinesisCov_CreateStream_DuplicateName_Error verifies that creating a
// stream with the same name twice returns ResourceInUseException.
func TestKinesisCov_CreateStream_DuplicateName_Error(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("dup-cov-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("dup-cov-stream"),
		ShardCount: aws.Int32(1),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceInUseException")
}

// TestKinesisCov_DescribeStream_HasShards verifies that DescribeStream returns
// a non-empty shard list.
func TestKinesisCov_DescribeStream_HasShards(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("shard-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	out, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("shard-stream"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.StreamDescription.Shards, "expected at least one shard")
}

// TestKinesisCov_DescribeStreamSummary_StreamStatus verifies that the stream
// summary reports ACTIVE status.
func TestKinesisCov_DescribeStreamSummary_StreamStatus(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("summary-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{
		StreamName: aws.String("summary-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, kinesistypes.StreamStatusActive, out.StreamDescriptionSummary.StreamStatus)
}

// TestKinesisCov_ListStreams_IncludesCreated verifies that a newly created
// stream appears in the ListStreams response.
func TestKinesisCov_ListStreams_IncludesCreated(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("listed-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.ListStreams(ctx, &awskinesis.ListStreamsInput{})
	require.NoError(t, err)

	found := false
	for _, name := range out.StreamNames {
		if name == "listed-stream" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected listed-stream to appear in ListStreams")
}

// TestKinesisCov_ListStreams_Pagination verifies that when more streams exist
// than the specified Limit, a NextToken is returned.
func TestKinesisCov_ListStreams_Pagination(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	for _, name := range []string{"page-stream-a", "page-stream-b", "page-stream-c"} {
		_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
			StreamName: aws.String(name),
			ShardCount: aws.Int32(1),
		})
		require.NoError(t, err)
	}

	out, err := client.ListStreams(ctx, &awskinesis.ListStreamsInput{
		Limit: aws.Int32(2),
	})
	require.NoError(t, err)
	// With 3 streams and Limit=2 we expect HasMoreStreams=true and a token.
	require.NotNil(t, out.HasMoreStreams)
	assert.True(t, *out.HasMoreStreams, "expected HasMoreStreams=true with 3 streams and Limit=2")
	assert.Len(t, out.StreamNames, 2)
}

// TestKinesisCov_DeleteStream_RemovesIt verifies that after deletion a stream no
// longer appears in ListStreams.
func TestKinesisCov_DeleteStream_RemovesIt(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("delete-me"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{
		StreamName: aws.String("delete-me"),
	})
	require.NoError(t, err)

	out, err := client.ListStreams(ctx, &awskinesis.ListStreamsInput{})
	require.NoError(t, err)
	for _, name := range out.StreamNames {
		assert.NotEqual(t, "delete-me", name, "deleted stream should not appear in ListStreams")
	}
}

// ─── PutRecord / GetRecords ───────────────────────────────────────────────────

// TestKinesisCov_PutRecord_Success verifies that PutRecord returns a non-empty
// SequenceNumber.
func TestKinesisCov_PutRecord_Success(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("put-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("put-stream"),
		PartitionKey: aws.String("pk1"),
		Data:         []byte("hello kinesis"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *out.SequenceNumber)
}

// TestKinesisCov_PutRecord_PartitionKeyDeterminesShard verifies that PutRecord
// returns a non-empty ShardId.
func TestKinesisCov_PutRecord_PartitionKeyDeterminesShard(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("shard-route-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	out, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("shard-route-stream"),
		PartitionKey: aws.String("my-partition-key"),
		Data:         []byte("data"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *out.ShardId, "expected a non-empty ShardId")
}

// TestKinesisCov_PutRecord_DataTooLarge_Error verifies that data exceeding 1 MB
// is rejected with an error.
func TestKinesisCov_PutRecord_DataTooLarge_Error(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("big-data-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// 1 MB + 1 byte
	bigData := make([]byte, 1024*1024+1)
	_, err = client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("big-data-stream"),
		PartitionKey: aws.String("pk"),
		Data:         bigData,
	})
	require.Error(t, err)
}

// TestKinesisCov_PutRecords_AllSuccess verifies that a batch put returns
// FailedRecordCount=0.
func TestKinesisCov_PutRecords_AllSuccess(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("batch-put-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	records := []kinesistypes.PutRecordsRequestEntry{
		{PartitionKey: aws.String("pk1"), Data: []byte("record1")},
		{PartitionKey: aws.String("pk2"), Data: []byte("record2")},
		{PartitionKey: aws.String("pk3"), Data: []byte("record3")},
	}

	out, err := client.PutRecords(ctx, &awskinesis.PutRecordsInput{
		StreamName: aws.String("batch-put-stream"),
		Records:    records,
	})
	require.NoError(t, err)
	require.NotNil(t, out.FailedRecordCount)
	assert.Equal(t, int32(0), *out.FailedRecordCount)
}

// TestKinesisCov_PutRecords_ReturnSequenceNumbers verifies that each successful
// record entry in PutRecords has a non-empty SequenceNumber.
func TestKinesisCov_PutRecords_ReturnSequenceNumbers(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("batch-seq-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	records := []kinesistypes.PutRecordsRequestEntry{
		{PartitionKey: aws.String("pk-a"), Data: []byte("data-a")},
		{PartitionKey: aws.String("pk-b"), Data: []byte("data-b")},
	}

	out, err := client.PutRecords(ctx, &awskinesis.PutRecordsInput{
		StreamName: aws.String("batch-seq-stream"),
		Records:    records,
	})
	require.NoError(t, err)
	require.Len(t, out.Records, 2)
	for i, r := range out.Records {
		assert.Empty(t, r.ErrorCode, "record[%d] should not have ErrorCode", i)
		assert.NotEmpty(t, *r.SequenceNumber, "record[%d] should have SequenceNumber", i)
	}
}

// TestKinesisCov_GetShardIterator_TrimHorizon verifies that a TRIM_HORIZON
// iterator can be obtained.
func TestKinesisCov_GetShardIterator_TrimHorizon(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("iter-th-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Get the shard ID.
	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("iter-th-stream"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards)
	shardID := *desc.StreamDescription.Shards[0].ShardId

	out, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("iter-th-stream"),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *out.ShardIterator)
}

// TestKinesisCov_GetShardIterator_Latest verifies that a LATEST iterator can be
// obtained and returns 0 records when used immediately after creation.
func TestKinesisCov_GetShardIterator_Latest(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("iter-latest-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Put a record first so LATEST means "after existing records".
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("iter-latest-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte("existing"),
	})
	require.NoError(t, err)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("iter-latest-stream"),
		ShardId:           putOut.ShardId,
		ShardIteratorType: kinesistypes.ShardIteratorTypeLatest,
	})
	require.NoError(t, err)

	// LATEST should yield 0 records because no new records were added after the iterator.
	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Empty(t, recOut.Records, "LATEST iterator should return 0 records immediately after creation")
}

// TestKinesisCov_GetRecords_AfterPut verifies that a TRIM_HORIZON iterator
// returns records previously put to the stream.
func TestKinesisCov_GetRecords_AfterPut(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("get-rec-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	payload := []byte("hello world")
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("get-rec-stream"),
		PartitionKey: aws.String("pk"),
		Data:         payload,
	})
	require.NoError(t, err)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("get-rec-stream"),
		ShardId:           putOut.ShardId,
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 1, "expected exactly 1 record")

	// SDK returns data as base64-decoded bytes.
	assert.Equal(t, payload, recOut.Records[0].Data)
}

// TestKinesisCov_GetRecords_NextIteratorAdvances verifies that the
// NextShardIterator returned by GetRecords is different from the first iterator
// and returns no records on a subsequent call (no new records were added).
func TestKinesisCov_GetRecords_NextIteratorAdvances(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("next-iter-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String("next-iter-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte("record"),
	})
	require.NoError(t, err)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("next-iter-stream"),
		ShardId:           putOut.ShardId,
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	// First call: reads the one record.
	firstGet, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, firstGet.Records, 1)
	require.NotEmpty(t, *firstGet.NextShardIterator)

	// Second call with the next iterator: no new records.
	secondGet, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: firstGet.NextShardIterator,
	})
	require.NoError(t, err)
	assert.Empty(t, secondGet.Records, "second GetRecords with advanced iterator should return 0 records")
}

// TestKinesisCov_GetRecords_LimitParameter verifies that the Limit parameter
// caps the number of records returned per call.
func TestKinesisCov_GetRecords_LimitParameter(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("limit-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Put 5 records.
	var shardID string
	for i := 0; i < 5; i++ {
		out, err2 := client.PutRecord(ctx, &awskinesis.PutRecordInput{
			StreamName:   aws.String("limit-stream"),
			PartitionKey: aws.String("pk"),
			Data:         []byte("data"),
		})
		require.NoError(t, err2)
		shardID = *out.ShardId
	}

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String("limit-stream"),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(2),
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(recOut.Records), 2, "GetRecords with Limit=2 should return at most 2 records")
}

// ─── Shards ───────────────────────────────────────────────────────────────────

// TestKinesisCov_ListShards_ReturnsShards verifies that ListShards returns one
// entry per shard in the stream.
func TestKinesisCov_ListShards_ReturnsShards(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("list-shards-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	out, err := client.ListShards(ctx, &awskinesis.ListShardsInput{
		StreamName: aws.String("list-shards-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 2)
}

// TestKinesisCov_ListShards_Pagination verifies that ShardFilter and MaxResults
// can be used together without error.
func TestKinesisCov_ListShards_Pagination(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ls-page-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	// Using ShardFilter with FROM_TRIM_HORIZON to select all open shards.
	out, err := client.ListShards(ctx, &awskinesis.ListShardsInput{
		StreamName: aws.String("ls-page-stream"),
		ShardFilter: &kinesistypes.ShardFilter{
			Type: kinesistypes.ShardFilterTypeFromTrimHorizon,
		},
		MaxResults: aws.Int32(10),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.Shards), 1, "expected at least one shard")
}

// TestKinesisCov_SplitShard_IncreasesCount verifies that splitting a shard
// produces 3 total shards: the original (now closed) plus two new open ones.
func TestKinesisCov_SplitShard_IncreasesCount(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("split-cov-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("split-cov-stream"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards)
	shardID := *desc.StreamDescription.Shards[0].ShardId

	// Midpoint of the full hash range 0..2^128-1.
	_, err = client.SplitShard(ctx, &awskinesis.SplitShardInput{
		StreamName:         aws.String("split-cov-stream"),
		ShardToSplit:       aws.String(shardID),
		NewStartingHashKey: aws.String("170141183460469231731687303715884105727"),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("split-cov-stream"),
	})
	require.NoError(t, err)
	// 1 closed parent + 2 open children = 3 total shards
	assert.Len(t, desc2.StreamDescription.Shards, 3)
}

// TestKinesisCov_MergeShards_DecreasesCount verifies that merging two adjacent
// shards produces 3 total shards: the two closed originals plus one new open one.
func TestKinesisCov_MergeShards_DecreasesCount(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("merge-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("merge-stream"),
	})
	require.NoError(t, err)
	require.Len(t, desc.StreamDescription.Shards, 2)

	shard0 := *desc.StreamDescription.Shards[0].ShardId
	shard1 := *desc.StreamDescription.Shards[1].ShardId

	_, err = client.MergeShards(ctx, &awskinesis.MergeShardsInput{
		StreamName:           aws.String("merge-stream"),
		ShardToMerge:         aws.String(shard0),
		AdjacentShardToMerge: aws.String(shard1),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("merge-stream"),
	})
	require.NoError(t, err)
	// 2 closed + 1 new open = 3 total shards
	assert.Len(t, desc2.StreamDescription.Shards, 3)
}

// TestKinesisCov_UpdateShardCount_Changes verifies that UpdateShardCount
// succeeds and returns the target count.
func TestKinesisCov_UpdateShardCount_Changes(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("update-shards-stream"),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)

	out, err := client.UpdateShardCount(ctx, &awskinesis.UpdateShardCountInput{
		StreamName:       aws.String("update-shards-stream"),
		TargetShardCount: aws.Int32(4),
		ScalingType:      kinesistypes.ScalingTypeUniformScaling,
	})
	require.NoError(t, err)
	require.NotNil(t, out.TargetShardCount)
	assert.Equal(t, int32(4), *out.TargetShardCount)
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

// TestKinesisCov_AddTagsToStream_Persists verifies that tags added to a stream
// are returned by ListTagsForStream.
func TestKinesisCov_AddTagsToStream_Persists(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("tag-add-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.AddTagsToStream(ctx, &awskinesis.AddTagsToStreamInput{
		StreamName: aws.String("tag-add-stream"),
		Tags:       map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForStream(ctx, &awskinesis.ListTagsForStreamInput{
		StreamName: aws.String("tag-add-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2, "expected 2 tags after adding 2")

	tagMap := make(map[string]string)
	for _, tag := range listOut.Tags {
		tagMap[*tag.Key] = *tag.Value
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
}

// TestKinesisCov_RemoveTagsFromStream verifies that removing a specific tag
// key leaves the other tags intact.
func TestKinesisCov_RemoveTagsFromStream(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("tag-remove-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.AddTagsToStream(ctx, &awskinesis.AddTagsToStreamInput{
		StreamName: aws.String("tag-remove-stream"),
		Tags:       map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	_, err = client.RemoveTagsFromStream(ctx, &awskinesis.RemoveTagsFromStreamInput{
		StreamName: aws.String("tag-remove-stream"),
		TagKeys:    []string{"env"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForStream(ctx, &awskinesis.ListTagsForStreamInput{
		StreamName: aws.String("tag-remove-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 1, "expected 1 tag after removing 1 of 2")
	assert.Equal(t, "team", *listOut.Tags[0].Key)
}

// TestKinesisCov_AddTagsToStream_TooMany_Error verifies that adding more than
// 50 tags is rejected.
func TestKinesisCov_AddTagsToStream_TooMany_Error(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("too-many-tags-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Build 51 tags, which exceeds the maximum of 50.
	tags := make(map[string]string, 51)
	for i := 0; i < 51; i++ {
		key := base64.StdEncoding.EncodeToString([]byte{byte(i), byte(i >> 8)})
		tags["tag-"+key] = "value"
	}

	_, err = client.AddTagsToStream(ctx, &awskinesis.AddTagsToStreamInput{
		StreamName: aws.String("too-many-tags-stream"),
		Tags:       tags,
	})
	require.Error(t, err)
}

// ─── Retention ────────────────────────────────────────────────────────────────

// TestKinesisCov_IncreaseRetentionPeriod verifies that IncreaseStreamRetentionPeriod
// updates the value reported by DescribeStream.
func TestKinesisCov_IncreaseRetentionPeriod(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ret-inc-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.IncreaseStreamRetentionPeriod(ctx, &awskinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           aws.String("ret-inc-stream"),
		RetentionPeriodHours: aws.Int32(48),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("ret-inc-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(48), desc.StreamDescription.RetentionPeriodHours)
}

// TestKinesisCov_DecreaseRetentionPeriod verifies that DecreaseStreamRetentionPeriod
// can reduce an elevated retention period back to the minimum of 24 hours.
func TestKinesisCov_DecreaseRetentionPeriod(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ret-dec-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Increase first so we have something to decrease.
	_, err = client.IncreaseStreamRetentionPeriod(ctx, &awskinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           aws.String("ret-dec-stream"),
		RetentionPeriodHours: aws.Int32(48),
	})
	require.NoError(t, err)

	_, err = client.DecreaseStreamRetentionPeriod(ctx, &awskinesis.DecreaseStreamRetentionPeriodInput{
		StreamName:           aws.String("ret-dec-stream"),
		RetentionPeriodHours: aws.Int32(24),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String("ret-dec-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(24), desc.StreamDescription.RetentionPeriodHours)
}

// TestKinesisCov_RetentionPeriod_TooShort_Error verifies that setting a
// retention period below the 24-hour minimum is rejected.
func TestKinesisCov_RetentionPeriod_TooShort_Error(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ret-short-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.DecreaseStreamRetentionPeriod(ctx, &awskinesis.DecreaseStreamRetentionPeriodInput{
		StreamName:           aws.String("ret-short-stream"),
		RetentionPeriodHours: aws.Int32(1),
	})
	require.Error(t, err)
}

// TestKinesisCov_RetentionPeriod_TooLong_Error verifies that setting a
// retention period above the 8760-hour (365-day) maximum is rejected.
func TestKinesisCov_RetentionPeriod_TooLong_Error(t *testing.T) {
	resetState(t)
	client := newKinesisClientCov(t)
	ctx := context.Background()

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String("ret-long-stream"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.IncreaseStreamRetentionPeriod(ctx, &awskinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           aws.String("ret-long-stream"),
		RetentionPeriodHours: aws.Int32(10000),
	})
	require.Error(t, err)
}

