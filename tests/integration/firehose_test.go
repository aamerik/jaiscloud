package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fhRoleARN = "arn:aws:iam::000000000000:role/firehose-role"
)

func fhBucketARN(bucket string) string {
	return "arn:aws:s3:::" + bucket
}

// TestFirehose_CreateDescribeDelete creates a delivery stream, describes it,
// then deletes it and asserts that a subsequent describe returns an error.
func TestFirehose_CreateDescribeDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	// Create backing bucket first
	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("fh-cdd-bucket"),
	})
	require.NoError(t, err)

	createOut, err := client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("cdd-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN("fh-cdd-bucket")),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createOut.DeliveryStreamARN))

	descOut, err := client.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("cdd-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, "cdd-stream", aws.ToString(descOut.DeliveryStreamDescription.DeliveryStreamName))
	assert.Equal(t, firehosetypes.DeliveryStreamStatusActive, descOut.DeliveryStreamDescription.DeliveryStreamStatus)

	_, err = client.DeleteDeliveryStream(ctx, &awsfirehose.DeleteDeliveryStreamInput{
		DeliveryStreamName: aws.String("cdd-stream"),
	})
	require.NoError(t, err)

	// After deletion the stream should not be found
	_, err = client.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("cdd-stream"),
	})
	require.Error(t, err)
}

// TestFirehose_ListDeliveryStreams creates 2 streams and asserts both names appear
// in the ListDeliveryStreams response.
func TestFirehose_ListDeliveryStreams(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	for _, name := range []string{"list-stream-one", "list-stream-two"} {
		bucket := name + "-bucket"
		_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
		require.NoError(t, err)
		_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
			DeliveryStreamName: aws.String(name),
			S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
				BucketARN: aws.String(fhBucketARN(bucket)),
				RoleARN:   aws.String(fhRoleARN),
			},
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListDeliveryStreams(ctx, &awsfirehose.ListDeliveryStreamsInput{})
	require.NoError(t, err)

	nameSet := make(map[string]bool)
	for _, n := range listOut.DeliveryStreamNames {
		nameSet[n] = true
	}
	assert.True(t, nameSet["list-stream-one"], "expected list-stream-one in ListDeliveryStreams")
	assert.True(t, nameSet["list-stream-two"], "expected list-stream-two in ListDeliveryStreams")
}

// TestFirehose_PutRecord creates a delivery stream with an S3 destination, puts a
// record, and asserts a non-empty RecordId is returned.
func TestFirehose_PutRecord(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("fh-put-bucket")})
	require.NoError(t, err)

	_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("put-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN("fh-put-bucket")),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)

	putOut, err := client.PutRecord(ctx, &awsfirehose.PutRecordInput{
		DeliveryStreamName: aws.String("put-stream"),
		Record: &firehosetypes.Record{
			Data: []byte("hello firehose"),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.RecordId))
}

// TestFirehose_PutRecordBatch puts 3 records as a batch and asserts FailedPutCount=0
// with 3 RequestResponses returned.
func TestFirehose_PutRecordBatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("fh-batch-bucket")})
	require.NoError(t, err)

	_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("batch-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN("fh-batch-bucket")),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)

	records := []firehosetypes.Record{
		{Data: []byte("record-1")},
		{Data: []byte("record-2")},
		{Data: []byte("record-3")},
	}
	batchOut, err := client.PutRecordBatch(ctx, &awsfirehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("batch-stream"),
		Records:            records,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), aws.ToInt32(batchOut.FailedPutCount))
	require.Len(t, batchOut.RequestResponses, 3)
	for i, rr := range batchOut.RequestResponses {
		assert.NotEmpty(t, aws.ToString(rr.RecordId), "record %d missing RecordId", i)
	}
}

// TestFirehose_Tags adds 2 tags, lists them, untags one, and asserts 1 remains.
func TestFirehose_Tags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("fh-tag-bucket")})
	require.NoError(t, err)

	_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("tag-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN("fh-tag-bucket")),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)

	_, err = client.TagDeliveryStream(ctx, &awsfirehose.TagDeliveryStreamInput{
		DeliveryStreamName: aws.String("tag-stream"),
		Tags: []firehosetypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForDeliveryStream(ctx, &awsfirehose.ListTagsForDeliveryStreamInput{
		DeliveryStreamName: aws.String("tag-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range listOut.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])

	// Untag "env"
	_, err = client.UntagDeliveryStream(ctx, &awsfirehose.UntagDeliveryStreamInput{
		DeliveryStreamName: aws.String("tag-stream"),
		TagKeys:            []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForDeliveryStream(ctx, &awsfirehose.ListTagsForDeliveryStreamInput{
		DeliveryStreamName: aws.String("tag-stream"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "team", aws.ToString(listOut2.Tags[0].Key))
}

// TestFirehose_UpdateDestination creates a delivery stream, describes it to get the
// current VersionId, calls UpdateDestination with new BufferingHints, and asserts
// the VersionId changed after the update.
func TestFirehose_UpdateDestination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("fh-upd-bucket")})
	require.NoError(t, err)

	_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("upd-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN("fh-upd-bucket")),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("upd-stream"),
	})
	require.NoError(t, err)
	initialVersionID := aws.ToString(descOut.DeliveryStreamDescription.VersionId)
	require.NotEmpty(t, initialVersionID)

	destinationID := aws.ToString(descOut.DeliveryStreamDescription.Destinations[0].DestinationId)

	_, err = client.UpdateDestination(ctx, &awsfirehose.UpdateDestinationInput{
		DeliveryStreamName:             aws.String("upd-stream"),
		CurrentDeliveryStreamVersionId: aws.String(initialVersionID),
		DestinationId:                  aws.String(destinationID),
		S3DestinationUpdate: &firehosetypes.S3DestinationUpdate{
			BucketARN: aws.String(fhBucketARN("fh-upd-bucket")),
			RoleARN:   aws.String(fhRoleARN),
			BufferingHints: &firehosetypes.BufferingHints{
				SizeInMBs:         aws.Int32(10),
				IntervalInSeconds: aws.Int32(120),
			},
		},
	})
	require.NoError(t, err)

	// VersionId should change after update
	descOut2, err := client.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("upd-stream"),
	})
	require.NoError(t, err)
	newVersionID := aws.ToString(descOut2.DeliveryStreamDescription.VersionId)
	assert.NotEqual(t, initialVersionID, newVersionID, "VersionId should change after UpdateDestination")
}

// TestFirehose_PutRecord_S3Delivery creates a bucket, creates a delivery stream pointing
// to it, puts 5 records, calls the admin flush endpoint, and asserts at least 1 object
// was written to the bucket.
func TestFirehose_PutRecord_S3Delivery(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	bucketName := "fh-s3delivery-bucket"
	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3delivery-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN(bucketName)),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)

	// Put 5 records
	for i := 0; i < 5; i++ {
		data := base64.StdEncoding.EncodeToString([]byte("delivery-record"))
		_, err = client.PutRecord(ctx, &awsfirehose.PutRecordInput{
			DeliveryStreamName: aws.String("s3delivery-stream"),
			Record: &firehosetypes.Record{
				Data: []byte(data),
			},
		})
		require.NoError(t, err)
	}

	// Trigger admin flush
	resp, err := http.Post(jaiscloudEndpoint+"/_jaiscloud/firehose/flush", "application/json", bytes.NewBufferString("{}"))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// At least 1 object should exist in the bucket
	listOut, err := s3c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.Contents), 1, "expected at least 1 object flushed to S3")
}

// TestFirehose_CreateStream_NotFound_Delete calls DeleteDeliveryStream with a non-existent
// stream name and asserts an error is returned.
func TestFirehose_CreateStream_NotFound_Delete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)

	_, err := client.DeleteDeliveryStream(ctx, &awsfirehose.DeleteDeliveryStreamInput{
		DeliveryStreamName: aws.String("does-not-exist-stream"),
	})
	require.Error(t, err)
}

// TestFirehose_DescribeStream_NotFound calls DescribeDeliveryStream with a non-existent
// stream name and asserts an error is returned.
func TestFirehose_DescribeStream_NotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)

	_, err := client.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("nonexistent-stream"),
	})
	require.Error(t, err)
}

// TestFirehose_PutRecordBatch_MultipleRecords puts 10 records as a batch and asserts
// that all 10 RequestResponse entries have a RecordId.
func TestFirehose_PutRecordBatch_MultipleRecords(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newFirehoseClient(t)
	s3c := newS3Client(t)

	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("fh-multi-bucket")})
	require.NoError(t, err)

	_, err = client.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("multi-batch-stream"),
		S3DestinationConfiguration: &firehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String(fhBucketARN("fh-multi-bucket")),
			RoleARN:   aws.String(fhRoleARN),
		},
	})
	require.NoError(t, err)

	records := make([]firehosetypes.Record, 10)
	for i := range records {
		records[i] = firehosetypes.Record{
			Data: []byte("multi-record"),
		}
	}

	batchOut, err := client.PutRecordBatch(ctx, &awsfirehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("multi-batch-stream"),
		Records:            records,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), aws.ToInt32(batchOut.FailedPutCount))
	require.Len(t, batchOut.RequestResponses, 10)
	for i, rr := range batchOut.RequestResponses {
		assert.NotEmpty(t, aws.ToString(rr.RecordId), "record %d missing RecordId", i)
	}
}
