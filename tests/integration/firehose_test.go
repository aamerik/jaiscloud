// Package integration provides Firehose round-trip integration tests.
// NOTE: Firehose is not yet implemented in JaisCloud; these tests are skipped
// until the provider is added.
package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	awsfirehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFirehose_CreateDeliveryStream(t *testing.T) {
	t.Skip("Firehose not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newFirehoseClient(t)

	out, err := c.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("test-stream-create"),
		DeliveryStreamType: awsfirehosetypes.DeliveryStreamTypeDirectPut,
		S3DestinationConfiguration: &awsfirehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::my-dest-bucket"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose-role"),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.DeliveryStreamARN))
}

func TestFirehose_PutRecord(t *testing.T) {
	t.Skip("Firehose not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newFirehoseClient(t)

	_, err := c.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("put-record-stream"),
		DeliveryStreamType: awsfirehosetypes.DeliveryStreamTypeDirectPut,
		S3DestinationConfiguration: &awsfirehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::my-dest-bucket"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose-role"),
		},
	})
	require.NoError(t, err)

	putOut, err := c.PutRecord(ctx, &awsfirehose.PutRecordInput{
		DeliveryStreamName: aws.String("put-record-stream"),
		Record:             &awsfirehosetypes.Record{Data: []byte("hello firehose")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.RecordId))
}

func TestFirehose_DescribeDeliveryStream(t *testing.T) {
	t.Skip("Firehose not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newFirehoseClient(t)

	_, err := c.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("describe-stream"),
		DeliveryStreamType: awsfirehosetypes.DeliveryStreamTypeDirectPut,
		S3DestinationConfiguration: &awsfirehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::my-dest-bucket"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose-role"),
		},
	})
	require.NoError(t, err)

	descOut, err := c.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("describe-stream"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.DeliveryStreamDescription)
	assert.Equal(t, "describe-stream", aws.ToString(descOut.DeliveryStreamDescription.DeliveryStreamName))
	assert.Equal(t, awsfirehosetypes.DeliveryStreamStatusActive, descOut.DeliveryStreamDescription.DeliveryStreamStatus)
}
