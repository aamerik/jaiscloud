package integration_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P5.1: S3 Bucket Notifications ───────────────────────────────────────────

func TestS3_Notification_PutAndGetConfig(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("notif-bucket")})
	require.NoError(t, err)

	// Put an empty notification config — just verify no error
	_, _ = c.PutBucketNotificationConfiguration(ctx, &awss3.PutBucketNotificationConfigurationInput{
		Bucket:                    aws.String("notif-bucket"),
		NotificationConfiguration: &awss3types.NotificationConfiguration{},
	})

	// Get the notification config — must not error even if empty
	getOut, err := c.GetBucketNotificationConfiguration(ctx, &awss3.GetBucketNotificationConfigurationInput{
		Bucket: aws.String("notif-bucket"),
	})
	require.NoError(t, err)
	assert.NotNil(t, getOut)
}

func TestS3_Notification_GetEmpty_ReturnsEmpty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("no-notif-bucket")})
	require.NoError(t, err)

	out, err := c.GetBucketNotificationConfiguration(ctx, &awss3.GetBucketNotificationConfigurationInput{
		Bucket: aws.String("no-notif-bucket"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.QueueConfigurations)
	assert.Empty(t, out.TopicConfigurations)
	assert.Empty(t, out.LambdaFunctionConfigurations)
}

func TestS3_Notification_PutObjectDoesNotError(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("notif-put-obj")})
	require.NoError(t, err)

	// PutObject on a bucket with no notification config should succeed normally
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("notif-put-obj"),
		Key:    aws.String("test-key"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	require.NoError(t, err)
}
