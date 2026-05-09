package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P3.4: S3 CopyObject Source Conditionals ─────────────────────────────────

func putObjectGetETag(t *testing.T, c *awss3.Client, bucket, key, body string) string {
	t.Helper()
	out, err := c.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(body)),
	})
	require.NoError(t, err)
	return aws.ToString(out.ETag)
}

func TestS3_CopyObject_IfMatch_Succeeds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ifmatch-src")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ifmatch-dst")})
	require.NoError(t, err)

	etag := putObjectGetETag(t, c, "ifmatch-src", "obj", "hello")

	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:            aws.String("ifmatch-dst"),
		Key:               aws.String("obj"),
		CopySource:        aws.String("ifmatch-src/obj"),
		CopySourceIfMatch: aws.String(etag),
	})
	require.NoError(t, err)
}

func TestS3_CopyObject_IfMatch_WrongETag_Fails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ifmatch-wrong-src")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ifmatch-wrong-dst")})
	require.NoError(t, err)

	putObjectGetETag(t, c, "ifmatch-wrong-src", "obj", "hello")

	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:            aws.String("ifmatch-wrong-dst"),
		Key:               aws.String("obj"),
		CopySource:        aws.String("ifmatch-wrong-src/obj"),
		CopySourceIfMatch: aws.String("\"wrongetag\""),
	})
	require.Error(t, err, "CopyObject with wrong If-Match ETag must return 412")
}

func TestS3_CopyObject_IfNoneMatch_MatchingETag_Fails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ifnonematch-src")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ifnonematch-dst")})
	require.NoError(t, err)

	etag := putObjectGetETag(t, c, "ifnonematch-src", "obj", "hello")

	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:                aws.String("ifnonematch-dst"),
		Key:                   aws.String("obj"),
		CopySource:            aws.String("ifnonematch-src/obj"),
		CopySourceIfNoneMatch: aws.String(etag),
	})
	require.Error(t, err, "CopyObject with matching If-None-Match ETag must return 304")
}

// ─── P3.5: S3 ListObjectVersions – split Versions / DeleteMarkers ────────────

func TestS3_ListObjectVersions_SplitsVersionsAndDeleteMarkers(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("vers-split")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("vers-split"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Two versions of the same key.
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("vers-split"),
		Key:    aws.String("k1"),
		Body:   strings.NewReader("v1"),
	})
	require.NoError(t, err)
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("vers-split"),
		Key:    aws.String("k1"),
		Body:   strings.NewReader("v2"),
	})
	require.NoError(t, err)

	// Delete creates a delete marker.
	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String("vers-split"),
		Key:    aws.String("k1"),
	})
	require.NoError(t, err)

	out, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String("vers-split"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Versions, 2, "should have 2 object versions")
	assert.Len(t, out.DeleteMarkers, 1, "should have 1 delete marker")
}
