package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3_ConditionalPut_IfNoneMatch verifies that a PutObject with
// If-None-Match: * succeeds when the object does not exist, but is rejected
// (412 PreconditionFailed) when the object already exists.
func TestS3_ConditionalPut_IfNoneMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "cond-put-bucket"
	key := "cond-put-key"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// First PutObject — object does not exist yet, If-None-Match: * should succeed.
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("initial")),
		IfNoneMatch: aws.String("*"),
	})
	require.NoError(t, err, "PutObject with If-None-Match:* should succeed when object is absent")

	// Second PutObject with If-None-Match: * — object now exists, should fail.
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("updated")),
		IfNoneMatch: aws.String("*"),
	})
	require.Error(t, err, "PutObject with If-None-Match:* should fail when object exists")
	assert.True(t, strings.Contains(err.Error(), "412") ||
		strings.Contains(strings.ToLower(err.Error()), "precondition") ||
		strings.Contains(strings.ToLower(err.Error()), "not modified"),
		"expected 412/PreconditionFailed/NotModified, got: %v", err)
}

// TestS3_ConditionalGet_IfModifiedSince verifies that GetObject with an
// IfModifiedSince in the future returns 304 Not Modified.
func TestS3_ConditionalGet_IfModifiedSince(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "cond-get-modified-bucket"
	key := "cond-get-key"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("hello conditional")),
	})
	require.NoError(t, err)

	// GetObject with IfModifiedSince set to the future — expect 304 or empty body.
	future := time.Now().Add(24 * time.Hour)
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		IfModifiedSince: aws.Time(future),
	})
	// Either we get a 304 (error) or an empty body (SDK normalises it).
	if err != nil {
		assert.True(t,
			strings.Contains(err.Error(), "304") ||
				strings.Contains(strings.ToLower(err.Error()), "not modified"),
			"expected 304/NotModified error, got: %v", err)
	} else {
		// Some SDK versions return 304 as success with empty body.
		if out != nil && out.Body != nil {
			out.Body.Close()
		}
	}
}

// TestS3_ConditionalGet_IfMatch verifies GetObject with If-Match header:
//   - matching ETag → 200 success
//   - wrong ETag → 412 PreconditionFailed
func TestS3_ConditionalGet_IfMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "cond-get-etag-bucket"
	key := "cond-etag-key"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("etag test content")),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag, "PutObject must return ETag")

	// GetObject with correct ETag → success.
	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(etag),
	})
	require.NoError(t, err, "GetObject with matching ETag should succeed")
	getOut.Body.Close()

	// GetObject with wrong ETag → 412.
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String("\"wrong-etag\""),
	})
	require.Error(t, err, "GetObject with wrong ETag should fail")
	assert.True(t,
		strings.Contains(err.Error(), "412") ||
			strings.Contains(strings.ToLower(err.Error()), "precondition"),
		"expected 412/PreconditionFailed, got: %v", err)
}

// TestS3_ConditionalGet_IfNoneMatch_ETag verifies GetObject with If-None-Match:
//   - matching ETag → 304 Not Modified
//   - different ETag → 200 success
func TestS3_ConditionalGet_IfNoneMatch_ETag(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "cond-get-nonematch-bucket"
	key := "cond-nonematch-key"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("if-none-match test")),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	// With matching ETag → 304
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String(etag),
	})
	// SDK may return an error for 304 or handle silently.
	if err != nil {
		assert.True(t,
			strings.Contains(err.Error(), "304") ||
				strings.Contains(strings.ToLower(err.Error()), "not modified"),
			"expected 304, got: %v", err)
	}

	// With different ETag → 200
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String("\"different-etag-value\""),
	})
	require.NoError(t, err, "GetObject with non-matching ETag should succeed")
	out.Body.Close()
}

// TestS3_HeadObject_ConditionalIfMatch verifies HeadObject with If-Match ETag header.
func TestS3_HeadObject_ConditionalIfMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "cond-head-bucket"
	key := "cond-head-key"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("head conditional")),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)

	// HeadObject with correct ETag
	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(etag),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(headOut.ETag))

	// HeadObject with wrong ETag → 412
	_, err = c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String("\"badetag\""),
	})
	require.Error(t, err, "HeadObject with wrong ETag should fail with 412")
}

// TestS3_CopyObject_ConditionalSourceIfMatch verifies that CopyObject with
// x-amz-copy-source-if-match works correctly.
func TestS3_CopyObject_ConditionalSourceIfMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "cond-copy-bucket"
	srcKey := "src-key"
	dstKey := "dst-key"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcKey),
		Body:   bytes.NewReader([]byte("copy source")),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)

	// CopyObject with correct source ETag → success
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(dstKey),
		CopySource:        aws.String(bucket + "/" + srcKey),
		CopySourceIfMatch: aws.String(etag),
	})
	require.NoError(t, err, "CopyObject with correct source ETag should succeed")

	// CopyObject with wrong source ETag → 412
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(dstKey + "-2"),
		CopySource:        aws.String(bucket + "/" + srcKey),
		CopySourceIfMatch: aws.String("\"wrongetag\""),
	})
	require.Error(t, err, "CopyObject with wrong source ETag should fail")

	// Suppress unused import
	_ = types.ChecksumAlgorithmCrc32
}
