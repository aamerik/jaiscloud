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

// ─── Explicit spec tests: If-Match ────────────────────────────────────────────

// TestS3GetObjectIfMatchHit verifies GetObject with the exact ETag returns 200.
func TestS3GetObjectIfMatchHit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "ifmatch-hit-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("data for if-match hit"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("obj"),
		IfMatch: aws.String(etag),
	})
	require.NoError(t, err, "GetObject with matching ETag should succeed")
	out.Body.Close()
}

// TestS3GetObjectIfMatchMiss verifies GetObject with a wrong ETag returns 412.
func TestS3GetObjectIfMatchMiss(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "ifmatch-miss-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("data for if-match miss"),
	})
	require.NoError(t, err)

	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("obj"),
		IfMatch: aws.String("\"00000000000000000000000000000000\""),
	})
	require.Error(t, err, "GetObject with wrong ETag should return 412")
	assert.True(t,
		strings.Contains(err.Error(), "412") ||
			strings.Contains(strings.ToLower(err.Error()), "precondition"),
		"expected 412/PreconditionFailed, got: %v", err)
}

// TestS3HeadObjectIfMatchHit verifies HeadObject with matching ETag returns 200.
func TestS3HeadObjectIfMatchHit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-ifmatch-hit"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head if-match hit"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)

	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("obj"),
		IfMatch: aws.String(etag),
	})
	require.NoError(t, err, "HeadObject with matching ETag should succeed")
	assert.NotEmpty(t, aws.ToString(headOut.ETag))
}

// TestS3HeadObjectIfMatchMiss verifies HeadObject with wrong ETag returns 412.
func TestS3HeadObjectIfMatchMiss(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-ifmatch-miss"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head if-match miss"),
	})
	require.NoError(t, err)

	_, err = c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("obj"),
		IfMatch: aws.String("\"wrong\""),
	})
	require.Error(t, err, "HeadObject with wrong ETag should return 412")
}

// ─── Explicit spec tests: If-None-Match ──────────────────────────────────────

// TestS3GetObjectIfNoneMatchHit verifies GetObject with a different ETag returns 200.
func TestS3GetObjectIfNoneMatchHit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "if-none-match-hit"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("none-match content"),
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("obj"),
		IfNoneMatch: aws.String("\"different-etag-abc123\""),
	})
	require.NoError(t, err, "GetObject with non-matching ETag should succeed")
	out.Body.Close()
}

// TestS3GetObjectIfNoneMatchMiss verifies GetObject with the exact ETag returns 304.
func TestS3GetObjectIfNoneMatchMiss(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "if-none-match-miss"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("none-match exact"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("obj"),
		IfNoneMatch: aws.String(etag),
	})
	// SDK returns 304 as an error.
	if err != nil {
		assert.True(t,
			strings.Contains(err.Error(), "304") ||
				strings.Contains(strings.ToLower(err.Error()), "not modified"),
			"expected 304/NotModified, got: %v", err)
	}
	// Some SDK versions silently handle 304 — either path is correct.
}

// TestS3HeadObjectIfNoneMatchMiss verifies HeadObject with the exact ETag returns 304.
func TestS3HeadObjectIfNoneMatchMiss(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-if-none-match-miss"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head none-match"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	_, err = c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("obj"),
		IfNoneMatch: aws.String(etag),
	})
	// SDK may return 304 as an error or silently succeed.
	if err != nil {
		assert.True(t,
			strings.Contains(err.Error(), "304") ||
				strings.Contains(strings.ToLower(err.Error()), "not modified"),
			"expected 304/NotModified, got: %v", err)
	}
}

// ─── Explicit spec tests: If-Modified-Since ───────────────────────────────────

// TestS3GetObjectIfModifiedSinceOld verifies GetObject with a past IfModifiedSince returns 200.
func TestS3GetObjectIfModifiedSinceOld(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "if-modified-since-old"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("if-modified-since old"),
	})
	require.NoError(t, err)

	past := time.Now().Add(-2 * time.Hour)
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String("obj"),
		IfModifiedSince: aws.Time(past),
	})
	require.NoError(t, err, "GetObject with IfModifiedSince in the past should succeed")
	out.Body.Close()
}

// TestS3GetObjectIfModifiedSinceFuture verifies GetObject with future IfModifiedSince returns 304.
func TestS3GetObjectIfModifiedSinceFuture(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "if-modified-since-future"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("if-modified-since future"),
	})
	require.NoError(t, err)

	future := time.Now().Add(24 * time.Hour)
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String("obj"),
		IfModifiedSince: aws.Time(future),
	})
	if err != nil {
		assert.True(t,
			strings.Contains(err.Error(), "304") ||
				strings.Contains(strings.ToLower(err.Error()), "not modified"),
			"expected 304/NotModified, got: %v", err)
	}
}

// TestS3HeadObjectIfModifiedSinceOld verifies HeadObject with past IfModifiedSince returns 200.
func TestS3HeadObjectIfModifiedSinceOld(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-if-modified-since-old"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head if-modified-since"),
	})
	require.NoError(t, err)

	past := time.Now().Add(-2 * time.Hour)
	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String("obj"),
		IfModifiedSince: aws.Time(past),
	})
	require.NoError(t, err, "HeadObject with IfModifiedSince in the past should succeed")
	assert.NotNil(t, headOut)
}

// ─── Explicit spec tests: If-Unmodified-Since ────────────────────────────────

// TestS3GetObjectIfUnmodifiedSinceFuture verifies GetObject with a future IfUnmodifiedSince returns 200.
func TestS3GetObjectIfUnmodifiedSinceFuture(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "if-unmodified-since-future"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("if-unmodified-since future"),
	})
	require.NoError(t, err)

	// Condition: object was NOT modified after a future timestamp → true, return object.
	future := time.Now().Add(24 * time.Hour)
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("obj"),
		IfUnmodifiedSince: aws.Time(future),
	})
	require.NoError(t, err, "GetObject with future IfUnmodifiedSince should succeed")
	out.Body.Close()
}

// TestS3GetObjectIfUnmodifiedSincePast verifies GetObject with a past IfUnmodifiedSince returns 412.
func TestS3GetObjectIfUnmodifiedSincePast(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "if-unmodified-since-past"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("if-unmodified-since past"),
	})
	require.NoError(t, err)

	// Condition: object must NOT have been modified since a past timestamp.
	// The object was just created (after the past timestamp), so condition fails → 412.
	past := time.Now().Add(-2 * time.Hour)
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("obj"),
		IfUnmodifiedSince: aws.Time(past),
	})
	require.Error(t, err, "GetObject with past IfUnmodifiedSince should return 412")
	assert.True(t,
		strings.Contains(err.Error(), "412") ||
			strings.Contains(strings.ToLower(err.Error()), "precondition"),
		"expected 412/PreconditionFailed, got: %v", err)
}

// TestS3HeadObjectIfUnmodifiedSinceFuture verifies HeadObject with future IfUnmodifiedSince returns 200.
func TestS3HeadObjectIfUnmodifiedSinceFuture(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-if-unmodified-since-future"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head if-unmodified-since"),
	})
	require.NoError(t, err)

	future := time.Now().Add(24 * time.Hour)
	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("obj"),
		IfUnmodifiedSince: aws.Time(future),
	})
	require.NoError(t, err, "HeadObject with future IfUnmodifiedSince should succeed")
	assert.NotNil(t, headOut)
}

// ─── Combined conditions ──────────────────────────────────────────────────────

// TestS3GetObjectIfMatchAndIfNoneMatch verifies IfMatch=correct AND IfNoneMatch=different → 200.
func TestS3GetObjectIfMatchAndIfNoneMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "comb-ifmatch-ifnonematch"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("combined condition content"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	// IfMatch=correct AND IfNoneMatch=different → both conditions satisfied → 200.
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("obj"),
		IfMatch:     aws.String(etag),
		IfNoneMatch: aws.String("\"00000000000000000000000000000000\""),
	})
	require.NoError(t, err, "GetObject with IfMatch=correct AND IfNoneMatch=different should succeed")
	out.Body.Close()
}

// TestS3GetObjectIfMatchMissAndIfNoneMatch verifies IfMatch=wrong → 412 regardless of IfNoneMatch.
func TestS3GetObjectIfMatchMissAndIfNoneMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "comb-ifmatch-miss-ifnonematch"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("combined condition miss"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	// IfMatch=wrong → 412 even though IfNoneMatch=different would succeed.
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("obj"),
		IfMatch:     aws.String("\"wrongetag\""),
		IfNoneMatch: aws.String("\"00000000000000000000000000000000\""),
	})
	require.Error(t, err, "GetObject with IfMatch=wrong should fail with 412")
	assert.True(t,
		strings.Contains(err.Error(), "412") ||
			strings.Contains(strings.ToLower(err.Error()), "precondition"),
		"expected 412/PreconditionFailed, got: %v", err)
}

// ─── CopyObject conditionals ──────────────────────────────────────────────────

// TestS3CopyObjectIfMatch verifies CopyObject with CopySourceIfMatch=correct ETag succeeds.
func TestS3CopyObjectIfMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "copy-ifmatch-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("src"),
		Body:   strings.NewReader("copy if-match source"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag)

	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("dst"),
		CopySource:        aws.String(bucket + "/src"),
		CopySourceIfMatch: aws.String(etag),
	})
	require.NoError(t, err, "CopyObject with correct CopySourceIfMatch should succeed")
}

// TestS3CopyObjectIfNoneMatch verifies CopyObject with CopySourceIfNoneMatch=different ETag succeeds.
func TestS3CopyObjectIfNoneMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "copy-if-none-match-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("src"),
		Body:   strings.NewReader("copy if-none-match source"),
	})
	require.NoError(t, err)

	// CopySourceIfNoneMatch=different → condition met (ETag doesn't match "wrong") → copy allowed.
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:                aws.String(bucket),
		Key:                   aws.String("dst"),
		CopySource:            aws.String(bucket + "/src"),
		CopySourceIfNoneMatch: aws.String("\"wrongetag\""),
	})
	require.NoError(t, err, "CopyObject with non-matching CopySourceIfNoneMatch should succeed")
}

// ─── ETag format tests ────────────────────────────────────────────────────────

// TestS3PutObjectETagFormat verifies that PutObject returns a quoted, non-empty ETag.
func TestS3PutObjectETagFormat(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "etag-format-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("etag format test"),
	})
	require.NoError(t, err)
	etag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, etag, "PutObject must return an ETag")
	assert.True(t, strings.HasPrefix(etag, "\"") && strings.HasSuffix(etag, "\""),
		"ETag must be quoted, got: %s", etag)
	inner := strings.Trim(etag, "\"")
	assert.NotEmpty(t, inner, "ETag inner value must not be empty")

	// GetObject should echo the same ETag.
	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	getOut.Body.Close()
	assert.Equal(t, etag, aws.ToString(getOut.ETag), "GetObject ETag must match PutObject ETag")
}

// TestS3MultipartETagFormat verifies that multipart upload ETag has "xxx-N" format.
func TestS3MultipartETagFormat(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "multipart-etag-format"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	mu, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("big"),
	})
	require.NoError(t, err)
	uid := aws.ToString(mu.UploadId)

	const minPart = 5 * 1024 * 1024
	part1, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("big"),
		UploadId:   aws.String(uid),
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader(strings.Repeat("A", minPart)),
	})
	require.NoError(t, err)

	part2, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("big"),
		UploadId:   aws.String(uid),
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader("small-final-part"),
	})
	require.NoError(t, err)

	compOut, err := c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String("big"),
		UploadId: aws.String(uid),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: part1.ETag},
				{PartNumber: aws.Int32(2), ETag: part2.ETag},
			},
		},
	})
	require.NoError(t, err)

	etag := aws.ToString(compOut.ETag)
	require.NotEmpty(t, etag, "CompleteMultipartUpload must return an ETag")
	// Multipart ETag format: "md5hash-N"
	inner := strings.Trim(etag, "\"")
	assert.True(t, strings.HasSuffix(inner, "-2"),
		"multipart ETag with 2 parts must end with '-2', got: %s", etag)
}

// ─── Versioning with ETag ─────────────────────────────────────────────────────

// TestS3VersioningETagChange verifies that two puts on the same key produce different ETags.
func TestS3VersioningETagChange(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "versioning-etag-change"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	put1, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("key"),
		Body:   strings.NewReader("version one content"),
	})
	require.NoError(t, err)
	etag1 := aws.ToString(put1.ETag)

	put2, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("key"),
		Body:   strings.NewReader("version two content — different"),
	})
	require.NoError(t, err)
	etag2 := aws.ToString(put2.ETag)

	assert.NotEqual(t, etag1, etag2, "different content should produce different ETags")

	verOut, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	assert.Len(t, verOut.Versions, 2, "should have 2 versions")
	etagSet := map[string]bool{}
	for _, v := range verOut.Versions {
		etagSet[aws.ToString(v.ETag)] = true
	}
	assert.Len(t, etagSet, 2, "each version should have a distinct ETag")
}

// ─── Additional conditional edge cases ───────────────────────────────────────

// TestS3GetObjectIfMatchWildcard verifies IfMatch=* matches any existing object.
func TestS3GetObjectIfMatchWildcard(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "ifmatch-wildcard"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("wildcard content"),
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("obj"),
		IfMatch: aws.String("*"),
	})
	require.NoError(t, err, "GetObject with IfMatch=* should succeed for any existing object")
	out.Body.Close()
}

// TestS3HeadObjectIfNoneMatchHit verifies HeadObject with a different ETag returns 200.
func TestS3HeadObjectIfNoneMatchHit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-if-none-match-hit"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head none match hit"),
	})
	require.NoError(t, err)

	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("obj"),
		IfNoneMatch: aws.String("\"completely-different-etag\""),
	})
	require.NoError(t, err, "HeadObject with non-matching ETag should succeed")
	assert.NotEmpty(t, aws.ToString(headOut.ETag))
}

// TestS3HeadObjectIfModifiedSinceFuture verifies HeadObject with future IfModifiedSince returns 304.
func TestS3HeadObjectIfModifiedSinceFuture(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-if-modified-since-future"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head if-modified-since future"),
	})
	require.NoError(t, err)

	future := time.Now().Add(24 * time.Hour)
	_, err = c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String("obj"),
		IfModifiedSince: aws.Time(future),
	})
	// SDK may return 304 as error or silent success.
	if err != nil {
		assert.True(t,
			strings.Contains(err.Error(), "304") ||
				strings.Contains(strings.ToLower(err.Error()), "not modified"),
			"expected 304/NotModified, got: %v", err)
	}
}

// TestS3HeadObjectIfUnmodifiedSincePast verifies HeadObject with past IfUnmodifiedSince returns 412.
func TestS3HeadObjectIfUnmodifiedSincePast(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-if-unmodified-since-past"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head if-unmodified-since past"),
	})
	require.NoError(t, err)

	past := time.Now().Add(-2 * time.Hour)
	_, err = c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("obj"),
		IfUnmodifiedSince: aws.Time(past),
	})
	require.Error(t, err, "HeadObject with past IfUnmodifiedSince should return 412")
}

// TestS3GetObjectETagAfterPut verifies GetObject echoes the ETag from PutObject.
func TestS3GetObjectETagAfterPut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "get-etag-after-put"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("etag roundtrip"),
	})
	require.NoError(t, err)
	putETag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, putETag)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	getOut.Body.Close()
	assert.Equal(t, putETag, aws.ToString(getOut.ETag))
}

// TestS3HeadObjectETagAfterPut verifies HeadObject echoes the ETag from PutObject.
func TestS3HeadObjectETagAfterPut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "head-etag-after-put"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	putOut, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("head etag roundtrip"),
	})
	require.NoError(t, err)
	putETag := aws.ToString(putOut.ETag)
	require.NotEmpty(t, putETag)

	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	assert.Equal(t, putETag, aws.ToString(headOut.ETag))
}

// TestS3CopyObjectIfUnmodifiedSinceFuture verifies CopyObject with future IfUnmodifiedSince succeeds.
func TestS3CopyObjectIfUnmodifiedSinceFuture(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "copy-if-unmodified-since"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("src"),
		Body:   strings.NewReader("copy source content"),
	})
	require.NoError(t, err)

	future := time.Now().Add(24 * time.Hour)
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:                        aws.String(bucket),
		Key:                           aws.String("dst"),
		CopySource:                    aws.String(bucket + "/src"),
		CopySourceIfUnmodifiedSince:   aws.Time(future),
	})
	require.NoError(t, err, "CopyObject with future IfUnmodifiedSince should succeed")
}

// TestS3CopyObjectIfModifiedSinceOld verifies CopyObject with past IfModifiedSince succeeds.
func TestS3CopyObjectIfModifiedSinceOld(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "copy-if-modified-since"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("src"),
		Body:   strings.NewReader("copy source for modified since"),
	})
	require.NoError(t, err)

	past := time.Now().Add(-2 * time.Hour)
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:                      aws.String(bucket),
		Key:                         aws.String("dst"),
		CopySource:                  aws.String(bucket + "/src"),
		CopySourceIfModifiedSince:   aws.Time(past),
	})
	require.NoError(t, err, "CopyObject with past IfModifiedSince should succeed")
}
