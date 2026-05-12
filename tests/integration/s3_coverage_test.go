package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Phase 1: S3 Multipart Listing ───────────────────────────────────────────

func TestS3_ListMultipartUploads_AfterCreate_Listed(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-bucket")})
	require.NoError(t, err)

	mu, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-bucket"),
		Key:    aws.String("bigfile.bin"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(mu.UploadId)
	require.NotEmpty(t, uploadID)

	list, err := c.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, list.Uploads, 1)
	assert.Equal(t, "bigfile.bin", aws.ToString(list.Uploads[0].Key))
	assert.Equal(t, uploadID, aws.ToString(list.Uploads[0].UploadId))
}

func TestS3_ListMultipartUploads_AfterComplete_NotListed(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-bucket2")})
	require.NoError(t, err)

	mu, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-bucket2"),
		Key:    aws.String("obj.bin"),
	})
	require.NoError(t, err)

	// Upload one part
	data := bytes.Repeat([]byte("x"), 5*1024*1024) // 5MB min part size
	up, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String("mp-bucket2"),
		Key:        aws.String("obj.bin"),
		UploadId:   mu.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(data),
	})
	require.NoError(t, err)

	_, err = c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-bucket2"),
		Key:      aws.String("obj.bin"),
		UploadId: mu.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: []s3types.CompletedPart{{ETag: up.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	require.NoError(t, err)

	list, err := c.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-bucket2"),
	})
	require.NoError(t, err)
	assert.Empty(t, list.Uploads)
}

func TestS3_ListMultipartUploads_AfterAbort_NotListed(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-abort-bucket")})
	require.NoError(t, err)

	mu, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-abort-bucket"),
		Key:    aws.String("abandoned.bin"),
	})
	require.NoError(t, err)

	_, err = c.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   aws.String("mp-abort-bucket"),
		Key:      aws.String("abandoned.bin"),
		UploadId: mu.UploadId,
	})
	require.NoError(t, err)

	list, err := c.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-abort-bucket"),
	})
	require.NoError(t, err)
	assert.Empty(t, list.Uploads)
}

func TestS3_ListMultipartUploads_PrefixFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-prefix-bucket")})
	require.NoError(t, err)

	for _, key := range []string{"logs/a.bin", "logs/b.bin", "data/c.bin"} {
		_, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
			Bucket: aws.String("mp-prefix-bucket"),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
	}

	list, err := c.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-prefix-bucket"),
		Prefix: aws.String("logs/"),
	})
	require.NoError(t, err)
	assert.Len(t, list.Uploads, 2)
}

// ─── Phase 1: ListParts ───────────────────────────────────────────────────────

func TestS3_ListParts_AfterUploadParts_AllListed(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("parts-bucket")})
	require.NoError(t, err)

	mu, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("parts-bucket"),
		Key:    aws.String("big.bin"),
	})
	require.NoError(t, err)

	// Upload 3 parts (each 5MB)
	chunk := bytes.Repeat([]byte("p"), 5*1024*1024)
	for i := int32(1); i <= 3; i++ {
		_, err = c.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:     aws.String("parts-bucket"),
			Key:        aws.String("big.bin"),
			UploadId:   mu.UploadId,
			PartNumber: aws.Int32(i),
			Body:       bytes.NewReader(chunk),
		})
		require.NoError(t, err)
	}

	parts, err := c.ListParts(ctx, &awss3.ListPartsInput{
		Bucket:   aws.String("parts-bucket"),
		Key:      aws.String("big.bin"),
		UploadId: mu.UploadId,
	})
	require.NoError(t, err)
	assert.Len(t, parts.Parts, 3)
	assert.Equal(t, int32(1), aws.ToInt32(parts.Parts[0].PartNumber))
	assert.Equal(t, int32(2), aws.ToInt32(parts.Parts[1].PartNumber))
	assert.Equal(t, int32(3), aws.ToInt32(parts.Parts[2].PartNumber))
}

func TestS3_ListParts_InvalidUploadId_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("parts-err-bucket")})
	require.NoError(t, err)

	_, err = c.ListParts(ctx, &awss3.ListPartsInput{
		Bucket:   aws.String("parts-err-bucket"),
		Key:      aws.String("nokey"),
		UploadId: aws.String("nonexistent-upload-id"),
	})
	require.Error(t, err)
}

// ─── Phase 1: Bucket Policy ───────────────────────────────────────────────────

func TestS3_BucketPolicy_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("policy-bucket")})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::policy-bucket/*"}]}`

	_, err = c.PutBucketPolicy(ctx, &awss3.PutBucketPolicyInput{
		Bucket: aws.String("policy-bucket"),
		Policy: aws.String(policy),
	})
	require.NoError(t, err)

	getOut, err := c.GetBucketPolicy(ctx, &awss3.GetBucketPolicyInput{
		Bucket: aws.String("policy-bucket"),
	})
	require.NoError(t, err)
	// Verify valid JSON returned
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(getOut.Policy)), &parsed))
	assert.Equal(t, "2012-10-17", parsed["Version"])

	_, err = c.DeleteBucketPolicy(ctx, &awss3.DeleteBucketPolicyInput{
		Bucket: aws.String("policy-bucket"),
	})
	require.NoError(t, err)
}

func TestS3_GetBucketPolicy_NoPolicy_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("nopolicy-bucket")})
	require.NoError(t, err)

	_, err = c.GetBucketPolicy(ctx, &awss3.GetBucketPolicyInput{
		Bucket: aws.String("nopolicy-bucket"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchBucketPolicy")
}

func TestS3_BucketPolicy_NonExistentBucket_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.GetBucketPolicy(ctx, &awss3.GetBucketPolicyInput{
		Bucket: aws.String("ghost-bucket"),
	})
	require.Error(t, err)
}

// ─── Phase 1: Bucket Website ──────────────────────────────────────────────────

func TestS3_BucketWebsite_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("web-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketWebsite(ctx, &awss3.PutBucketWebsiteInput{
		Bucket: aws.String("web-bucket"),
		WebsiteConfiguration: &s3types.WebsiteConfiguration{
			IndexDocument: &s3types.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3types.ErrorDocument{Key: aws.String("error.html")},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetBucketWebsite(ctx, &awss3.GetBucketWebsiteInput{
		Bucket: aws.String("web-bucket"),
	})
	require.NoError(t, err)
	assert.Equal(t, "index.html", aws.ToString(getOut.IndexDocument.Suffix))
	assert.Equal(t, "error.html", aws.ToString(getOut.ErrorDocument.Key))

	_, err = c.DeleteBucketWebsite(ctx, &awss3.DeleteBucketWebsiteInput{
		Bucket: aws.String("web-bucket"),
	})
	require.NoError(t, err)
}

func TestS3_GetBucketWebsite_NoConfig_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("noweb-bucket")})
	require.NoError(t, err)

	_, err = c.GetBucketWebsite(ctx, &awss3.GetBucketWebsiteInput{
		Bucket: aws.String("noweb-bucket"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchWebsiteConfiguration")
}

// ─── Phase 1: Bucket Logging ──────────────────────────────────────────────────

func TestS3_BucketLogging_PutGetDisable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("log-src-bucket")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("log-dest-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketLogging(ctx, &awss3.PutBucketLoggingInput{
		Bucket: aws.String("log-src-bucket"),
		BucketLoggingStatus: &s3types.BucketLoggingStatus{
			LoggingEnabled: &s3types.LoggingEnabled{
				TargetBucket: aws.String("log-dest-bucket"),
				TargetPrefix: aws.String("logs/"),
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetBucketLogging(ctx, &awss3.GetBucketLoggingInput{
		Bucket: aws.String("log-src-bucket"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.LoggingEnabled)
	assert.Equal(t, "log-dest-bucket", aws.ToString(getOut.LoggingEnabled.TargetBucket))
	assert.Equal(t, "logs/", aws.ToString(getOut.LoggingEnabled.TargetPrefix))

	// Disable logging
	_, err = c.PutBucketLogging(ctx, &awss3.PutBucketLoggingInput{
		Bucket:              aws.String("log-src-bucket"),
		BucketLoggingStatus: &s3types.BucketLoggingStatus{},
	})
	require.NoError(t, err)

	getOut2, err := c.GetBucketLogging(ctx, &awss3.GetBucketLoggingInput{
		Bucket: aws.String("log-src-bucket"),
	})
	require.NoError(t, err)
	assert.Nil(t, getOut2.LoggingEnabled)
}

// ─── Phase 1: Bucket Replication ─────────────────────────────────────────────

func TestS3_BucketReplication_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("repl-bucket")})
	require.NoError(t, err)

	// Enable versioning first (required for replication)
	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("repl-bucket"),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = c.PutBucketReplication(ctx, &awss3.PutBucketReplicationInput{
		Bucket: aws.String("repl-bucket"),
		ReplicationConfiguration: &s3types.ReplicationConfiguration{
			Role: aws.String("arn:aws:iam::000000000000:role/repl-role"),
			Rules: []s3types.ReplicationRule{
				{
					ID:     aws.String("rule-1"),
					Status: s3types.ReplicationRuleStatusEnabled,
					Filter: &s3types.ReplicationRuleFilter{Prefix: aws.String("")},
					Destination: &s3types.Destination{
						Bucket: aws.String("arn:aws:s3:::dest-bucket"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetBucketReplication(ctx, &awss3.GetBucketReplicationInput{
		Bucket: aws.String("repl-bucket"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ReplicationConfiguration)
	assert.Len(t, getOut.ReplicationConfiguration.Rules, 1)

	_, err = c.DeleteBucketReplication(ctx, &awss3.DeleteBucketReplicationInput{
		Bucket: aws.String("repl-bucket"),
	})
	require.NoError(t, err)
}

func TestS3_GetBucketReplication_NoConfig_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("norepl-bucket")})
	require.NoError(t, err)

	_, err = c.GetBucketReplication(ctx, &awss3.GetBucketReplicationInput{
		Bucket: aws.String("norepl-bucket"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReplicationConfigurationNotFoundError")
}

// ─── S3 Object Operations ─────────────────────────────────────────────────────

func TestS3_PutGetDelete_RoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("rtt-bucket")})
	require.NoError(t, err)

	body := "hello world"
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String("rtt-bucket"),
		Key:         aws.String("key1"),
		Body:        strings.NewReader(body),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("rtt-bucket"),
		Key:    aws.String("key1"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	data, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	assert.Equal(t, body, string(data))

	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String("rtt-bucket"),
		Key:    aws.String("key1"),
	})
	require.NoError(t, err)

	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("rtt-bucket"),
		Key:    aws.String("key1"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchKey")
}

func TestS3_ListObjectsV2_Prefix(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("list-prefix-bucket")})
	require.NoError(t, err)

	for _, key := range []string{"a/1", "a/2", "b/1", "c/1"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("list-prefix-bucket"),
			Key:    aws.String(key),
			Body:   strings.NewReader("data"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String("list-prefix-bucket"),
		Prefix: aws.String("a/"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(out.KeyCount))
}

func TestS3_CopyObject_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("copy-src")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("copy-dst")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("copy-src"),
		Key:    aws.String("original"),
		Body:   strings.NewReader("original content"),
	})
	require.NoError(t, err)

	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String("copy-dst"),
		Key:        aws.String("copy"),
		CopySource: aws.String("copy-src/original"),
	})
	require.NoError(t, err)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("copy-dst"),
		Key:    aws.String("copy"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	data, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, "original content", string(data))
}

func TestS3_DeleteObjects_Batch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("batch-del-bucket")})
	require.NoError(t, err)

	for _, key := range []string{"k1", "k2", "k3"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("batch-del-bucket"),
			Key:    aws.String(key),
			Body:   strings.NewReader("data"),
		})
		require.NoError(t, err)
	}

	delOut, err := c.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String("batch-del-bucket"),
		Delete: &s3types.Delete{
			Objects: []s3types.ObjectIdentifier{
				{Key: aws.String("k1")},
				{Key: aws.String("k2")},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, delOut.Deleted, 2)
	assert.Empty(t, delOut.Errors)
}

func TestS3_HeadObject_Metadata(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("head-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String("head-bucket"),
		Key:         aws.String("obj"),
		Body:        strings.NewReader("data"),
		ContentType: aws.String("application/octet-stream"),
		Metadata:    map[string]string{"custom": "value"},
	})
	require.NoError(t, err)

	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String("head-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4), aws.ToInt64(headOut.ContentLength))
	assert.Equal(t, "application/octet-stream", aws.ToString(headOut.ContentType))
	assert.Equal(t, "value", headOut.Metadata["custom"])
}

func TestS3_Tagging_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("tag-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged-obj"),
		Body:   strings.NewReader("data"),
	})
	require.NoError(t, err)

	_, err = c.PutObjectTagging(ctx, &awss3.PutObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged-obj"),
		Tagging: &s3types.Tagging{
			TagSet: []s3types.Tag{
				{Key: aws.String("env"), Value: aws.String("test")},
				{Key: aws.String("owner"), Value: aws.String("alice")},
			},
		},
	})
	require.NoError(t, err)

	tagsOut, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged-obj"),
	})
	require.NoError(t, err)
	assert.Len(t, tagsOut.TagSet, 2)

	_, err = c.DeleteObjectTagging(ctx, &awss3.DeleteObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged-obj"),
	})
	require.NoError(t, err)

	tagsOut2, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged-obj"),
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut2.TagSet)
}

func TestS3_GetObject_NonExistent_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("missing-bucket")})
	require.NoError(t, err)

	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("missing-bucket"),
		Key:    aws.String("does-not-exist"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchKey")
}

func TestS3_ListObjectVersions_AfterPutAndDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ver-list-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("ver-list-bucket"),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Put two versions
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("ver-list-bucket"),
		Key:    aws.String("versioned"),
		Body:   strings.NewReader("v1"),
	})
	require.NoError(t, err)
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("ver-list-bucket"),
		Key:    aws.String("versioned"),
		Body:   strings.NewReader("v2"),
	})
	require.NoError(t, err)

	listOut, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String("ver-list-bucket"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.Versions), 2)
}

func TestS3_Reset_ClearsState(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("reset-test-bucket")})
	require.NoError(t, err)

	resetState(t)

	_, err = c.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String("reset-test-bucket")})
	require.Error(t, err)
}
