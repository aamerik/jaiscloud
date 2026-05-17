package integration_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Object Tagging ──────────────────────────────────────────────────────────

// TestS3_ObjectTagging_PutGet puts two tags on an object and verifies both
// are returned by GetObjectTagging.
func TestS3_ObjectTagging_PutGet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("tag-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged.txt"),
		Body:   strings.NewReader("tag test content"),
	})
	require.NoError(t, err)

	_, err = c.PutObjectTagging(ctx, &awss3.PutObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged.txt"),
		Tagging: &s3types.Tagging{
			TagSet: []s3types.Tag{
				{Key: aws.String("env"), Value: aws.String("test")},
				{Key: aws.String("owner"), Value: aws.String("alice")},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("tagged.txt"),
	})
	require.NoError(t, err)
	assert.Len(t, out.TagSet, 2, "should have 2 tags")

	tagMap := map[string]string{}
	for _, tag := range out.TagSet {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "alice", tagMap["owner"])
}

// TestS3_ObjectTagging_DeleteTags puts tags, deletes them with DeleteObjectTagging,
// and asserts GetObjectTagging returns an empty tag set.
func TestS3_ObjectTagging_DeleteTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("deltag-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("deltag-bucket"),
		Key:    aws.String("obj.txt"),
		Body:   strings.NewReader("content"),
	})
	require.NoError(t, err)

	_, err = c.PutObjectTagging(ctx, &awss3.PutObjectTaggingInput{
		Bucket: aws.String("deltag-bucket"),
		Key:    aws.String("obj.txt"),
		Tagging: &s3types.Tagging{
			TagSet: []s3types.Tag{
				{Key: aws.String("temp"), Value: aws.String("value")},
			},
		},
	})
	require.NoError(t, err)

	_, err = c.DeleteObjectTagging(ctx, &awss3.DeleteObjectTaggingInput{
		Bucket: aws.String("deltag-bucket"),
		Key:    aws.String("obj.txt"),
	})
	require.NoError(t, err)

	out, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("deltag-bucket"),
		Key:    aws.String("obj.txt"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.TagSet, "tag set should be empty after DeleteObjectTagging")
}

// ─── Bucket Versioning ────────────────────────────────────────────────────────

// TestS3_BucketVersioning_Enable enables versioning on a bucket and verifies
// GetBucketVersioning returns Enabled status.
func TestS3_BucketVersioning_Enable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ver-enable")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("ver-enable"),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	out, err := c.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{
		Bucket: aws.String("ver-enable"),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.BucketVersioningStatusEnabled, out.Status,
		"versioning should be Enabled")
}

// TestS3_BucketVersioning_Suspend enables versioning then suspends it, verifying
// GetBucketVersioning returns Suspended.
func TestS3_BucketVersioning_Suspend(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ver-suspend")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("ver-suspend"),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("ver-suspend"),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusSuspended,
		},
	})
	require.NoError(t, err)

	out, err := c.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{
		Bucket: aws.String("ver-suspend"),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.BucketVersioningStatusSuspended, out.Status,
		"versioning should be Suspended")
}

// TestS3_BucketVersioning_MultiVersion enables versioning, puts the same key
// twice, and verifies ListObjectVersions returns 2 version entries.
func TestS3_BucketVersioning_MultiVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "ver-multi"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	for _, content := range []string{"first version", "second version"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("versioned.txt"),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)
	}

	verOut, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	assert.Len(t, verOut.Versions, 2, "should have 2 object versions")
}

// ─── Multipart Upload ─────────────────────────────────────────────────────────

// TestS3_MultipartUpload_CompleteFlow tests the full multipart upload lifecycle:
// create, upload two parts, complete, then get and verify the data.
func TestS3_MultipartUpload_CompleteFlow(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-flow-bucket")})
	require.NoError(t, err)

	createOut, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket:      aws.String("mp-flow-bucket"),
		Key:         aws.String("assembled.bin"),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)
	require.NotEmpty(t, uploadID)

	// Part 1 must be at least 5 MB (AWS minimum for non-final parts).
	const minPart = 5 * 1024 * 1024
	part1Data := bytes.Repeat([]byte("X"), minPart)
	part2Data := []byte("final-part-content")

	p1, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String("mp-flow-bucket"),
		Key:        aws.String("assembled.bin"),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(p1.ETag))

	p2, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String("mp-flow-bucket"),
		Key:        aws.String("assembled.bin"),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(p2.ETag))

	_, err = c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-flow-bucket"),
		Key:      aws.String("assembled.bin"),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: []s3types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("mp-flow-bucket"),
		Key:    aws.String("assembled.bin"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	combined, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	expected := append(part1Data, part2Data...)
	assert.Equal(t, expected, combined, "assembled object should equal concatenated parts")
}

// TestS3_MultipartUpload_Abort creates a multipart upload, uploads one part,
// then aborts; verifies the upload no longer appears in ListMultipartUploads.
func TestS3_MultipartUpload_Abort(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-abort-bucket")})
	require.NoError(t, err)

	createOut, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-abort-bucket"),
		Key:    aws.String("aborted.bin"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)
	require.NotEmpty(t, uploadID)

	// Upload a part (minimum 5 MB for non-final; keep small since we abort immediately)
	_, err = c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String("mp-abort-bucket"),
		Key:        aws.String("aborted.bin"),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(bytes.Repeat([]byte("Y"), 5*1024*1024)),
	})
	require.NoError(t, err)

	_, err = c.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   aws.String("mp-abort-bucket"),
		Key:      aws.String("aborted.bin"),
		UploadId: aws.String(uploadID),
	})
	require.NoError(t, err)

	listOut, err := c.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-abort-bucket"),
	})
	require.NoError(t, err)
	for _, u := range listOut.Uploads {
		assert.NotEqual(t, uploadID, aws.ToString(u.UploadId),
			"aborted upload should not appear in list")
	}
}

// TestS3_ListMultipartUploads creates two multipart uploads without completing
// them and verifies both appear in ListMultipartUploads.
func TestS3_ListMultipartUploads(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-list-bucket")})
	require.NoError(t, err)

	var uploadIDs []string
	for _, key := range []string{"upload-a.bin", "upload-b.bin"} {
		out, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
			Bucket: aws.String("mp-list-bucket"),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		uploadIDs = append(uploadIDs, aws.ToString(out.UploadId))
	}

	listOut, err := c.ListMultipartUploads(ctx, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("mp-list-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Uploads, 2, "should have 2 in-progress multipart uploads")

	foundIDs := map[string]bool{}
	for _, u := range listOut.Uploads {
		foundIDs[aws.ToString(u.UploadId)] = true
	}
	for _, id := range uploadIDs {
		assert.True(t, foundIDs[id], "upload ID %s should be in list", id)
	}
}

// ─── Bucket Policy ────────────────────────────────────────────────────────────

// TestS3_BucketPolicy_PutGet puts a JSON bucket policy and verifies GetBucketPolicy
// returns the same policy string.
func TestS3_BucketPolicy_PutGet(t *testing.T) {
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

	out, err := c.GetBucketPolicy(ctx, &awss3.GetBucketPolicyInput{
		Bucket: aws.String("policy-bucket"),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(out.Policy), "retrieved policy should match the stored policy")
}

// ─── Bucket ACL ───────────────────────────────────────────────────────────────

// TestS3_BucketACL_PutGet sets the bucket ACL to private and verifies
// GetBucketAcl returns an owner.
func TestS3_BucketACL_PutGet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("acl-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketAcl(ctx, &awss3.PutBucketAclInput{
		Bucket: aws.String("acl-bucket"),
		ACL:    s3types.BucketCannedACLPrivate,
	})
	require.NoError(t, err)

	out, err := c.GetBucketAcl(ctx, &awss3.GetBucketAclInput{
		Bucket: aws.String("acl-bucket"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Owner, "GetBucketAcl should return an owner")
	assert.NotEmpty(t, aws.ToString(out.Owner.ID), "owner ID should not be empty")
}
