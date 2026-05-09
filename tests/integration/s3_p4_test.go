package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P4.4: S3 Checksum Algorithms ────────────────────────────────────────────

func TestS3_PutObject_ChecksumCRC32_RoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("cksum-crc32")})
	require.NoError(t, err)

	body := []byte("checksum-crc32-content")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("cksum-crc32"),
		Key:               aws.String("obj"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32,
	})
	require.NoError(t, err)

	attrOut, err := c.GetObjectAttributes(ctx, &awss3.GetObjectAttributesInput{
		Bucket:           aws.String("cksum-crc32"),
		Key:              aws.String("obj"),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesChecksum},
	})
	require.NoError(t, err)
	assert.NotNil(t, attrOut.Checksum, "GetObjectAttributes should return Checksum block")
	assert.NotEmpty(t, attrOut.Checksum.ChecksumCRC32, "CRC32 checksum should be present")
}

func TestS3_PutObject_ChecksumSHA256_RoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("cksum-sha256")})
	require.NoError(t, err)

	body := []byte("checksum-sha256-content")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("cksum-sha256"),
		Key:               aws.String("obj"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	attrOut, err := c.GetObjectAttributes(ctx, &awss3.GetObjectAttributesInput{
		Bucket:           aws.String("cksum-sha256"),
		Key:              aws.String("obj"),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesChecksum},
	})
	require.NoError(t, err)
	assert.NotNil(t, attrOut.Checksum)
	assert.NotEmpty(t, attrOut.Checksum.ChecksumSHA256, "SHA256 checksum should be present")
}

func TestS3_PutObject_ChecksumSHA1_RoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("cksum-sha1")})
	require.NoError(t, err)

	body := []byte("checksum-sha1-content")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("cksum-sha1"),
		Key:               aws.String("obj"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha1,
	})
	require.NoError(t, err)

	attrOut, err := c.GetObjectAttributes(ctx, &awss3.GetObjectAttributesInput{
		Bucket:           aws.String("cksum-sha1"),
		Key:              aws.String("obj"),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesChecksum},
	})
	require.NoError(t, err)
	assert.NotNil(t, attrOut.Checksum)
	assert.NotEmpty(t, attrOut.Checksum.ChecksumSHA1, "SHA1 checksum should be present")
}

func TestS3_PutObject_ChecksumCRC32C_RoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("cksum-crc32c")})
	require.NoError(t, err)

	body := []byte("checksum-crc32c-content")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("cksum-crc32c"),
		Key:               aws.String("obj"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32c,
	})
	require.NoError(t, err)

	attrOut, err := c.GetObjectAttributes(ctx, &awss3.GetObjectAttributesInput{
		Bucket:           aws.String("cksum-crc32c"),
		Key:              aws.String("obj"),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesChecksum},
	})
	require.NoError(t, err)
	assert.NotNil(t, attrOut.Checksum)
	assert.NotEmpty(t, attrOut.Checksum.ChecksumCRC32C, "CRC32C checksum should be present")
}

// ─── P4.12: GetObjectAttributes ───────────────────────────────────────────────

func TestS3_GetObjectAttributes_SizeAndETag(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("attrs-bucket")})
	require.NoError(t, err)

	body := strings.Repeat("a", 512)
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("attrs-bucket"),
		Key:    aws.String("large"),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err)

	attrOut, err := c.GetObjectAttributes(ctx, &awss3.GetObjectAttributesInput{
		Bucket: aws.String("attrs-bucket"),
		Key:    aws.String("large"),
		ObjectAttributes: []s3types.ObjectAttributes{
			s3types.ObjectAttributesObjectSize,
			s3types.ObjectAttributesEtag,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(512), aws.ToInt64(attrOut.ObjectSize))
	assert.NotEmpty(t, aws.ToString(attrOut.ETag))
}

func TestS3_GetObjectAttributes_MissingObjectReturnsNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("attrs-404")})
	require.NoError(t, err)

	_, err = c.GetObjectAttributes(ctx, &awss3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-404"),
		Key:              aws.String("nonexistent"),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesObjectSize},
	})
	require.Error(t, err, "GetObjectAttributes on missing object must return error")
}
