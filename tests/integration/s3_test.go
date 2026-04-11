package integration_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3_CreateListDeleteBucket(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("my-bucket")})
	require.NoError(t, err)

	out, err := c.ListBuckets(ctx, &awss3.ListBucketsInput{})
	require.NoError(t, err)
	names := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	assert.Contains(t, names, "my-bucket")

	_, err = c.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String("my-bucket")})
	require.NoError(t, err)

	out2, err := c.ListBuckets(ctx, &awss3.ListBucketsInput{})
	require.NoError(t, err)
	for _, b := range out2.Buckets {
		assert.NotEqual(t, "my-bucket", aws.ToString(b.Name))
	}
}

func TestS3_PutGetDeleteObject(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("test-bucket")})
	require.NoError(t, err)

	content := []byte("hello, s3!")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String("test-bucket"),
		Key:         aws.String("hello.txt"),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("hello.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	body, err := io.ReadAll(getOut.Body)
	require.NoError(t, err)
	assert.Equal(t, content, body)

	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("hello.txt"),
	})
	require.NoError(t, err)

	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("hello.txt"),
	})
	require.Error(t, err)
}

func TestS3_HeadObject(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("head-bucket")})
	require.NoError(t, err)

	content := []byte("head test")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("head-bucket"),
		Key:    aws.String("obj.bin"),
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err)

	headOut, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String("head-bucket"),
		Key:    aws.String("obj.bin"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), aws.ToInt64(headOut.ContentLength))
}

func TestS3_ListObjectsV2(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("list-bucket")})
	require.NoError(t, err)

	keys := []string{"a/1.txt", "a/2.txt", "b/3.txt"}
	for _, k := range keys {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("list-bucket"),
			Key:    aws.String(k),
			Body:   strings.NewReader("data"),
		})
		require.NoError(t, err)
	}

	// List all
	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String("list-bucket"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), aws.ToInt32(out.KeyCount))

	// List with prefix
	out2, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String("list-bucket"),
		Prefix: aws.String("a/"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(out2.KeyCount))
}

func TestS3_ListObjectsV2_Delimiter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("delim-bucket")})
	require.NoError(t, err)

	for _, k := range []string{"folder/a.txt", "folder/b.txt", "root.txt"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("delim-bucket"),
			Key:    aws.String(k),
			Body:   strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    aws.String("delim-bucket"),
		Delimiter: aws.String("/"),
	})
	require.NoError(t, err)
	// root.txt is a direct object, folder/ is a common prefix
	assert.Len(t, out.Contents, 1)
	assert.Len(t, out.CommonPrefixes, 1)
	assert.Equal(t, "folder/", aws.ToString(out.CommonPrefixes[0].Prefix))
}

func TestS3_CopyObject(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("copy-bucket")})
	require.NoError(t, err)

	orig := []byte("original content")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("copy-bucket"),
		Key:    aws.String("src.txt"),
		Body:   bytes.NewReader(orig),
	})
	require.NoError(t, err)

	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String("copy-bucket"),
		Key:        aws.String("dst.txt"),
		CopySource: aws.String("copy-bucket/src.txt"),
	})
	require.NoError(t, err)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("copy-bucket"),
		Key:    aws.String("dst.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, orig, body)
}

func TestS3_DeleteObjects(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("del-bucket")})
	require.NoError(t, err)

	for _, k := range []string{"a.txt", "b.txt", "c.txt"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("del-bucket"),
			Key:    aws.String(k),
			Body:   strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	_, err = c.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String("del-bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("a.txt")},
				{Key: aws.String("b.txt")},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String("del-bucket"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(out.KeyCount))
	assert.Equal(t, "c.txt", aws.ToString(out.Contents[0].Key))
}

func TestS3_MultipartUpload(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-bucket")})
	require.NoError(t, err)

	createOut, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-bucket"),
		Key:    aws.String("big.bin"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)
	require.NotEmpty(t, uploadID)

	// Upload two parts (min 5 MB each for real S3, but our emulator has no size limit)
	part1Data := bytes.Repeat([]byte("A"), 1024)
	part2Data := bytes.Repeat([]byte("B"), 1024)

	p1, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String("mp-bucket"),
		Key:        aws.String("big.bin"),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err)

	p2, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String("mp-bucket"),
		Key:        aws.String("big.bin"),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err)

	_, err = c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-bucket"),
		Key:      aws.String("big.bin"),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("mp-bucket"),
		Key:    aws.String("big.bin"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()
	combined, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, append(part1Data, part2Data...), combined)
}

func TestS3_GetBucketLocation(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("loc-bucket")})
	require.NoError(t, err)

	out, err := c.GetBucketLocation(ctx, &awss3.GetBucketLocationInput{
		Bucket: aws.String("loc-bucket"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, string(out.LocationConstraint))
}

func TestS3_DeleteBucketNotEmpty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("notempty")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("notempty"),
		Key:    aws.String("obj.txt"),
		Body:   strings.NewReader("data"),
	})
	require.NoError(t, err)

	_, err = c.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String("notempty")})
	require.Error(t, err, "expected error when deleting non-empty bucket")
}

// ─── S3 Flexible Checksum (PutObject) ─────────────────────────────────────────

func TestS3_PutObject_ChecksumCRC32(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("chk-bucket")})
	require.NoError(t, err)

	body := []byte("checksum-test-body")

	// SDK sends CRC32 checksum; response must echo it back
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("chk-bucket"),
		Key:               aws.String("obj-crc32"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	require.NoError(t, err)

	// Verify the object is retrievable with correct content
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("chk-bucket"),
		Key:    aws.String("obj-crc32"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	assert.Equal(t, body, got)
}

func TestS3_PutObject_ChecksumCRC32C(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("chk-bucket-c")})
	require.NoError(t, err)

	body := []byte("crc32c-test-body")

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("chk-bucket-c"),
		Key:               aws.String("obj-crc32c"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("chk-bucket-c"),
		Key:    aws.String("obj-crc32c"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	assert.Equal(t, body, got)
}

func TestS3_PutObject_ChecksumSHA256(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("chk-bucket-sha")})
	require.NoError(t, err)

	body := []byte("sha256-test-body")

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String("chk-bucket-sha"),
		Key:               aws.String("obj-sha256"),
		Body:              bytes.NewReader(body),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("chk-bucket-sha"),
		Key:    aws.String("obj-sha256"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	assert.Equal(t, body, got)
}

func TestS3_PutObject_NoChecksum_FallbackCRC32(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("chk-bucket-none")})
	require.NoError(t, err)

	body := []byte("no-checksum-body")

	// No ChecksumAlgorithm → server computes CRC32 fallback, no SDK error
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("chk-bucket-none"),
		Key:    aws.String("obj-no-chk"),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("chk-bucket-none"),
		Key:    aws.String("obj-no-chk"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	assert.Equal(t, body, got)
}
