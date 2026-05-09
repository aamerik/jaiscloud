package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
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

	// Part 1 must be at least 5 MB (AWS minimum for non-final parts); part 2 can be any size.
	const minPart = 5 * 1024 * 1024
	part1Data := bytes.Repeat([]byte("A"), minPart)
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
	// AWS returns empty LocationConstraint for us-east-1 (the default region).
	assert.Equal(t, types.BucketLocationConstraint(""), out.LocationConstraint)
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

func TestS3_RangeRead(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("range-bucket")})
	require.NoError(t, err)

	body := []byte("Hello, World!")
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("range-bucket"),
		Key:    aws.String("obj"),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err)

	// bytes=7-11 → "World"
	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("range-bucket"),
		Key:    aws.String("obj"),
		Range:  aws.String("bytes=7-11"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	assert.Equal(t, []byte("World"), got)

	// bytes=0-4 → "Hello"
	out2, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("range-bucket"),
		Key:    aws.String("obj"),
		Range:  aws.String("bytes=0-4"),
	})
	require.NoError(t, err)
	got2, _ := io.ReadAll(out2.Body)
	assert.Equal(t, []byte("Hello"), got2)
}

func TestS3_ObjectUserMetadata(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("meta-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:   aws.String("meta-bucket"),
		Key:      aws.String("obj"),
		Body:     strings.NewReader("payload"),
		Metadata: map[string]string{"author": "alice", "version": "2"},
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("meta-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	io.ReadAll(out.Body)
	assert.Equal(t, "alice", out.Metadata["author"])
	assert.Equal(t, "2", out.Metadata["version"])
}

// ─── Streaming upload / download ──────────────────────────────────────────────

// TestS3_Streaming_PutGet uploads a 1 MB object and reads it back, verifying
// that the server does not buffer the body (content correctness check).
func TestS3_Streaming_PutGet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("stream-bucket")})
	require.NoError(t, err)

	const size = 1 << 20 // 1 MB
	payload := make([]byte, size)
	_, err = io.ReadFull(rand.Reader, payload)
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String("stream-bucket"),
		Key:           aws.String("large.bin"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("stream-bucket"),
		Key:    aws.String("large.bin"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	assert.Equal(t, int64(size), aws.ToInt64(out.ContentLength))
}

// TestS3_Streaming_RangeRead uploads a 512 KB object and performs byte-range
// reads, exercising the GetStream offset+length path.
func TestS3_Streaming_RangeRead(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("range-stream-bucket")})
	require.NoError(t, err)

	const size = 512 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251) // deterministic, non-zero
	}

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("range-stream-bucket"),
		Key:    aws.String("data.bin"),
		Body:   bytes.NewReader(payload),
	})
	require.NoError(t, err)

	cases := []struct {
		name       string
		rangeHdr   string
		wantSlice  []byte
	}{
		{"first 1KB", "bytes=0-1023", payload[:1024]},
		{"middle 4KB", "bytes=4096-8191", payload[4096:8192]},
		{"last 512B", "bytes=523776-524287", payload[523776:524288]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String("range-stream-bucket"),
				Key:    aws.String("data.bin"),
				Range:  aws.String(tc.rangeHdr),
			})
			require.NoError(t, err)
			defer out.Body.Close()
			got, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSlice, got)
		})
	}
}

// TestS3_Streaming_Multipart uploads a multi-part object using the SDK's
// manual multipart API and verifies the reassembled content.
func TestS3_Streaming_Multipart(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-stream-bucket")})
	require.NoError(t, err)

	createOut, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-stream-bucket"),
		Key:    aws.String("assembled.bin"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)

	// Parts 1 and 2 must be >= 5 MB (AWS minimum for non-final parts); part 3 can be smaller.
	const minPart = 5 * 1024 * 1024
	partSizes := [3]int{minPart, minPart, 256 * 1024}
	var partData [3][]byte
	var completedParts []types.CompletedPart
	for i := 0; i < 3; i++ {
		partData[i] = make([]byte, partSizes[i])
		for j := range partData[i] {
			partData[i][j] = byte(i + 1)
		}
		pn := int32(i + 1)
		p, err := c.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:     aws.String("mp-stream-bucket"),
			Key:        aws.String("assembled.bin"),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(pn),
			Body:       bytes.NewReader(partData[i]),
		})
		require.NoError(t, err)
		completedParts = append(completedParts, types.CompletedPart{
			PartNumber: aws.Int32(pn),
			ETag:       p.ETag,
		})
	}

	_, err = c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-stream-bucket"),
		Key:      aws.String("assembled.bin"),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("mp-stream-bucket"),
		Key:    aws.String("assembled.bin"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	var want []byte
	for _, d := range partData {
		want = append(want, d...)
	}
	assert.Equal(t, want, got)
	assert.Equal(t, int64(partSizes[0]+partSizes[1]+partSizes[2]), aws.ToInt64(out.ContentLength))
}

// TestS3_Streaming_HeadAfterStreamPut verifies metadata (size, ETag) is
// correct after a streaming PutObject.
func TestS3_Streaming_HeadAfterStreamPut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("head-stream-bucket")})
	require.NoError(t, err)

	const size = 128 * 1024
	payload := bytes.Repeat([]byte("x"), size)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("head-stream-bucket"),
		Key:    aws.String("obj.bin"),
		Body:   bytes.NewReader(payload),
	})
	require.NoError(t, err)

	head, err := c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String("head-stream-bucket"),
		Key:    aws.String("obj.bin"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(size), aws.ToInt64(head.ContentLength))
	assert.NotEmpty(t, aws.ToString(head.ETag))
}

func TestS3_BatchDeleteObjects(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("batch-del-bucket")})
	require.NoError(t, err)

	for _, key := range []string{"a", "b", "c"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("batch-del-bucket"),
			Key:    aws.String(key),
			Body:   strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	delOut, err := c.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String("batch-del-bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("a")},
				{Key: aws.String("c")},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, delOut.Deleted, 2)

	listOut, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String("batch-del-bucket")})
	require.NoError(t, err)
	require.Len(t, listOut.Contents, 1)
	assert.Equal(t, "b", aws.ToString(listOut.Contents[0].Key))
}

// TestS3_ListObjectsV2_TruncationCommonPrefixesCountTowardMaxKeys is the core regression test.
// AWS counts both result keys AND unique common prefixes toward MaxKeys. The old code only counted
// result keys, so listings with a delimiter would over-return and report truncated=false incorrectly.
//
// Setup: 5 keys under 3 top-level "directories" (a/*, b/*, c/*), delimiter="/", MaxKeys=2.
// Expected: 2 common prefixes returned (a/, b/), IsTruncated=true (c/ not returned).
func TestS3_ListObjectsV2_TruncationCommonPrefixesCountTowardMaxKeys(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("trunc-cp-bucket")})
	require.NoError(t, err)

	for _, k := range []string{"a/1", "a/2", "b/1", "b/2", "c/1"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("trunc-cp-bucket"),
			Key:    aws.String(k),
			Body:   strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    aws.String("trunc-cp-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(2),
	})
	require.NoError(t, err)

	assert.True(t, aws.ToBool(out.IsTruncated), "expected IsTruncated=true: common prefixes must count toward MaxKeys")
	assert.Len(t, out.Contents, 0, "expected 0 result keys: all items under delimited prefixes")
	require.Len(t, out.CommonPrefixes, 2, "expected exactly 2 common prefixes: a/ and b/")
	assert.Equal(t, "a/", aws.ToString(out.CommonPrefixes[0].Prefix))
	assert.Equal(t, "b/", aws.ToString(out.CommonPrefixes[1].Prefix))
}

// TestS3_ListObjectsV2_TruncationMixedKeysAndPrefixes verifies that bare keys and common
// prefixes together fill the MaxKeys budget, leaving later items unreturned with IsTruncated=true.
//
// Setup: a/1 (yields prefix a/), b (bare key), c/1 (yields prefix c/), d (bare key).
// Sorted: a/1, b, c/1, d → a/ (prefix), b (key), c/ (prefix) = 3 → MaxKeys=3 → truncated, d excluded.
func TestS3_ListObjectsV2_TruncationMixedKeysAndPrefixes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("trunc-mixed-bucket")})
	require.NoError(t, err)

	for _, k := range []string{"a/1", "b", "c/1", "d"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("trunc-mixed-bucket"),
			Key:    aws.String(k),
			Body:   strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    aws.String("trunc-mixed-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(3),
	})
	require.NoError(t, err)

	assert.True(t, aws.ToBool(out.IsTruncated), "expected IsTruncated=true: d is beyond the 3-item boundary")
	// AWS KeyCount counts only keys in Contents; total page size = keys + prefixes.
	total := len(out.Contents) + len(out.CommonPrefixes)
	assert.Equal(t, 3, total, "expected 3 items (keys + common prefixes) filling MaxKeys=3")
	for _, obj := range out.Contents {
		assert.NotEqual(t, "d", aws.ToString(obj.Key), "d must not appear in a truncated page")
	}
}

// TestS3_ListObjectsV2_PaginationWithDelimiter verifies that ContinuationToken-based
// pagination works correctly after a truncated delimiter listing, returning remaining prefixes.
func TestS3_ListObjectsV2_PaginationWithDelimiter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("page-delim-bucket")})
	require.NoError(t, err)

	// 4 top-level "directories", 2 files each
	for _, prefix := range []string{"a", "b", "c", "d"} {
		for _, suffix := range []string{"1", "2"} {
			_, err = c.PutObject(ctx, &awss3.PutObjectInput{
				Bucket: aws.String("page-delim-bucket"),
				Key:    aws.String(prefix + "/" + suffix),
				Body:   strings.NewReader("x"),
			})
			require.NoError(t, err)
		}
	}

	// Paginate with MaxKeys=2 per page; collect all common prefixes.
	var allPrefixes []string
	var token *string
	for {
		out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String("page-delim-bucket"),
			Delimiter:         aws.String("/"),
			MaxKeys:           aws.Int32(2),
			ContinuationToken: token,
		})
		require.NoError(t, err)
		for _, cp := range out.CommonPrefixes {
			allPrefixes = append(allPrefixes, aws.ToString(cp.Prefix))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}

	assert.Equal(t, []string{"a/", "b/", "c/", "d/"}, allPrefixes,
		"all 4 directories must be returned across pages without duplicates or gaps")
}

// TestS3_ListObjectsV2_ExactBoundaryNotTruncated verifies that when the result count
// equals MaxKeys exactly, IsTruncated is false (no more items exist).
func TestS3_ListObjectsV2_ExactBoundaryNotTruncated(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("exact-bound-bucket")})
	require.NoError(t, err)

	for _, k := range []string{"a/1", "b/1"} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("exact-bound-bucket"),
			Key:    aws.String(k),
			Body:   strings.NewReader("x"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    aws.String("exact-bound-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(2),
	})
	require.NoError(t, err)

	assert.False(t, aws.ToBool(out.IsTruncated), "IsTruncated must be false when result count == MaxKeys and no more items exist")
	assert.Len(t, out.CommonPrefixes, 2)
}
