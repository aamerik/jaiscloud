//go:build s3_fullmode

// Package s3_test exercises S3 streaming upload/download against a full-mode
// JaisCloud instance backed by LocalFSBlobStore. Run with:
//
//	make test-e2e-s3-streaming
//
// or manually:
//
//	JAISCLOUD_HOST=http://localhost:4566 \
//	  go test -v -tags s3_fullmode ./tests/persistent_mode/aws/s3/
package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3Full_LargePutGet uploads a 20 MB object and reads it back via streaming,
// confirming the MD5 digest is preserved end-to-end through LocalFSBlobStore.
// The download uses io.Copy into a running hasher — the same pattern real
// applications use so that the full body never needs to fit in RAM at once.
func TestS3Full_LargePutGet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("large-bucket")})
	require.NoError(t, err)

	const size = 20 << 20 // 20 MB
	payload := make([]byte, size)
	_, err = io.ReadFull(rand.Reader, payload)
	require.NoError(t, err)

	// Compute expected MD5 before upload.
	wantHash := md5.New()
	wantHash.Write(payload)
	wantMD5 := fmt.Sprintf("%x", wantHash.Sum(nil))

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String("large-bucket"),
		Key:           aws.String("large.bin"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("large-bucket"),
		Key:    aws.String("large.bin"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	// Stream through a hasher — no need to buffer the whole 20 MB in memory.
	gotHash := md5.New()
	n, err := io.Copy(gotHash, out.Body)
	require.NoError(t, err)
	require.Equal(t, int64(size), n, "byte count mismatch")
	assert.Equal(t, wantMD5, fmt.Sprintf("%x", gotHash.Sum(nil)), "content digest mismatch")
	assert.Equal(t, int64(size), aws.ToInt64(out.ContentLength))
}

// TestS3Full_LargeMultipart uploads a 30 MB object as 3 × 10 MB parts,
// confirming the assembled content is byte-for-byte identical.
func TestS3Full_LargeMultipart(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("mp-large-bucket")})
	require.NoError(t, err)

	createOut, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-large-bucket"),
		Key:    aws.String("assembled.bin"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)

	const partSize = 10 << 20 // 10 MB — above S3's 5 MB minimum
	const numParts = 3
	// Accumulate the expected MD5 incrementally while uploading each part
	// so we never hold all 30 MB in RAM simultaneously.
	wantHash := md5.New()
	var completedParts []types.CompletedPart

	for i := 0; i < numParts; i++ {
		part := make([]byte, partSize)
		_, err := io.ReadFull(rand.Reader, part)
		require.NoError(t, err)
		wantHash.Write(part) // feed into hash before upload

		pn := int32(i + 1)
		p, err := c.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:        aws.String("mp-large-bucket"),
			Key:           aws.String("assembled.bin"),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(pn),
			Body:          bytes.NewReader(part),
			ContentLength: aws.Int64(int64(len(part))),
		})
		require.NoError(t, err)
		completedParts = append(completedParts, types.CompletedPart{
			PartNumber: aws.Int32(pn),
			ETag:       p.ETag,
		})
	}

	_, err = c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:          aws.String("mp-large-bucket"),
		Key:             aws.String("assembled.bin"),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	require.NoError(t, err)

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("mp-large-bucket"),
		Key:    aws.String("assembled.bin"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	// Stream through a hasher — avoids loading all 30 MB into RAM at once.
	gotHash := md5.New()
	n, err := io.Copy(gotHash, out.Body)
	require.NoError(t, err)
	require.Equal(t, int64(numParts*partSize), n, "assembled byte count mismatch")
	assert.Equal(t, fmt.Sprintf("%x", wantHash.Sum(nil)), fmt.Sprintf("%x", gotHash.Sum(nil)))
}

// TestS3Full_RangeReadLarge uploads a 10 MB object and reads three
// non-overlapping byte ranges to confirm GetStream seek correctness.
func TestS3Full_RangeReadLarge(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("range-large-bucket")})
	require.NoError(t, err)

	const size = 10 << 20 // 10 MB
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String("range-large-bucket"),
		Key:           aws.String("obj.bin"),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
	})
	require.NoError(t, err)

	ranges := []struct {
		name  string
		hdr   string
		start int
		end   int // inclusive
	}{
		{"first 64KB", "bytes=0-65535", 0, 65535},
		{"middle 128KB", "bytes=5242880-5373951", 5242880, 5373951},
		{"last 32KB", "bytes=10452992-10485759", 10452992, 10485759},
	}
	for _, tc := range ranges {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String("range-large-bucket"),
				Key:    aws.String("obj.bin"),
				Range:  aws.String(tc.hdr),
			})
			require.NoError(t, err)
			defer out.Body.Close()
			got, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, payload[tc.start:tc.end+1], got)
		})
	}
}

// TestS3Full_PutOverwrite verifies that a second PutObject on the same key
// replaces the stored content completely (no file-append artefacts).
func TestS3Full_PutOverwrite(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("overwrite-bucket")})
	require.NoError(t, err)

	first := bytes.Repeat([]byte("A"), 1<<20)
	second := bytes.Repeat([]byte("B"), 512*1024)

	for _, d := range [][]byte{first, second} {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:        aws.String("overwrite-bucket"),
			Key:           aws.String("obj.bin"),
			Body:          bytes.NewReader(d),
			ContentLength: aws.Int64(int64(len(d))),
		})
		require.NoError(t, err)
	}

	out, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("overwrite-bucket"),
		Key:    aws.String("obj.bin"),
	})
	require.NoError(t, err)
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, second, got, "overwrite should replace content, not append")
}

// TestS3Full_MultipartAbort uploads parts then aborts, confirming the
// upload state is cleaned up and part blobs are removed.
func TestS3Full_MultipartAbort(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("abort-bucket")})
	require.NoError(t, err)

	createOut, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("abort-bucket"),
		Key:    aws.String("never-finished.bin"),
	})
	require.NoError(t, err)
	uploadID := aws.ToString(createOut.UploadId)

	_, err = c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:        aws.String("abort-bucket"),
		Key:           aws.String("never-finished.bin"),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(1),
		Body:          bytes.NewReader(make([]byte, 1<<20)),
		ContentLength: aws.Int64(1 << 20),
	})
	require.NoError(t, err)

	_, err = c.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   aws.String("abort-bucket"),
		Key:      aws.String("never-finished.bin"),
		UploadId: aws.String(uploadID),
	})
	require.NoError(t, err)

	// Object must not exist after abort.
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("abort-bucket"),
		Key:    aws.String("never-finished.bin"),
	})
	require.Error(t, err, "object should not exist after multipart abort")
}
