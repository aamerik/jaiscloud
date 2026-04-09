// Package s3 provides the S3 object metadata store interface and implementations.
package s3

import (
	"context"
	"time"
)

// ObjectMeta holds metadata for a single S3 object.
type ObjectMeta struct {
	Key          string
	ETag         string
	Size         int64
	ContentType  string
	LastModified time.Time
	Metadata     map[string]string // x-amz-meta-* headers
	StorageClass string
	VersionID    string
}

// PartMeta holds metadata for a single multipart upload part.
type PartMeta struct {
	PartNumber int
	ETag       string
	Size       int64
}

// S3ObjectMetaStore manages S3 bucket and object metadata.
// Actual object bytes are stored in blobfs.BlobStore.
type S3ObjectMetaStore interface {
	// Buckets
	CreateBucket(ctx context.Context, bucket string, meta map[string]any) error
	GetBucket(ctx context.Context, bucket string) (map[string]any, error)
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]map[string]any, error)

	// Objects
	PutObjectMeta(ctx context.Context, bucket, key string, meta ObjectMeta) error
	GetObjectMeta(ctx context.Context, bucket, key string) (ObjectMeta, error)
	DeleteObjectMeta(ctx context.Context, bucket, key string) error
	ListObjectMeta(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) ([]ObjectMeta, []string, bool, error)

	// Multipart
	InitMultipart(ctx context.Context, bucket, key, uploadID string, meta map[string]any) error
	PutPart(ctx context.Context, uploadID string, partNumber int, part PartMeta) error
	CompleteMultipart(ctx context.Context, bucket, key, uploadID string) ([]PartMeta, error)
	AbortMultipart(ctx context.Context, uploadID string) error
	GetMultipartMeta(ctx context.Context, uploadID string) (bucket, key string, meta map[string]any, err error)

	Reset()
}
