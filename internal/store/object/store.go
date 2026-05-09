// Package objectstore defines the cloud-neutral object-storage metadata interface.
// Concrete implementations live under internal/store/<cloud>/ (e.g. internal/store/aws/s3/).
package objectstore

import (
	"context"
	"time"
)

// ObjectMeta holds metadata for a single stored object.
type ObjectMeta struct {
	Key          string            `json:"key"`
	ETag         string            `json:"etag"`
	// CRC32 is the base64-encoded IEEE CRC32 checksum of the object body.
	CRC32              string            `json:"crc32"`
	// ChecksumAlgorithm is the algorithm specified by the client at upload time
	// (e.g. "CRC32", "CRC32C", "SHA1", "SHA256"). Empty means CRC32 (legacy).
	ChecksumAlgorithm  string            `json:"checksum_algorithm,omitempty"`
	// ChecksumValue is the base64-encoded checksum matching ChecksumAlgorithm.
	ChecksumValue      string            `json:"checksum_value,omitempty"`
	Size            int64             `json:"size"`
	ContentType     string            `json:"content_type"`
	LastModified    time.Time         `json:"last_modified"`
	Metadata        map[string]string `json:"metadata"`
	StorageClass    string            `json:"storage_class"`
	VersionID       string            `json:"version_id"`
	// Tagging
	Tags            map[string]string `json:"tags,omitempty"`
	// Encryption
	Encryption      string            `json:"encryption,omitempty"`
	KMSKeyID        string            `json:"kms_key_id,omitempty"`
	SSECAlgorithm   string            `json:"ssec_algorithm,omitempty"`
	SSECKeyMD5      string            `json:"ssec_key_md5,omitempty"`
	// Versioning
	IsDeleteMarker  bool              `json:"is_delete_marker,omitempty"`
	IsLatest        bool              `json:"is_latest,omitempty"`
	// Object Lock
	LockMode        string            `json:"lock_mode,omitempty"`
	LockRetainUntil *time.Time        `json:"lock_retain_until,omitempty"`
	LegalHoldStatus string            `json:"legal_hold_status,omitempty"`
	// ACL
	ACL             string            `json:"acl,omitempty"`
}

// PartMeta holds metadata for a single multipart upload part.
type PartMeta struct {
	PartNumber int
	ETag       string
	Size       int64
}

// Versioning status constants used by GetBucketVersioning / SetBucketVersioning.
// Concrete implementations must return/accept these values regardless of the
// underlying cloud's native terminology (e.g. GCS generations map to Enabled;
// Azure Blob Versioning maps enabled=true to Enabled).
const (
	VersioningDisabled  = ""          // never enabled (default)
	VersioningEnabled   = "Enabled"   // active versioning
	VersioningSuspended = "Suspended" // paused; new objects get "null" version
)

// ObjectMetaStore manages bucket and object metadata.
// Actual object bytes are stored separately in blobfs.BlobStore.
type ObjectMetaStore interface {
	// Buckets
	CreateBucket(ctx context.Context, bucket string, meta map[string]any) error
	GetBucket(ctx context.Context, bucket string) (map[string]any, error)
	UpdateBucketMeta(ctx context.Context, bucket string, meta map[string]any) error
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]map[string]any, error)

	// Objects
	PutObjectMeta(ctx context.Context, bucket, key string, meta ObjectMeta) error
	GetObjectMeta(ctx context.Context, bucket, key string) (ObjectMeta, error)
	DeleteObjectMeta(ctx context.Context, bucket, key string) error
	// ListObjectMeta returns (objects, commonPrefixes, truncated, nextMarker, error).
	// nextMarker is the last raw key examined on the page and must be used as the
	// continuation-token for the next page. Empty when truncated=false.
	ListObjectMeta(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) ([]ObjectMeta, []string, bool, string, error)

	// Multipart
	InitMultipart(ctx context.Context, bucket, key, uploadID string, meta map[string]any) error
	PutPart(ctx context.Context, uploadID string, partNumber int, part PartMeta) error
	CompleteMultipart(ctx context.Context, bucket, key, uploadID string) ([]PartMeta, error)
	AbortMultipart(ctx context.Context, uploadID string) error
	GetMultipartMeta(ctx context.Context, uploadID string) (bucket, key string, meta map[string]any, err error)

	// Versioning
	GetBucketVersioning(ctx context.Context, bucket string) (string, error) // "" | "Enabled" | "Suspended"
	SetBucketVersioning(ctx context.Context, bucket, status string) error
	PutObjectVersion(ctx context.Context, bucket, key string, meta ObjectMeta) (versionID string, err error)
	// UpdateObjectVersion updates an existing version record in-place without
	// changing version ordering or creating a new version.
	UpdateObjectVersion(ctx context.Context, bucket, key string, meta ObjectMeta) error
	GetObjectVersion(ctx context.Context, bucket, key, versionID string) (ObjectMeta, error)
	DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error
	ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker string, maxKeys int) (versions []ObjectMeta, truncated bool, err error)

	Reset()
}
