package s3_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	objectstore "jaiscloud/internal/aws/store/object"
	s3store "jaiscloud/internal/aws/store/s3"
)

func roundTripMemoryS3(t *testing.T, s *s3store.MemoryS3ObjectMetaStore) *s3store.MemoryS3ObjectMetaStore {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s2 := s3store.NewMemoryS3ObjectMetaStore()
	if err := s2.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return s2
}

// ─── IsEmpty ──────────────────────────────────────────────────────────────────

func TestMemoryS3ObjectMetaStore_IsEmpty_NewStore(t *testing.T) {
	s := s3store.NewMemoryS3ObjectMetaStore()
	empty, err := s.IsEmpty(context.Background())
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("new store must be empty")
	}
}

func TestMemoryS3ObjectMetaStore_IsEmpty_WithBucket(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()
	if err := s.CreateBucket(ctx, "my-bucket", map[string]any{"owner": "test"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	empty, err := s.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if empty {
		t.Fatal("store with a bucket must not be empty")
	}
}

// ─── Snapshot round-trips ─────────────────────────────────────────────────────

func TestMemoryS3ObjectMetaStore_Snapshot_Empty(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()
	s2 := roundTripMemoryS3(t, s)
	empty, err := s2.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("restored empty store must still be empty")
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_BucketMetaSurvives(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()

	meta := map[string]any{"region": "us-east-1", "owner": "alice"}
	if err := s.CreateBucket(ctx, "test-bucket", meta); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	s2 := roundTripMemoryS3(t, s)

	got, err := s2.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket after restore: %v", err)
	}
	if got["owner"] != "alice" {
		t.Fatalf("bucket meta 'owner' not restored: %v", got)
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_ObjectMetaSurvives(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()

	s.CreateBucket(ctx, "snap-bucket", nil)

	obj := objectstore.ObjectMeta{
		Key:          "data/file.txt",
		ETag:         "abc123",
		Size:         1024,
		ContentType:  "text/plain",
		LastModified: time.Now().Truncate(time.Second),
		Metadata:     map[string]string{"x-custom": "value"},
		Tags:         map[string]string{"env": "test", "owner": "alice"},
		StorageClass: "STANDARD",
	}
	if err := s.PutObjectMeta(ctx, "snap-bucket", "data/file.txt", obj); err != nil {
		t.Fatalf("PutObjectMeta: %v", err)
	}

	s2 := roundTripMemoryS3(t, s)

	got, err := s2.GetObjectMeta(ctx, "snap-bucket", "data/file.txt")
	if err != nil {
		t.Fatalf("GetObjectMeta after restore: %v", err)
	}
	if got.ETag != "abc123" {
		t.Fatalf("ETag mismatch: got %q, want %q", got.ETag, "abc123")
	}
	if got.Size != 1024 {
		t.Fatalf("Size mismatch: got %d, want 1024", got.Size)
	}
	if got.ContentType != "text/plain" {
		t.Fatalf("ContentType mismatch: got %q", got.ContentType)
	}
	if got.Metadata["x-custom"] != "value" {
		t.Fatalf("user metadata not restored: %v", got.Metadata)
	}
	if got.Tags["env"] != "test" || got.Tags["owner"] != "alice" {
		t.Fatalf("tags not restored: %v", got.Tags)
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_MultipleObjectsSurvive(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()
	s.CreateBucket(ctx, "multi-bucket", nil)

	keys := []string{"a.txt", "b.txt", "c.txt"}
	for i, k := range keys {
		s.PutObjectMeta(ctx, "multi-bucket", k, objectstore.ObjectMeta{
			Key: k, ETag: string(rune('a' + i)), Size: int64(i + 1), LastModified: time.Now(),
		})
	}

	s2 := roundTripMemoryS3(t, s)

	objs, _, _, _, err := s2.ListObjectMeta(ctx, "multi-bucket", "", "", "", 100)
	if err != nil {
		t.Fatalf("ListObjectMeta after restore: %v", err)
	}
	if len(objs) != 3 {
		t.Fatalf("expected 3 objects after restore, got %d", len(objs))
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_VersioningStatusSurvives(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()
	s.CreateBucket(ctx, "versioned-bucket", nil)

	if err := s.SetBucketVersioning(ctx, "versioned-bucket", "Enabled"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	s2 := roundTripMemoryS3(t, s)

	status, err := s2.GetBucketVersioning(ctx, "versioned-bucket")
	if err != nil {
		t.Fatalf("GetBucketVersioning after restore: %v", err)
	}
	if status != "Enabled" {
		t.Fatalf("versioning status not restored: got %q, want %q", status, "Enabled")
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_ObjectVersionsSurvive(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()
	s.CreateBucket(ctx, "ver-bucket", nil)
	s.SetBucketVersioning(ctx, "ver-bucket", "Enabled")

	// Put two versions of the same key.
	v1, err := s.PutObjectVersion(ctx, "ver-bucket", "file.txt", objectstore.ObjectMeta{
		Key: "file.txt", ETag: "etag-v1", Size: 100, LastModified: time.Now(),
	})
	if err != nil {
		t.Fatalf("PutObjectVersion v1: %v", err)
	}
	_, err = s.PutObjectVersion(ctx, "ver-bucket", "file.txt", objectstore.ObjectMeta{
		Key: "file.txt", ETag: "etag-v2", Size: 200, LastModified: time.Now(),
	})
	if err != nil {
		t.Fatalf("PutObjectVersion v2: %v", err)
	}

	s2 := roundTripMemoryS3(t, s)

	// First version must still be retrievable.
	got, err := s2.GetObjectVersion(ctx, "ver-bucket", "file.txt", v1)
	if err != nil {
		t.Fatalf("GetObjectVersion v1 after restore: %v", err)
	}
	if got.ETag != "etag-v1" {
		t.Fatalf("v1 ETag not restored: got %q", got.ETag)
	}

	// Both versions must appear in ListObjectVersions.
	versions, _, err := s2.ListObjectVersions(ctx, "ver-bucket", "", "", "", 100)
	if err != nil {
		t.Fatalf("ListObjectVersions after restore: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions after restore, got %d", len(versions))
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_MultipartUploadSurvives(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()
	s.CreateBucket(ctx, "mp-bucket", nil)

	const uploadID = "upload-abc-123"
	if err := s.InitMultipart(ctx, "mp-bucket", "large-file.bin", uploadID,
		map[string]any{"content-type": "application/octet-stream"}); err != nil {
		t.Fatalf("InitMultipart: %v", err)
	}

	// Add two parts — note int part numbers (must survive JSON int→string→int conversion).
	for _, partNum := range []int{1, 2} {
		if err := s.PutPart(ctx, uploadID, partNum, objectstore.PartMeta{
			PartNumber: partNum,
			ETag:       "etag-part-" + string(rune('0'+partNum)),
			Size:       512 * 1024,
		}); err != nil {
			t.Fatalf("PutPart %d: %v", partNum, err)
		}
	}

	s2 := roundTripMemoryS3(t, s)

	// CompleteMultipart must succeed and return both parts.
	parts, err := s2.CompleteMultipart(ctx, "mp-bucket", "large-file.bin", uploadID)
	if err != nil {
		t.Fatalf("CompleteMultipart after restore: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts after restore, got %d", len(parts))
	}
	for _, p := range parts {
		if p.Size != 512*1024 {
			t.Fatalf("part size not restored correctly: %d", p.Size)
		}
	}
}

func TestMemoryS3ObjectMetaStore_Snapshot_MultipleBucketsSurvive(t *testing.T) {
	ctx := context.Background()
	s := s3store.NewMemoryS3ObjectMetaStore()

	const accountID = "000000000000"
	for _, bucket := range []string{"bucket-x", "bucket-y", "bucket-z"} {
		s.CreateBucket(ctx, bucket, map[string]any{"name": bucket, "AccountID": accountID})
		s.PutObjectMeta(ctx, bucket, "obj.txt", objectstore.ObjectMeta{
			Key: "obj.txt", ETag: bucket + "-etag", Size: 1, LastModified: time.Now(),
		})
	}

	s2 := roundTripMemoryS3(t, s)

	buckets, err := s2.ListBuckets(ctx, accountID)
	if err != nil {
		t.Fatalf("ListBuckets after restore: %v", err)
	}
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(buckets))
	}
	for _, bucket := range []string{"bucket-x", "bucket-y", "bucket-z"} {
		obj, err := s2.GetObjectMeta(ctx, bucket, "obj.txt")
		if err != nil {
			t.Fatalf("GetObjectMeta %s after restore: %v", bucket, err)
		}
		if obj.ETag != bucket+"-etag" {
			t.Fatalf("%s: ETag not restored: got %q", bucket, obj.ETag)
		}
	}
}
