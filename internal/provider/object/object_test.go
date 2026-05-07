package object

import (
	"context"
	"errors"
	"io"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/model"
	s3store "jaiscloud/internal/store/aws/s3"
)

// ─── stubs ────────────────────────────────────────────────────────────────────

// stubMeta wraps MemoryS3ObjectMetaStore and lets tests inject controlled errors.
type stubMeta struct {
	*s3store.MemoryS3ObjectMetaStore
	deleteMetaErr    error
	deleteMetaCalled bool
	getMetaCallCount int
	// getMetaErrAfter: first N calls succeed; N+1 onward return an error.
	// Set to -1 to never fail.
	getMetaErrAfter int
}

func newStubMeta() *stubMeta {
	return &stubMeta{
		MemoryS3ObjectMetaStore: s3store.NewMemoryS3ObjectMetaStore(),
		getMetaErrAfter:         -1,
	}
}

func (s *stubMeta) DeleteObjectMeta(ctx context.Context, bucket, key string) error {
	s.deleteMetaCalled = true
	if s.deleteMetaErr != nil {
		return s.deleteMetaErr
	}
	return s.MemoryS3ObjectMetaStore.DeleteObjectMeta(ctx, bucket, key)
}

func (s *stubMeta) GetObjectMeta(ctx context.Context, bucket, key string) (s3store.ObjectMeta, error) {
	s.getMetaCallCount++
	if s.getMetaErrAfter >= 0 && s.getMetaCallCount > s.getMetaErrAfter {
		return s3store.ObjectMeta{}, errors.New("not found")
	}
	return s.MemoryS3ObjectMetaStore.GetObjectMeta(ctx, bucket, key)
}

// stubBlob wraps MemoryBlobStore and lets tests inject failures on specific ops.
type stubBlob struct {
	*blobfs.MemoryBlobStore
	deleteCalled bool
	deleteErr    error
	getStreamErr error
}

func newStubBlob() *stubBlob {
	return &stubBlob{MemoryBlobStore: blobfs.NewMemoryBlobStore()}
}

func (s *stubBlob) Delete(ctx context.Context, bucket, key string) error {
	s.deleteCalled = true
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.MemoryBlobStore.Delete(ctx, bucket, key)
}

func (s *stubBlob) GetStream(ctx context.Context, bucket, key string, offset, length int64) (io.ReadCloser, error) {
	if s.getStreamErr != nil {
		return nil, s.getStreamErr
	}
	return s.MemoryBlobStore.GetStream(ctx, bucket, key, offset, length)
}

func makeNR(bucket, key string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Params: map[string]any{"_bucket": bucket, "_key": key},
	}
}

// ─── DeleteObject ─────────────────────────────────────────────────────────────

func TestDeleteObject_MetadataFailsReturns204BlobUntouched(t *testing.T) {
	// When metadata delete fails, blob.Delete must not be called (would create
	// reverse torn state: metadata present + blob absent).
	meta := newStubMeta()
	meta.deleteMetaErr = errors.New("store unavailable")
	blob := newStubBlob()
	p := New(meta, blob)

	resp, err := p.DeleteObject(context.Background(), makeNR("b", "k"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.HTTPStatus != 204 {
		t.Fatalf("status=%d want 204", resp.HTTPStatus)
	}
	if blob.deleteCalled {
		t.Fatal("blob.Delete must not be called when metadata delete fails")
	}
}

func TestDeleteObject_BlobFailIsGracefulReturns204(t *testing.T) {
	// Blob delete failure after metadata delete must not surface as an error.
	ctx := context.Background()
	meta := newStubMeta()
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", s3store.ObjectMeta{Key: "k"})
	blob := newStubBlob()
	blob.deleteErr = errors.New("blob store down")
	p := New(meta, blob)

	resp, err := p.DeleteObject(ctx, makeNR("b", "k"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.HTTPStatus != 204 {
		t.Fatalf("status=%d want 204", resp.HTTPStatus)
	}
}

func TestDeleteObject_HappyPath(t *testing.T) {
	ctx := context.Background()
	meta := s3store.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)

	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", s3store.ObjectMeta{Key: "k", Size: 4})
	_ = blob.Put(ctx, "b", "k", []byte("data"))

	resp, err := p.DeleteObject(ctx, makeNR("b", "k"))
	if err != nil || resp.HTTPStatus != 204 {
		t.Fatalf("status=%d err=%v", resp.HTTPStatus, err)
	}
	if _, err := meta.GetObjectMeta(ctx, "b", "k"); err == nil {
		t.Fatal("metadata should be deleted after DeleteObject")
	}
}

// ─── GetObject ────────────────────────────────────────────────────────────────

func TestGetObject_BlobMissAfterConcurrentDelete_Returns404(t *testing.T) {
	// Simulates: GetObjectMeta succeeds (first call), GetStream fails,
	// GetObjectMeta (recheck) fails → the delete already completed → 404.
	ctx := context.Background()
	meta := newStubMeta()
	meta.getMetaErrAfter = 1 // first call OK, second call returns error
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", s3store.ObjectMeta{Key: "k", Size: 4})

	blob := newStubBlob()
	blob.getStreamErr = errors.New("object not found in blob store")
	p := New(meta, blob)

	_, err := p.GetObject(ctx, makeNR("b", "k"))
	if err == nil {
		t.Fatal("expected error for concurrent delete")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 404 {
		t.Fatalf("status=%d want 404 (concurrent delete)", pe.HTTPStatus)
	}
}

func TestGetObject_BlobMissingMetadataPresent_Returns500(t *testing.T) {
	// Simulates: GetObjectMeta succeeds both times but blob is absent → storage
	// corruption → 500.
	ctx := context.Background()
	meta := newStubMeta()
	// getMetaErrAfter = -1 means both calls succeed.
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", s3store.ObjectMeta{Key: "k", Size: 4})

	blob := newStubBlob()
	blob.getStreamErr = errors.New("blob file missing")
	p := New(meta, blob)

	_, err := p.GetObject(ctx, makeNR("b", "k"))
	if err == nil {
		t.Fatal("expected error for blob corruption")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 500 {
		t.Fatalf("status=%d want 500 (storage corruption)", pe.HTTPStatus)
	}
}

func TestGetObject_MetadataMissReturns404(t *testing.T) {
	// Object does not exist at all → first GetObjectMeta fails → 404.
	ctx := context.Background()
	p := New(s3store.NewMemoryS3ObjectMetaStore(), blobfs.NewMemoryBlobStore())

	_, err := p.GetObject(ctx, makeNR("b", "missing"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("want 404, got %v", err)
	}
}

// ─── DeleteObjects ────────────────────────────────────────────────────────────

func TestDeleteObjects_ReturnsAllDeletedKeys(t *testing.T) {
	ctx := context.Background()
	meta := s3store.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)

	_ = meta.CreateBucket(ctx, "b", nil)
	for _, k := range []string{"k1", "k2", "k3"} {
		_ = meta.PutObjectMeta(ctx, "b", k, s3store.ObjectMeta{Key: k})
		_ = blob.Put(ctx, "b", k, []byte("data"))
	}

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b",
			"Delete": map[string]any{
				"Object": []any{
					map[string]any{"Key": "k1"},
					map[string]any{"Key": "k2"},
				},
			},
		},
	}
	resp, err := p.DeleteObjects(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	deleted, _ := resp.Data["Deleted"].([]map[string]any)
	if len(deleted) != 2 {
		t.Fatalf("deleted count=%d want 2", len(deleted))
	}
	// k3 untouched
	if _, err := meta.GetObjectMeta(ctx, "b", "k3"); err != nil {
		t.Fatal("k3 should still exist")
	}
}

func TestDeleteObjects_BlobFailDoesNotAbortMetadataDelete(t *testing.T) {
	// Blob delete failure must not prevent metadata from being deleted.
	ctx := context.Background()
	meta := s3store.NewMemoryS3ObjectMetaStore()
	blob := newStubBlob()
	blob.deleteErr = errors.New("blob store down")
	p := New(meta, blob)

	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", s3store.ObjectMeta{Key: "k"})

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b",
			"Delete": map[string]any{
				"Object": []any{map[string]any{"Key": "k"}},
			},
		},
	}
	resp, err := p.DeleteObjects(ctx, nr)
	if err != nil {
		t.Fatalf("blob failure should not propagate: %v", err)
	}
	deleted, _ := resp.Data["Deleted"].([]map[string]any)
	if len(deleted) != 1 {
		t.Fatalf("deleted count=%d want 1", len(deleted))
	}
	// Metadata must be gone — it was deleted before the blob failure.
	if _, err := meta.GetObjectMeta(ctx, "b", "k"); err == nil {
		t.Fatal("metadata must be deleted before blob delete is attempted")
	}
}

func TestDeleteObjects_EmptyBodyReturnsEmptyDeleted(t *testing.T) {
	p := New(s3store.NewMemoryS3ObjectMetaStore(), blobfs.NewMemoryBlobStore())
	nr := &model.NormalizedRequest{Params: map[string]any{"_bucket": "b"}}
	resp, err := p.DeleteObjects(context.Background(), nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	deleted, _ := resp.Data["Deleted"].([]any)
	if len(deleted) != 0 {
		t.Fatalf("want empty Deleted, got %v", deleted)
	}
}

// ─── GetBucketLocation ────────────────────────────────────────────────────────

func TestGetBucketLocation_UsEast1ReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	meta := s3store.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Region: "us-east-1",
		Params: map[string]any{"_bucket": "b"},
	}
	resp, err := p.GetBucketLocation(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lc, _ := resp.Data["LocationConstraint"].(string)
	if lc != "" {
		t.Fatalf("us-east-1 must return empty LocationConstraint, got %q", lc)
	}
}

func TestGetBucketLocation_OtherRegionReturnsRegion(t *testing.T) {
	ctx := context.Background()
	meta := s3store.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Region: "eu-west-1",
		Params: map[string]any{"_bucket": "b"},
	}
	resp, err := p.GetBucketLocation(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lc, _ := resp.Data["LocationConstraint"].(string)
	if lc != "eu-west-1" {
		t.Fatalf("non-us-east-1 must return region name, got %q", lc)
	}
}
