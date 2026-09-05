package gcs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryObjectStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()

	// Bucket create/get/list.
	if err := s.CreateBucket(ctx, "proj", "bkt", map[string]any{"location": "US"}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
	meta, err := s.GetBucket(ctx, "bkt")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if meta["projectId"] != "proj" {
		t.Errorf("expected projectId proj, got %v", meta["projectId"])
	}
	buckets, err := s.ListBuckets(ctx, "proj")
	if err != nil || len(buckets) != 1 {
		t.Fatalf("list buckets: %v / %d", err, len(buckets))
	}
	if _, err := s.GetBucket(ctx, "missing"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}

	// Object put/get/list/delete.
	o := ObjectMeta{Bucket: "bkt", Name: "a.txt", Generation: "1", ContentType: "text/plain", Size: 3, MD5Hash: "x", StorageClass: "STANDARD", TimeCreated: time.Now(), Updated: time.Now()}
	if err := s.PutObjectMeta(ctx, "bkt", "a.txt", o); err != nil {
		t.Fatalf("put object: %v", err)
	}
	got, err := s.GetObjectMeta(ctx, "bkt", "a.txt")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if got.ContentType != "text/plain" || got.Generation != "1" {
		t.Errorf("unexpected object: %+v", got)
	}
	list, err := s.ListObjects(ctx, "bkt")
	if err != nil || len(list) != 1 {
		t.Fatalf("list objects: %v / %d", err, len(list))
	}
	if _, err := s.GetObjectMeta(ctx, "bkt", "missing"); !errors.Is(err, ErrNoSuchObject) {
		t.Fatalf("expected ErrNoSuchObject, got %v", err)
	}

	// DeleteBucket must refuse a non-empty bucket.
	if err := s.DeleteBucket(ctx, "bkt"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("expected ErrBucketNotEmpty, got %v", err)
	}
	if err := s.DeleteObjectMeta(ctx, "bkt", "a.txt"); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if err := s.DeleteBucket(ctx, "bkt"); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	// Resumable sessions.
	sess := ResumableSession{UploadID: "u1", Bucket: "b", Name: "o", Length: 5, LastAccess: time.Now().Add(-2 * time.Hour)}
	if err := s.InitResumable(ctx, sess); err != nil {
		t.Fatalf("init resumable: %v", err)
	}
	gs, err := s.GetResumable(ctx, "u1")
	if err != nil || gs.Length != 5 {
		t.Fatalf("get resumable: %v / %+v", err, gs)
	}
	sess.Length = 10
	if err := s.UpdateResumable(ctx, sess); err != nil {
		t.Fatalf("update resumable: %v", err)
	}
	stale, err := s.ListStaleResumable(ctx, time.Now().Add(-time.Hour))
	if err != nil || len(stale) != 1 {
		t.Fatalf("list stale: %v / %d", err, len(stale))
	}
	if err := s.DeleteResumable(ctx, "u1"); err != nil {
		t.Fatalf("delete resumable: %v", err)
	}
	if _, err := s.GetResumable(ctx, "u1"); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("expected ErrNoSuchUpload, got %v", err)
	}

	// Reset clears everything.
	if err := s.CreateBucket(ctx, "proj", "x", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	s.Reset(ctx)
	if _, err := s.GetBucket(ctx, "x"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("expected ErrNoSuchBucket after reset, got %v", err)
	}
}
