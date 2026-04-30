package blobfs

import (
	"context"
	"strings"
	"testing"
)

func TestS3BlobFetcher_HappyPath(t *testing.T) {
	store := NewMemoryBlobStore()
	store.Put(context.Background(), "my-bucket", "scripts/stage.sh", []byte("#!/bin/sh\necho ok"))
	f := NewS3BlobFetcher(store)
	data, err := f.Fetch(context.Background(), "s3://my-bucket/scripts/stage.sh")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "#!/bin/sh\necho ok" {
		t.Errorf("unexpected data: %q", data)
	}
}

func TestS3BlobFetcher_S3aScheme(t *testing.T) {
	store := NewMemoryBlobStore()
	store.Put(context.Background(), "my-bucket", "file.txt", []byte("hello"))
	f := NewS3BlobFetcher(store)
	data, err := f.Fetch(context.Background(), "s3a://my-bucket/file.txt")
	if err != nil {
		t.Fatalf("s3a:// should be accepted: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("unexpected data: %q", data)
	}
}

func TestS3BlobFetcher_MissingObject(t *testing.T) {
	store := NewMemoryBlobStore()
	f := NewS3BlobFetcher(store)
	_, err := f.Fetch(context.Background(), "s3://bucket/missing.sh")
	if err == nil {
		t.Fatal("expected error for missing object")
	}
}

func TestS3BlobFetcher_UnsupportedScheme_GS(t *testing.T) {
	f := NewS3BlobFetcher(NewMemoryBlobStore())
	_, err := f.Fetch(context.Background(), "gs://bucket/file")
	if err == nil {
		t.Fatal("expected error for gs:// scheme")
	}
	if !strings.Contains(err.Error(), "gs") {
		t.Errorf("error should mention scheme; got: %v", err)
	}
}

func TestS3BlobFetcher_UnsupportedScheme_ABFSS(t *testing.T) {
	f := NewS3BlobFetcher(NewMemoryBlobStore())
	_, err := f.Fetch(context.Background(), "abfss://container@account.dfs.core.windows.net/file")
	if err == nil {
		t.Fatal("expected error for abfss:// scheme")
	}
	if !strings.Contains(err.Error(), "abfss") {
		t.Errorf("error should mention scheme; got: %v", err)
	}
}

func TestS3BlobFetcher_EmptyBucket(t *testing.T) {
	f := NewS3BlobFetcher(NewMemoryBlobStore())
	_, err := f.Fetch(context.Background(), "s3:///key")
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestS3BlobFetcher_EmptyKey(t *testing.T) {
	f := NewS3BlobFetcher(NewMemoryBlobStore())
	_, err := f.Fetch(context.Background(), "s3://bucket/")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}
