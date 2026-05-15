package function

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/blobfs"
)

func TestStoreAndLoadCode(t *testing.T) {
	blobs := blobfs.NewMemoryBlobStore()
	p := &FunctionProvider{blobs: blobs}
	ctx := context.Background()

	original := []byte("fakecontent")
	sha, size, key, err := p.storeCode(ctx, "000", "foo", "$LATEST", original)
	if err != nil {
		t.Fatalf("storeCode: %v", err)
	}
	if len(sha) != 64 {
		t.Errorf("expected 64-char hex sha256, got %d chars", len(sha))
	}
	if size != int64(len(original)) {
		t.Errorf("size: got %d want %d", size, len(original))
	}
	if !strings.Contains(key, "foo") {
		t.Errorf("blobKey %q should contain function name", key)
	}

	loaded, err := p.loadCode(ctx, "000", "foo", "$LATEST")
	if err != nil {
		t.Fatalf("loadCode: %v", err)
	}
	if string(loaded) != string(original) {
		t.Errorf("loaded content mismatch: got %q want %q", loaded, original)
	}
}
