package kms

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"})
	s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{ID: "k", Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION"})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := dst.GetCryptoKey(ctx, "proj", "global", "kr", "k"); err != nil {
		t.Fatalf("restored key missing: %v", err)
	}
	// Key material must survive (DEK is snapshotted too).
	if _, err := dst.KeyMaterial(ctx, "proj", "global", "kr", "k", "1"); err != nil {
		t.Fatalf("restored key material unreadable: %v", err)
	}
}
