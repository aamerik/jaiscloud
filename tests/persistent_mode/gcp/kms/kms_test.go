//go:build gcp_persistence

// Package kms_test verifies the Postgres-backed KMS store against a live
// database, exercising the snapshot/restore and concurrent version-allocation
// paths that the memory-store suite cannot cover.
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
package kms_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// newKMSStore opens a Postgres-backed KMS store against the shared "gcp" schema.
func newKMSStore(t *testing.T, dsn string) *kms.PostgresStore {
	t.Helper()
	pg, err := store.NewPostgresResourceStore(context.Background(), dsn, "gcp")
	if err != nil {
		t.Fatalf("postgres store: %v", err)
	}
	t.Cleanup(pg.Close)
	if err := store.RunMigrations(context.Background(), pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("gcp migrations: %v", err)
	}
	return kms.NewPostgresStore(pg.Pool())
}

func kmsIDs() (project, location, keyring, key string) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	return "proj", "global", "kr-" + suffix, "k-" + suffix
}

// TestPostgresDEKSnapshotRoundTrip verifies that a KMS snapshot captures the
// raw server DEK so key material survives export→import (Snapshot → Reset →
// Restore) and remains decryptable.
func TestPostgresDEKSnapshotRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Purpose: "ENCRYPT_DECRYPT", Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Wipe the store (including jc_kms_dek) to simulate a fresh destination.
	s.Reset(ctx)

	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	mat, err := s.KeyMaterial(ctx, project, location, keyring, key, "1")
	if err != nil {
		t.Fatalf("restored key material unreadable (DEK lost?): %v", err)
	}
	if len(mat) != 32 {
		t.Fatalf("expected 32-byte key material, got %d", len(mat))
	}
	ct, err := kms.EncryptData(mat, []byte("hello"), []byte("aad"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	pt, err := kms.DecryptData(mat, ct, []byte("aad"))
	if err != nil || string(pt) != "hello" {
		t.Fatalf("decrypt round-trip failed: pt=%q err=%v", pt, err)
	}
}

// TestPostgresConcurrentCreateVersion verifies concurrent CreateVersion calls
// allocate distinct version numbers without a unique-violation.
func TestPostgresConcurrentCreateVersion(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	const n = 16
	vers := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vers[i], errs[i] = s.CreateVersion(ctx, project, location, keyring, key, kms.Version{})
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: unexpected error: %v", i, errs[i])
		}
	}
	seen := map[string]bool{}
	for _, v := range vers {
		if seen[v] {
			t.Fatalf("duplicate version %q allocated", v)
		}
		seen[v] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct versions, got %d", n, len(seen))
	}

	all, err := s.ListVersions(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(all) != n+1 {
		t.Fatalf("expected %d total versions (primary + %d), got %d", n+1, n, len(all))
	}
}
