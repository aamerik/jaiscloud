package blobfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// ── flat-key coexistence ──────────────────────────────────────────────────────

// TestLocalFSBlobStore_DirBeforeParent: child key written first, then the same
// prefix is written as a standalone key (the S3A FileOutputCommitter scenario).
func TestLocalFSBlobStore_DirBeforeParent(t *testing.T) {
	s := mustNew(t)
	ctx := context.Background()

	if err := s.Put(ctx, "b", "foo/bar/baz.txt", []byte("child")); err != nil {
		t.Fatalf("Put child: %v", err)
	}
	if err := s.Put(ctx, "b", "foo/bar", []byte("parent")); err != nil {
		t.Fatalf("Put parent (dir conflict): %v", err)
	}

	assertGet(t, s, "b", "foo/bar", "parent")
	assertGet(t, s, "b", "foo/bar/baz.txt", "child")
	assertList(t, s, "b", "foo/", "foo/bar", "foo/bar/baz.txt")

	// Both sentinel files must exist inside the conflict directory.
	base := filepath.Join(s.baseDir, "b", "foo", "bar")
	assertFileExists(t, filepath.Join(base, objMarker))
	assertFileExists(t, filepath.Join(base, dirObjFlag))

	// Delete parent; child must survive.
	if err := s.Delete(ctx, "b", "foo/bar"); err != nil {
		t.Fatalf("Delete parent: %v", err)
	}
	assertMissing(t, s, "b", "foo/bar")
	assertGet(t, s, "b", "foo/bar/baz.txt", "child")
	// Flag file removed alongside marker.
	assertNoFile(t, filepath.Join(base, dirObjFlag))
}

// TestLocalFSBlobStore_ParentBeforeDir: parent key written first, then child
// written underneath it (triggers promotion).
func TestLocalFSBlobStore_ParentBeforeDir(t *testing.T) {
	s := mustNew(t)
	ctx := context.Background()

	if err := s.Put(ctx, "b", "staging/0", []byte("marker")); err != nil {
		t.Fatalf("Put staging/0: %v", err)
	}
	if err := s.Put(ctx, "b", "staging/0/task/part.json", []byte("data")); err != nil {
		t.Fatalf("Put child after promotion: %v", err)
	}

	assertGet(t, s, "b", "staging/0", "marker")
	assertGet(t, s, "b", "staging/0/task/part.json", "data")
	assertList(t, s, "b", "staging/", "staging/0", "staging/0/task/part.json")
}

// TestLocalFSBlobStore_LiteralObjMarkerKey: user writes a key whose last
// component happens to be ".jaiscloud_obj". Without the dir-flag, List must
// NOT translate it to the parent key.
func TestLocalFSBlobStore_LiteralObjMarkerKey(t *testing.T) {
	s := mustNew(t)
	ctx := context.Background()

	if err := s.Put(ctx, "b", "foo/.jaiscloud_obj", []byte("user-data")); err != nil {
		t.Fatalf("Put literal .jaiscloud_obj key: %v", err)
	}

	assertGet(t, s, "b", "foo/.jaiscloud_obj", "user-data")

	keys, err := s.List(ctx, "b", "foo/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 || keys[0] != "foo/.jaiscloud_obj" {
		t.Errorf("expected [foo/.jaiscloud_obj], got %v", keys)
	}
}

// ── atomic write ─────────────────────────────────────────────────────────────

// TestLocalFSBlobStore_AtomicPut: after a successful Put no temp file remains.
func TestLocalFSBlobStore_AtomicPut(t *testing.T) {
	s := mustNew(t)
	ctx := context.Background()

	if err := s.Put(ctx, "b", "obj", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// No temp files should remain.
	entries, _ := os.ReadDir(filepath.Join(s.baseDir, "b"))
	for _, e := range entries {
		if hasPrefix(e.Name(), putTmpPrefix) {
			t.Errorf("leftover temp file after Put: %s", e.Name())
		}
	}
}

// ── crash recovery ────────────────────────────────────────────────────────────

// TestLocalFSBlobStore_RecoverPutTemp: an orphaned put-temp file left by a
// crashed write should be removed by recoverOrphans.
func TestLocalFSBlobStore_RecoverPutTemp(t *testing.T) {
	dir := t.TempDir()
	bucketDir := filepath.Join(dir, "b")
	os.MkdirAll(bucketDir, 0o755)

	// Simulate a crashed Put that left a temp file.
	orphan := filepath.Join(bucketDir, putTmpPrefix+"crashed")
	os.WriteFile(orphan, []byte("partial"), 0o644)

	// Opening the store should remove the orphan.
	s, err := NewLocalFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewLocalFSBlobStore: %v", err)
	}
	_ = s

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("expected orphaned put-temp to be removed on startup")
	}
}

// TestLocalFSBlobStore_RecoverPromoAfterMkdir: promotion crashed after mkdir
// but before the final rename. recoverOrphans must complete it.
func TestLocalFSBlobStore_RecoverPromoAfterMkdir(t *testing.T) {
	dir := t.TempDir()
	bucketDir := filepath.Join(dir, "b")
	objDir := filepath.Join(bucketDir, "foo")

	// Simulate crash state: promo temp exists AND the directory was already created.
	os.MkdirAll(objDir, 0o755)
	promoFile := filepath.Join(bucketDir, "foo"+promoSuffix)
	os.WriteFile(promoFile, []byte("object-data"), 0o644)

	s, err := NewLocalFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewLocalFSBlobStore: %v", err)
	}

	// Recovery should have moved the data into the directory.
	got, err := s.Get(context.Background(), "b", "foo")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(got) != "object-data" {
		t.Errorf("expected %q, got %q", "object-data", got)
	}
}

// TestLocalFSBlobStore_RecoverPromoBeforeMkdir: promotion crashed after rename
// but before mkdir. recoverOrphans must reverse it.
func TestLocalFSBlobStore_RecoverPromoBeforeMkdir(t *testing.T) {
	dir := t.TempDir()
	bucketDir := filepath.Join(dir, "b")
	os.MkdirAll(bucketDir, 0o755)

	// Simulate crash state: promo temp exists, directory does NOT exist.
	promoFile := filepath.Join(bucketDir, "foo"+promoSuffix)
	os.WriteFile(promoFile, []byte("object-data"), 0o644)

	s, err := NewLocalFSBlobStore(dir)
	if err != nil {
		t.Fatalf("NewLocalFSBlobStore: %v", err)
	}

	// Recovery should have renamed the temp back to the original path.
	got, err := s.Get(context.Background(), "b", "foo")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(got) != "object-data" {
		t.Errorf("expected %q, got %q", "object-data", got)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func mustNew(t *testing.T) *LocalFSBlobStore {
	t.Helper()
	s, err := NewLocalFSBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFSBlobStore: %v", err)
	}
	return s
}

func assertGet(t *testing.T, s *LocalFSBlobStore, bucket, key, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), bucket, key)
	if err != nil {
		t.Errorf("Get(%s/%s): %v", bucket, key, err)
		return
	}
	if string(got) != want {
		t.Errorf("Get(%s/%s): want %q, got %q", bucket, key, want, got)
	}
}

func assertMissing(t *testing.T, s *LocalFSBlobStore, bucket, key string) {
	t.Helper()
	if _, err := s.Get(context.Background(), bucket, key); err == nil {
		t.Errorf("expected Get(%s/%s) to fail after Delete", bucket, key)
	}
}

func assertList(t *testing.T, s *LocalFSBlobStore, bucket, prefix string, wantKeys ...string) {
	t.Helper()
	got, err := s.List(context.Background(), bucket, prefix)
	if err != nil {
		t.Fatalf("List(%s, %s): %v", bucket, prefix, err)
	}
	want := make(map[string]bool, len(wantKeys))
	for _, k := range wantKeys {
		want[k] = true
	}
	have := make(map[string]bool, len(got))
	for _, k := range got {
		have[k] = true
	}
	for k := range want {
		if !have[k] {
			t.Errorf("List missing key %q; got %v", k, got)
		}
	}
	for k := range have {
		if !want[k] {
			t.Errorf("List returned unexpected key %q", k)
		}
	}
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected file to exist: %s", path)
	}
}

func assertNoFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file to be absent: %s", path)
	}
}

func hasPrefix(s, prefix string) bool { return len(s) >= len(prefix) && s[:len(prefix)] == prefix }
