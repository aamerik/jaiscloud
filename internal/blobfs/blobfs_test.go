package blobfs

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// ── WriteTarball / ReadTarball ────────────────────────────────────────────────

func TestLocalFSBlobStore_WriteTarball_Empty(t *testing.T) {
	s := mustNew(t)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := s.WriteTarball(context.Background(), tw); err != nil {
		t.Fatalf("WriteTarball: %v", err)
	}
	tw.Close()

	// Reading the tarball should yield no entries under "blobs/".
	tr := tar.NewReader(&buf)
	count := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		if strings.HasPrefix(hdr.Name, "blobs/") {
			count++
		}
	}
	if count != 0 {
		t.Errorf("expected 0 blob entries, got %d", count)
	}
}

func TestLocalFSBlobStore_Tarball_RoundTrip(t *testing.T) {
	src := mustNew(t)
	ctx := context.Background()

	// Write some blobs.
	if err := src.Put(ctx, "s3", "key1", []byte("value1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := src.Put(ctx, "s3", "nested/key2", []byte("value2")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Export to tarball.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := src.WriteTarball(ctx, tw); err != nil {
		t.Fatalf("WriteTarball: %v", err)
	}
	tw.Close()

	// Import into a fresh store.
	dst := mustNew(t)
	tr := tar.NewReader(&buf)
	if err := dst.ReadTarball(ctx, tr); err != nil {
		t.Fatalf("ReadTarball: %v", err)
	}

	// Verify contents.
	assertGet(t, dst, "s3", "key1", "value1")
	assertGet(t, dst, "s3", "nested/key2", "value2")
}

// TestLocalFSBlobStore_WriteTarball_PathSanitised verifies that ReadTarball
// rejects tarball entries that contain path-traversal sequences ("../").
func TestLocalFSBlobStore_WriteTarball_PathSanitised(t *testing.T) {
	s := mustNew(t)

	// Build a malicious tarball with a path-traversal entry.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	content := []byte("pwned")
	hdr := &tar.Header{
		Name:     "blobs/../../etc/passwd",
		Typeflag: tar.TypeReg,
		Size:     int64(len(content)),
		Mode:     0600,
	}
	tw.WriteHeader(hdr)
	tw.Write(content)
	tw.Close()

	tr := tar.NewReader(&buf)
	err := s.ReadTarball(context.Background(), tr)
	if err == nil {
		t.Fatal("expected error for path traversal, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "unsafe") && !strings.Contains(msg, "blobs") {
		t.Errorf("expected error to mention \"unsafe\" or \"blobs\", got: %v", err)
	}
}

// TestLocalFSBlobStore_WriteTarball_NonRegularEntry verifies that ReadTarball
// silently skips non-regular entries (e.g. symlinks) without returning an error
// and without writing any file for that entry.
func TestLocalFSBlobStore_WriteTarball_NonRegularEntry(t *testing.T) {
	s := mustNew(t)

	// Build a tarball that contains a symlink entry.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name:     "blobs/mylink",
		Typeflag: tar.TypeSymlink,
		Linkname: "/etc/passwd",
	}
	tw.WriteHeader(hdr)
	tw.Close()

	tr := tar.NewReader(&buf)
	if err := s.ReadTarball(context.Background(), tr); err != nil {
		t.Fatalf("expected nil error for symlink entry, got: %v", err)
	}

	// The store must not have created a file named "mylink".
	symlinkPath := filepath.Join(s.baseDir, "mylink")
	assertNoFile(t, symlinkPath)
}

// TestLocalFSBlobStore_SessionDirLifecycle verifies that NewSessionBlobStore
// creates a working store whose session directory is removed by Cleanup.
func TestLocalFSBlobStore_SessionDirLifecycle(t *testing.T) {
	instanceID := "test-instance-lifecycle"
	store, err := NewSessionBlobStore(instanceID)
	if err != nil {
		t.Fatalf("NewSessionBlobStore: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	baseDir := store.BaseDir()
	if _, err := os.Stat(baseDir); err != nil {
		t.Fatalf("expected base dir to exist after NewSessionBlobStore: %v", err)
	}

	// Write a blob to confirm the store is functional.
	if err := store.Put(context.Background(), "b", "hello", []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	assertGet(t, store, "b", "hello", "world")

	// Cleanup must remove the session directory (parent of baseDir).
	if err := store.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	// The parent session directory should no longer exist.
	sessionDir := filepath.Dir(baseDir)
	if _, err := os.Stat(sessionDir); !os.IsNotExist(err) {
		t.Errorf("expected session dir %s to be removed after Cleanup", sessionDir)
	}
}

func TestLocalFSBlobStore_Tarball_BlobsSortedByPath(t *testing.T) {
	s := mustNew(t)
	ctx := context.Background()

	keys := []string{"c", "a", "b/z", "b/a"}
	for _, k := range keys {
		if err := s.Put(ctx, "bucket", k, []byte(k)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := s.WriteTarball(ctx, tw); err != nil {
		t.Fatalf("WriteTarball: %v", err)
	}
	tw.Close()

	tr := tar.NewReader(&buf)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		if strings.HasPrefix(hdr.Name, "blobs/") {
			names = append(names, hdr.Name)
		}
	}

	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)
	for i, n := range names {
		if n != sorted[i] {
			t.Errorf("entries not sorted: position %d is %q, expected %q", i, n, sorted[i])
		}
	}
}
