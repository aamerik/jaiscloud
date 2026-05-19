// Package blobfs provides blob (binary large object) storage used by S3 and Lambda.
// Two implementations are available: MemoryBlobStore (lite mode) and
// LocalFSBlobStore (full mode, persists to disk).
package blobfs

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// BlobStore stores and retrieves raw bytes keyed by (bucket, key).
// The "bucket" namespace separates concerns (e.g. "s3", "lambda-zips").
type BlobStore interface {
	Put(ctx context.Context, bucket, key string, data []byte) error
	Get(ctx context.Context, bucket, key string) ([]byte, error)
	// PutStream writes r to the store, returning bytes written. Implementations
	// may buffer internally (MemoryBlobStore) or stream to disk (LocalFSBlobStore).
	PutStream(ctx context.Context, bucket, key string, r io.Reader) (int64, error)
	// GetStream opens the object for reading. offset is the byte offset to start
	// at; length is the number of bytes to read (-1 means read to end). The
	// caller must close the returned ReadCloser.
	GetStream(ctx context.Context, bucket, key string, offset, length int64) (io.ReadCloser, error)
	Delete(ctx context.Context, bucket, key string) error
	List(ctx context.Context, bucket, prefix string) ([]string, error)
	Reset()
}

// limitReadCloser wraps an io.Reader with a separate io.Closer so that
// io.LimitReader (which returns a plain Reader) can be paired with a file.
type limitReadCloser struct {
	io.Reader
	closer io.Closer
}

func (l *limitReadCloser) Close() error { return l.closer.Close() }

// -- MemoryBlobStore --

// MemoryBlobStore is an in-memory BlobStore for lite mode.
type MemoryBlobStore struct {
	mu   sync.RWMutex
	data map[string][]byte // key: bucket + "\x00" + key
}

func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{data: make(map[string][]byte)}
}

func blobKey(bucket, key string) string { return bucket + "\x00" + key }

func (s *MemoryBlobStore) Put(_ context.Context, bucket, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[blobKey(bucket, key)] = cp
	return nil
}

func (s *MemoryBlobStore) Get(_ context.Context, bucket, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.data[blobKey(bucket, key)]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s/%s", bucket, key)
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (s *MemoryBlobStore) Delete(_ context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, blobKey(bucket, key))
	return nil
}

func (s *MemoryBlobStore) List(_ context.Context, bucket, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sep := bucket + "\x00"
	var keys []string
	for k := range s.data {
		if strings.HasPrefix(k, sep) {
			rest := k[len(sep):]
			if strings.HasPrefix(rest, prefix) {
				keys = append(keys, rest)
			}
		}
	}
	return keys, nil
}

func (s *MemoryBlobStore) PutStream(ctx context.Context, bucket, key string, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	if err := s.Put(ctx, bucket, key, data); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func (s *MemoryBlobStore) GetStream(_ context.Context, bucket, key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.data[blobKey(bucket, key)]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s/%s", bucket, key)
	}
	if offset >= int64(len(b)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := int64(len(b))
	if length >= 0 && offset+length < end {
		end = offset + length
	}
	// Copy slice to avoid a race with concurrent Put/Delete.
	cp := make([]byte, end-offset)
	copy(cp, b[offset:end])
	return io.NopCloser(bytes.NewReader(cp)), nil
}

func (s *MemoryBlobStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string][]byte)
}

// -- LocalFSBlobStore --

// LocalFSBlobStore is a filesystem-backed BlobStore for full mode.
//
// # Flat-key semantics on a hierarchical filesystem
//
// Real object stores (S3, Azure Blob flat-namespace, GCS) are flat key-value
// stores: key "foo/bar" and key "foo/bar/baz" are completely independent
// objects. On a local filesystem "foo/bar" cannot simultaneously be a regular
// file AND a directory containing "foo/bar/baz".
//
// We resolve this with two hidden files placed inside a conflict directory:
//
//	.jaiscloud_obj      — the actual object bytes for the conflicting key
//	.jaiscloud_dir_flag — zero-byte sentinel: "this directory IS an S3 key"
//
// Both files must be present for a directory to be treated as a dir-object.
// This lets us distinguish our internal markers from a user who deliberately
// wrote a key whose name happens to be ".jaiscloud_obj".
//
// Example layout for keys "a/b" and "a/b/c":
//
//	a/
//	  b/                      ← directory (represents both a/b and a/b/*)
//	    .jaiscloud_dir_flag   ← zero bytes: "a/b is also an S3 object"
//	    .jaiscloud_obj        ← actual bytes of object "a/b"
//	    c                     ← object "a/b/c"
//
// # Atomic writes
//
// Put writes to a temp file in the same directory, then renames it into place.
// rename(2) is atomic on POSIX: a reader either sees the old file or the new
// one, never a partial write.
//
// # Crash recovery
//
// NewLocalFSBlobStore runs recoverOrphans on startup, which handles two kinds
// of leftover files created by interrupted promotions or writes:
//   - ".jaiscloud_tmp_*"  — incomplete atomic Put; safe to delete
//   - "*.jaiscloud_promo" — interrupted promotion temp; complete or reverse it
type LocalFSBlobStore struct {
	mu      sync.RWMutex
	baseDir string
}

const (
	// objMarker holds the object bytes when a key conflicts with a directory.
	objMarker = ".jaiscloud_obj"

	// dirObjFlag is a zero-byte sentinel placed next to objMarker. Its presence
	// distinguishes our dir-object from a user-written key named ".jaiscloud_obj".
	dirObjFlag = ".jaiscloud_dir_flag"

	// putTmpPrefix is the temp-file prefix for atomic writes.
	putTmpPrefix = ".jaiscloud_tmp_"

	// promoSuffix is the temp-file suffix during directory promotion.
	promoSuffix = ".jaiscloud_promo"
)

// BaseDir returns the root directory of this blob store.
func (s *LocalFSBlobStore) BaseDir() string { return s.baseDir }

func NewLocalFSBlobStore(baseDir string) (*LocalFSBlobStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("blobfs mkdir %s: %w", baseDir, err)
	}
	s := &LocalFSBlobStore{baseDir: baseDir}
	s.recoverOrphans()
	return s, nil
}

// recoverOrphans cleans up files left behind by interrupted Put or promotion
// operations. Called once at startup, before the store is in use.
func (s *LocalFSBlobStore) recoverOrphans() {
	filepath.WalkDir(s.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		switch {
		case strings.HasPrefix(name, putTmpPrefix):
			// Incomplete atomic write — the rename never happened, so the
			// destination was never updated. Safe to remove.
			os.Remove(path)

		case strings.HasSuffix(name, promoSuffix):
			// Interrupted promotion. Determine how far it got and finish it.
			naturalPath := strings.TrimSuffix(path, promoSuffix)
			if info, err := os.Stat(naturalPath); err == nil && info.IsDir() {
				// Step 2 (mkdir) completed; step 3 (place marker) did not.
				// Complete the promotion: move the temp file into the dir.
				os.Rename(path, filepath.Join(naturalPath, objMarker))
				os.WriteFile(filepath.Join(naturalPath, dirObjFlag), nil, 0o644)
			} else {
				// Step 1 completed; step 2 (mkdir) did not. Reverse it.
				os.Rename(path, naturalPath)
			}
		}
		return nil
	})
}

// path returns the canonical filesystem path for (bucket, key).
func (s *LocalFSBlobStore) path(bucket, key string) string {
	clean := filepath.Clean("/" + key)[1:]
	return filepath.Join(s.baseDir, bucket, clean)
}

// isDirObj reports whether naturalPath is a directory that also represents an
// S3 object (i.e. both .jaiscloud_obj and .jaiscloud_dir_flag exist inside it).
func isDirObj(naturalPath string) bool {
	_, err := os.Stat(filepath.Join(naturalPath, dirObjFlag))
	return err == nil
}

// resolveRead returns the effective path to read from for an S3 key.
// Does not modify the filesystem (safe to call under RLock).
func (s *LocalFSBlobStore) resolveRead(naturalPath string) string {
	if isDirObj(naturalPath) {
		return filepath.Join(naturalPath, objMarker)
	}
	return naturalPath
}

// resolveWrite returns the effective path to write to for an S3 key and
// creates the dir-object sentinel files if the natural path is a directory.
// Must be called with mu held for writing.
func (s *LocalFSBlobStore) resolveWrite(naturalPath string) (string, error) {
	info, err := os.Stat(naturalPath)
	if err != nil || !info.IsDir() {
		return naturalPath, nil
	}
	// naturalPath is a directory — write object data inside it.
	if err := os.WriteFile(filepath.Join(naturalPath, dirObjFlag), nil, 0o644); err != nil {
		return "", fmt.Errorf("blobfs dir-obj flag: %w", err)
	}
	return filepath.Join(naturalPath, objMarker), nil
}

// writeAtomic writes data to dst atomically using a temp-file + rename.
// The temp file is created in the same directory as dst so the rename is
// guaranteed to be on the same filesystem (rename across devices is not atomic).
func (s *LocalFSBlobStore) writeAtomic(dst string, data []byte) (retErr error) {
	f, err := os.CreateTemp(filepath.Dir(dst), putTmpPrefix)
	if err != nil {
		return fmt.Errorf("blobfs: create temp: %w", err)
	}
	tmpName := f.Name()
	defer func() {
		if retErr != nil {
			os.Remove(tmpName)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("blobfs: write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("blobfs: sync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("blobfs: close temp: %w", err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("blobfs: rename to final: %w", err)
	}
	return nil
}

// ensureDir creates all directories in the path, promoting any plain-file
// path component to a directory so that a child key can be written under it.
// Promotion uses a named temp file (*.jaiscloud_promo) that recoverOrphans
// can detect and finish if the process crashes mid-promotion.
func (s *LocalFSBlobStore) ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err == nil {
		return nil
	}
	sep := string(filepath.Separator)
	parts := strings.Split(filepath.Clean(dir), sep)
	cur := ""
	if filepath.IsAbs(dir) {
		cur = sep
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		info, err := os.Stat(cur)
		if err != nil {
			break // does not exist yet; MkdirAll below will create it
		}
		if info.IsDir() {
			continue
		}
		// cur is a plain file — promote it to a directory.
		// Use a stable temp name (not random) so recoverOrphans can find it.
		tmp := cur + promoSuffix
		if err := os.Rename(cur, tmp); err != nil {
			return fmt.Errorf("blobfs promote %s: rename to promo: %w", cur, err)
		}
		if err := os.Mkdir(cur, 0o755); err != nil {
			_ = os.Rename(tmp, cur) // best-effort rollback
			return fmt.Errorf("blobfs promote %s: mkdir: %w", cur, err)
		}
		// Write the sentinel before moving the data so recoverOrphans knows
		// the directory was created intentionally as a dir-object.
		if err := os.WriteFile(filepath.Join(cur, dirObjFlag), nil, 0o644); err != nil {
			return fmt.Errorf("blobfs promote %s: write flag: %w", cur, err)
		}
		if err := os.Rename(tmp, filepath.Join(cur, objMarker)); err != nil {
			return fmt.Errorf("blobfs promote %s: place marker: %w", cur, err)
		}
	}
	return os.MkdirAll(dir, 0o755)
}

func (s *LocalFSBlobStore) Put(_ context.Context, bucket, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.path(bucket, key)
	if err := s.ensureDir(filepath.Dir(p)); err != nil {
		return err
	}
	target, err := s.resolveWrite(p)
	if err != nil {
		return err
	}
	return s.writeAtomic(target, data)
}

func (s *LocalFSBlobStore) Get(_ context.Context, bucket, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := os.ReadFile(s.resolveRead(s.path(bucket, key)))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("blob not found: %s/%s", bucket, key)
	}
	return b, err
}

func (s *LocalFSBlobStore) Delete(_ context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.resolveRead(s.path(bucket, key))
	err := os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// If we just deleted a dir-object marker, remove the now-orphaned flag too.
	if filepath.Base(p) == objMarker {
		os.Remove(filepath.Join(filepath.Dir(p), dirObjFlag))
	}
	return nil
}

func (s *LocalFSBlobStore) List(_ context.Context, bucket, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucketDir := filepath.Join(s.baseDir, bucket)
	var keys []string
	err := filepath.WalkDir(bucketDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		// Skip all internal housekeeping files.
		if name == dirObjFlag ||
			strings.HasPrefix(name, putTmpPrefix) ||
			strings.HasSuffix(name, promoSuffix) {
			return nil
		}
		rel, _ := filepath.Rel(bucketDir, path)
		rel = filepath.ToSlash(rel)
		// Translate a dir-object marker back to its S3 key.
		// Only do this when the sibling dirObjFlag is present — that flag is
		// what proves this .jaiscloud_obj was written by us, not by a user who
		// happened to choose ".jaiscloud_obj" as their key suffix.
		if name == objMarker {
			parentDir := filepath.Dir(path)
			if _, err := os.Stat(filepath.Join(parentDir, dirObjFlag)); err == nil {
				parentRel, _ := filepath.Rel(bucketDir, parentDir)
				rel = filepath.ToSlash(parentRel)
			}
		}
		if strings.HasPrefix(rel, prefix) {
			keys = append(keys, rel)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil, nil
	}
	return keys, err
}

// PutStream streams r to the store using a three-phase protocol to avoid a
// deadlock when the caller's reader itself calls GetStream on the same store
// (e.g. CompleteMultipartUpload assembling parts via seqPartReader):
//
//  1. Lock briefly to set up paths and create the temp file.
//  2. Release the lock and stream data into the temp file — the reader can
//     now acquire RLock freely.
//  3. Reacquire the lock for the atomic rename.
func (s *LocalFSBlobStore) PutStream(_ context.Context, bucket, key string, r io.Reader) (n int64, retErr error) {
	// Phase 1: resolve paths and create temp file under write lock.
	s.mu.Lock()
	p := s.path(bucket, key)
	if err := s.ensureDir(filepath.Dir(p)); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	target, err := s.resolveWrite(p)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	f, err := os.CreateTemp(filepath.Dir(target), putTmpPrefix)
	s.mu.Unlock() // release before io.Copy so readers can acquire RLock
	if err != nil {
		return 0, fmt.Errorf("blobfs: create temp: %w", err)
	}
	tmpName := f.Name()
	defer func() {
		if retErr != nil {
			os.Remove(tmpName)
		}
	}()

	// Phase 2: stream to the temp file — no lock held.
	n, err = io.Copy(f, r)
	if err != nil {
		f.Close()
		return 0, fmt.Errorf("blobfs: stream write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, fmt.Errorf("blobfs: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("blobfs: close temp: %w", err)
	}

	// Phase 3: atomic rename under write lock.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Rename(tmpName, target); err != nil {
		return 0, fmt.Errorf("blobfs: rename: %w", err)
	}
	return n, nil
}

// GetStream opens the object for streaming. The mutex is released after the
// file is opened; POSIX keeps the inode alive until the last fd is closed, so
// a concurrent Delete cannot corrupt an in-progress read.
func (s *LocalFSBlobStore) GetStream(_ context.Context, bucket, key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.RLock()
	p := s.resolveRead(s.path(bucket, key))
	f, err := os.Open(p)
	s.mu.RUnlock()
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("blob not found: %s/%s", bucket, key)
	}
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	if length >= 0 {
		return &limitReadCloser{Reader: io.LimitReader(f, length), closer: f}, nil
	}
	return f, nil
}

func (s *LocalFSBlobStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = os.RemoveAll(s.baseDir)
	_ = os.MkdirAll(s.baseDir, 0o755)
}

// WriteTarball walks s.baseDir recursively and writes each regular file as a
// tar entry with a path of the form "blobs/<relative-path>". Entries are
// written in sorted order so the tarball is deterministic.
func (s *LocalFSBlobStore) WriteTarball(ctx context.Context, tw *tar.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all regular file paths relative to baseDir.
	var relPaths []string
	err := filepath.WalkDir(s.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			return nil
		}
		// Skip internal housekeeping files.
		name := d.Name()
		if name == dirObjFlag ||
			strings.HasPrefix(name, putTmpPrefix) ||
			strings.HasSuffix(name, promoSuffix) {
			return nil
		}
		rel, err := filepath.Rel(s.baseDir, path)
		if err != nil {
			return nil
		}
		relPaths = append(relPaths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return fmt.Errorf("blobfs WriteTarball walk: %w", err)
	}
	sort.Strings(relPaths)

	for _, rel := range relPaths {
		absPath := filepath.Join(s.baseDir, filepath.FromSlash(rel))
		data, err := os.ReadFile(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue // deleted between walk and read
			}
			return fmt.Errorf("blobfs WriteTarball read %s: %w", rel, err)
		}
		hdr := &tar.Header{
			Name:     "blobs/" + rel,
			Typeflag: tar.TypeReg,
			Size:     int64(len(data)),
			Mode:     0600,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("blobfs WriteTarball header %s: %w", rel, err)
		}
		if _, err := tw.Write(data); err != nil {
			return fmt.Errorf("blobfs WriteTarball write %s: %w", rel, err)
		}
	}
	return nil
}

// ReadTarball extracts tar entries whose name starts with "blobs/" into s.baseDir.
// Path sanitisation: rejects entries with ".." components or absolute paths.
func (s *LocalFSBlobStore) ReadTarball(ctx context.Context, tr *tar.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("blobfs ReadTarball next: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		if !strings.HasPrefix(hdr.Name, "blobs/") {
			continue
		}
		// Strip the "blobs/" prefix to get the path relative to baseDir.
		rel := hdr.Name[len("blobs/"):]
		if rel == "" {
			continue
		}
		// Security: reject absolute paths and ".." components.
		clean := filepath.Clean(rel)
		if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
			return fmt.Errorf("blobfs ReadTarball: unsafe path %q", hdr.Name)
		}

		dst := filepath.Join(s.baseDir, clean)
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return fmt.Errorf("blobfs ReadTarball mkdir %s: %w", filepath.Dir(dst), err)
		}
		f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return fmt.Errorf("blobfs ReadTarball create %s: %w", dst, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return fmt.Errorf("blobfs ReadTarball copy %s: %w", dst, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("blobfs ReadTarball close %s: %w", dst, err)
		}
	}
	return nil
}

// NewSessionBlobStore creates a session-scoped LocalFSBlobStore under
// /tmp/jaiscloud-<instanceID>/blobs/. The caller must call Cleanup() on
// graceful shutdown to remove the session directory.
func NewSessionBlobStore(instanceID string) (*LocalFSBlobStore, error) {
	dir := filepath.Join(os.TempDir(), "jaiscloud-"+instanceID, "blobs")
	return NewLocalFSBlobStore(dir)
}

// Cleanup removes the session directory (parent of baseDir) created by
// NewSessionBlobStore. Safe to call even if the directory no longer exists.
func (s *LocalFSBlobStore) Cleanup() error {
	// parent is /tmp/jaiscloud-<instanceID>
	parent := filepath.Dir(s.baseDir)
	return os.RemoveAll(parent)
}
