// Package blobfs provides blob (binary large object) storage used by S3 and Lambda.
// Two implementations are available: MemoryBlobStore (lite mode) and
// LocalFSBlobStore (full mode, persists to disk).
package blobfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// BlobStore stores and retrieves raw bytes keyed by (bucket, key).
// The "bucket" namespace separates concerns (e.g. "s3", "lambda-zips").
type BlobStore interface {
	Put(ctx context.Context, bucket, key string, data []byte) error
	Get(ctx context.Context, bucket, key string) ([]byte, error)
	Delete(ctx context.Context, bucket, key string) error
	List(ctx context.Context, bucket, prefix string) ([]string, error)
	Reset()
}

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

func (s *MemoryBlobStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string][]byte)
}

// -- LocalFSBlobStore --

// LocalFSBlobStore is a filesystem-backed BlobStore for full mode.
// Layout: {baseDir}/{bucket}/{key}
type LocalFSBlobStore struct {
	baseDir string
}

func NewLocalFSBlobStore(baseDir string) (*LocalFSBlobStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("blobfs mkdir %s: %w", baseDir, err)
	}
	return &LocalFSBlobStore{baseDir: baseDir}, nil
}

func (s *LocalFSBlobStore) path(bucket, key string) string {
	// Sanitize key to avoid path traversal.
	clean := filepath.Clean("/" + key)[1:]
	return filepath.Join(s.baseDir, bucket, clean)
}

func (s *LocalFSBlobStore) Put(_ context.Context, bucket, key string, data []byte) error {
	p := s.path(bucket, key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (s *LocalFSBlobStore) Get(_ context.Context, bucket, key string) ([]byte, error) {
	b, err := os.ReadFile(s.path(bucket, key))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("blob not found: %s/%s", bucket, key)
	}
	return b, err
}

func (s *LocalFSBlobStore) Delete(_ context.Context, bucket, key string) error {
	err := os.Remove(s.path(bucket, key))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *LocalFSBlobStore) List(_ context.Context, bucket, prefix string) ([]string, error) {
	bucketDir := filepath.Join(s.baseDir, bucket)
	var keys []string
	err := filepath.WalkDir(bucketDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(bucketDir, path)
		rel = filepath.ToSlash(rel)
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

func (s *LocalFSBlobStore) Reset() {
	_ = os.RemoveAll(s.baseDir)
	_ = os.MkdirAll(s.baseDir, 0o755)
}
