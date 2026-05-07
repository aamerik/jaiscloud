package s3

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MemoryS3ObjectMetaStore is an in-memory S3ObjectMetaStore.
type MemoryS3ObjectMetaStore struct {
	mu      sync.RWMutex
	buckets map[string]map[string]any        // bucketName → meta
	objects map[string]map[string]ObjectMeta // bucketName → key → meta
	uploads map[string]multipartUpload       // uploadID → upload
}

type multipartUpload struct {
	Bucket string
	Key    string
	Meta   map[string]any
	Parts  map[int]PartMeta
}

func NewMemoryS3ObjectMetaStore() *MemoryS3ObjectMetaStore {
	return &MemoryS3ObjectMetaStore{
		buckets: make(map[string]map[string]any),
		objects: make(map[string]map[string]ObjectMeta),
		uploads: make(map[string]multipartUpload),
	}
}

func (s *MemoryS3ObjectMetaStore) CreateBucket(_ context.Context, bucket string, meta map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket]; exists {
		return fmt.Errorf("bucket already exists")
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["Name"] = bucket
	meta["CreationDate"] = time.Now().UTC().Format(time.RFC3339)
	s.buckets[bucket] = meta
	s.objects[bucket] = make(map[string]ObjectMeta)
	return nil
}

func (s *MemoryS3ObjectMetaStore) GetBucket(_ context.Context, bucket string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("NoSuchBucket")
	}
	return meta, nil
}

func (s *MemoryS3ObjectMetaStore) DeleteBucket(_ context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[bucket]; !ok {
		return fmt.Errorf("NoSuchBucket")
	}
	// Must be empty.
	if len(s.objects[bucket]) > 0 {
		return fmt.Errorf("BucketNotEmpty")
	}
	delete(s.buckets, bucket)
	delete(s.objects, bucket)
	return nil
}

func (s *MemoryS3ObjectMetaStore) ListBuckets(_ context.Context) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []map[string]any
	for _, meta := range s.buckets {
		result = append(result, meta)
	}
	return result, nil
}

func (s *MemoryS3ObjectMetaStore) PutObjectMeta(_ context.Context, bucket, key string, meta ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects[bucket] == nil {
		s.objects[bucket] = make(map[string]ObjectMeta)
	}
	meta.Key = key
	if meta.LastModified.IsZero() {
		meta.LastModified = time.Now().UTC()
	}
	s.objects[bucket][key] = meta
	return nil
}

func (s *MemoryS3ObjectMetaStore) GetObjectMeta(_ context.Context, bucket, key string) (ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return ObjectMeta{}, fmt.Errorf("NoSuchBucket")
	}
	meta, ok := objs[key]
	if !ok {
		return ObjectMeta{}, fmt.Errorf("NoSuchKey")
	}
	return meta, nil
}

func (s *MemoryS3ObjectMetaStore) DeleteObjectMeta(_ context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects[bucket] != nil {
		delete(s.objects[bucket], key)
	}
	return nil
}

func (s *MemoryS3ObjectMetaStore) ListObjectMeta(_ context.Context, bucket, prefix, delimiter, marker string, maxKeys int) ([]ObjectMeta, []string, bool, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objs, ok := s.objects[bucket]
	if !ok {
		return nil, nil, false, "", fmt.Errorf("NoSuchBucket")
	}
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	// Collect and filter keys.
	var keys []string
	for k := range objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	// Sort keys for deterministic output.
	sortStrings(keys)

	// Apply marker (pagination).
	// If no key is strictly greater than the marker (marker is at or past the last
	// key), return an empty slice rather than wrapping around to the beginning.
	start := len(keys)
	if marker == "" {
		start = 0
	} else {
		for i, k := range keys {
			if k > marker {
				start = i
				break
			}
		}
	}
	keys = keys[start:]

	// Apply delimiter (common prefixes).
	// AWS counts both result keys and unique common prefixes toward maxKeys.
	commonPrefixes := map[string]bool{}
	var result []ObjectMeta
	truncated := false
	var lastExaminedKey string
	breakIdx := len(keys)
	for i, k := range keys {
		if len(result)+len(commonPrefixes) >= maxKeys {
			truncated = true
			breakIdx = i
			break
		}
		lastExaminedKey = k
		if delimiter != "" {
			// Find delimiter after prefix.
			rest := k[len(prefix):]
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				commonPrefixes[cp] = true
				continue
			}
		}
		result = append(result, objs[k])
	}

	var cpList []string
	for cp := range commonPrefixes {
		cpList = append(cpList, cp)
	}
	sortStrings(cpList)

	nextMarker := ""
	if truncated {
		// Advance nextMarker past all keys in the same common prefix group as
		// the last item on this page.  Without this, using nextMarker as the
		// continuation-token would re-emit the last common prefix on the next page.
		if delimiter != "" && len(cpList) > 0 {
			lastCP := cpList[len(cpList)-1]
			for _, k := range keys[breakIdx:] {
				if strings.HasPrefix(k, lastCP) {
					lastExaminedKey = k
				} else {
					break
				}
			}
		}
		nextMarker = lastExaminedKey
	}
	return result, cpList, truncated, nextMarker, nil
}

func sortStrings(ss []string) {
	n := len(ss)
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if ss[i] > ss[j] {
				ss[i], ss[j] = ss[j], ss[i]
			}
		}
	}
}

func (s *MemoryS3ObjectMetaStore) InitMultipart(_ context.Context, bucket, key, uploadID string, meta map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads[uploadID] = multipartUpload{
		Bucket: bucket, Key: key, Meta: meta,
		Parts: make(map[int]PartMeta),
	}
	return nil
}

func (s *MemoryS3ObjectMetaStore) PutPart(_ context.Context, uploadID string, partNumber int, part PartMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.uploads[uploadID]
	if !ok {
		return fmt.Errorf("NoSuchUpload")
	}
	u.Parts[partNumber] = part
	s.uploads[uploadID] = u
	return nil
}

func (s *MemoryS3ObjectMetaStore) CompleteMultipart(_ context.Context, bucket, key, uploadID string) ([]PartMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.uploads[uploadID]
	if !ok {
		return nil, fmt.Errorf("NoSuchUpload")
	}
	// Return parts in order.
	var parts []PartMeta
	for i := 1; i <= len(u.Parts); i++ {
		if p, ok := u.Parts[i]; ok {
			parts = append(parts, p)
		}
	}
	delete(s.uploads, uploadID)
	return parts, nil
}

func (s *MemoryS3ObjectMetaStore) AbortMultipart(_ context.Context, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.uploads, uploadID)
	return nil
}

func (s *MemoryS3ObjectMetaStore) GetMultipartMeta(_ context.Context, uploadID string) (string, string, map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.uploads[uploadID]
	if !ok {
		return "", "", nil, fmt.Errorf("NoSuchUpload")
	}
	return u.Bucket, u.Key, u.Meta, nil
}

func (s *MemoryS3ObjectMetaStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = make(map[string]map[string]any)
	s.objects = make(map[string]map[string]ObjectMeta)
	s.uploads = make(map[string]multipartUpload)
}
