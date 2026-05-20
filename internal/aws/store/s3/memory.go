package s3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MemoryS3ObjectMetaStore is an in-memory S3ObjectMetaStore.
type MemoryS3ObjectMetaStore struct {
	mu               sync.RWMutex
	buckets          map[string]map[string]any        // bucketName → meta
	objects          map[string]map[string]ObjectMeta // bucketName → key → meta
	uploads          map[string]multipartUpload       // uploadID → upload
	versions         map[string]map[string][]ObjectMeta // bucket → key → versions (newest first)
	versioningStatus map[string]string                  // bucket → "" | "Enabled" | "Suspended"
}

type multipartUpload struct {
	Bucket    string
	Key       string
	Meta      map[string]any
	Parts     map[int]PartMeta
	Initiated time.Time
}

func NewMemoryS3ObjectMetaStore() *MemoryS3ObjectMetaStore {
	return &MemoryS3ObjectMetaStore{
		buckets:          make(map[string]map[string]any),
		objects:          make(map[string]map[string]ObjectMeta),
		uploads:          make(map[string]multipartUpload),
		versions:         make(map[string]map[string][]ObjectMeta),
		versioningStatus: make(map[string]string),
	}
}

func newVersionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *MemoryS3ObjectMetaStore) UpdateBucketMeta(_ context.Context, bucket string, meta map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket]; !exists {
		return fmt.Errorf("NoSuchBucket")
	}
	s.buckets[bucket] = meta
	return nil
}

func (s *MemoryS3ObjectMetaStore) GetBucketVersioning(_ context.Context, bucket string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.buckets[bucket]; !ok {
		return "", fmt.Errorf("NoSuchBucket")
	}
	return s.versioningStatus[bucket], nil
}

func (s *MemoryS3ObjectMetaStore) SetBucketVersioning(_ context.Context, bucket, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket]; !exists {
		return fmt.Errorf("NoSuchBucket")
	}
	s.versioningStatus[bucket] = status
	if s.versions[bucket] == nil {
		s.versions[bucket] = make(map[string][]ObjectMeta)
	}
	return nil
}

func (s *MemoryS3ObjectMetaStore) PutObjectVersion(_ context.Context, bucket, key string, meta ObjectMeta) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket]; !exists {
		return "", fmt.Errorf("NoSuchBucket")
	}
	if s.versions[bucket] == nil {
		s.versions[bucket] = make(map[string][]ObjectMeta)
	}
	vID := meta.VersionID
	if vID == "" {
		vID = newVersionID()
	}
	meta.Key = key
	meta.VersionID = vID
	if meta.LastModified.IsZero() {
		meta.LastModified = time.Now().UTC()
	}
	// Mark all existing versions as not-latest.
	existing := s.versions[bucket][key]
	for i := range existing {
		existing[i].IsLatest = false
	}
	meta.IsLatest = true
	// Prepend (newest first).
	s.versions[bucket][key] = append([]ObjectMeta{meta}, existing...)
	return vID, nil
}

func (s *MemoryS3ObjectMetaStore) UpdateObjectVersion(_ context.Context, bucket, key string, meta ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := s.versions[bucket][key]
	for i, v := range vs {
		if v.VersionID == meta.VersionID {
			vs[i] = meta
			s.versions[bucket][key] = vs
			return nil
		}
	}
	return fmt.Errorf("NoSuchVersion")
}

func (s *MemoryS3ObjectMetaStore) GetObjectVersion(_ context.Context, bucket, key, versionID string) (ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.versions[bucket][key] {
		if v.VersionID == versionID {
			return v, nil
		}
	}
	return ObjectMeta{}, fmt.Errorf("NoSuchVersion")
}

func (s *MemoryS3ObjectMetaStore) DeleteObjectVersion(_ context.Context, bucket, key, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := s.versions[bucket][key]
	for i, v := range vs {
		if v.VersionID == versionID {
			s.versions[bucket][key] = append(vs[:i], vs[i+1:]...)
			// If we deleted the latest, mark next as latest.
			remaining := s.versions[bucket][key]
			if len(remaining) > 0 {
				remaining[0].IsLatest = true
				s.versions[bucket][key] = remaining
			}
			return nil
		}
	}
	return fmt.Errorf("NoSuchVersion")
}

func (s *MemoryS3ObjectMetaStore) ListObjectVersions(_ context.Context, bucket, prefix, keyMarker, _ string, maxKeys int) ([]ObjectMeta, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	var keys []string
	for k := range s.versions[bucket] {
		if strings.HasPrefix(k, prefix) && k > keyMarker {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var result []ObjectMeta
	for _, k := range keys {
		result = append(result, s.versions[bucket][k]...)
		if len(result) >= maxKeys {
			return result[:maxKeys], true, nil
		}
	}
	return result, false, nil
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

func (s *MemoryS3ObjectMetaStore) ListBuckets(_ context.Context, accountID string) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []map[string]any
	for _, meta := range s.buckets {
		if accountID != "" {
			owner, _ := meta["AccountID"].(string)
			if owner != accountID {
				continue
			}
		}
		result = append(result, meta)
	}
	return result, nil
}

func (s *MemoryS3ObjectMetaStore) PutObjectMeta(_ context.Context, bucket, key string, meta ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket]; !exists {
		return fmt.Errorf("NoSuchBucket")
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
		Parts:     make(map[int]PartMeta),
		Initiated: time.Now().UTC(),
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
	// Return parts in ascending part-number order.
	// Sort explicit part numbers to handle non-contiguous uploads (e.g. parts 1, 3, 5).
	nums := make([]int, 0, len(u.Parts))
	for n := range u.Parts {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	parts := make([]PartMeta, 0, len(nums))
	for _, n := range nums {
		parts = append(parts, u.Parts[n])
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

func (s *MemoryS3ObjectMetaStore) ListParts(_ context.Context, uploadID string) ([]PartMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.uploads[uploadID]
	if !ok {
		return nil, fmt.Errorf("NoSuchUpload")
	}
	nums := make([]int, 0, len(u.Parts))
	for n := range u.Parts {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	parts := make([]PartMeta, 0, len(nums))
	for _, n := range nums {
		parts = append(parts, u.Parts[n])
	}
	return parts, nil
}

func (s *MemoryS3ObjectMetaStore) ListActiveUploads(_ context.Context, bucket string) ([]ActiveUpload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []ActiveUpload
	for id, u := range s.uploads {
		if u.Bucket == bucket {
			result = append(result, ActiveUpload{
				Bucket:    u.Bucket,
				Key:       u.Key,
				UploadID:  id,
				Initiated: u.Initiated,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Key != result[j].Key {
			return result[i].Key < result[j].Key
		}
		return result[i].UploadID < result[j].UploadID
	})
	return result, nil
}

func (s *MemoryS3ObjectMetaStore) Reset(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = make(map[string]map[string]any)
	s.objects = make(map[string]map[string]ObjectMeta)
	s.uploads = make(map[string]multipartUpload)
	s.versions = make(map[string]map[string][]ObjectMeta)
	s.versioningStatus = make(map[string]string)
}

// ─── Snapshotter ─────────────────────────────────────────────────────────────

type s3MultipartSnap struct {
	Bucket    string             `json:"bucket"`
	Key       string             `json:"key"`
	Meta      map[string]any     `json:"meta"`
	Parts     map[string]PartMeta `json:"parts"`
	Initiated time.Time          `json:"initiated"`
}

type s3MemSnap struct {
	Buckets          map[string]map[string]any          `json:"buckets"`
	Objects          map[string]map[string]ObjectMeta   `json:"objects"`
	Uploads          map[string]s3MultipartSnap         `json:"uploads"`
	Versions         map[string]map[string][]ObjectMeta `json:"versions"`
	VersioningStatus map[string]string                  `json:"versioning_status"`
}

func (s *MemoryS3ObjectMetaStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.buckets) == 0, nil
}

func (s *MemoryS3ObjectMetaStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	uploads := make(map[string]s3MultipartSnap, len(s.uploads))
	for id, u := range s.uploads {
		parts := make(map[string]PartMeta, len(u.Parts))
		for n, p := range u.Parts {
			parts[strconv.Itoa(n)] = p
		}
		uploads[id] = s3MultipartSnap{Bucket: u.Bucket, Key: u.Key, Meta: u.Meta, Parts: parts, Initiated: u.Initiated}
	}
	return json.NewEncoder(w).Encode(s3MemSnap{
		Buckets:          s.buckets,
		Objects:          s.objects,
		Uploads:          uploads,
		Versions:         s.versions,
		VersioningStatus: s.versioningStatus,
	})
}

func (s *MemoryS3ObjectMetaStore) Restore(_ context.Context, r io.Reader) error {
	var snap s3MemSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Buckets == nil {
		snap.Buckets = make(map[string]map[string]any)
	}
	if snap.Objects == nil {
		snap.Objects = make(map[string]map[string]ObjectMeta)
	}
	uploads := make(map[string]multipartUpload, len(snap.Uploads))
	for id, u := range snap.Uploads {
		parts := make(map[int]PartMeta, len(u.Parts))
		for ns, p := range u.Parts {
			if n, err := strconv.Atoi(ns); err == nil {
				parts[n] = p
			}
		}
		uploads[id] = multipartUpload{Bucket: u.Bucket, Key: u.Key, Meta: u.Meta, Parts: parts, Initiated: u.Initiated}
	}
	s.buckets = snap.Buckets
	s.objects = snap.Objects
	s.uploads = uploads
	if snap.Versions != nil {
		s.versions = snap.Versions
	} else {
		s.versions = make(map[string]map[string][]ObjectMeta)
	}
	if snap.VersioningStatus != nil {
		s.versioningStatus = snap.VersioningStatus
	} else {
		s.versioningStatus = make(map[string]string)
	}
	return nil
}
