package s3

import (
	"context"
	"testing"
	"time"
)

func setupBucketWithObjects(t *testing.T, store S3ObjectMetaStore, bucket string, keys []string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, bucket, nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for i, k := range keys {
		err := store.PutObjectMeta(ctx, bucket, k, ObjectMeta{
			Key:          k,
			ETag:         "etag",
			Size:         int64(i + 1),
			LastModified: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("PutObjectMeta %q: %v", k, err)
		}
	}
}

// TestListObjectMeta_NoTruncation verifies that when total items < maxKeys,
// truncated is false and all items are returned.
func TestListObjectMeta_NoTruncation(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"a/1", "a/2", "b/1"})

	objs, cps, truncated, nextMarker, err := store.ListObjectMeta(context.Background(), "b", "", "", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if truncated {
		t.Error("expected truncated=false, got true")
	}
	if nextMarker != "" {
		t.Errorf("expected empty nextMarker when not truncated, got %q", nextMarker)
	}
	if len(objs) != 3 {
		t.Errorf("expected 3 objects, got %d", len(objs))
	}
	if len(cps) != 0 {
		t.Errorf("expected 0 common prefixes, got %d", len(cps))
	}
}

// TestListObjectMeta_KeysOnlyTruncation verifies keys-only (no delimiter) truncation:
// when keys exceed maxKeys, truncated=true and only maxKeys items returned.
func TestListObjectMeta_KeysOnlyTruncation(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"k1", "k2", "k3", "k4", "k5"})

	objs, cps, truncated, nextMarker, err := store.ListObjectMeta(context.Background(), "b", "", "", "", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !truncated {
		t.Error("expected truncated=true, got false")
	}
	if len(objs) != 3 {
		t.Errorf("expected 3 objects, got %d", len(objs))
	}
	if len(cps) != 0 {
		t.Errorf("expected 0 common prefixes, got %d", len(cps))
	}
	// nextMarker must be the last key returned so next page starts after it.
	if nextMarker != "k3" {
		t.Errorf("expected nextMarker=%q, got %q", "k3", nextMarker)
	}
}

// TestListObjectMeta_CommonPrefixesCountTowardMaxKeys is the core bug scenario.
// With delimiter "/", common prefixes must count toward maxKeys.
// Keys: a/1, a/2, b/1, b/2, c/1  — maxKeys=2
// Expected: 2 common prefixes (a/, b/), 0 result keys, truncated=true.
// The old code counted only result keys and would wrongly return all 3 prefixes with truncated=false.
func TestListObjectMeta_CommonPrefixesCountTowardMaxKeys(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"a/1", "a/2", "b/1", "b/2", "c/1"})

	objs, cps, truncated, nextMarker, err := store.ListObjectMeta(context.Background(), "b", "", "/", "", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !truncated {
		t.Error("expected truncated=true (common prefixes fill maxKeys), got false")
	}
	if len(objs) != 0 {
		t.Errorf("expected 0 result objects, got %d", len(objs))
	}
	if len(cps) != 2 {
		t.Errorf("expected 2 common prefixes (a/, b/), got %d: %v", len(cps), cps)
	}
	if cps[0] != "a/" || cps[1] != "b/" {
		t.Errorf("expected [a/, b/], got %v", cps)
	}
	// nextMarker must be "b/2" — the last raw key examined — so the next page starts at c/1.
	if nextMarker != "b/2" {
		t.Errorf("expected nextMarker=%q (last examined key), got %q", "b/2", nextMarker)
	}
}

// TestListObjectMeta_MixedKeysAndCommonPrefixes verifies that a mix of bare keys and
// common prefixes combined fill maxKeys correctly.
// Keys: a/1, b, c/1, d  — delimiter "/", maxKeys=3
// Sorted: a/1, b, c/1, d → a/ (prefix), b (key), c/ (prefix) = 3 total → truncated, d not returned.
func TestListObjectMeta_MixedKeysAndCommonPrefixes(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"a/1", "b", "c/1", "d"})

	objs, cps, truncated, nextMarker, err := store.ListObjectMeta(context.Background(), "b", "", "/", "", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !truncated {
		t.Error("expected truncated=true (3 items fill maxKeys, d excluded), got false")
	}
	// a/ (prefix) + b (key) + c/ (prefix) = 3 total → stop before d
	if len(objs)+len(cps) != 3 {
		t.Errorf("expected total 3 (keys+prefixes), got %d+%d", len(objs), len(cps))
	}
	for _, obj := range objs {
		if obj.Key == "d" {
			t.Error("d should not appear — it is beyond the maxKeys boundary")
		}
	}
	// c/1 is the last examined key (contributes to prefix c/).
	if nextMarker != "c/1" {
		t.Errorf("expected nextMarker=%q, got %q", "c/1", nextMarker)
	}
}

// TestListObjectMeta_ExactBoundary verifies that when total items == maxKeys, truncated=false.
// Keys: a/1, b/1 — delimiter "/", maxKeys=2 → exactly 2 common prefixes (a/, b/) → truncated=false.
func TestListObjectMeta_ExactBoundary(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"a/1", "b/1"})

	objs, cps, truncated, nextMarker, err := store.ListObjectMeta(context.Background(), "b", "", "/", "", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if truncated {
		t.Error("expected truncated=false (exactly maxKeys items), got true")
	}
	if nextMarker != "" {
		t.Errorf("expected empty nextMarker when not truncated, got %q", nextMarker)
	}
	if len(objs) != 0 {
		t.Errorf("expected 0 result objects, got %d", len(objs))
	}
	if len(cps) != 2 {
		t.Errorf("expected 2 common prefixes, got %d", len(cps))
	}
}

// TestListObjectMeta_NextMarkerPaginationWithDelimiter verifies end-to-end pagination
// using nextMarker as the continuation token across pages with a delimiter.
// This exercises the fix for: without nextMarker, ContinuationToken-based pagination
// would loop forever returning the first page.
func TestListObjectMeta_NextMarkerPaginationWithDelimiter(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	// 4 top-level "directories", 2 keys each
	setupBucketWithObjects(t, store, "b", []string{
		"a/1", "a/2", "b/1", "b/2", "c/1", "c/2", "d/1", "d/2",
	})
	ctx := context.Background()

	var allPrefixes []string
	marker := ""
	for {
		_, cps, truncated, nextMarker, err := store.ListObjectMeta(ctx, "b", "", "/", marker, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		allPrefixes = append(allPrefixes, cps...)
		if !truncated {
			break
		}
		if nextMarker == "" {
			t.Fatal("truncated=true but nextMarker is empty — would cause infinite loop")
		}
		marker = nextMarker
	}

	expected := []string{"a/", "b/", "c/", "d/"}
	if len(allPrefixes) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, allPrefixes)
	}
	for i, p := range allPrefixes {
		if p != expected[i] {
			t.Errorf("page position %d: expected %q, got %q", i, expected[i], p)
		}
	}
}

// TestListObjectMeta_MarkerPaginationContinuity verifies that marker-based pagination
// correctly continues from a truncated page without repeating or skipping items.
func TestListObjectMeta_MarkerPaginationContinuity(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	// 6 keys, no delimiter, page size 2 → 3 pages
	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6"}
	setupBucketWithObjects(t, store, "b", keys)
	ctx := context.Background()

	var allKeys []string
	marker := ""
	for {
		objs, _, truncated, nextMarker, err := store.ListObjectMeta(ctx, "b", "", "", marker, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, o := range objs {
			allKeys = append(allKeys, o.Key)
		}
		if !truncated {
			break
		}
		marker = nextMarker
	}

	if len(allKeys) != 6 {
		t.Errorf("expected 6 keys across pages, got %d: %v", len(allKeys), allKeys)
	}
	for i, k := range allKeys {
		if k != keys[i] {
			t.Errorf("page position %d: expected %q, got %q", i, keys[i], k)
		}
	}
}

// TestListObjectMeta_PrefixFilter verifies that only keys matching the prefix are returned.
func TestListObjectMeta_PrefixFilter(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"logs/2024/a", "logs/2025/b", "data/c"})

	objs, _, truncated, _, err := store.ListObjectMeta(context.Background(), "b", "logs/", "", "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if truncated {
		t.Error("expected truncated=false")
	}
	if len(objs) != 2 {
		t.Errorf("expected 2 objects with prefix logs/, got %d", len(objs))
	}
}

// TestListObjectMeta_DelimiterWithPrefix verifies prefix + delimiter combined
// (the common Hadoop/Spark "list directory" pattern).
// Keys: prefix/dir1/file1, prefix/dir2/file2, prefix/file3
// Query: prefix="prefix/", delimiter="/", maxKeys=10
// Expected: 1 key (prefix/file3), 2 common prefixes (prefix/dir1/, prefix/dir2/).
func TestListObjectMeta_DelimiterWithPrefix(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{
		"prefix/dir1/file1",
		"prefix/dir2/file2",
		"prefix/file3",
	})

	objs, cps, truncated, _, err := store.ListObjectMeta(context.Background(), "b", "prefix/", "/", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if truncated {
		t.Error("expected truncated=false")
	}
	if len(objs) != 1 || objs[0].Key != "prefix/file3" {
		t.Errorf("expected [prefix/file3], got %v", objs)
	}
	if len(cps) != 2 {
		t.Errorf("expected 2 common prefixes, got %d: %v", len(cps), cps)
	}
}

// TestListObjectMeta_TruncatedAfterPrefixAndDelimiter covers a variant of the core bug
// where a mix of prefix and delimiter narrows the key set but common prefixes still fill
// maxKeys before all items are returned.
// Keys: top/a/1, top/b/1, top/c/1, top/c/2 — prefix="top/", delimiter="/", maxKeys=2
// Expected: 2 common prefixes (top/a/, top/b/), truncated=true (top/c/ not returned).
func TestListObjectMeta_TruncatedAfterPrefixAndDelimiter(t *testing.T) {
	store := NewMemoryS3ObjectMetaStore()
	setupBucketWithObjects(t, store, "b", []string{"top/a/1", "top/b/1", "top/c/1", "top/c/2"})

	objs, cps, truncated, nextMarker, err := store.ListObjectMeta(context.Background(), "b", "top/", "/", "", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !truncated {
		t.Errorf("expected truncated=true, got false (cps=%v, objs=%v)", cps, objs)
	}
	if len(objs) != 0 {
		t.Errorf("expected 0 result objects, got %d", len(objs))
	}
	if len(cps) != 2 {
		t.Errorf("expected 2 common prefixes, got %d: %v", len(cps), cps)
	}
	// top/b/1 is the last examined key; next page should start at top/c/*.
	if nextMarker != "top/b/1" {
		t.Errorf("expected nextMarker=%q, got %q", "top/b/1", nextMarker)
	}
}
