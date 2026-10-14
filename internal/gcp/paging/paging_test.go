package paging

import (
	"testing"

	"jaiscloud/internal/store"
)

func TestPagePagination(t *testing.T) {
	var entries []store.ResourceEntry
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		entries = append(entries, store.ResourceEntry{ID: id})
	}

	// Page 1 of size 2.
	page, next := Apply(entries, map[string]any{"pageSize": "2"})
	if len(page) != 2 || page[0].ID != "a" || page[1].ID != "b" {
		t.Fatalf("page 1 = %+v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken")
	}

	// Page 2.
	page, next = Apply(entries, map[string]any{"pageSize": "2", "pageToken": next})
	if len(page) != 2 || page[0].ID != "c" || page[1].ID != "d" {
		t.Fatalf("page 2 = %+v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken on page 2")
	}

	// Page 3 (final).
	page, next = Apply(entries, map[string]any{"pageSize": "2", "pageToken": next})
	if len(page) != 1 || page[0].ID != "e" {
		t.Fatalf("page 3 = %+v", page)
	}
	if next != "" {
		t.Fatalf("expected empty nextPageToken on final page, got %q", next)
	}
}

func TestPageGeneric(t *testing.T) {
	items := []string{"z", "a", "m"}
	page, next := Page(items, func(s string) string { return s }, map[string]any{"pageSize": "2"})
	if len(page) != 2 || page[0] != "a" || page[1] != "m" {
		t.Fatalf("generic page = %v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken")
	}
}

// TestPageOrderedPagination covers the order-aware variant: the caller owns the
// ordering, the token is the caller's cursor value, and the position the token
// denotes is honored even when the item it was minted from is gone.
func TestPageOrderedPagination(t *testing.T) {
	// Descending order, cursor is the bare key.
	items := []string{"e", "d", "c", "b", "a"}
	cursor := func(s string) string { return s }
	after := func(s, tok string) bool { return s < tok }

	page, next := PageOrdered(items, map[string]any{"pageSize": "2"}, cursor, after)
	if len(page) != 2 || page[0] != "e" || page[1] != "d" || next == "" {
		t.Fatalf("page 1 = %v, next=%q", page, next)
	}
	page, next = PageOrdered(items, map[string]any{"pageSize": "2", "pageToken": next}, cursor, after)
	if len(page) != 2 || page[0] != "c" || page[1] != "b" || next == "" {
		t.Fatalf("page 2 = %v, next=%q", page, next)
	}
	page, next = PageOrdered(items, map[string]any{"pageSize": "2", "pageToken": next}, cursor, after)
	if len(page) != 1 || page[0] != "a" || next != "" {
		t.Fatalf("page 3 = %v, next=%q", page, next)
	}

	// Resuming from a token whose origin item is absent still returns exactly
	// the following items (no repeats, no skips).
	cursorToken := encodeCursor("c")
	page, _ = PageOrdered([]string{"e", "d", "b", "a"}, map[string]any{"pageSize": "2", "pageToken": cursorToken}, cursor, after)
	if len(page) != 2 || page[0] != "b" || page[1] != "a" {
		t.Fatalf("resume after a deleted origin = %v", page)
	}
}

func TestPageSizeFallback(t *testing.T) {
	if got := PageSize(map[string]any{"pageSize": "0"}); got != 1000 {
		t.Errorf("expected default 1000 for non-positive size, got %d", got)
	}
	if got := PageSize(map[string]any{"pageSize": "7"}); got != 7 {
		t.Errorf("expected 7, got %d", got)
	}
}
