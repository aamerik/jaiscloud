package paging

import (
	"errors"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func TestPagePagination(t *testing.T) {
	var entries []store.ResourceEntry
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		entries = append(entries, store.ResourceEntry{ID: id})
	}

	// Page 1 of size 2.
	page, next, err := Apply(entries, map[string]any{"pageSize": "2"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(page) != 2 || page[0].ID != "a" || page[1].ID != "b" {
		t.Fatalf("page 1 = %+v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken")
	}

	// Page 2.
	page, next, err = Apply(entries, map[string]any{"pageSize": "2", "pageToken": next})
	if err != nil {
		t.Fatalf("Apply page 2: %v", err)
	}
	if len(page) != 2 || page[0].ID != "c" || page[1].ID != "d" {
		t.Fatalf("page 2 = %+v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken on page 2")
	}

	// Page 3 (final).
	page, next, err = Apply(entries, map[string]any{"pageSize": "2", "pageToken": next})
	if err != nil {
		t.Fatalf("Apply page 3: %v", err)
	}
	if len(page) != 1 || page[0].ID != "e" {
		t.Fatalf("page 3 = %+v", page)
	}
	if next != "" {
		t.Fatalf("expected empty nextPageToken on final page, got %q", next)
	}
}

func TestPageGeneric(t *testing.T) {
	items := []string{"z", "a", "m"}
	page, next, err := Page(items, func(s string) string { return s }, map[string]any{"pageSize": "2"})
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
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

	page, next, err := PageOrdered(items, map[string]any{"pageSize": "2"}, cursor, after)
	if err != nil {
		t.Fatalf("PageOrdered page 1: %v", err)
	}
	if len(page) != 2 || page[0] != "e" || page[1] != "d" || next == "" {
		t.Fatalf("page 1 = %v, next=%q", page, next)
	}
	page, next, err = PageOrdered(items, map[string]any{"pageSize": "2", "pageToken": next}, cursor, after)
	if err != nil {
		t.Fatalf("PageOrdered page 2: %v", err)
	}
	if len(page) != 2 || page[0] != "c" || page[1] != "b" || next == "" {
		t.Fatalf("page 2 = %v, next=%q", page, next)
	}
	page, next, err = PageOrdered(items, map[string]any{"pageSize": "2", "pageToken": next}, cursor, after)
	if err != nil {
		t.Fatalf("PageOrdered page 3: %v", err)
	}
	if len(page) != 1 || page[0] != "a" || next != "" {
		t.Fatalf("page 3 = %v, next=%q", page, next)
	}

	// Resuming from a token whose origin item is absent still returns exactly
	// the following items (no repeats, no skips).
	cursorToken := encodeCursor("c")
	page, _, err = PageOrdered([]string{"e", "d", "b", "a"}, map[string]any{"pageSize": "2", "pageToken": cursorToken}, cursor, after)
	if err != nil {
		t.Fatalf("PageOrdered resume: %v", err)
	}
	if len(page) != 2 || page[0] != "b" || page[1] != "a" {
		t.Fatalf("resume after a deleted origin = %v", page)
	}
}

func TestPageSizeFallback(t *testing.T) {
	for _, params := range []map[string]any{
		{"pageSize": "0"},
		{"pageSize": 0},
		{"pageSize": float64(0)},
		{}, // absent
		{"pageSize": "not-a-number"},
	} {
		got, err := PageSize(params)
		if err != nil {
			t.Errorf("PageSize(%v) error = %v, want nil", params, err)
			continue
		}
		if got != defaultPageSize {
			t.Errorf("PageSize(%v) = %d, want default %d", params, got, defaultPageSize)
		}
	}
	for _, params := range []map[string]any{
		{"pageSize": "7"},
		{"pageSize": 7},
		{"pageSize": float64(7)},
	} {
		got, err := PageSize(params)
		if err != nil || got != 7 {
			t.Errorf("PageSize(%v) = %d, %v; want 7, nil", params, got, err)
		}
	}
}

// TestSizeBespokePager pins the rule a bespoke pager applies to its own parsed
// pageSize: zero (an absent parameter) and an over-large value both select the
// documented maximum, while a negative value is INVALID_ARGUMENT (AIP-158).
func TestSizeBespokePager(t *testing.T) {
	for _, tc := range []struct{ n, max, want int }{
		{0, 500, 500},   // absent / zero → the maximum
		{250, 500, 250}, // in range passes through
		{500, 500, 500}, // exactly the maximum
		{501, 500, 500}, // over-large coerces down
		{100000, 9800, 9800},
		{0, 0, 1}, // a non-positive ceiling still yields a usable page size
	} {
		got, err := Size(tc.n, tc.max)
		if err != nil || got != tc.want {
			t.Errorf("Size(%d, %d) = %d, %v; want %d, nil", tc.n, tc.max, got, err, tc.want)
		}
	}
	for _, n := range []int{-1, -500} {
		got, err := Size(n, 500)
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Fatalf("Size(%d, 500) error = %v, want a *model.ProviderError", n, err)
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("Size(%d, 500) = %+v, want InvalidArgument/400", n, perr)
		}
		if got != 0 {
			t.Errorf("Size(%d, 500) = %d, want 0 alongside the error", n, got)
		}
	}
}

// TestPageSizeNegativeRejected pins AIP-158: a negative pageSize is
// INVALID_ARGUMENT rather than a silent fall-back to the default.
func TestPageSizeNegativeRejected(t *testing.T) {
	for _, params := range []map[string]any{
		{"pageSize": "-1"},
		{"pageSize": -1},
		{"pageSize": float64(-1)},
		{"pageSize": float64(-0.5)}, // sign is checked before truncation
	} {
		_, err := PageSize(params)
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Fatalf("PageSize(%v) error = %v, want a *model.ProviderError", params, err)
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("PageSize(%v) = %+v, want InvalidArgument/400", params, perr)
		}
	}

	// The page entry points surface it too, and must not return a page.
	items := []string{"a", "b"}
	if page, next, err := Page(items, func(s string) string { return s }, map[string]any{"pageSize": "-1"}); err == nil || page != nil || next != "" {
		t.Errorf("Page(negative) = %v, %q, %v; want nil, empty, error", page, next, err)
	}
	if page, next, err := PageOrdered(items, map[string]any{"pageSize": -1}, func(s string) string { return s }, func(s, tok string) bool { return s > tok }); err == nil || page != nil || next != "" {
		t.Errorf("PageOrdered(negative) = %v, %q, %v; want nil, empty, error", page, next, err)
	}
	if page, next, err := Apply([]store.ResourceEntry{{ID: "a"}}, map[string]any{"pageSize": "-1"}); err == nil || page != nil || next != "" {
		t.Errorf("Apply(negative) = %v, %q, %v; want nil, empty, error", page, next, err)
	}
}
