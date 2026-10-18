package paging

import (
	"encoding/json"
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

// TestParseSizeRejectsNonInteger pins the int32 binding rule the bespoke REST
// pagers previously skipped: a pageSize that is not an integer (or is outside
// int32) is INVALID_ARGUMENT, never silently folded into "absent" — the same
// answer real GCP's query binding gives.
func TestParseSizeRejectsNonInteger(t *testing.T) {
	for _, v := range []any{
		"abc", "-0.5", "1.5", "1e3", " 5", "5 ",
		float64(-0.5), float64(1.5),
		"2147483648", "-2147483649",
		float64(2147483648),
		true,
	} {
		got, err := ParseSize(v)
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Errorf("ParseSize(%#v) error = %v, want a *model.ProviderError", v, err)
			continue
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("ParseSize(%#v) = %+v, want InvalidArgument/400", v, perr)
		}
		if got != 0 {
			t.Errorf("ParseSize(%#v) = %d, want 0 alongside the error", v, got)
		}
	}
	for _, tc := range []struct {
		v    any
		want int
	}{
		{nil, 0},
		{"", 0}, // an empty query value is treated as absent
		{"7", 7},
		{int(7), 7},
		{int32(7), 7},
		{int64(7), 7},
		{float64(7), 7},
		{json.Number("7"), 7},
		{"-3", -3},
		{"2147483647", 2147483647},
		{"-2147483648", -2147483648},
	} {
		got, err := ParseSize(tc.v)
		if err != nil || got != tc.want {
			t.Errorf("ParseSize(%#v) = %d, %v; want %d, nil", tc.v, got, err, tc.want)
		}
	}
}

// TestParseSizeNamedParam pins the named resolver the legacy `maxResults`
// pagers use: the binding rule is the same int32 parse, but the rejection
// message names the parameter the client actually sent, as the Google API
// frontend does, instead of always saying "pageSize".
func TestParseSizeNamedParam(t *testing.T) {
	for _, v := range []any{"abc", "1.5", "-0.5", "2147483648", 2147483648} {
		_, err := ParseSizeNamed(v, "maxResults")
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Errorf("ParseSizeNamed(%#v) error = %v, want a *model.ProviderError", v, err)
			continue
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("ParseSizeNamed(%#v) = %+v, want InvalidArgument/400", v, perr)
		}
		if perr.Message != "Invalid value for int32 type: maxResults" {
			t.Errorf("ParseSizeNamed(%#v) message = %q, want the named int32 binding message", v, perr.Message)
		}
	}
	if n, err := ParseSizeNamed("7", "maxResults"); err != nil || n != 7 {
		t.Errorf(`ParseSizeNamed("7", "maxResults") = %d, %v; want 7, nil`, n, err)
	}
	for _, tc := range []struct {
		v    any
		want int
	}{
		{nil, 0},
		{"", 0},
		{int32(7), 7},
		{int64(7), 7},
		{float64(7), 7},
		{json.Number("7"), 7},
		{"-3", -3}, // a legal int32; the caller keeps its non-positive policy
	} {
		got, err := ParseSizeNamed(tc.v, "maxResults")
		if err != nil || got != tc.want {
			t.Errorf("ParseSizeNamed(%#v) = %d, %v; want %d, nil", tc.v, got, err, tc.want)
		}
	}
}

// TestParseUintSizeNamed pins the uint32 binding Compute Engine and Cloud SQL's
// instances/operations lists declare for maxResults: a negative value is not a
// valid binding ("Invalid value for uint32 type"), the message names the
// parameter, and a value above MaxInt32 is still a legal uint32 (the caller's
// range policy then clamps it).
func TestParseUintSizeNamed(t *testing.T) {
	for _, v := range []any{
		"abc", "1.5", "-1", "-0.5", "4294967296", float64(1.5), float64(-1),
		int64(-1), int32(-1),
	} {
		_, err := ParseUintSizeNamed(v, "maxResults")
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Errorf("ParseUintSizeNamed(%#v) error = %v, want a *model.ProviderError", v, err)
			continue
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("ParseUintSizeNamed(%#v) = %+v, want InvalidArgument/400", v, perr)
		}
		if perr.Message != "Invalid value for uint32 type: maxResults" {
			t.Errorf("ParseUintSizeNamed(%#v) message = %q, want the named uint32 binding message", v, perr.Message)
		}
	}
	for _, tc := range []struct {
		v    any
		want int
	}{
		{nil, 0},
		{"", 0},
		{"0", 0},
		{"500", 500},
		{"2147483648", 2147483648}, // above int32, still a legal uint32
		{"4294967295", 4294967295},
		{int(3), 3},
	} {
		got, err := ParseUintSizeNamed(tc.v, "maxResults")
		if err != nil || got != tc.want {
			t.Errorf("ParseUintSizeNamed(%#v) = %d, %v; want %d, nil", tc.v, got, err, tc.want)
		}
	}
}

// TestMaxResultsParam pins the legacy maxResults resolver the bespoke pagers
// share: it reads the declared parameter before the emulator's pageSize alias,
// rejects a value the declared binding cannot parse (naming the parameter), and
// falls back to the caller's default for an absent, empty, zero or non-positive
// value — the legacy parameter documents no minimum, so a negative keeps the
// default page rather than inventing an AIP-158 400.
func TestMaxResultsParam(t *testing.T) {
	keys := []string{"maxResults", "pageSize"}
	for _, tc := range []struct {
		label  string
		params map[string]any
		want   int
	}{
		{"absent", nil, 100},
		{"empty", map[string]any{"maxResults": ""}, 100},
		{"zero", map[string]any{"maxResults": "0"}, 100},
		{"negative", map[string]any{"maxResults": "-1"}, 100},
		{"value", map[string]any{"maxResults": "2"}, 2},
		{"alias", map[string]any{"pageSize": "3"}, 3},
		// maxResults wins over the alias, as it did before the binding check.
		{"both", map[string]any{"maxResults": "2", "pageSize": "3"}, 2},
	} {
		got, err := MaxResultsInt32(tc.params, keys, 100)
		if err != nil || got != tc.want {
			t.Errorf("%s: MaxResultsInt32(%v) = %d, %v; want %d, nil", tc.label, tc.params, got, err, tc.want)
		}
	}
	for _, v := range []any{"abc", "1.5", "-0.5", "2147483648"} {
		_, err := MaxResultsInt32(map[string]any{"maxResults": v}, keys, 100)
		var perr *model.ProviderError
		if !errors.As(err, &perr) || perr.Message != "Invalid value for int32 type: maxResults" {
			t.Errorf("MaxResultsInt32(maxResults=%#v) = %v, want the named int32 binding error", v, err)
		}
	}
	// The unsigned form rejects a negative value as an illegal binding and
	// accepts a value above int32.
	if _, err := MaxResultsUint32(map[string]any{"maxResults": "-1"}, keys, 500); err == nil {
		t.Error("MaxResultsUint32(maxResults=-1) = nil error, want the uint32 binding error")
	}
	if got, err := MaxResultsUint32(map[string]any{"maxResults": "3000000000"}, keys, 500); err != nil || got != 3000000000 {
		t.Errorf("MaxResultsUint32(maxResults=3000000000) = %d, %v; want 3000000000, nil", got, err)
	}
	if got, err := MaxResultsUint32(nil, keys, 500); err != nil || got != 500 {
		t.Errorf("MaxResultsUint32(absent) = %d, %v; want the 500 default", got, err)
	}
}

// TestSizeFromBespokePager pins SizeFrom: the response to a raw query value
// that a bespoke pager hands straight in — a non-integer is rejected, while
// zero/absent selects the documented maximum and a negative value keeps the
// AIP-158 negative error.
func TestSizeFromBespokePager(t *testing.T) {
	for _, v := range []any{"abc", "-0.5", "1.5", 1.5} {
		if _, err := SizeFrom(v, 500); err == nil {
			t.Errorf("SizeFrom(%#v, 500) = nil error, want InvalidArgument", v)
		}
	}
	for _, tc := range []struct {
		v    any
		max  int
		want int
	}{
		{nil, 500, 500},
		{"0", 500, 500},
		{"250", 500, 250},
		{"501", 500, 500},
		{float64(42), 500, 42},
	} {
		got, err := SizeFrom(tc.v, tc.max)
		if err != nil || got != tc.want {
			t.Errorf("SizeFrom(%#v, %d) = %d, %v; want %d, nil", tc.v, tc.max, got, err, tc.want)
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
		// A negative fraction is rejected by the int32 binding ("invalid value")
		// before the sign rule is reached; either way it is not truncated to 0.
		{"pageSize": float64(-0.5)},
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
