// Package paging provides the cursor-based pagination shared by the GCP v1
// service providers (Pub/Sub, Secret Manager, KMS, IAM). It honors the
// pageSize/pageToken query parameters and emits nextPageToken, matching the
// GCP REST convention (distinct from GCS's maxResults).
//
// The pageSize contract follows AIP-158: an absent or zero pageSize selects the
// server default, an over-large one is a suggestion the server may return fewer
// than, and a negative one is INVALID_ARGUMENT. The last rule is the reason
// every entry point here returns an error: a caller must surface it to the
// client rather than silently falling back to the default. A list method whose
// Discovery document instead documents that a non-positive value is ignored or
// selects the default — Cloud Logging's non-positive-ignored methods
// (logs.list, monitoredResourceDescriptors.list, metrics.list, …) and Cloud
// Run's projects.locations.operations.list ("If not set or set to less than or
// equal to 0, the default page size is 100") — must clear a negative size
// before calling Page/PageOrdered, not lean on the default. A list method with
// a bespoke cursor (Cloud Workflows ListWorkflowRevisions) validates the rule
// itself.
package paging

import (
	"encoding/base64"
	"sort"
	"strconv"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// defaultPageSize is the page size applied when the request omits pageSize or
// sets it to zero, matching the shared GCP default.
const defaultPageSize = 1000

// Apply returns the page of ResourceEntry values for the current request and
// the next-page token (empty when there are no more results). A negative
// pageSize is an InvalidArgument error.
func Apply(entries []store.ResourceEntry, params map[string]any) (page []store.ResourceEntry, nextToken string, err error) {
	return Page(entries, func(e store.ResourceEntry) string { return e.ID }, params)
}

// Page is the generic cursor-pagination core: it sorts items by key and
// applies pageSize/pageToken cursor pagination, returning the page and the
// next-page token (empty when there are no more results). A negative pageSize
// is an InvalidArgument error.
func Page[T any](items []T, key func(T) string, params map[string]any) (page []T, nextToken string, err error) {
	size, err := PageSize(params)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })

	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cursor := decodeCursor(tok); cursor != "" {
			for start < len(items) && key(items[start]) <= cursor {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextToken = encodeCursor(key(items[end-1]))
	}
	return page, nextToken, nil
}

// PageOrdered applies pageSize/pageToken cursor pagination to items the caller
// has already ordered. cursor returns an item's cursor payload (the value that
// identifies its position, before the page token's base64 encoding) and
// afterCursor reports whether item sorts strictly after the position a decoded
// token denotes. Unlike Page it never sorts, so a non-name orderBy (a service
// list whose caller supplied orderBy) composes with pagination instead of the
// token silently resuming in a different order. As in Page, a token that cannot
// be decoded is treated as absent, so the scan restarts at the first item, and
// a negative pageSize is an InvalidArgument error.
func PageOrdered[T any](items []T, params map[string]any,
	cursor func(T) string, afterCursor func(item T, token string) bool) (page []T, nextToken string, err error) {
	size, err := PageSize(params)
	if err != nil {
		return nil, "", err
	}
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cur := decodeCursor(tok); cur != "" {
			for start < len(items) && !afterCursor(items[start], cur) {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextToken = encodeCursor(cursor(items[end-1]))
	}
	return page, nextToken, nil
}

// PageSize parses the pageSize query parameter (string, int, or float64),
// falling back to the GCP default of 1000 when it is absent or zero. A negative
// value is rejected with INVALID_ARGUMENT per AIP-158.
func PageSize(params map[string]any) (int, error) {
	switch v := params["pageSize"].(type) {
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return normalizePageSize(n)
		}
	case int:
		return normalizePageSize(v)
	case float64:
		// Check the sign before truncating: int(-0.5) is 0, which would
		// otherwise be mistaken for the default.
		if v < 0 {
			return 0, negativePageSize()
		}
		return normalizePageSize(int(v))
	}
	return defaultPageSize, nil
}

// normalizePageSize applies the AIP-158 rule to a parsed integer size: zero
// selects the default, a negative value is invalid, and a positive value is
// passed through.
func normalizePageSize(n int) (int, error) {
	if n < 0 {
		return 0, negativePageSize()
	}
	if n == 0 {
		return defaultPageSize, nil
	}
	return n, nil
}

func negativePageSize() error {
	return model.NewProviderError("InvalidArgument", "pageSize must not be negative", 400)
}

func encodeCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeCursor(token string) string {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ""
	}
	return string(b)
}
