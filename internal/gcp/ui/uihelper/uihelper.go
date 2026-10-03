// Package uihelper provides shared helpers for GCP UI API handlers. It mirrors
// internal/aws/ui/uihelper but uses the GCP resource-name formatter and cloud
// identity, and never imports internal/aws.
package uihelper

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/ui/middleware"
)

// AsSlice coerces a provider response value that may be a typed slice into
// []any so handlers can range over it uniformly. Returns nil for non-slices.
func AsSlice(v any) []any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out
}

// RegionFrom reads region from the query string or request context.
func RegionFrom(r *http.Request) string {
	if v := r.URL.Query().Get("region"); v != "" {
		return v
	}
	if v := r.Context().Value(middleware.CtxKeyRegion); v != nil {
		return v.(string)
	}
	return ""
}

// AccountFrom reads the project ID from the ?account= query param, falling back
// to the request context.
func AccountFrom(r *http.Request) string {
	if v := r.URL.Query().Get("account"); v != "" {
		return v
	}
	if v := r.Context().Value(middleware.CtxKeyAccount); v != nil {
		return v.(string)
	}
	return ""
}

// PageSizeFrom parses ?pageSize clamped to [1, maxSize].
func PageSizeFrom(r *http.Request, defaultSize, maxSize int) int {
	s := r.URL.Query().Get("pageSize")
	if s == "" {
		return defaultSize
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return defaultSize
	}
	if n > maxSize {
		return maxSize
	}
	return n
}

// WriteJSON encodes v as JSON with Content-Type application/json.
func WriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// WriteError translates a provider error to { "code": "...", "message": "..." }.
func WriteError(w http.ResponseWriter, err error) {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(pe.HTTPStatus)
		json.NewEncoder(w).Encode(map[string]string{"code": pe.Code, "message": pe.Message}) //nolint:errcheck
		return
	}
	http.Error(w, `{"code":"InternalError","message":"internal server error"}`, http.StatusInternalServerError)
}

// UIError writes a UI-specific error response.
func UIError(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message}) //nolint:errcheck
}

// NR constructs a NormalizedRequest for direct provider calls from UI handlers.
// Port is cfg.Port (wire port) — NOT the UI port. Clock is cfg.Clock; providers
// panic on a nil Clock.
func NR(ctx context.Context, cfg *config.Config, service, action, region, accountID string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    service,
		Action:     action,
		Params:     make(map[string]any),
		Clock:      cfg.Clock,
		Region:     region,
		AccountID:  accountID,
		Port:       cfg.Port,
		Cloud:      model.CloudGCP,
		ResourceID: resource.ResourceID(accountID),
	}
}
