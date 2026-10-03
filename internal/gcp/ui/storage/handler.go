// Package storageui serves the Cloud Storage UI API. Handlers call the GCS
// provider directly (in-process) rather than over the wire.
package storageui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Cloud Storage UI API requests by calling the GCS provider.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func bucketFromMap(m map[string]any) Bucket {
	versioning, _ := m["versioning"].(map[string]any)
	enabled, _ := versioning["enabled"].(bool)
	return Bucket{
		Name:         str(m, "name"),
		Location:     str(m, "location"),
		StorageClass: str(m, "storageClass"),
		TimeCreated:  str(m, "timeCreated"),
		Updated:      str(m, "updated"),
		Versioning:   enabled,
	}
}

// GET /buckets
func (h *Handler) ListBuckets(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsList", "global", account)

	resp, err := h.provider.BucketsList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["items"])
	buckets := make([]Bucket, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			buckets = append(buckets, bucketFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListBucketsResponse{Items: buckets, Total: len(buckets)})
}

// POST /buckets
func (h *Handler) CreateBucket(w http.ResponseWriter, r *http.Request) {
	var req CreateBucketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsInsert", "global", account)
	body := map[string]any{"name": req.Name}
	if req.Location != "" {
		body["location"] = req.Location
	}
	if req.StorageClass != "" {
		body["storageClass"] = req.StorageClass
	}
	nr.Params["body"] = body

	resp, err := h.provider.BucketsInsert(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, bucketFromMap(resp.Data))
}

// DELETE /buckets/{bucket}
func (h *Handler) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsDelete", "global", account)
	nr.Params["bucket"] = bucket

	if _, err := h.provider.BucketsDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /buckets/{bucket}/objects
func (h *Handler) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsList", "global", account)
	nr.Params["bucket"] = bucket
	if prefix := r.URL.Query().Get("prefix"); prefix != "" {
		nr.Params["prefix"] = prefix
	}
	delimiter := r.URL.Query().Get("delimiter")
	if delimiter == "" {
		delimiter = "/"
	}
	nr.Params["delimiter"] = delimiter
	if max := r.URL.Query().Get("maxResults"); max != "" {
		nr.Params["maxResults"] = max
	}
	if token := r.URL.Query().Get("pageToken"); token != "" {
		nr.Params["pageToken"] = token
	}

	resp, err := h.provider.ObjectsList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["items"])
	objects := make([]GCSObject, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		objects = append(objects, GCSObject{
			Name:         str(m, "name"),
			Size:         str(m, "size"),
			ContentType:  str(m, "contentType"),
			StorageClass: str(m, "storageClass"),
			Updated:      str(m, "updated"),
			MD5:          str(m, "md5Hash"),
		})
	}
	prefixes, _ := resp.Data["prefixes"].([]string)
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListObjectsResponse{Items: objects, Prefixes: prefixes, NextPageToken: next})
}
