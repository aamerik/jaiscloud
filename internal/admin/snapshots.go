package admin

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"jaiscloud/internal/persistence/version"

	"github.com/go-chi/chi/v5"
)

// SnapshotCreate handles POST /_jaiscloud/snapshot
// Body: {"name":"<n>","description":"<d>"}
func (h *Handler) SnapshotCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "body must be JSON with non-empty 'name'", http.StatusBadRequest)
		return
	}
	if !isValidSnapshotName(req.Name) {
		http.Error(w, "name must contain only letters, digits, hyphens, and underscores", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	dataDir := h.dataDir
	meta := h.meta
	kekFP := h.kekFingerprint
	snapshots := make(map[string]Snapshotter, len(h.snapshotters))
	for k, v := range h.snapshotters {
		snapshots[k] = v
	}
	blobStore := h.blobStore
	h.mu.Unlock()

	if dataDir == "" {
		http.Error(w, "snapshot management requires --data-dir to be set", http.StatusPreconditionFailed)
		return
	}

	// Build envelope.
	stores := make(map[string]json.RawMessage, len(snapshots))
	storeCounts := make(map[string]int, len(snapshots))
	for name, s := range snapshots {
		var buf bytes.Buffer
		if err := s.Snapshot(r.Context(), &buf); err != nil {
			http.Error(w, "snapshot "+name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
		raw := buf.Bytes()
		stores[name] = json.RawMessage(raw)
		// Count entries (approximate: count top-level JSON keys).
		var m map[string]json.RawMessage
		if json.Unmarshal(raw, &m) == nil {
			storeCounts[name] = len(m)
		}
	}

	env := version.Envelope{
		SchemaVersion:  version.CodeSnapshotVersion,
		InstanceID:     meta.InstanceID,
		Cloud:          meta.Cloud,
		Region:         meta.Region,
		AccountID:      meta.AccountID,
		CreatedAt:      time.Now().UTC(),
		KEKFingerprint: kekFP,
		Stores:         stores,
	}
	envJSON, err := json.Marshal(env)
	if err != nil {
		http.Error(w, "marshal envelope: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the tarball.
	tarPath := snapshotTarball(dataDir, req.Name)
	if err := os.MkdirAll(snapshotDir(dataDir, req.Name), 0o700); err != nil {
		http.Error(w, "mkdir: "+err.Error(), http.StatusInternalServerError)
		return
	}
	f, err := os.OpenFile(tarPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		http.Error(w, "create tarball: "+err.Error(), http.StatusInternalServerError)
		return
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	hdr := &tar.Header{
		Name:     "envelope.json",
		Typeflag: tar.TypeReg,
		Size:     int64(len(envJSON)),
		Mode:     0600,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		f.Close()
		http.Error(w, "tar header: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := tw.Write(envJSON); err != nil {
		f.Close()
		http.Error(w, "tar write: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if blobStore != nil {
		if err := blobStore.WriteTarball(r.Context(), tw); err != nil {
			f.Close()
			http.Error(w, "write blobs: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	tw.Close()
	gz.Close()
	f.Close()

	// Get tarball size.
	info, _ := os.Stat(tarPath)
	sizeBytes := int64(0)
	if info != nil {
		sizeBytes = info.Size()
	}

	snMeta := SnapshotMetadata{
		Name:          req.Name,
		Description:   req.Description,
		CreatedAt:     env.CreatedAt,
		SchemaVersion: version.CodeSnapshotVersion,
		Cloud:         meta.Cloud,
		SizeBytes:     sizeBytes,
		StoreCounts:   storeCounts,
	}
	if err := writeSnapshotMeta(dataDir, req.Name, snMeta); err != nil {
		slog.Warn("snapshot create: write metadata failed", "err", err)
	}

	slog.Info("snapshot created", "name", req.Name, "size_bytes", sizeBytes)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(snMeta)
}

// SnapshotList handles GET /_jaiscloud/snapshots
func (h *Handler) SnapshotList(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	dataDir := h.dataDir
	h.mu.Unlock()

	if dataDir == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]SnapshotMetadata{})
		return
	}

	metas, err := listSnapshots(dataDir)
	if err != nil {
		http.Error(w, "list snapshots: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if metas == nil {
		metas = []SnapshotMetadata{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metas)
}

// SnapshotRevert handles POST /_jaiscloud/snapshot/{name}/revert
// Query param ?reset_first=true atomically resets then restores.
func (h *Handler) SnapshotRevert(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		http.Error(w, "snapshot name is required", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	dataDir := h.dataDir
	h.mu.Unlock()

	if dataDir == "" {
		http.Error(w, "snapshot management requires --data-dir to be set", http.StatusPreconditionFailed)
		return
	}

	tarPath := snapshotTarball(dataDir, name)
	data, err := os.ReadFile(tarPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "snapshot not found: "+name, http.StatusNotFound)
			return
		}
		http.Error(w, "read snapshot: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resetFirst := r.URL.Query().Get("reset_first") == "true"
	if resetFirst {
		// Acquire write lock and reset before restore.
		h.mu.Lock()
		barrier := h.barrier
		h.mu.Unlock()

		if barrier != nil {
			release, err := barrier.WriteBegin(r.Context())
			if err != nil {
				http.Error(w, "acquire barrier: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
			defer release()
		}
		h.resetNoBarrier()
	}

	// Re-use the Import handler logic by forwarding as a POST with the tarball body.
	proxyReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "/_jaiscloud/import", bytes.NewReader(data))
	proxyReq.Header.Set("Content-Type", "application/x-tar")
	h.Import(w, proxyReq)
}

// SnapshotDelete handles DELETE /_jaiscloud/snapshot/{name}
// Query param ?yes=true is required as a safety guard.
func (h *Handler) SnapshotDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if r.URL.Query().Get("yes") != "true" {
		http.Error(w, "pass ?yes=true to confirm deletion", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	dataDir := h.dataDir
	h.mu.Unlock()

	if dataDir == "" {
		http.Error(w, "snapshot management requires --data-dir", http.StatusPreconditionFailed)
		return
	}

	dir := snapshotDir(dataDir, name)
	if err := os.RemoveAll(dir); err != nil {
		http.Error(w, "delete snapshot: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "name": name})
}

// SnapshotInspect handles GET /_jaiscloud/snapshot/{name}
func (h *Handler) SnapshotInspect(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	h.mu.Lock()
	dataDir := h.dataDir
	h.mu.Unlock()

	if dataDir == "" {
		http.Error(w, "snapshot management requires --data-dir", http.StatusPreconditionFailed)
		return
	}

	meta, err := readSnapshotMeta(dataDir, name)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "snapshot not found: "+name, http.StatusNotFound)
			return
		}
		http.Error(w, "read metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}

// isValidSnapshotName returns true when name contains only safe filesystem characters.
func isValidSnapshotName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// Ensure io is used (for io.ReadAll in SnapshotRevert if needed).
var _ = io.Discard
var _ = fmt.Sprintf
