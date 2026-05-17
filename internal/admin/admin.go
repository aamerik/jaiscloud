package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"jaiscloud/internal/config"
)

// Resetter is implemented by any store that can wipe its state.
type Resetter interface {
	Reset()
}

// Snapshotter is implemented by stores that support export/import.
type Snapshotter interface {
	Snapshot() (json.RawMessage, error)
	Restore(data json.RawMessage) error
}

// SnapshotEnvelope is the top-level container for exported state (schema v2).
type SnapshotEnvelope struct {
	SchemaVersion int                        `json:"schema_version"`
	InstanceID    string                     `json:"instance_id,omitempty"`
	Cloud         string                     `json:"cloud,omitempty"`
	Region        string                     `json:"region,omitempty"`
	AccountID     string                     `json:"account_id,omitempty"`
	Stores        map[string]json.RawMessage `json:"stores"`
}

// HandlerMeta carries identity fields injected into every export envelope.
type HandlerMeta struct {
	InstanceID string
	Cloud      string
	Region     string
	AccountID  string
	StateDir   string // used for --new-instance instance-id persistence
}

// Handler serves the /_jaiscloud/* admin endpoints.
type Handler struct {
	mu               sync.Mutex
	meta             HandlerMeta
	resetters        []Resetter
	snapshotters     map[string]Snapshotter
	lambdaCode       LambdaCodeFetcher
	firehoseFlusher  FirehoseFlusher
	cwAlarmEvaluator CWAlarmEvaluator
}

func NewHandler() *Handler {
	return &Handler{
		snapshotters: make(map[string]Snapshotter),
	}
}

// SetMeta stores identity metadata used when building export envelopes.
func (h *Handler) SetMeta(m HandlerMeta) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.meta = m
}

// RegisterResetter adds a store that will be reset on POST /_jaiscloud/reset.
func (h *Handler) RegisterResetter(r Resetter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resetters = append(h.resetters, r)
}

// RegisterSnapshotter adds a named store for export/import.
func (h *Handler) RegisterSnapshotter(name string, s Snapshotter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshotters[name] = s
}

// Health handles GET /_jaiscloud/health.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Reset handles POST /_jaiscloud/reset — wipes all in-memory state.
func (h *Handler) Reset(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	resetters := make([]Resetter, len(h.resetters))
	copy(resetters, h.resetters)
	h.mu.Unlock()

	for _, rs := range resetters {
		rs.Reset()
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "reset"}); err != nil {
		slog.Warn("admin: reset encode failed", "err", err)
	}
}

// Export handles GET /_jaiscloud/export — returns a schema-v2 envelope with all state.
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	snapshots := make(map[string]Snapshotter, len(h.snapshotters))
	for name, s := range h.snapshotters {
		snapshots[name] = s
	}
	meta := h.meta
	h.mu.Unlock()

	stores := make(map[string]json.RawMessage, len(snapshots))
	for name, s := range snapshots {
		data, err := s.Snapshot()
		if err != nil {
			http.Error(w, "snapshot error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		stores[name] = data
	}
	env := SnapshotEnvelope{
		SchemaVersion: 2,
		InstanceID:    meta.InstanceID,
		Cloud:         meta.Cloud,
		Region:        meta.Region,
		AccountID:     meta.AccountID,
		Stores:        stores,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(env); err != nil {
		slog.Warn("admin: export encode failed", "err", err)
	}
}

// Import handles POST /_jaiscloud/import — restores state from a JSON snapshot.
// Accepts both schema v1 (bare map) and schema v2 (SnapshotEnvelope).
// Returns 409 if the envelope's cloud field does not match the running cloud.
// Query param ?new_instance=true generates a fresh instance ID instead of
// preserving the snapshot's instance ID; it also blocks imports of snapshots
// containing KMS key material (which would be undecryptable under a new ID).
func (h *Handler) Import(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Try schema v2 envelope first, fall back to bare map (schema v1).
	var stores map[string]json.RawMessage
	var env SnapshotEnvelope
	if json.Unmarshal(body, &env) == nil && env.Stores != nil {
		// Schema v2: validate cloud identity.
		h.mu.Lock()
		cloud := h.meta.Cloud
		h.mu.Unlock()

		if cloud != "" && env.Cloud != "" && env.Cloud != cloud {
			http.Error(w, fmt.Sprintf("snapshot cloud %q does not match running cloud %q", env.Cloud, cloud), http.StatusConflict)
			return
		}
		stores = env.Stores
	} else {
		// Schema v1: bare map. No instance identity fields to process.
		if err := json.Unmarshal(body, &stores); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	newInstance := r.URL.Query().Get("new_instance") == "true"

	// KMS safety gate: if --new-instance, refuse if the snapshot contains KMS key material.
	if newInstance && snapshotHasKMSMaterial(stores) {
		http.Error(w,
			"snapshot contains KMS key material that cannot be decrypted under a new instance identity. "+
				"Import without --new-instance (rollback workflow) or strip kms_key rows from the snapshot first.",
			http.StatusConflict)
		return
	}

	// Instance ID handling for schema v2 envelopes.
	h.mu.Lock()
	stateDir := h.meta.StateDir
	h.mu.Unlock()

	if env.InstanceID != "" && stateDir != "" {
		if newInstance {
			newID, err := config.GenerateNewInstanceID(stateDir)
			if err != nil {
				http.Error(w, "generate new instance id: "+err.Error(), http.StatusInternalServerError)
				return
			}
			slog.Info("admin: import --new-instance; assigned fresh instance id",
				"old", env.InstanceID, "new", newID)
		} else {
			if err := config.WriteInstanceID(stateDir, env.InstanceID); err != nil {
				http.Error(w, "write instance id: "+err.Error(), http.StatusInternalServerError)
				return
			}
			slog.Info("admin: import preserved snapshot instance id", "id", env.InstanceID)
		}
	}

	h.mu.Lock()
	snapshotters := make(map[string]Snapshotter, len(h.snapshotters))
	for name, s := range h.snapshotters {
		snapshotters[name] = s
	}
	h.mu.Unlock()

	for name, s := range snapshotters {
		data, ok := stores[name]
		if !ok {
			continue
		}
		if err := s.Restore(data); err != nil {
			http.Error(w, "restore "+name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "imported"}); err != nil {
		slog.Warn("admin: import encode failed", "err", err)
	}
}

// snapshotHasKMSMaterial returns true if the stores map contains non-empty
// KMS key rows in the "resources" snapshot.
func snapshotHasKMSMaterial(stores map[string]json.RawMessage) bool {
	resourcesData, ok := stores["resources"]
	if !ok {
		return false
	}
	var entries map[string]struct {
		Type string `json:"Type"`
	}
	if err := json.Unmarshal(resourcesData, &entries); err != nil {
		return true // fail-safe: treat unparseable snapshot as having KMS material
	}
	for _, e := range entries {
		if e.Type == "kms_key" {
			return true
		}
	}
	return false
}
