package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
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
}

// Handler serves the /_jaiscloud/* admin endpoints.
type Handler struct {
	mu           sync.Mutex
	meta         HandlerMeta
	resetters    []Resetter
	snapshotters map[string]Snapshotter
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
	defer h.mu.Unlock()
	for _, rs := range h.resetters {
		rs.Reset()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}

// Export handles GET /_jaiscloud/export — returns a schema-v2 envelope with all state.
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()

	stores := make(map[string]json.RawMessage, len(h.snapshotters))
	for name, s := range h.snapshotters {
		data, err := s.Snapshot()
		if err != nil {
			http.Error(w, "snapshot error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		stores[name] = data
	}
	env := SnapshotEnvelope{
		SchemaVersion: 2,
		InstanceID:    h.meta.InstanceID,
		Cloud:         h.meta.Cloud,
		Region:        h.meta.Region,
		AccountID:     h.meta.AccountID,
		Stores:        stores,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(env)
}

// Import handles POST /_jaiscloud/import — restores state from a JSON snapshot.
// Accepts both schema v1 (bare map) and schema v2 (SnapshotEnvelope).
// Returns 409 if the envelope's cloud field does not match the running cloud.
func (h *Handler) Import(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()

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
		if h.meta.Cloud != "" && env.Cloud != "" && env.Cloud != h.meta.Cloud {
			http.Error(w, fmt.Sprintf("snapshot cloud %q does not match running cloud %q", env.Cloud, h.meta.Cloud), http.StatusConflict)
			return
		}
		stores = env.Stores
	} else {
		// Schema v1: bare map.
		if err := json.Unmarshal(body, &stores); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	for name, s := range h.snapshotters {
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
	json.NewEncoder(w).Encode(map[string]string{"status": "imported"})
}
