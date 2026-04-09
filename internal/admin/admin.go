package admin

import (
	"encoding/json"
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

// Handler serves the /_jaiscloud/* admin endpoints.
type Handler struct {
	mu           sync.Mutex
	resetters    []Resetter
	snapshotters map[string]Snapshotter
}

func NewHandler() *Handler {
	return &Handler{
		snapshotters: make(map[string]Snapshotter),
	}
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

// Export handles GET /_jaiscloud/export — returns a JSON snapshot of all state.
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make(map[string]json.RawMessage, len(h.snapshotters))
	for name, s := range h.snapshotters {
		data, err := s.Snapshot()
		if err != nil {
			http.Error(w, "snapshot error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		out[name] = data
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// Import handles POST /_jaiscloud/import — restores state from a JSON snapshot.
func (h *Handler) Import(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var snap map[string]json.RawMessage
	if err := json.Unmarshal(body, &snap); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	for name, s := range h.snapshotters {
		data, ok := snap[name]
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
