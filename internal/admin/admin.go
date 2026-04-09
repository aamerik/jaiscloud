package admin

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Resetter is implemented by any store that can wipe its state.
type Resetter interface {
	Reset()
}

// Handler serves the /_localcloud/* admin endpoints.
type Handler struct {
	mu       sync.Mutex
	resetters []Resetter
}

func NewHandler() *Handler {
	return &Handler{}
}

// RegisterResetter adds a store that will be reset on POST /_localcloud/reset.
func (h *Handler) RegisterResetter(r Resetter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resetters = append(h.resetters, r)
}

// Health handles GET /_localcloud/health.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Reset handles POST /_localcloud/reset — wipes all in-memory state.
func (h *Handler) Reset(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, rs := range h.resetters {
		rs.Reset()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}
