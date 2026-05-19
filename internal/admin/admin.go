package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"

	"jaiscloud/internal/config"
)

// Resetter is implemented by any store that can wipe its state.
type Resetter interface {
	Reset()
}

// ScopedResetter is implemented by stores that support per-account/region wipes.
type ScopedResetter interface {
	Resetter
	ResetScope(account, region string)
	ResetAccount(account string)
}

// Snapshotter is implemented by stores that support export/import.
type Snapshotter interface {
	// Snapshot writes deterministic JSON state to w. Keys must be sorted.
	Snapshot(ctx context.Context, w io.Writer) error
	// Restore reads JSON state from r, replacing the store's contents atomically.
	Restore(ctx context.Context, r io.Reader) error
	// IsEmpty returns true when the store holds no externally-visible state.
	// Must NOT acquire the barrier — it is called inside WriteBegin.
	IsEmpty(ctx context.Context) (bool, error)
}

// CloudMismatchError is returned when the snapshot cloud does not match the running binary cloud.
// Field is named Code (not Error) to avoid shadowing the error interface method.
type CloudMismatchError struct {
	Code          string `json:"error"`
	Message       string `json:"message"`
	EnvelopeCloud string `json:"envelope_cloud"`
	InstanceCloud string `json:"instance_cloud"`
}

func (e *CloudMismatchError) Error() string { return e.Message }

// NonEmptyStateError is returned when import finds existing data.
type NonEmptyStateError struct {
	Code           string   `json:"error"`
	Message        string   `json:"message"`
	NonEmptyStores []string `json:"non_empty_stores"`
}

func (e *NonEmptyStateError) Error() string { return e.Message }

// SnapshotEnvelope is the top-level container for exported state (schema v3).
// DefaultRegion is informational — stores are account+region-keyed internally.
type SnapshotEnvelope struct {
	SchemaVersion int                        `json:"schema_version"`
	InstanceID    string                     `json:"instance_id,omitempty"`
	Cloud         string                     `json:"cloud,omitempty"`
	DefaultRegion string                     `json:"default_region,omitempty"`
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

// Reset handles POST /_jaiscloud/reset — wipes state.
// Optional query params narrow the scope:
//
//	?account=111111111111              — wipes one account across all regions
//	?account=111111111111&region=us-east-1 — wipes one (account, region)
//
// Without params, all state is wiped (original behaviour).
func (h *Handler) Reset(w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	region := r.URL.Query().Get("region")

	h.mu.Lock()
	resetters := make([]Resetter, len(h.resetters))
	copy(resetters, h.resetters)
	h.mu.Unlock()

	for _, rs := range resetters {
		if account == "" {
			rs.Reset()
			continue
		}
		if sr, ok := rs.(ScopedResetter); ok {
			if region != "" {
				sr.ResetScope(account, region)
			} else {
				sr.ResetAccount(account)
			}
		} else {
			// Non-scoped resetter: over-wipe (acceptable — no per-account state).
			rs.Reset()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "reset"}); err != nil {
		slog.Warn("admin: reset encode failed", "err", err)
	}
}

// Export handles GET /_jaiscloud/export — returns a schema-v3 envelope with all state.
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
		var buf bytes.Buffer
		if err := s.Snapshot(r.Context(), &buf); err != nil {
			http.Error(w, "snapshot error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		stores[name] = json.RawMessage(buf.Bytes())
	}
	env := SnapshotEnvelope{
		SchemaVersion: 3,
		InstanceID:    meta.InstanceID,
		Cloud:         meta.Cloud,
		DefaultRegion: meta.Region,
		AccountID:     meta.AccountID,
		Stores:        stores,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(env); err != nil {
		slog.Warn("admin: export encode failed", "err", err)
	}
}

// Import handles POST /_jaiscloud/import — restores state from a JSON snapshot.
// Returns 409 if the envelope's cloud field does not match the running cloud.
// Returns 409 if any registered store already has state (non-empty guard).
// Query param ?new_instance=true generates a fresh instance ID instead of
// preserving the snapshot's instance ID; it also blocks imports of snapshots
// containing KMS key material (which would be undecryptable under a new ID).
func (h *Handler) Import(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var env SnapshotEnvelope
	if err := json.Unmarshal(body, &env); err != nil || env.Stores == nil {
		http.Error(w, "invalid snapshot body: must be a schema-v3 SnapshotEnvelope with non-nil stores", http.StatusBadRequest)
		return
	}

	// Step 1: Cloud identity check — must match running binary.
	h.mu.Lock()
	cloud := h.meta.Cloud
	h.mu.Unlock()

	if cloud != "" && env.Cloud != "" && env.Cloud != cloud {
		resp := &CloudMismatchError{
			Code:          "cloud_mismatch",
			Message:       fmt.Sprintf("snapshot cloud %q does not match this binary (%s). Use jaiscloud-%s to import this snapshot.", env.Cloud, cloud, env.Cloud),
			EnvelopeCloud: env.Cloud,
			InstanceCloud: cloud,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Warn("admin: import cloud-mismatch encode failed", "err", err)
		}
		return
	}

	stores := env.Stores
	newInstance := r.URL.Query().Get("new_instance") == "true"

	// KMS safety gate: if --new-instance, refuse if the snapshot contains KMS key material.
	if newInstance && snapshotHasKMSMaterial(stores) {
		http.Error(w,
			"snapshot contains KMS key material that cannot be decrypted under a new instance identity. "+
				"Import without --new-instance (rollback workflow) or strip kms_key rows from the snapshot first.",
			http.StatusConflict)
		return
	}

	// Instance ID handling.
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

	// Non-empty guard: refuse import if any store already has state.
	var nonEmpty []string
	for name, s := range snapshotters {
		empty, err := s.IsEmpty(r.Context())
		if err != nil {
			http.Error(w, "isempty "+name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
		if !empty {
			nonEmpty = append(nonEmpty, name)
		}
	}
	if len(nonEmpty) > 0 {
		sort.Strings(nonEmpty)
		resp := &NonEmptyStateError{
			Code:           "non_empty_state",
			Message:        "existing state found in stores: " + fmt.Sprintf("%v", nonEmpty) + ". Clear state first using one of:\n  1. POST /_jaiscloud/reset (then retry import)\n  2. Restart the server with --fresh-start\n  3. Start a new instance with --data-dir <empty-path>",
			NonEmptyStores: nonEmpty,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Warn("admin: import non-empty encode failed", "err", err)
		}
		return
	}

	for name, s := range snapshotters {
		data, ok := stores[name]
		if !ok {
			continue
		}
		if err := s.Restore(r.Context(), bytes.NewReader(data)); err != nil {
			http.Error(w, "restore "+name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "imported"}); err != nil {
		slog.Warn("admin: import encode failed", "err", err)
	}
}

// snapshotHasKMSMaterial returns true if the stores map contains a non-empty
// KMS key store snapshot. The KMS store is registered under "keys".
func snapshotHasKMSMaterial(stores map[string]json.RawMessage) bool {
	data, ok := stores["keys"]
	if !ok {
		return false
	}
	// An empty JSON object "{}" or null/empty means no keys.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return true // fail-safe: unparseable → treat as having material
	}
	return len(raw) > 0
}
