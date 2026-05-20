package bundle

import (
	"context"
	"encoding/json"
	"sync"
)

// =============================================================================
// CrossAccountBundle — truly global (S3 bucket-name registry, …)
// =============================================================================

// CrossAccountBundle[T] holds a single lazily-constructed global store.
type CrossAccountBundle[T any] struct {
	mu        sync.Mutex
	store     *T
	ready     bool
	construct func() *T
}

// NewCrossAccount creates a CrossAccountBundle with the given constructor.
func NewCrossAccount[T any](construct func() *T) *CrossAccountBundle[T] {
	return &CrossAccountBundle[T]{construct: construct}
}

// Get returns the single global store, constructing it exactly once.
func (b *CrossAccountBundle[T]) Get() *T {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.ready {
		b.store = b.construct()
		b.ready = true
	}
	return b.store
}

// Reset wipes the global store IN PLACE if T implements Resetter.
func (b *CrossAccountBundle[T]) Reset(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store != nil {
		if r, ok := any(b.store).(Resetter); ok {
			r.Reset(ctx)
		}
	}
}

// Snapshot serialises the bundle into the §12.1.1 CrossAccountBundle JSON shape.
func (b *CrossAccountBundle[T]) Snapshot() ([]byte, error) {
	st := b.Get()
	return json.Marshal(map[string]any{"kind": "cross-account", "data": st})
}

// Restore loads state from a snapshot. If Get has not yet been called, the
// restored value is installed directly so construct is never invoked.
func (b *CrossAccountBundle[T]) Restore(data []byte) error {
	var envelope struct {
		Kind string `json:"kind"`
		Data *T     `json:"data"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.store = envelope.Data
	b.ready = true
	return nil
}
