package resourcemgr

import (
	"context"

	"jaiscloud/internal/store"
)

// StoreAdapter bridges internal/store.ResourceStore to resourcemgr.ResourceStore.
// The host creates a StoreAdapter and passes it to resourcemgr.New so the Manager
// can check existence and perform cascades without importing provider-layer code.
type StoreAdapter struct {
	s store.ResourceStore
}

// NewStoreAdapter wraps a store.ResourceStore for use with resourcemgr.Manager.
func NewStoreAdapter(s store.ResourceStore) *StoreAdapter {
	return &StoreAdapter{s: s}
}

func (a *StoreAdapter) Create(ctx context.Context, entry ResourceEntry) error {
	return a.s.Create(ctx, store.ResourceEntry{
		Type: entry.Type,
		ID:   entry.ID,
		Data: entry.Data,
	})
}

func (a *StoreAdapter) Exists(ctx context.Context, resourceType, resourceID string) (bool, error) {
	_, err := a.s.Get(ctx, resourceType, resourceID)
	if err == nil {
		return true, nil
	}
	if err == store.ErrNotFound {
		return false, nil
	}
	return false, err
}

func (a *StoreAdapter) List(ctx context.Context, resourceType, prefix string) ([]ResourceEntry, error) {
	entries, err := a.s.List(ctx, resourceType, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]ResourceEntry, len(entries))
	for i, e := range entries {
		out[i] = ResourceEntry{Type: e.Type, ID: e.ID, Data: []byte(e.Data)}
	}
	return out, nil
}

func (a *StoreAdapter) Delete(ctx context.Context, resourceType, resourceID string) error {
	return a.s.Delete(ctx, resourceType, resourceID)
}

func (a *StoreAdapter) Update(ctx context.Context, entry ResourceEntry) error {
	return a.s.Update(ctx, store.ResourceEntry{
		Type: entry.Type,
		ID:   entry.ID,
		Data: entry.Data,
	})
}

func (a *StoreAdapter) Get(ctx context.Context, resourceType, resourceID string) (ResourceEntry, error) {
	e, err := a.s.Get(ctx, resourceType, resourceID)
	if err != nil {
		return ResourceEntry{}, err
	}
	return ResourceEntry{Type: e.Type, ID: e.ID, Data: []byte(e.Data)}, nil
}
