package bundle

import (
	"context"
	"encoding/json"
	"sync"
)

// =============================================================================
// CrossRegionBundle — per-account, all regions (IAM, Route53, …)
// =============================================================================

// CrossRegionBundle[T] vivifies a distinct *T per account (region-agnostic).
type CrossRegionBundle[T any] struct {
	mu        sync.RWMutex
	stores    map[string]*T
	construct func(account string) *T
	validate  func(account string) error
}

// CrossRegionOption is a functional option for NewCrossRegion.
type CrossRegionOption[T any] func(*CrossRegionBundle[T])

// NewCrossRegion creates a CrossRegionBundle with the given constructor and options.
func NewCrossRegion[T any](construct func(account string) *T, opts ...CrossRegionOption[T]) *CrossRegionBundle[T] {
	b := &CrossRegionBundle[T]{
		stores:    make(map[string]*T),
		construct: construct,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Get returns the per-account store, creating it on first access (double-checked locking).
func (b *CrossRegionBundle[T]) Get(account string) (*T, error) {
	if b.validate != nil {
		if err := b.validate(account); err != nil {
			return nil, err
		}
	}

	b.mu.RLock()
	if st, ok := b.stores[account]; ok {
		b.mu.RUnlock()
		return st, nil
	}
	b.mu.RUnlock()

	st := b.construct(account)

	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.stores[account]; ok {
		return existing, nil
	}
	b.stores[account] = st
	return st, nil
}

// Iter walks every (account, store) under the read lock.
// The callback must NOT call back into this bundle.
func (b *CrossRegionBundle[T]) Iter(fn func(account string, store *T)) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for k, st := range b.stores {
		fn(k, st)
	}
}

// Reset wipes every account's store IN PLACE if T implements Resetter.
func (b *CrossRegionBundle[T]) Reset(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, st := range b.stores {
		if r, ok := any(st).(Resetter); ok {
			r.Reset(ctx)
		}
	}
}

// ResetAccount resets only the store for the given account.
func (b *CrossRegionBundle[T]) ResetAccount(account string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, ok := b.stores[account]; ok {
		if r, ok := any(st).(Resetter); ok {
			r.Reset(context.Background())
		}
	}
}

// Snapshot serialises the bundle into the §12.1.1 CrossRegionBundle JSON shape.
func (b *CrossRegionBundle[T]) Snapshot() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	type accountEntry struct {
		Account string `json:"account"`
		Data    *T     `json:"data"`
	}
	accounts := make([]accountEntry, 0, len(b.stores))
	for k, st := range b.stores {
		accounts = append(accounts, accountEntry{Account: k, Data: st})
	}
	return json.Marshal(map[string]any{"kind": "cross-region", "accounts": accounts})
}

// Restore loads state from a snapshot produced by Snapshot.
func (b *CrossRegionBundle[T]) Restore(data []byte) error {
	type accountEntry struct {
		Account string `json:"account"`
		Data    *T     `json:"data"`
	}
	var envelope struct {
		Kind     string         `json:"kind"`
		Accounts []accountEntry `json:"accounts"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stores = make(map[string]*T, len(envelope.Accounts))
	for _, a := range envelope.Accounts {
		b.stores[a.Account] = a.Data
	}
	return nil
}

// CrossRegionWithValidate enables strict 12-digit account-ID validation on Get.
func CrossRegionWithValidate[T any]() CrossRegionOption[T] {
	return func(b *CrossRegionBundle[T]) {
		b.validate = validateAccount
	}
}
