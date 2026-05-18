// Package bundle provides generic per-scope store wrappers for multi-account
// isolation. Three concrete types mirror LocalStack's LocalAttribute,
// CrossRegionAttribute, and CrossAccountAttribute semantics.
package bundle

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// Resetter is implemented by stores that know how to wipe their own data
// in place (preferred) rather than letting the bundle drop the instance.
// LocalStack semantics: reset() preserves store object identity so that
// goroutines holding a reference see empty data, not a dangling pointer.
type Resetter interface {
	Reset()
}

// twelveDigit is the strict-anchored 12-digit account-ID validator.
// We intentionally deviate from LocalStack's unanchored \d{12} to avoid
// the foot-gun where a 13-char key yields a 13-char "account" string.
var twelveDigit = regexp.MustCompile(`^\d{12}$`)

func validateAccountRegion(account, region string) error {
	if !twelveDigit.MatchString(account) {
		return fmt.Errorf("invalid account id %q", account)
	}
	if region == "" {
		return fmt.Errorf("region must not be empty for LocalBundle")
	}
	return nil
}

func validateAccount(account string) error {
	if !twelveDigit.MatchString(account) {
		return fmt.Errorf("invalid account id %q", account)
	}
	return nil
}

func splitScope(s string) (account, region string, ok bool) {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return "", "", false
}

// =============================================================================
// LocalBundle — per-(account, region)
// =============================================================================

// LocalBundle[T] vivifies a distinct *T per (account, region) pair.
type LocalBundle[T any] struct {
	mu        sync.RWMutex
	stores    map[localKey]*T
	construct func(account, region string) *T
	validate  func(account, region string) error
}

type localKey struct{ Account, Region string }

// NewLocal creates a LocalBundle with the given constructor and options.
func NewLocal[T any](construct func(account, region string) *T, opts ...LocalOption[T]) *LocalBundle[T] {
	b := &LocalBundle[T]{
		stores:    make(map[localKey]*T),
		construct: construct,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Get returns the per-(account, region) store, creating it on first access.
//
// Concurrency: double-checked locking. The construct callback is invoked
// OUTSIDE the write lock so it cannot deadlock if construct calls back into
// the bundle. If two goroutines race on the same key, one build is discarded.
//
// INVARIANT: callers must NOT hold any per-store lock when calling Get.
// Lock order is always: bundle → store, never the reverse.
func (b *LocalBundle[T]) Get(account, region string) (*T, error) {
	if b.validate != nil {
		if err := b.validate(account, region); err != nil {
			return nil, err
		}
	}
	k := localKey{account, region}

	b.mu.RLock()
	if st, ok := b.stores[k]; ok {
		b.mu.RUnlock()
		return st, nil
	}
	b.mu.RUnlock()

	// Build outside the write lock.
	st := b.construct(account, region)

	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.stores[k]; ok {
		// Lost the race — discard our build; GC handles st.
		return existing, nil
	}
	b.stores[k] = st
	return st, nil
}

// Iter walks every (account, region, store) under the read lock.
// The callback must NOT call back into this bundle (deadlock).
func (b *LocalBundle[T]) Iter(fn func(account, region string, store *T)) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for k, st := range b.stores {
		fn(k.Account, k.Region, st)
	}
}

// Reset wipes every store IN PLACE if T implements Resetter, preserving *T
// identity so goroutines holding a reference see empty state.
func (b *LocalBundle[T]) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, st := range b.stores {
		if r, ok := any(st).(Resetter); ok {
			r.Reset()
		}
	}
}

// ResetAndDiscard resets each store and drops all store instances.
func (b *LocalBundle[T]) ResetAndDiscard() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, st := range b.stores {
		if r, ok := any(st).(Resetter); ok {
			r.Reset()
		}
	}
	b.stores = make(map[localKey]*T)
}

// ResetScope resets only the store for the given (account, region).
func (b *LocalBundle[T]) ResetScope(account, region string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := localKey{account, region}
	if st, ok := b.stores[k]; ok {
		if r, ok := any(st).(Resetter); ok {
			r.Reset()
		}
	}
}

// ResetAccount resets all regions for the given account.
func (b *LocalBundle[T]) ResetAccount(account string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, st := range b.stores {
		if k.Account != account {
			continue
		}
		if r, ok := any(st).(Resetter); ok {
			r.Reset()
		}
	}
}

// Snapshot serialises the bundle into the §12.1.1 LocalBundle JSON shape.
func (b *LocalBundle[T]) Snapshot() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	type scopeEntry struct {
		Account string `json:"account"`
		Region  string `json:"region"`
		Data    *T     `json:"data"`
	}
	scopes := make([]scopeEntry, 0, len(b.stores))
	for k, st := range b.stores {
		scopes = append(scopes, scopeEntry{Account: k.Account, Region: k.Region, Data: st})
	}
	return json.Marshal(map[string]any{"kind": "local", "scopes": scopes})
}

// Restore loads state from a snapshot produced by Snapshot.
func (b *LocalBundle[T]) Restore(data []byte) error {
	type scopeEntry struct {
		Account string `json:"account"`
		Region  string `json:"region"`
		Data    *T     `json:"data"`
	}
	var envelope struct {
		Kind   string       `json:"kind"`
		Scopes []scopeEntry `json:"scopes"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stores = make(map[localKey]*T, len(envelope.Scopes))
	for _, s := range envelope.Scopes {
		b.stores[localKey{s.Account, s.Region}] = s.Data
	}
	return nil
}

// LocalOption is a functional option for NewLocal.
type LocalOption[T any] func(*LocalBundle[T])

// LocalWithValidate enables strict 12-digit account-ID + non-empty region
// validation on every Get call.
func LocalWithValidate[T any]() LocalOption[T] {
	return func(b *LocalBundle[T]) {
		b.validate = validateAccountRegion
	}
}

// splitScope is used by Restore implementations to parse "account:region" keys.
var _ = splitScope // suppress unused warning; used in cross_region.go
