package resourcemgr

import (
	"sync"
	"time"
)

type lockEntry struct {
	acquiredAt time.Time
}

// DeletionLock is an in-memory set of resources currently being deleted.
// It prevents new children from being created while the parent is mid-delete,
// closing the CheckParent-passes → AcquireDelete race window.
//
// Uses sync.RWMutex so concurrent IsDeleting calls (RLock) do not serialise
// on each other, while Acquire/Release hold the write lock only briefly.
type DeletionLock struct {
	mu      sync.RWMutex
	entries map[string]lockEntry // "type\x00id" → entry
}

func newDeletionLock() *DeletionLock {
	return &DeletionLock{entries: make(map[string]lockEntry)}
}

func (l *DeletionLock) key(resourceType, id string) string {
	return resourceType + "\x00" + id
}

// Acquire marks the resource as deleting. Returns false if already deleting.
func (l *DeletionLock) Acquire(resourceType, id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := l.key(resourceType, id)
	if _, exists := l.entries[k]; exists {
		return false
	}
	l.entries[k] = lockEntry{acquiredAt: time.Now()}
	return true
}

// Release removes the deletion mark for the resource.
func (l *DeletionLock) Release(resourceType, id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, l.key(resourceType, id))
}

// IsDeleting returns true if the resource is currently being deleted.
// Uses RLock — multiple concurrent callers do not block each other.
func (l *DeletionLock) IsDeleting(resourceType, id string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, ok := l.entries[l.key(resourceType, id)]
	return ok
}

// SweepStale releases any lock held longer than ttl, logging a warning per entry.
// logf must accept (msg string, args ...any) in structured slog style.
func (l *DeletionLock) SweepStale(ttl time.Duration, logf func(string, ...any)) {
	cutoff := time.Now().Add(-ttl)
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, e := range l.entries {
		if e.acquiredAt.Before(cutoff) {
			logf("resourcemgr: stale deletion lock released",
				"key", k,
				"heldFor", time.Since(e.acquiredAt).Round(time.Second))
			delete(l.entries, k)
		}
	}
}

// Reset clears all locks. Safe to call from /_jaiscloud/reset.
// Any DeletionHandle.Release() calls after Reset are no-ops (deleting a missing key is safe).
func (l *DeletionLock) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = make(map[string]lockEntry)
}
