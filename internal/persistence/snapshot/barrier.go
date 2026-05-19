// Package snapshot provides the global persistence barrier and atomic write utilities.
package snapshot

import (
	"context"
	"sync"
)

// Barrier is the authoritative concurrency primitive for snapshot consistency.
// Writers (import, reset) acquire WriteBegin — all cloud requests block with 503 while held.
// Readers (periodic save, export) acquire ReadBegin — multiple readers coexist with cloud requests.
type Barrier struct {
	mu sync.RWMutex
}

// NewBarrier returns a new Barrier.
func NewBarrier() *Barrier { return &Barrier{} }

// ReadBegin acquires a shared read lock. Multiple callers may hold it concurrently.
// Returns release (must be called exactly once) and any context error.
func (b *Barrier) ReadBegin(ctx context.Context) (release func(), err error) {
	done := make(chan struct{})
	go func() {
		b.mu.RLock()
		close(done)
	}()
	select {
	case <-ctx.Done():
		// Goroutine is still blocking on RLock; we must drain it to avoid a leak.
		// Unblock by waiting — RLock will eventually acquire, then we unlock.
		go func() {
			<-done
			b.mu.RUnlock()
		}()
		return nil, ctx.Err()
	case <-done:
		return b.mu.RUnlock, nil
	}
}

// WriteBegin acquires an exclusive write lock. Blocks until all read-lock holders release.
// Returns release (must be called exactly once) and any context error.
func (b *Barrier) WriteBegin(ctx context.Context) (release func(), err error) {
	done := make(chan struct{})
	go func() {
		b.mu.Lock()
		close(done)
	}()
	select {
	case <-ctx.Done():
		go func() {
			<-done
			b.mu.Unlock()
		}()
		return nil, ctx.Err()
	case <-done:
		return b.mu.Unlock, nil
	}
}

// TryReadBegin attempts a non-blocking shared read lock.
// Returns (release, true) if acquired, (nil, false) if the write lock is held.
// Used by the gateway middleware to return 503 while import/reset is in progress.
func (b *Barrier) TryReadBegin() (release func(), ok bool) {
	if !b.mu.TryRLock() {
		return nil, false
	}
	return b.mu.RUnlock, true
}
