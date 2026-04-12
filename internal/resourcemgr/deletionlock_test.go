package resourcemgr

import (
	"sync"
	"testing"
	"time"
)

func TestDeletionLock_AcquireRelease(t *testing.T) {
	l := newDeletionLock()

	if !l.Acquire("cluster", "c1") {
		t.Fatal("expected first Acquire to succeed")
	}
	if l.Acquire("cluster", "c1") {
		t.Fatal("expected second Acquire to fail (already locked)")
	}
	l.Release("cluster", "c1")
	if !l.Acquire("cluster", "c1") {
		t.Fatal("expected Acquire after Release to succeed")
	}
	l.Release("cluster", "c1")
}

func TestDeletionLock_IsDeleting(t *testing.T) {
	l := newDeletionLock()

	if l.IsDeleting("cluster", "c1") {
		t.Fatal("should not be deleting before Acquire")
	}
	l.Acquire("cluster", "c1")
	if !l.IsDeleting("cluster", "c1") {
		t.Fatal("should be deleting after Acquire")
	}
	l.Release("cluster", "c1")
	if l.IsDeleting("cluster", "c1") {
		t.Fatal("should not be deleting after Release")
	}
}

func TestDeletionLock_DifferentResources(t *testing.T) {
	l := newDeletionLock()

	l.Acquire("cluster", "c1")
	if !l.Acquire("cluster", "c2") {
		t.Fatal("different ID should acquire independently")
	}
	if !l.Acquire("job", "c1") {
		t.Fatal("different type/same ID should acquire independently")
	}
	l.Release("cluster", "c1")
	l.Release("cluster", "c2")
	l.Release("job", "c1")
}

func TestDeletionLock_ReleaseNonExistent(t *testing.T) {
	l := newDeletionLock()
	// Must not panic
	l.Release("cluster", "does-not-exist")
}

func TestDeletionLock_Reset(t *testing.T) {
	l := newDeletionLock()
	l.Acquire("cluster", "c1")
	l.Acquire("cluster", "c2")
	l.Reset()

	if l.IsDeleting("cluster", "c1") || l.IsDeleting("cluster", "c2") {
		t.Fatal("Reset should clear all locks")
	}
	// After reset, Acquire should work again
	if !l.Acquire("cluster", "c1") {
		t.Fatal("Acquire after Reset should succeed")
	}
	l.Release("cluster", "c1")
}

func TestDeletionLock_SweepStale(t *testing.T) {
	l := newDeletionLock()

	// Set acquiredAt in the past by directly manipulating entries
	l.mu.Lock()
	l.entries["cluster\x00old"] = lockEntry{acquiredAt: time.Now().Add(-10 * time.Minute)}
	l.entries["cluster\x00fresh"] = lockEntry{acquiredAt: time.Now()}
	l.mu.Unlock()

	var logged []string
	l.SweepStale(5*time.Minute, func(msg string, args ...any) {
		logged = append(logged, msg)
	})

	if l.IsDeleting("cluster", "old") {
		t.Error("stale lock should have been swept")
	}
	if !l.IsDeleting("cluster", "fresh") {
		t.Error("fresh lock should not be swept")
	}
	if len(logged) != 1 {
		t.Errorf("expected 1 log entry, got %d", len(logged))
	}
	l.Release("cluster", "fresh")
}

func TestDeletionLock_Concurrency(t *testing.T) {
	l := newDeletionLock()
	const goroutines = 100

	var wg sync.WaitGroup
	acquired := make(chan struct{}, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Acquire("res", "r1") {
				acquired <- struct{}{}
				// Hold briefly, then release
				l.Release("res", "r1")
			}
		}()
	}
	wg.Wait()
	close(acquired)

	count := 0
	for range acquired {
		count++
	}
	if count == 0 {
		t.Error("at least one goroutine should have acquired the lock")
	}
}

func TestDeletionLock_IsDeleting_ConcurrentReads(t *testing.T) {
	l := newDeletionLock()
	l.Acquire("cluster", "c1")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !l.IsDeleting("cluster", "c1") {
				t.Error("IsDeleting should return true while lock is held")
			}
		}()
	}
	wg.Wait()
	l.Release("cluster", "c1")
}
