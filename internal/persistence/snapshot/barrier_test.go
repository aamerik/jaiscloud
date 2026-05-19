package snapshot

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBarrier_ReadRead_Concurrent(t *testing.T) {
	b := NewBarrier()
	ctx := context.Background()

	r1, err := b.ReadBegin(ctx)
	if err != nil {
		t.Fatalf("ReadBegin 1: %v", err)
	}
	r2, err := b.ReadBegin(ctx)
	if err != nil {
		t.Fatalf("ReadBegin 2: %v", err)
	}
	r1()
	r2()
}

func TestBarrier_WriteExcludes_Readers(t *testing.T) {
	b := NewBarrier()
	ctx := context.Background()

	// Acquire a read lock.
	rel, err := b.ReadBegin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// WriteBegin should block until the read lock is released.
	writeDone := make(chan struct{})
	go func() {
		wrel, werr := b.WriteBegin(ctx)
		if werr == nil {
			wrel()
		}
		close(writeDone)
	}()

	// Release read lock after a short delay.
	time.Sleep(20 * time.Millisecond)
	rel()

	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("WriteBegin did not complete after read lock released")
	}
}

func TestBarrier_TryReadBegin_FailsWhileWriteLocked(t *testing.T) {
	b := NewBarrier()
	ctx := context.Background()

	wrel, err := b.WriteBegin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer wrel()

	_, ok := b.TryReadBegin()
	if ok {
		t.Fatal("TryReadBegin should return ok=false while write-locked")
	}
}

func TestBarrier_TryReadBegin_SucceedsWhenFree(t *testing.T) {
	b := NewBarrier()
	rel, ok := b.TryReadBegin()
	if !ok {
		t.Fatal("TryReadBegin should succeed when barrier is free")
	}
	rel()
}

func TestBarrier_ContextCancellation(t *testing.T) {
	b := NewBarrier()

	// Hold a write lock so ReadBegin will block.
	ctx := context.Background()
	wrel, _ := b.WriteBegin(ctx)
	defer wrel()

	cancelCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	var gotErr error
	go func() {
		defer wg.Done()
		_, gotErr = b.ReadBegin(cancelCtx)
	}()
	wg.Wait()

	if gotErr == nil {
		t.Fatal("expected context cancellation error from ReadBegin")
	}
}
