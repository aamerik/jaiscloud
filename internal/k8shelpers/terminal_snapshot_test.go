package k8shelpers

import (
	"context"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/store"
	memstore "jaiscloud/internal/store"
)

// memTerminalStore wraps MemoryResourceStore to satisfy TerminalStore.
type memTerminalStore struct {
	inner *memstore.MemoryResourceStore
}

func newMemStore() *memTerminalStore {
	return &memTerminalStore{inner: memstore.NewMemoryResourceStore()}
}

func (m *memTerminalStore) Create(ctx context.Context, entry store.ResourceEntry) error {
	return m.inner.Create(ctx, entry)
}

func (m *memTerminalStore) Get(ctx context.Context, resourceType, id string) (store.ResourceEntry, error) {
	return m.inner.Get(ctx, resourceType, id)
}

func TestPersistTerminalSnapshot_RoundTrip(t *testing.T) {
	s := newMemStore()
	ctx := context.Background()

	snap := Snapshot{
		State:     "COMPLETED",
		Reason:    "done",
		StartTime: time.Now().Add(-10 * time.Second),
		EndTime:   time.Now(),
		ExitCode:  0,
		LogURIs:   map[string]string{"stdout": "s3://bucket/logs/stdout.gz"},
		CallerMeta: map[string]string{"cluster": "c-123"},
	}

	if err := PersistTerminalSnapshot(ctx, s, "emr/steps", "step-001", snap); err != nil {
		t.Fatalf("PersistTerminalSnapshot: %v", err)
	}

	loaded, found, err := LoadTerminalSnapshot(ctx, s, "emr/steps", "step-001")
	if err != nil {
		t.Fatalf("LoadTerminalSnapshot: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if loaded.State != "COMPLETED" {
		t.Errorf("expected State=COMPLETED, got %s", loaded.State)
	}
	if loaded.LogURIs["stdout"] != "s3://bucket/logs/stdout.gz" {
		t.Errorf("expected LogURIs preserved, got %v", loaded.LogURIs)
	}
}

func TestPersistTerminalSnapshot_DoubleSave_NoError(t *testing.T) {
	s := newMemStore()
	ctx := context.Background()

	snap := Snapshot{State: "COMPLETED", Reason: "first"}

	if err := PersistTerminalSnapshot(ctx, s, "emr/steps", "step-002", snap); err != nil {
		t.Fatalf("first save: %v", err)
	}

	snap2 := Snapshot{State: "FAILED", Reason: "second"}
	if err := PersistTerminalSnapshot(ctx, s, "emr/steps", "step-002", snap2); err != nil {
		t.Fatalf("second save should not error: %v", err)
	}

	// First write wins.
	loaded, _, _ := LoadTerminalSnapshot(ctx, s, "emr/steps", "step-002")
	if loaded.State != "COMPLETED" {
		t.Errorf("expected first-write State=COMPLETED to win, got %s", loaded.State)
	}
}

func TestLoadTerminalSnapshot_NotFound(t *testing.T) {
	s := newMemStore()
	ctx := context.Background()

	snap, found, err := LoadTerminalSnapshot(ctx, s, "emr/steps", "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false")
	}
	if snap.State != "" {
		t.Errorf("expected empty Snapshot, got %+v", snap)
	}
}

func TestBuildSnapshotFromError(t *testing.T) {
	snap := BuildSnapshotFromError(errors.New("submit failed"))
	if snap.State != "FAILED" {
		t.Errorf("expected State=FAILED, got %s", snap.State)
	}
	if snap.Reason != "submit failed" {
		t.Errorf("expected Reason=submit failed, got %s", snap.Reason)
	}
}
