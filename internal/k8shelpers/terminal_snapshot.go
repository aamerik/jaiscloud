package k8shelpers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"jaiscloud/internal/store"
)

// TerminalStore is the subset of store.ResourceStore that terminal-snapshot
// persistence needs. Defined narrowly so providers can substitute in tests.
type TerminalStore interface {
	Create(ctx context.Context, entry store.ResourceEntry) error
	Get(ctx context.Context, resourceType, id string) (store.ResourceEntry, error)
}

const snapshotResourceType = "k8s_terminal_snapshot"

// PersistTerminalSnapshot writes a Snapshot to the store keyed by a
// provider-supplied prefix + jobID.
//
// Idempotent: if a snapshot already exists, logs WARN and returns nil
// (first-write value wins).
func PersistTerminalSnapshot(ctx context.Context, s TerminalStore, prefix, jobID string, snap Snapshot) error {
	key := prefix + "/" + jobID
	snap.JobID = jobID

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("k8shelpers: marshal snapshot: %w", err)
	}

	err = s.Create(ctx, store.ResourceEntry{
		Type:      snapshotResourceType,
		ID:        key,
		Data:      data,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	if errors.Is(err, store.ErrAlreadyExists) {
		slog.Warn("k8shelpers: terminal snapshot already exists, keeping first write", "key", key)
		return nil
	}
	return err
}

// LoadTerminalSnapshot reads the previously-persisted Snapshot.
// Returns (Snapshot{}, false, nil) if no snapshot exists.
func LoadTerminalSnapshot(ctx context.Context, s TerminalStore, prefix, jobID string) (Snapshot, bool, error) {
	key := prefix + "/" + jobID
	entry, err := s.Get(ctx, snapshotResourceType, key)
	if errors.Is(err, store.ErrNotFound) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(entry.Data, &snap); err != nil {
		return Snapshot{}, false, fmt.Errorf("k8shelpers: unmarshal snapshot: %w", err)
	}
	return snap, true, nil
}

// BuildSnapshot renders a Snapshot from a Final (success path).
func BuildSnapshot(final Final, state string) Snapshot {
	return Snapshot{
		State:     state,
		Reason:    final.Reason,
		Message:   final.Message,
		StartTime: final.StartTime,
		EndTime:   final.EndTime,
		ExitCode:  final.ExitCode,
	}
}

// BuildSnapshotFromError renders a Snapshot for an early-failure path
// (before WaitTerminal ever returned).
func BuildSnapshotFromError(err error) Snapshot {
	return Snapshot{
		State:   "FAILED",
		Reason:  err.Error(),
		EndTime: time.Now(),
	}
}
