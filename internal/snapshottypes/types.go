// Package snapshottypes defines shared interfaces for snapshot/restore operations.
// It exists as a separate package to avoid import cycles between internal/admin
// and internal/persistence/snapshot.
package snapshottypes

import (
	"context"
	"io"
)

// Snapshotter is implemented by stores that support export/import.
type Snapshotter interface {
	// Snapshot writes deterministic JSON state to w. Keys must be sorted.
	Snapshot(ctx context.Context, w io.Writer) error
	// Restore reads JSON state from r, replacing the store's contents atomically.
	Restore(ctx context.Context, r io.Reader) error
	// IsEmpty returns true when the store holds no externally-visible state.
	// Must NOT acquire the barrier — it is called inside WriteBegin.
	IsEmpty(ctx context.Context) (bool, error)
}

// Resetter is implemented by any store that can wipe its state.
type Resetter interface {
	Reset(ctx context.Context)
}

// PostRestoreHook is called after all stores have been restored from a snapshot.
type PostRestoreHook interface {
	Name() string
	OnRestore(ctx context.Context) error
}
