package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/persistence/version"
	"jaiscloud/internal/snapshottypes"
)

// SnapshotLoopConfig holds the configuration for the periodic snapshot loop.
type SnapshotLoopConfig struct {
	Barrier     *Barrier
	Stores      map[string]snapshottypes.Snapshotter
	BlobStore   *blobfs.LocalFSBlobStore
	DataDir     string
	Interval    time.Duration
	Clock       clock.Clock
	SaveTimeout time.Duration // default 10s if zero
}

// SnapshotLoop periodically persists all registered store state to
// <DataDir>/state.json using atomic writes.
type SnapshotLoop struct {
	cfg     SnapshotLoopConfig
	saving  atomic.Bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	mu      sync.Mutex
}

// NewSnapshotLoop creates a new SnapshotLoop. Call Start to begin periodic saves.
func NewSnapshotLoop(cfg SnapshotLoopConfig) *SnapshotLoop {
	if cfg.SaveTimeout == 0 {
		cfg.SaveTimeout = 10 * time.Second
	}
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	return &SnapshotLoop{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the periodic save goroutine. The loop runs until Stop is called
// or ctx is cancelled.
func (loop *SnapshotLoop) Start(ctx context.Context) {
	go func() {
		defer close(loop.doneCh)
		ticker := time.NewTicker(loop.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-loop.stopCh:
				return
			case <-ticker.C:
				saveCtx, cancel := context.WithTimeout(ctx, loop.cfg.SaveTimeout)
				if err := loop.SaveNow(saveCtx); err != nil {
					slog.Warn("snapshot loop: periodic save failed", "err", err)
				}
				cancel()
			}
		}
	}()
}

// Stop signals the loop to stop and waits for the goroutine to exit.
// It then runs a final SaveNow with context.Background() to capture the
// last state before shutdown.
func (loop *SnapshotLoop) Stop() {
	loop.mu.Lock()
	select {
	case <-loop.stopCh:
		// already closed
	default:
		close(loop.stopCh)
	}
	loop.mu.Unlock()
	<-loop.doneCh

	// Final save on graceful shutdown.
	saveCtx, cancel := context.WithTimeout(context.Background(), loop.cfg.SaveTimeout)
	defer cancel()
	if err := loop.SaveNow(saveCtx); err != nil {
		slog.Warn("snapshot loop: final save on shutdown failed", "err", err)
	}
}

// SaveNow performs a single snapshot save. If a save is already in progress,
// it returns nil immediately (at-most-once semantics).
func (loop *SnapshotLoop) SaveNow(ctx context.Context) error {
	if !loop.saving.CompareAndSwap(false, true) {
		return nil // another save already in progress
	}
	defer loop.saving.Store(false)

	if loop.cfg.DataDir == "" {
		return fmt.Errorf("snapshot loop: DataDir not set")
	}

	// Acquire shared read lock so imports/resets don't race with our read.
	var releaseFn func()
	if loop.cfg.Barrier != nil {
		release, err := loop.cfg.Barrier.ReadBegin(ctx)
		if err != nil {
			return fmt.Errorf("snapshot loop: acquire barrier: %w", err)
		}
		releaseFn = release
	}
	defer func() {
		if releaseFn != nil {
			releaseFn()
		}
	}()

	// Snapshot all stores.
	stores := make(map[string]json.RawMessage, len(loop.cfg.Stores))
	for name, s := range loop.cfg.Stores {
		var buf bytes.Buffer
		if err := s.Snapshot(ctx, &buf); err != nil {
			return fmt.Errorf("snapshot loop: snapshot %s: %w", name, err)
		}
		stores[name] = json.RawMessage(buf.Bytes())
	}

	env := version.Envelope{
		SchemaVersion: version.CodeSnapshotVersion,
		CreatedAt:     time.Now().UTC(),
		Stores:        stores,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("snapshot loop: marshal: %w", err)
	}

	dst := loop.cfg.DataDir + "/state.json"
	if err := WriteAtomic(dst, data); err != nil {
		return fmt.Errorf("snapshot loop: write %s: %w", dst, err)
	}
	slog.Debug("snapshot loop: state saved", "path", dst, "stores", len(stores))
	return nil
}
