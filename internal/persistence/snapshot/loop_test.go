package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/persistence/version"
	"jaiscloud/internal/snapshottypes"
)

// mockSnapshotter implements snapshottypes.Snapshotter for tests.
type mockSnapshotter struct {
	data    map[string]string
	snapCnt atomic.Int64
}

func newMockSnapshotter() *mockSnapshotter {
	return &mockSnapshotter{data: map[string]string{"key": "value"}}
}

func (m *mockSnapshotter) Snapshot(_ context.Context, w io.Writer) error {
	m.snapCnt.Add(1)
	return json.NewEncoder(w).Encode(m.data)
}

func (m *mockSnapshotter) Restore(_ context.Context, r io.Reader) error {
	return json.NewDecoder(r).Decode(&m.data)
}

func (m *mockSnapshotter) IsEmpty(_ context.Context) (bool, error) {
	return len(m.data) == 0, nil
}

// TestSnapshotLoop_PeriodicSave verifies that the loop saves state to
// DataDir/state.json at least once during its run.
func TestSnapshotLoop_PeriodicSave(t *testing.T) {
	dir := t.TempDir()
	snap := newMockSnapshotter()

	cfg := SnapshotLoopConfig{
		Stores:      map[string]snapshottypes.Snapshotter{"test": snap},
		DataDir:     dir,
		Interval:    20 * time.Millisecond,
		Clock:       clock.RealClock{},
		SaveTimeout: 5 * time.Second,
	}
	loop := NewSnapshotLoop(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	loop.Start(ctx)

	// Wait for at least one save.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap.snapCnt.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	loop.Stop()

	if snap.snapCnt.Load() == 0 {
		t.Fatal("expected at least one snapshot to be taken")
	}

	// Verify the file exists and contains valid JSON.
	data, err := os.ReadFile(dir + "/state.json")
	if err != nil {
		t.Fatalf("read state.json: %v", err)
	}
	var env version.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("parse state.json: %v", err)
	}
	if env.SchemaVersion != version.CodeSnapshotVersion {
		t.Errorf("expected schema version %d, got %d", version.CodeSnapshotVersion, env.SchemaVersion)
	}
}

// TestSnapshotLoop_ConcurrentSave_OnlyOne verifies that concurrent SaveNow
// calls do not double-snapshot (at-most-once semantics).
func TestSnapshotLoop_ConcurrentSave_OnlyOne(t *testing.T) {
	dir := t.TempDir()

	// slowSnapshotter blocks until released, letting us interleave goroutines.
	var blocked atomic.Bool
	var callCnt atomic.Int64
	releaseC := make(chan struct{})

	slow := &slowSnapshotter{
		blocked:  &blocked,
		callCnt:  &callCnt,
		releaseC: releaseC,
	}

	cfg := SnapshotLoopConfig{
		Stores:      map[string]snapshottypes.Snapshotter{"slow": slow},
		DataDir:     dir,
		Interval:    1 * time.Hour, // no periodic saves during the test
		Clock:       clock.RealClock{},
		SaveTimeout: 5 * time.Second,
	}
	loop := NewSnapshotLoop(cfg)

	// Start two concurrent SaveNow calls.
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	blocked.Store(true) // block first save
	go func() { done1 <- loop.SaveNow(context.Background()) }()
	// Give the first goroutine time to enter SaveNow.
	time.Sleep(10 * time.Millisecond)
	go func() { done2 <- loop.SaveNow(context.Background()) }()

	// Release the first save.
	close(releaseC)

	if err := <-done1; err != nil {
		t.Fatalf("first SaveNow: %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("second SaveNow: %v", err)
	}

	// Only one snapshot should have been taken — the second returned early.
	if n := callCnt.Load(); n != 1 {
		t.Errorf("expected exactly 1 snapshot call, got %d", n)
	}
}

// slowSnapshotter blocks its Snapshot call until releaseC is closed.
type slowSnapshotter struct {
	blocked  *atomic.Bool
	callCnt  *atomic.Int64
	releaseC chan struct{}
}

func (s *slowSnapshotter) Snapshot(_ context.Context, w io.Writer) error {
	s.callCnt.Add(1)
	if s.blocked.Load() {
		<-s.releaseC
	}
	return json.NewEncoder(w).Encode(map[string]string{})
}

func (s *slowSnapshotter) Restore(_ context.Context, r io.Reader) error {
	var m map[string]string
	return json.NewDecoder(r).Decode(&m)
}

func (s *slowSnapshotter) IsEmpty(_ context.Context) (bool, error) { return true, nil }

// Compile-time check that mockSnapshotter / slowSnapshotter satisfy the interface.
var _ snapshottypes.Snapshotter = (*mockSnapshotter)(nil)
var _ snapshottypes.Snapshotter = (*slowSnapshotter)(nil)

// mustReadEnvelope reads and parses state.json from dir.
func mustReadEnvelope(t *testing.T, dir string) version.Envelope {
	t.Helper()
	data, err := os.ReadFile(dir + "/state.json")
	if err != nil {
		t.Fatalf("read state.json: %v", err)
	}
	var env version.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("parse state.json: %v", err)
	}
	return env
}

// keep compiler happy — bytes is used by the import.
var _ = bytes.Buffer{}

// TestSnapshotLoop_SaveNow_AtomicRename verifies that SaveNow does not leave a
// .tmp file on success.
func TestSnapshotLoop_SaveNow_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	snap := newMockSnapshotter()

	cfg := SnapshotLoopConfig{
		Stores:      map[string]snapshottypes.Snapshotter{"test": snap},
		DataDir:     dir,
		Interval:    1 * time.Hour,
		Clock:       clock.RealClock{},
		SaveTimeout: 5 * time.Second,
	}
	loop := NewSnapshotLoop(cfg)

	if err := loop.SaveNow(context.Background()); err != nil {
		t.Fatalf("SaveNow: %v", err)
	}

	// state.json must exist.
	if _, err := os.Stat(dir + "/state.json"); err != nil {
		t.Fatalf("expected state.json to exist: %v", err)
	}

	// No .tmp file must remain.
	tmpFiles, err := filepath.Glob(dir + "/*.tmp")
	if err != nil {
		t.Fatalf("filepath.Glob: %v", err)
	}
	if len(tmpFiles) > 0 {
		t.Errorf("unexpected .tmp files left behind: %v", tmpFiles)
	}
}

// TestSnapshotLoop_SaveNow_BarrierRLock verifies that SaveNow blocks while the
// barrier write lock is held and proceeds once the lock is released.
func TestSnapshotLoop_SaveNow_BarrierRLock(t *testing.T) {
	dir := t.TempDir()
	snap := newMockSnapshotter()
	barrier := NewBarrier()

	cfg := SnapshotLoopConfig{
		Barrier:     barrier,
		Stores:      map[string]snapshottypes.Snapshotter{"test": snap},
		DataDir:     dir,
		Interval:    1 * time.Hour,
		Clock:       clock.RealClock{},
		SaveTimeout: 5 * time.Second,
	}
	loop := NewSnapshotLoop(cfg)

	// Acquire the write lock so SaveNow will block on ReadBegin.
	ctx := context.Background()
	release, err := barrier.WriteBegin(ctx)
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}

	saveNowDone := make(chan error, 1)
	go func() {
		saveNowDone <- loop.SaveNow(ctx)
	}()

	// After 20ms, SaveNow should still be waiting for the read lock.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-saveNowDone:
		t.Fatal("SaveNow completed before write lock was released — expected it to block")
	default:
		// Good — still waiting.
	}

	// Release the write lock.
	release()

	// SaveNow must complete within 500ms.
	select {
	case err := <-saveNowDone:
		if err != nil {
			t.Fatalf("SaveNow returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SaveNow did not complete within 500ms after write lock was released")
	}

	// Verify state.json was written.
	if _, err := os.Stat(dir + "/state.json"); err != nil {
		t.Fatalf("expected state.json to exist after SaveNow: %v", err)
	}
}

// TestSnapshotLoop_PeriodicSave_Timeout verifies that a slow Snapshot
// implementation that exceeds SaveTimeout does not crash the loop; the loop
// should log a warning and continue running.
func TestSnapshotLoop_PeriodicSave_Timeout(t *testing.T) {
	dir := t.TempDir()

	slow := &sleepSnapshotter{sleep: 200 * time.Millisecond}

	cfg := SnapshotLoopConfig{
		Stores:      map[string]snapshottypes.Snapshotter{"slow": slow},
		DataDir:     dir,
		Interval:    30 * time.Millisecond,
		Clock:       clock.RealClock{},
		SaveTimeout: 50 * time.Millisecond,
	}
	loop := NewSnapshotLoop(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	loop.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	cancel()
	loop.Stop()
	// If we reach this point the loop exited cleanly without panicking.
}

// sleepSnapshotter is a Snapshotter whose Snapshot blocks for a fixed duration.
type sleepSnapshotter struct {
	sleep time.Duration
}

func (s *sleepSnapshotter) Snapshot(ctx context.Context, w io.Writer) error {
	select {
	case <-time.After(s.sleep):
		return json.NewEncoder(w).Encode(map[string]string{})
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *sleepSnapshotter) Restore(_ context.Context, r io.Reader) error {
	var m map[string]string
	return json.NewDecoder(r).Decode(&m)
}

func (s *sleepSnapshotter) IsEmpty(_ context.Context) (bool, error) { return true, nil }

var _ snapshottypes.Snapshotter = (*sleepSnapshotter)(nil)
