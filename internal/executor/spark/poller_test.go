package spark_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/executor/spark"
)

func TestStatusPoller_TrackAndDetectTransition(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(20 * time.Millisecond)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})

	var mu sync.Mutex
	var events []spark.StateChangeEvent

	poller := spark.NewStatusPoller(m, 10*time.Millisecond, func(e spark.StateChangeEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	poller.Track("j1", spark.StateRunning)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	poller.Start(ctx)
	defer poller.Stop()

	// Wait for transition
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("expected at least one state change event")
	}
	e := events[0]
	if e.JobID != "j1" || e.OldState != spark.StateRunning || e.NewState != spark.StateCompleted {
		t.Errorf("unexpected event: %+v", e)
	}
}

func TestStatusPoller_CurrentState(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(10 * time.Minute)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})

	poller := spark.NewStatusPoller(m, time.Hour, nil)
	poller.Track("j1", spark.StateRunning)

	state := poller.CurrentState("j1")
	if state != spark.StateRunning {
		t.Errorf("expected RUNNING, got %s", state)
	}

	state = poller.CurrentState("unknown")
	if state != "" {
		t.Errorf("unknown job should return empty string, got %s", state)
	}
}

func TestStatusPoller_Reset_ClearsJobs(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(time.Hour)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})

	poller := spark.NewStatusPoller(m, time.Hour, nil)
	poller.Track("j1", spark.StateRunning)
	poller.Reset()

	state := poller.CurrentState("j1")
	if state != "" {
		t.Errorf("after Reset, state should be empty, got %s", state)
	}
}

func TestStatusPoller_NoCallbackOnNoChange(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(10 * time.Minute)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})

	called := 0
	poller := spark.NewStatusPoller(m, 10*time.Millisecond, func(_ spark.StateChangeEvent) {
		called++
	})
	poller.Track("j1", spark.StateRunning)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	poller.Start(ctx)
	<-ctx.Done()
	poller.Stop()

	if called != 0 {
		t.Errorf("expected no state change events (job stays RUNNING), got %d", called)
	}
}

func TestStatusPoller_StopIdempotent(t *testing.T) {
	m := spark.NewMockExecutor()
	poller := spark.NewStatusPoller(m, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	poller.Start(ctx)
	cancel()
	poller.Stop() // should not deadlock
}

// ── Stale-job reconciliation (Phase 7 §9.7) ─────────────────────────────────

// notFoundExec is a minimal SparkExecutor whose Status always returns ErrJobNotFound.
type notFoundExec struct{}

func (notFoundExec) Submit(ctx context.Context, job spark.SparkJob) error { return nil }
func (notFoundExec) Status(ctx context.Context, jobID string) (spark.SparkStatus, error) {
	return spark.SparkStatus{}, fmt.Errorf("not found: %w", spark.ErrJobNotFound)
}
func (notFoundExec) Cancel(ctx context.Context, jobID string) error { return nil }
func (notFoundExec) Close() error                                    { return nil }
func (notFoundExec) Reset()                                          {}

func TestStatusPoller_StaleJobReconciliation(t *testing.T) {
	fc := clock.FixedClock{T: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	var mu sync.Mutex
	var events []spark.StateChangeEvent

	poller := spark.NewStatusPoller(notFoundExec{}, time.Hour, func(e spark.StateChangeEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}).WithReconcileTimeout(5*time.Minute).WithClock(fc)

	poller.Track("j1", spark.StateRunning)

	ctx := context.Background()

	// First tick: job is missing — missingSince starts, no event.
	poller.PollAll(ctx)
	mu.Lock()
	n := len(events)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("first tick should not fire event, got %d", n)
	}

	// Advance clock past reconcile timeout.
	fc.T = fc.T.Add(6 * time.Minute)
	poller.WithClock(fc)

	// Second tick: timeout exceeded — should fire FAILED.
	poller.PollAll(ctx)
	mu.Lock()
	got := len(events)
	mu.Unlock()
	if got != 1 {
		t.Fatalf("second tick should fire 1 FAILED event, got %d", got)
	}
	if events[0].NewState != spark.StateFailed {
		t.Errorf("reconciled state should be FAILED, got %q", events[0].NewState)
	}
}
