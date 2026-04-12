package spark

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MockExecutor is an in-memory SparkExecutor for testing.
// Jobs transition from RUNNING → COMPLETED after a configurable delay.
// Default delay is 0 (immediate COMPLETED on first Status poll).
type MockExecutor struct {
	mu    sync.RWMutex
	jobs  map[string]*mockJob
	delay time.Duration // how long until a job moves to COMPLETED
}

type mockJob struct {
	status    SparkStatus
	startedAt time.Time
	cancelled bool
}

// NewMockExecutor creates a MockExecutor with immediate COMPLETED transitions.
func NewMockExecutor() *MockExecutor {
	return &MockExecutor{jobs: make(map[string]*mockJob)}
}

// NewMockExecutorWithDelay creates a MockExecutor where jobs complete after delay.
func NewMockExecutorWithDelay(delay time.Duration) *MockExecutor {
	return &MockExecutor{jobs: make(map[string]*mockJob), delay: delay}
}

func (m *MockExecutor) Submit(_ context.Context, job SparkJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.JobID] = &mockJob{
		status:    SparkStatus{JobID: job.JobID, State: StateRunning},
		startedAt: time.Now(),
	}
	return nil
}

func (m *MockExecutor) Status(_ context.Context, jobID string) (SparkStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return SparkStatus{}, fmt.Errorf("mock: unknown job %q", jobID)
	}
	if j.cancelled {
		j.status.State = StateCancelled
		return j.status, nil
	}
	// Transition to COMPLETED after delay
	if !j.status.State.IsTerminal() && time.Since(j.startedAt) >= m.delay {
		j.status.State = StateCompleted
	}
	return j.status, nil
}

func (m *MockExecutor) Cancel(_ context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return nil // already gone — no-op
	}
	j.cancelled = true
	j.status.State = StateCancelled
	return nil
}

func (m *MockExecutor) Close() error { return nil }

// Reset clears all job records. Called from plugin.Reset().
func (m *MockExecutor) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = make(map[string]*mockJob)
}

// ForceState sets a job to a specific state. Used in tests to fast-forward.
func (m *MockExecutor) ForceState(jobID string, state SparkState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobID]; ok {
		j.status.State = state
	}
}
