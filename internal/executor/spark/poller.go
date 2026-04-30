package spark

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// StateChangeEvent is emitted by the StatusPoller on every job state transition.
type StateChangeEvent struct {
	JobID    string
	OldState SparkState
	NewState SparkState
	// Message carries the executor's human-readable detail (e.g. stderr on failure).
	Message string
}

// OnStateChange is a callback invoked on every state transition.
type OnStateChange func(event StateChangeEvent)

// trackedJob holds the current known state of a job being polled.
type trackedJob struct {
	jobID    string
	lastState SparkState
}

// StatusPoller polls a SparkExecutor for job state changes and notifies via callbacks.
// It runs a single background goroutine that polls all tracked jobs at the given interval.
type StatusPoller struct {
	executor SparkExecutor
	interval time.Duration
	onChange OnStateChange

	mu       sync.Mutex
	jobs     map[string]*trackedJob
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewStatusPoller creates a StatusPoller that polls executor every interval.
// onChange is called on each state transition; may be nil.
func NewStatusPoller(executor SparkExecutor, interval time.Duration, onChange OnStateChange) *StatusPoller {
	return &StatusPoller{
		executor: executor,
		interval: interval,
		onChange: onChange,
		jobs:     make(map[string]*trackedJob),
		stopCh:   make(chan struct{}),
	}
}

// Track starts polling for the given jobID, using initialState as the known state.
// If the job is already tracked, Track is a no-op.
func (p *StatusPoller) Track(jobID string, initialState SparkState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.jobs[jobID]; !exists {
		p.jobs[jobID] = &trackedJob{jobID: jobID, lastState: initialState}
	}
}

// Start begins the background polling goroutine.
// Call Stop (or cancel ctx) to stop polling.
func (p *StatusPoller) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.pollLoop(ctx)
}

// Stop signals the poll loop to exit and waits for it to finish.
// Safe to call multiple times.
func (p *StatusPoller) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.wg.Wait()
}

// Reset clears all tracked jobs. Called from plugin.Reset().
func (p *StatusPoller) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.jobs = make(map[string]*trackedJob)
}

func (p *StatusPoller) pollLoop(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.pollAll(ctx)
		}
	}
}

func (p *StatusPoller) pollAll(ctx context.Context) {
	p.mu.Lock()
	snapshot := make([]*trackedJob, 0, len(p.jobs))
	for _, j := range p.jobs {
		if !j.lastState.IsTerminal() {
			snapshot = append(snapshot, j)
		}
	}
	p.mu.Unlock()

	for _, j := range snapshot {
		status, err := p.executor.Status(ctx, j.jobID)
		if err != nil {
			slog.Warn("poller: status check failed", "jobID", j.jobID, "err", err)
			continue
		}

		if status.State != j.lastState {
			old := j.lastState
			p.mu.Lock()
			j.lastState = status.State
			p.mu.Unlock()

			if status.State == StateFailed {
				slog.Warn("poller: job failed", "jobID", j.jobID, "from", old, "message", status.Message)
			} else {
				slog.Info("poller: job state changed", "jobID", j.jobID, "from", old, "to", status.State)
			}
			if p.onChange != nil {
				p.onChange(StateChangeEvent{JobID: j.jobID, OldState: old, NewState: status.State, Message: status.Message})
			}
		}
	}
}

// CurrentState returns the last known state for a job, or "" if not tracked.
func (p *StatusPoller) CurrentState(jobID string) SparkState {
	p.mu.Lock()
	defer p.mu.Unlock()
	if j, ok := p.jobs[jobID]; ok {
		return j.lastState
	}
	return ""
}
