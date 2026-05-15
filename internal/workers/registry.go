package workers

import (
	"context"
	"log/slog"
	"sync"
)

// Worker is implemented by any background goroutine that can be started and stopped.
type Worker interface {
	Run(ctx context.Context)
}

type namedWorker struct {
	name string
	w    Worker
}

// Registry centralises lifecycle for all background goroutines.
type Registry struct {
	mu      sync.Mutex
	workers []namedWorker
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	ctx     context.Context
}

// New returns a new Registry.
func New() *Registry {
	return &Registry{}
}

// Add registers a worker to be started when Start is called.
// Must be called before Start.
func (r *Registry) Add(name string, w Worker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers = append(r.workers, namedWorker{name: name, w: w})
}

// Start creates a child context and launches all registered workers.
func (r *Registry) Start(parent context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	r.ctx = ctx
	r.cancel = cancel
	for _, nw := range r.workers {
		r.wg.Add(1)
		go func(nw namedWorker) {
			defer r.wg.Done()
			slog.Debug("worker started", "name", nw.name)
			nw.w.Run(ctx)
			slog.Debug("worker stopped", "name", nw.name)
		}(nw)
	}
}

// Stop cancels the shared context and waits for all workers to finish.
func (r *Registry) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
}
