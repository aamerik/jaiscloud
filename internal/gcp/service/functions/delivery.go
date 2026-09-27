package functions

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// Delivery tuning. Real Cloud Functions retries a failed invocation for up to
// seven days with an exponential backoff capped at ten seconds; the emulator
// bounds the attempt count and uses a short backoff so retries and dead-letter
// outcomes are observable in tests. This is a documented emulator cap, not a
// fidelity claim.
const (
	deliveryWorkers     = 4
	deliveryQueueSize   = 1024
	maxDeliveryAttempts = 3
	deliveryBackoffBase = 200 * time.Millisecond
	deliveryBackoffMax  = 2 * time.Second
)

// deliveryTarget is one function an event resolves to.
type deliveryTarget struct {
	project  string
	location string
	id       string
	retry    bool
}

// deliveryJob is one queued delivery: the resolved function, the source event,
// and the persisted record it updates as attempts are made.
type deliveryJob struct {
	gen   uint64 // engine generation; a mismatch means Reset invalidated the job
	rec   functionsstore.Delivery
	retry bool // the trigger's failure policy retries a failed invocation
}

// deliveryEngine delivers produced events to event-triggered functions. It runs
// a small bounded worker pool so a producer (a Pub/Sub publish, a GCS object
// write) returns without waiting for the function, while every attempt is
// recorded in the functions store for observability.
type deliveryEngine struct {
	svc  *Service
	jobs chan deliveryJob
	gen  atomic.Uint64
	// started is true once Start has launched the workers. Before that (unit
	// tests, or a binary that forgot to start it) dispatch runs inline so an
	// event is never silently dropped.
	started atomic.Bool
	wg      sync.WaitGroup
	ctx     context.Context
	mu      sync.Mutex
}

func newDeliveryEngine(svc *Service) *deliveryEngine {
	return &deliveryEngine{svc: svc, jobs: make(chan deliveryJob, deliveryQueueSize), ctx: context.Background()}
}

// Start launches the delivery workers. It is a no-op if already started. The
// context bounds the workers' lifetime and is also the parent of every delivery
// attempt, so a request context is never used after the request returns.
func (e *deliveryEngine) Start(ctx context.Context) {
	e.mu.Lock()
	if e.started.Load() {
		e.mu.Unlock()
		return
	}
	e.ctx = ctx
	e.started.Store(true)
	e.mu.Unlock()
	for i := 0; i < deliveryWorkers; i++ {
		e.wg.Add(1)
		go e.worker(ctx)
	}
}

// context returns the delivery-lifetime context for the rare overflow path.
func (e *deliveryEngine) context() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ctx
}

func (e *deliveryEngine) worker(ctx context.Context) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-e.jobs:
			e.process(ctx, job)
		}
	}
}

// reset invalidates in-flight deliveries; the underlying store reset clears the
// records.
func (e *deliveryEngine) reset() { e.gen.Add(1) }

// dispatch resolves an event to its target functions, persists a pending record
// for each, and queues (or, when not started, runs) the delivery.
func (e *deliveryEngine) dispatch(ctx context.Context, ev eventing.Event) {
	for _, t := range e.svc.resolveTargets(ctx, ev) {
		rec := newDelivery(ev, t)
		if err := e.svc.functions.CreateDelivery(ctx, t.project, t.location, rec); err != nil {
			slog.Warn("functions: persist delivery", "function", t.id, "err", err)
			continue
		}
		job := deliveryJob{gen: e.gen.Load(), rec: rec, retry: t.retry}
		if !e.started.Load() {
			// Not started (unit tests, or a binary that never called Start):
			// run inline so the event is never silently dropped.
			e.process(context.Background(), job)
			continue
		}
		select {
		case e.jobs <- job:
		default:
			// Queue full: run detached rather than dropping the event.
			go e.process(e.context(), job)
		}
	}
}

// process performs the invocation attempts for one delivery, updating the
// persisted record on success, terminal failure, or dead-letter. ctx is the
// delivery lifetime (never a producer request context, which ends when the
// request returns).
func (e *deliveryEngine) process(ctx context.Context, job deliveryJob) {
	rec := job.rec
	var lastErr string
	for attempt := 1; attempt <= maxDeliveryAttempts; attempt++ {
		if !e.active(ctx, job.gen) {
			return
		}
		_, result, invokeErr, err := e.svc.CallFunction(ctx, rec.Project, rec.Location, rec.FunctionID, rec.Data)
		rec.Attempts = attempt
		rec.UpdateTime = now()
		switch {
		case err == nil && invokeErr == "":
			rec.Status = functionsstore.DeliveryDelivered
			rec.Result = result
			rec.Error = ""
			e.persist(ctx, rec)
			return
		case err != nil:
			lastErr = err.Error()
		default:
			lastErr = invokeErr
		}
		rec.Error = lastErr
		if !job.retry || attempt >= maxDeliveryAttempts {
			break
		}
		if !sleepCtx(ctx, backoffFor(attempt)) {
			return
		}
	}
	if job.retry {
		rec.Status = functionsstore.DeliveryDeadLetter
	} else {
		rec.Status = functionsstore.DeliveryFailed
	}
	rec.Error = lastErr
	e.persist(ctx, rec)
}

func (e *deliveryEngine) active(ctx context.Context, gen uint64) bool {
	return e.gen.Load() == gen && ctx.Err() == nil
}

func (e *deliveryEngine) persist(ctx context.Context, rec functionsstore.Delivery) {
	if err := e.svc.functions.UpdateDelivery(ctx, rec.Project, rec.Location, rec); err != nil {
		slog.Warn("functions: update delivery", "function", rec.FunctionID, "err", err)
	}
}

// resolveTargets returns the de-duplicated functions an event is delivered to:
// those whose stored eventTrigger matches, plus any Eventarc trigger (whose
// destination is a cloudFunction) that routes this event.
func (s *Service) resolveTargets(ctx context.Context, ev eventing.Event) []deliveryTarget {
	seen := map[string]bool{}
	var out []deliveryTarget
	add := func(t deliveryTarget) {
		if t.project == "" || t.location == "" || t.id == "" {
			return
		}
		key := t.location + "\x00" + t.id
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, t)
	}
	if fns, err := s.functions.ListFunctionsAllLocations(ctx, ev.Project); err == nil {
		for _, f := range fns {
			if !functionMatchesEvent(f, ev) {
				continue
			}
			add(deliveryTarget{project: ev.Project, location: f.Location, id: f.ID, retry: f.EventTrigger.Retries()})
		}
	} else {
		slog.Warn("functions: list functions for event", "err", err)
	}
	if s.eventTargets != nil {
		for _, t := range s.eventTargets.TargetsForEvent(ctx, ev) {
			add(deliveryTarget{project: t.Project, location: t.Location, id: t.FunctionID, retry: t.Retry})
		}
	}
	return out
}

// functionMatchesEvent reports whether a function's eventTrigger subscribes to
// an event: the event type must match (v1 legacy and v2 forms are normalized)
// and the resource must name the same topic/bucket.
func functionMatchesEvent(f functionsstore.Function, ev eventing.Event) bool {
	et := f.EventTrigger
	if et == nil || et.Resource == "" {
		return false
	}
	return eventing.TypeMatches(et.EventType, ev.EventType) &&
		eventing.ResourceID(et.Resource) == eventing.ResourceID(ev.Resource)
}

// newDelivery builds the pending record for one target.
func newDelivery(ev eventing.Event, t deliveryTarget) functionsstore.Delivery {
	return functionsstore.Delivery{
		ID:         newUUID(),
		Project:    t.project,
		Location:   t.location,
		FunctionID: t.id,
		Source:     ev.Source,
		EventType:  ev.EventType,
		Resource:   ev.Resource,
		EventID:    ev.EventID,
		Data:       string(ev.Data),
		Attributes: ev.Attributes,
		Status:     functionsstore.DeliveryPending,
		CreateTime: now(),
		UpdateTime: now(),
	}
}

// backoffFor returns the exponential backoff before retry attempt n (1-based).
func backoffFor(attempt int) time.Duration {
	d := deliveryBackoffBase << (attempt - 1)
	if d > deliveryBackoffMax || d <= 0 {
		return deliveryBackoffMax
	}
	return d
}

// sleepCtx waits for d or the context to end, reporting whether it waited the
// full duration.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// FunctionExists reports whether a function exists, for Eventarc's
// destination.cloudFunction validation (its FunctionExister implementation).
func (s *Service) FunctionExists(ctx context.Context, project, location, id string) (bool, error) {
	if location == "" || id == "" {
		return false, nil
	}
	_, err := s.functions.GetFunction(ctx, project, location, id)
	if err != nil {
		if errors.Is(err, functionsstore.ErrNoSuchFunction) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DispatchEvent is the eventing.Dispatcher entry point: producers hand it an
// event and it is delivered to every matching function.
func (s *Service) DispatchEvent(ctx context.Context, ev eventing.Event) {
	if s == nil || s.deliveries == nil {
		return
	}
	s.deliveries.dispatch(ctx, ev)
}

// ListDeliveries returns the persisted delivery records for a location (used by
// tests and operators; real Cloud Functions has no delivery record surface).
func (s *Service) ListDeliveries(ctx context.Context, project, location string) ([]functionsstore.Delivery, error) {
	return s.functions.ListDeliveries(ctx, project, location)
}
