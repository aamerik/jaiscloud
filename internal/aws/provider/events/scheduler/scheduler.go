// Package scheduler implements the EventBridge cron/rate rule scheduler.
package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"jaiscloud/internal/aws/provider/events/targets"
	"jaiscloud/internal/clock"
)

// HandlerCtx carries the identity context for background goroutines.
type HandlerCtx struct {
	Cloud     string
	Region    string
	AccountID string
}

type targetRef struct {
	Target targets.Target
}

type entry struct {
	ruleARN  string
	schedule cron.Schedule
	hctx     HandlerCtx
	targets  []targetRef
	nextFire time.Time
	index    int
}

type minHeap []*entry

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].nextFire.Before(h[j].nextFire) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *minHeap) Push(x any)        { e := x.(*entry); e.index = len(*h); *h = append(*h, e) }
func (h *minHeap) Pop() any          { old := *h; n := len(old); e := old[n-1]; *h = old[:n-1]; return e }

// Dispatcher is the interface used by the scheduler to deliver events.
type Dispatcher interface {
	Send(ctx context.Context, t targets.Target, envelope map[string]any) error
}

// Scheduler fires EventBridge rules on cron/rate schedules.
type Scheduler struct {
	mu         sync.Mutex
	entries    map[string]*entry
	heap       minHeap
	dispatcher Dispatcher
	clk        clock.Clock
	changed    chan struct{}
}

// New constructs a Scheduler.
func New(dispatcher Dispatcher, clk clock.Clock) *Scheduler {
	return &Scheduler{
		entries:    make(map[string]*entry),
		dispatcher: dispatcher,
		clk:        clk,
		changed:    make(chan struct{}, 1),
	}
}

// Add registers or replaces a scheduled rule.
func (s *Scheduler) Add(ruleARN, expr string, hctx HandlerCtx, tgts []targets.Target) error {
	sched, err := parseSchedule(expr)
	if err != nil {
		return err
	}
	now := s.clk.Now()
	refs := make([]targetRef, len(tgts))
	for i, t := range tgts {
		refs[i] = targetRef{Target: t}
	}
	e := &entry{
		ruleARN:  ruleARN,
		schedule: sched,
		hctx:     hctx,
		targets:  refs,
		nextFire: sched.Next(now),
	}
	s.mu.Lock()
	if old, ok := s.entries[ruleARN]; ok {
		heap.Remove(&s.heap, old.index)
	}
	s.entries[ruleARN] = e
	heap.Push(&s.heap, e)
	s.mu.Unlock()
	s.notify()
	return nil
}

// Remove deregisters a scheduled rule.
func (s *Scheduler) Remove(ruleARN string) {
	s.mu.Lock()
	if e, ok := s.entries[ruleARN]; ok {
		heap.Remove(&s.heap, e.index)
		delete(s.entries, ruleARN)
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Scheduler) notify() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

// Run implements workers.Worker — blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		s.mu.Lock()
		var delay time.Duration
		if len(s.heap) == 0 {
			delay = time.Hour
		} else {
			delay = time.Until(s.heap[0].nextFire)
		}
		s.mu.Unlock()

		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.changed:
			timer.Stop()
			continue
		case <-timer.C:
			s.fire(ctx)
		}
	}
}

func (s *Scheduler) fire(ctx context.Context) {
	now := s.clk.Now()
	for {
		s.mu.Lock()
		if len(s.heap) == 0 || s.heap[0].nextFire.After(now) {
			s.mu.Unlock()
			return
		}
		e := heap.Pop(&s.heap).(*entry)
		e.nextFire = e.schedule.Next(now)
		heap.Push(&s.heap, e)
		hctx := e.hctx
		tgts := e.targets
		s.mu.Unlock()

		envelope := buildScheduledEventEnvelope(hctx, now)
		for _, ref := range tgts {
			go func(t targets.Target) {
				_ = s.dispatcher.Send(ctx, t, envelope)
			}(ref.Target)
		}
	}
}

func buildScheduledEventEnvelope(hctx HandlerCtx, now time.Time) map[string]any {
	return map[string]any{
		"version":     "0",
		"id":          fmt.Sprintf("sched-%d", now.UnixNano()),
		"source":      "aws.events",
		"detail-type": "Scheduled Event",
		"detail":      map[string]any{},
		"account":     hctx.AccountID,
		"region":      hctx.Region,
		"time":        now.UTC().Format(time.RFC3339),
		"resources":   []any{},
	}
}

var rateRE = regexp.MustCompile(`^rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)$`)

func parseSchedule(expr string) (cron.Schedule, error) {
	if m := rateRE.FindStringSubmatch(expr); m != nil {
		n, _ := strconv.Atoi(m[1])
		if n == 0 {
			return nil, fmt.Errorf("rate: value must be positive")
		}
		unit := m[2]
		// AWS requires singular form when n==1 and plural when n>1.
		if n == 1 && strings.HasSuffix(unit, "s") {
			return nil, fmt.Errorf("rate: use singular unit (e.g. '1 minute') when value is 1")
		}
		if n > 1 && !strings.HasSuffix(unit, "s") {
			return nil, fmt.Errorf("rate: use plural unit (e.g. '2 minutes') when value is > 1")
		}
		var d time.Duration
		switch strings.TrimSuffix(unit, "s") {
		case "minute":
			d = time.Duration(n) * time.Minute
		case "hour":
			d = time.Duration(n) * time.Hour
		case "day":
			d = time.Duration(n) * 24 * time.Hour
		}
		return &fixedInterval{d: d}, nil
	}
	if strings.HasPrefix(expr, "cron(") && strings.HasSuffix(expr, ")") {
		// AWS cron has 6 fields (including year); strip the year field.
		inner := expr[5 : len(expr)-1]
		fields := strings.Fields(inner)
		if len(fields) == 6 {
			// Drop the last (year) field to get standard 5-field POSIX cron.
			inner = strings.Join(fields[:5], " ")
		}
		return cron.ParseStandard(inner)
	}
	return nil, fmt.Errorf("scheduler: unsupported expression %q (expected rate(...) or cron(...))", expr)
}

type fixedInterval struct{ d time.Duration }

func (f *fixedInterval) Next(t time.Time) time.Time { return t.Add(f.d) }
