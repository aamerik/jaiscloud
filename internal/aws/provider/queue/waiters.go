package queue

import (
	"context"
	"sync"
	"time"

	sqsstore "jaiscloud/internal/aws/store/sqs"
	"jaiscloud/internal/clock"
)

// Waiters holds per-queue channels for long-polling notification.
type Waiters struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

// NewWaiters constructs an empty Waiters.
func NewWaiters() *Waiters {
	return &Waiters{waiters: make(map[string][]chan struct{})}
}

// Register returns a channel that will receive a signal when a message arrives
// on queueURL, plus a deregistration closure the caller must invoke when done.
func (w *Waiters) Register(queueURL string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	w.waiters[queueURL] = append(w.waiters[queueURL], ch)
	w.mu.Unlock()
	return ch, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		list := w.waiters[queueURL]
		for i, c := range list {
			if c == ch {
				w.waiters[queueURL] = append(list[:i], list[i+1:]...)
				return
			}
		}
	}
}

// Notify wakes all waiters for queueURL without blocking.
func (w *Waiters) Notify(queueURL string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.waiters[queueURL] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Reset drains all channels and wipes the waiter map (used by admin reset).
func (w *Waiters) Reset(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waiters = make(map[string][]chan struct{})
}

// WaitForMessages polls store.Receive until at least one message is available
// or the context / waitTime deadline is reached.
func WaitForMessages(
	ctx context.Context,
	store sqsstore.SQSMessageStore,
	waiters *Waiters,
	account, region string,
	queueURL string,
	maxMessages int,
	waitTime time.Duration,
	clk clock.Clock,
) ([]sqsstore.SQSMessage, error) {
	deadline := clk.Now().Add(waitTime)
	for {
		msgs, err := store.Receive(ctx, account, region, queueURL, maxMessages, clk.Now())
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			return msgs, nil
		}
		remaining := deadline.Sub(clk.Now())
		if remaining <= 0 {
			return nil, nil
		}
		ch, deregister := waiters.Register(queueURL)
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			deregister()
			return nil, nil
		}
		timer.Stop()
		deregister()
	}
}
