package dynamodb

import (
	"sync"
	"time"

	"jaiscloud/internal/clock"
)

type tokenBucket struct {
	capacity   int
	tokens     float64
	refillRate float64 // tokens per second
	last       time.Time
	mu         sync.Mutex
}

func newTokenBucket(capacity int, refillRate float64) *tokenBucket {
	return &tokenBucket{
		capacity:   capacity,
		tokens:     float64(capacity),
		refillRate: refillRate,
		// simulated time. A frozen clock would make elapsed=0 on every call,
		// preventing token refill and throttling all DynamoDB operations.
		last: clock.RealNow(),
	}
}

// TryConsume attempts to consume n tokens. Returns false if insufficient tokens.
func (b *tokenBucket) TryConsume(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := clock.RealNow()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	cap := float64(b.capacity)
	refilled := b.tokens + elapsed*b.refillRate
	if refilled > cap {
		refilled = cap
	}
	b.tokens = refilled
	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}
