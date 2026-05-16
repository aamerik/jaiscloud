package dynamodb

import (
	"sync"
	"time"
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
		last:       time.Now(),
	}
}

// TryConsume attempts to consume n tokens. Returns false if insufficient tokens.
func (b *tokenBucket) TryConsume(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
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
