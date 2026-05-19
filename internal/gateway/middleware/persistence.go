package middleware

import (
	"net/http"
)

// BarrierMiddleware is implemented by the persistence barrier.
type BarrierMiddleware interface {
	// TryReadBegin attempts a non-blocking shared read lock.
	// Returns (release, true) if acquired, (nil, false) if the write lock is held (import/reset in progress).
	TryReadBegin() (release func(), ok bool)
}

// Persistence returns middleware that acquires a barrier read lock for every cloud request.
// If the write lock is held (import or reset in progress), the middleware returns
// 503 Service Unavailable with Retry-After: 1 so callers know to retry.
//
// Admin routes (/_jaiscloud/*) bypass this middleware — they manage the barrier themselves.
func Persistence(b BarrierMiddleware) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			release, ok := b.TryReadBegin()
			if !ok {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "service unavailable: state restore in progress", http.StatusServiceUnavailable)
				return
			}
			defer release()
			next.ServeHTTP(w, r)
		})
	}
}
