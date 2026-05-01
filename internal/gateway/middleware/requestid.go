package middleware

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"

	"jaiscloud/internal/reqctx"
)

// RequestID injects a unique X-Request-Id header into each request/response.
// If randSrc is nil, uses crypto-random source via math/rand default.
func RequestID(randSrc rand.Source) func(http.Handler) http.Handler {
	var rng *rand.Rand
	if randSrc != nil {
		rng = rand.New(randSrc)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Request-Id")
			if id == "" {
				if rng != nil {
					id = fmt.Sprintf("%016x", rng.Int63())
				} else {
					id = fmt.Sprintf("%016x", rand.Int63())
				}
			}
			ctx := reqctx.WithRequestID(r.Context(), id)
			w.Header().Set("X-Request-Id", id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetRequestID retrieves the request ID from context.
// Delegates to reqctx.GetRequestID; kept here so existing middleware callers compile unchanged.
func GetRequestID(ctx context.Context) string {
	return reqctx.GetRequestID(ctx)
}
