// Package reqctx stores per-request values in context.
// It is imported by both gateway/middleware (to set) and providers (to read),
// keeping providers free of any dependency on the gateway layer.
package reqctx

import "context"

type ctxKey string

const requestIDKey ctxKey = "request_id"

// WithRequestID returns a context carrying the given request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// GetRequestID returns the request ID stored in ctx, or "" if absent.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}
