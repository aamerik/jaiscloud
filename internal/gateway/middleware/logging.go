package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Logging logs each request. Successful requests (< 400) are logged at the
// configured level. Error responses (>= 400) are always logged at Error so
// failures are visible regardless of the configured log level.
func Logging(level string) func(http.Handler) http.Handler {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Inject a mutable holder so handleCloudRequest can populate labels
			// after codec decode. Reading after next.ServeHTTP gives us the final values.
			holder := &labelsHolder{}
			r = r.WithContext(context.WithValue(r.Context(), labelsHolderKey{}, holder))

			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)

			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", GetRequestID(r.Context()),
			}

			// Service and action are populated by WithRequestLabels inside the handler.
			if holder.service != "" {
				attrs = append(attrs, "service", holder.service, "action", holder.action)
			}

			if rw.status >= 400 {
				slog.Error("request error", attrs...)
			} else {
				slog.Log(r.Context(), logLevel, "request", attrs...)
			}
		})
	}
}
