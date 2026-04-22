package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Context key types — unexported to prevent collisions with other packages.
type cloudContextKey struct{}
type serviceContextKey struct{}
type actionContextKey struct{}
type labelsHolderKey struct{}

// labelsHolder is a mutable struct injected into the context by outer middleware
// so that inner handlers can populate cloud/service/action after decoding. Using
// a pointer allows mutation without replacing the context value.
type labelsHolder struct {
	cloud, service, action string
}

// WithRequestLabels mutates the labelsHolder in ctx (if present) and also sets
// immutable context values for callers that read via GetRequestLabels directly.
func WithRequestLabels(ctx context.Context, cloud, service, action string) context.Context {
	if h, ok := ctx.Value(labelsHolderKey{}).(*labelsHolder); ok {
		h.cloud = cloud
		h.service = service
		h.action = action
	}
	ctx = context.WithValue(ctx, cloudContextKey{}, cloud)
	ctx = context.WithValue(ctx, serviceContextKey{}, service)
	ctx = context.WithValue(ctx, actionContextKey{}, action)
	return ctx
}

// GetRequestLabels retrieves the service and action labels stored by WithRequestLabels.
// Returns empty strings if the labels have not been set (e.g. admin endpoints).
func GetRequestLabels(ctx context.Context) (service, action string) {
	if s, ok := ctx.Value(serviceContextKey{}).(string); ok {
		service = s
	}
	if a, ok := ctx.Value(actionContextKey{}).(string); ok {
		action = a
	}
	return
}

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaiscloud_requests_total",
		Help: "Total number of requests processed.",
	}, []string{"cloud", "service", "action", "status"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaiscloud_request_duration_seconds",
		Help:    "Request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"cloud", "service", "action"})
)

// metricsResponseWriter captures the status code.
type metricsResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *metricsResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Metrics returns a middleware that records Prometheus metrics per request.
// Labels are read from the labelsHolder injected by the Logging middleware so
// that values set inside handleCloudRequest are visible here after the handler returns.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mw := &metricsResponseWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		next.ServeHTTP(mw, r)
		dur := time.Since(start).Seconds()

		// Read labels from the mutable holder (injected by Logging outer middleware)
		// when available; fall back to immutable context values set via
		// *r = *r.WithContext(WithRequestLabels(...)) for standalone Metrics usage.
		cloud := "unknown"
		service := "unknown"
		action := r.Method

		if h, ok := r.Context().Value(labelsHolderKey{}).(*labelsHolder); ok && h.service != "" {
			cloud = h.cloud
			service = h.service
			action = h.action
		} else if s := r.Context().Value(serviceContextKey{}); s != nil {
			cloud = func() string {
				if c := r.Context().Value(cloudContextKey{}); c != nil {
					return c.(string)
				}
				return "unknown"
			}()
			service = s.(string)
			if a := r.Context().Value(actionContextKey{}); a != nil {
				action = a.(string)
			}
		}

		requestsTotal.WithLabelValues(cloud, service, action, strconv.Itoa(mw.status)).Inc()
		requestDuration.WithLabelValues(cloud, service, action).Observe(dur)
	})
}
