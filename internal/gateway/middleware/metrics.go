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

// WithRequestLabels returns a new context with cloud/service/action labels set.
// The Metrics middleware reads these labels to populate Prometheus counters.
// Call this from the gateway after decoding each cloud request.
func WithRequestLabels(ctx context.Context, cloud, service, action string) context.Context {
	ctx = context.WithValue(ctx, cloudContextKey{}, cloud)
	ctx = context.WithValue(ctx, serviceContextKey{}, service)
	ctx = context.WithValue(ctx, actionContextKey{}, action)
	return ctx
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

// Metrics returns a middleware that records Prometheus metrics per request.
// service and action are extracted from the NormalizedRequest via context, so
// we instrument at the transport level using path heuristics here.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mw := &metricsResponseWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		next.ServeHTTP(mw, r)
		dur := time.Since(start).Seconds()

		// Best-effort label extraction from request context.
		// The cloud/service/action labels are set by the gateway after codec decode;
		// for admin and unrecognised requests we fall back to safe defaults.
		cloud := "unknown"
		service := "unknown"
		action := r.Method

		if c := r.Context().Value(cloudContextKey{}); c != nil {
			cloud = c.(string)
		}
		if s := r.Context().Value(serviceContextKey{}); s != nil {
			service = s.(string)
		}
		if a := r.Context().Value(actionContextKey{}); a != nil {
			action = a.(string)
		}

		requestsTotal.WithLabelValues(cloud, service, action, strconv.Itoa(mw.status)).Inc()
		requestDuration.WithLabelValues(cloud, service, action).Observe(dur)
	})
}
