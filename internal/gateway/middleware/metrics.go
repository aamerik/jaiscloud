package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaiscloud_requests_total",
		Help: "Total number of requests processed.",
	}, []string{"service", "action", "status"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaiscloud_request_duration_seconds",
		Help:    "Request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "action"})
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

		// Best-effort label extraction from path (/_jaiscloud/* vs service requests)
		service := "unknown"
		action := "unknown"
		if len(r.URL.Path) > 1 {
			service = "aws"
			action = r.Method
		}

		requestsTotal.WithLabelValues(service, action, strconv.Itoa(mw.status)).Inc()
		requestDuration.WithLabelValues(service, action).Observe(dur)
	})
}
