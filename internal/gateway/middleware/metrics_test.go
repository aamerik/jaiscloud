package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/gateway/middleware"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TestMetrics_FallbackLabels verifies that requests without WithRequestLabels get
// "unknown" cloud/service and the HTTP method as the action label — no panic.
func TestMetrics_FallbackLabels(t *testing.T) {
	handler := middleware.Metrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))

	req := httptest.NewRequest(http.MethodGet, "/unknown-path", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

// TestMetrics_WithRequestLabels_Propagates verifies that labels set via
// WithRequestLabels appear in scraped Prometheus output with the correct values.
func TestMetrics_WithRequestLabels_Propagates(t *testing.T) {
	cloud, service, action := "aws", "sqs", "SendMessage"

	// The inner handler enriches the request context with labels — exactly what
	// the gateway does after decoding the request.
	handler := middleware.Metrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*r = *r.WithContext(middleware.WithRequestLabels(r.Context(), cloud, service, action))
		w.WriteHeader(200)
	}))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	// Scrape from the default (global) Prometheus registry.
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(metricsRec, metricsReq)

	body := metricsRec.Body.String()
	wantSnippets := []string{
		`cloud="aws"`,
		`service="sqs"`,
		`action="SendMessage"`,
		`status="200"`,
	}
	for _, s := range wantSnippets {
		if !strings.Contains(body, s) {
			t.Errorf("Prometheus output missing %q\n--- output (first 800 chars) ---\n%.800s", s, body)
		}
	}
}

// TestMetrics_WithRequestLabels_OverridesExistingLabels verifies that calling
// WithRequestLabels twice within the same handler chain yields the most-recent values.
func TestMetrics_WithRequestLabels_OverridesExistingLabels(t *testing.T) {
	handler := middleware.Metrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First call: aws/s3/PutObject
		ctx := middleware.WithRequestLabels(r.Context(), "aws", "s3", "PutObject")
		// Second call overrides: azure/blob/UploadBlob
		ctx = middleware.WithRequestLabels(ctx, "azure", "blob", "UploadBlob")
		*r = *r.WithContext(ctx)
		w.WriteHeader(201)
	}))

	req := httptest.NewRequest(http.MethodPut, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != 201 {
		t.Fatalf("expected 201, got %d", rr.Code)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(metricsRec, metricsReq)

	body := metricsRec.Body.String()
	// The second (overriding) values must appear.
	for _, s := range []string{`cloud="azure"`, `service="blob"`, `action="UploadBlob"`} {
		if !strings.Contains(body, s) {
			t.Errorf("expected overridden label %q in Prometheus output", s)
		}
	}
}

// TestMetrics_StatusCodeLabel verifies that non-200 responses are recorded with
// the correct HTTP status label.
func TestMetrics_StatusCodeLabel(t *testing.T) {
	handler := middleware.Metrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*r = *r.WithContext(middleware.WithRequestLabels(r.Context(), "aws", "iam", "CreateUser"))
		w.WriteHeader(400)
	}))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(metricsRec, metricsReq)

	if !strings.Contains(metricsRec.Body.String(), `status="400"`) {
		t.Errorf("expected status=400 label in Prometheus output")
	}
}
