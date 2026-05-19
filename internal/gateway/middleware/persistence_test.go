package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/gateway/middleware"
)

type fakeBarrier struct{ locked bool }

func (f *fakeBarrier) TryReadBegin() (func(), bool) {
	if f.locked {
		return nil, false
	}
	return func() {}, true
}

func TestBarrier_Gateway503_UnderWriteLock(t *testing.T) {
	b := &fakeBarrier{locked: true}
	mw := middleware.Persistence(b)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/any-path", nil)
	mw(inner).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Errorf("expected Retry-After: 1, got %q", rec.Header().Get("Retry-After"))
	}
}

func TestBarrier_Gateway_PassesThrough_WhenFree(t *testing.T) {
	b := &fakeBarrier{locked: false}
	mw := middleware.Persistence(b)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/any-path", nil)
	mw(inner).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
