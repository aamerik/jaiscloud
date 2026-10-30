package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// stubObserverAdapter implements adapter.CloudAdapter plus the optional
// AccountObserver capability.
type stubObserverAdapter struct {
	stubFilterAdapter
	account  string
	observed []string
}

func (a *stubObserverAdapter) EnrichRequest(*http.Request, string, string) (string, string, string) {
	return "global", a.account, ""
}

func (a *stubObserverAdapter) ObserveAccount(_ context.Context, account, service string) {
	a.observed = append(a.observed, account+"/"+service)
}

// TestGatewayObservesServedAccount proves the gateway reports every served
// (account, service) pair to an adapter that opts into observation, so the GCP
// project-discovery registry sees traffic it would otherwise miss.
func TestGatewayObservesServedAccount(t *testing.T) {
	adp := &stubObserverAdapter{account: "proj"}
	s := newFilterServer(adp, &stubStorageProvider{})

	rec := httptest.NewRecorder()
	s.handleCloudRequest(rec, httptest.NewRequest(http.MethodGet, "/storage/v1/b/bkt/o/x", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if len(adp.observed) != 1 || adp.observed[0] != "proj/storage" {
		t.Fatalf("observed = %v, want [proj/storage]", adp.observed)
	}
}

// TestGatewaySkipsObservationWithoutAccount covers the guard: the gateway only
// reports pairs that are fully resolved.
func TestGatewaySkipsObservationWithoutAccount(t *testing.T) {
	adp := &stubObserverAdapter{account: ""}
	s := newFilterServer(adp, &stubStorageProvider{})

	rec := httptest.NewRecorder()
	s.handleCloudRequest(rec, httptest.NewRequest(http.MethodGet, "/storage/v1/b/bkt/o/x", nil))

	if len(adp.observed) != 0 {
		t.Fatalf("observed = %v, want none without a resolved account", adp.observed)
	}
}
