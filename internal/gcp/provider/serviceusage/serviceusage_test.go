package serviceusage

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     params,
		ResourceID: resource.ResourceID("proj"),
	}
}

func newProvider() *Provider { return New(store.NewMemoryResourceStore()) }

func opResponse(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("operation is not done: %v", resp.Data)
	}
	r, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("operation has no response object: %v", resp.Data)
	}
	return r
}

func TestEnableGetListRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	svc, _ := opResponse(t, resp)["service"].(map[string]any)
	if svc["state"] != stateEnabled {
		t.Fatalf("enable response state = %v, want ENABLED", svc["state"])
	}
	if svc["name"] != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("enable response name = %v", svc["name"])
	}
	if cfg, _ := svc["config"].(map[string]any); cfg["name"] != "run.googleapis.com" {
		t.Fatalf("enable response config = %v", svc["config"])
	}

	resp, err = p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["state"] != stateEnabled {
		t.Fatalf("get state = %v, want ENABLED", resp.Data["state"])
	}
	if resp.Data["parent"] != "projects/proj" {
		t.Fatalf("get parent = %v", resp.Data["parent"])
	}

	// filter=state:ENABLED must include it; state:DISABLED must not.
	resp, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "filter": "state:ENABLED"}))
	if err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 1 {
		t.Fatalf("list enabled count = %d, want 1", got)
	}
	resp, _ = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "filter": "state:DISABLED"}))
	if got := len(resp.Data["services"].([]any)); got != 0 {
		t.Fatalf("list disabled count = %d, want 0", got)
	}
	// No filter lists everything tracked.
	resp, _ = p.ListServices(ctx, newNR(map[string]any{"project": "proj"}))
	if got := len(resp.Data["services"].([]any)); got != 1 {
		t.Fatalf("unfiltered list count = %d, want 1", got)
	}
}

// failingStore surfaces a storage outage; embedded ResourceStore satisfies the
// remaining interface methods (nil is never called).
type failingStore struct{ store.ResourceStore }

func (failingStore) Get(context.Context, string, string, string, string) (store.ResourceEntry, error) {
	return store.ResourceEntry{}, store.ErrStorageUnavailable
}

func (failingStore) Upsert(context.Context, string, string, store.ResourceEntry) error {
	return store.ErrStorageUnavailable
}

func TestStorageErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	p := New(failingStore{})

	if _, err := p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("GetService error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("EnableService error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := p.DisableService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("DisableService error = %v, want ErrStorageUnavailable", err)
	}
}

func TestMissingParamsAreInvalidArgument(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	handlers := map[string]func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error){
		"list":        p.ListServices,
		"get":         p.GetService,
		"enable":      p.EnableService,
		"disable":     p.DisableService,
		"batchEnable": p.BatchEnableServices,
	}
	for name, fn := range handlers {
		_, err := fn(ctx, newNR(nil))
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Errorf("%s: err = %v, want 400 InvalidArgument", name, err)
		}
	}
}

func TestGetUnknownServiceIsDisabled(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "unknown.googleapis.com"}))
	if err != nil {
		t.Fatalf("get unknown: %v", err)
	}
	if resp.Data["state"] != stateDisabled {
		t.Fatalf("unknown service state = %v, want DISABLED", resp.Data["state"])
	}
}

func TestDisableNotEnabledIsFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	_, err := p.DisableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != "FailedPrecondition" || pe.HTTPStatus != 400 {
		t.Fatalf("disable not-enabled = %+v, want FailedPrecondition/400", pe)
	}

	// Enable then disable succeeds and flips the stored state.
	if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"})); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if _, err := p.DisableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	resp, _ := p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	if resp.Data["state"] != stateDisabled {
		t.Fatalf("state after disable = %v, want DISABLED", resp.Data["state"])
	}
}

func TestBatchEnable(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.BatchEnableServices(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    map[string]any{"serviceIds": []any{"a.googleapis.com", "b.googleapis.com"}},
	}))
	if err != nil {
		t.Fatalf("batchEnable: %v", err)
	}
	services := opResponse(t, resp)["services"].([]any)
	if len(services) != 2 {
		t.Fatalf("batchEnable services = %d, want 2", len(services))
	}
	for _, svc := range services {
		if svc.(map[string]any)["state"] != stateEnabled {
			t.Fatalf("batchEnable state = %v", svc)
		}
	}

	// Empty and oversized batches are rejected.
	if _, err := p.BatchEnableServices(ctx, newNR(map[string]any{"project": "proj", "body": map[string]any{}})); err == nil {
		t.Error("expected error for empty batch")
	}
	ids := make([]any, maxBatchEnable+1)
	for i := range ids {
		ids[i] = "s.googleapis.com"
	}
	if _, err := p.BatchEnableServices(ctx, newNR(map[string]any{"project": "proj", "body": map[string]any{"serviceIds": ids}})); err == nil {
		t.Error("expected error for oversized batch")
	}
}

func TestListPaginationAndFilterValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, s := range []string{"a.googleapis.com", "b.googleapis.com", "c.googleapis.com"} {
		if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": s})); err != nil {
			t.Fatalf("enable %s: %v", s, err)
		}
	}

	resp, err := p.ListServices(ctx, newNR(map[string]any{"project": "proj", "pageSize": "2"}))
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 2 {
		t.Fatalf("page 1 size = %d, want 2", got)
	}
	next, _ := resp.Data["nextPageToken"].(string)
	if next == "" {
		t.Fatal("expected nextPageToken on a full page")
	}
	resp, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "pageSize": "2", "pageToken": next}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 1 {
		t.Fatalf("page 2 size = %d, want 1", got)
	}

	// Invalid filter is a 400 InvalidArgument.
	_, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "filter": "state:RUNNING"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("invalid filter error = %v, want 400", err)
	}
}
