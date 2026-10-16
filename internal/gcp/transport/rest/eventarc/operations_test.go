package eventarc

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// TestOperationsSurface covers the Eventarc-host long-running-operations surface
// (reached only under the eventarc host — see adapter.operationsHostService).
// Eventarc mutations complete inline, so no operation is ever persisted:
// list is empty, get/delete are NotFound, and cancel is the shared no-op
// success contract.
func TestOperationsSurface(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	resp, err := p.ListOperations(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	ops, ok := resp.Data["operations"].([]any)
	if !ok || len(ops) != 0 {
		t.Fatalf("operations = %v, want an empty list", resp.Data["operations"])
	}

	for _, tc := range []struct {
		name string
		fn   func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"get", p.GetOperation},
		{"delete", p.DeleteOperation},
	} {
		_, err := tc.fn(ctx, newNR(map[string]any{"name": "projects/proj/locations/us-central1/operations/op1"}))
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 404 {
			t.Errorf("%s: err = %v, want 404 NotFound", tc.name, err)
		}
	}

	// A cancel of a (never-persisted) operation succeeds, mirroring the shared
	// google.longrunning cancel contract the gRPC operations service uses.
	if _, err := p.CancelOperation(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/us-central1/operations/op1",
	})); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}
}
