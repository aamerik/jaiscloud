package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
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

func binding(role, member string) map[string]any {
	return map[string]any{"role": role, "members": []any{member}}
}

func TestGetProject(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.GetProject(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("get project: %v", err)
	}
	if resp.Data["projectId"] != "proj" {
		t.Errorf("projectId = %v", resp.Data["projectId"])
	}
	if resp.Data["projectNumber"] != resource.ProjectNumber("proj") {
		t.Errorf("projectNumber = %v, want %v", resp.Data["projectNumber"], resource.ProjectNumber("proj"))
	}
	if resp.Data["lifecycleState"] != "ACTIVE" {
		t.Errorf("lifecycleState = %v, want ACTIVE", resp.Data["lifecycleState"])
	}
}

func TestIamPolicyMergeAndOcc(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	// Read the empty default policy and use its etag for the first write.
	resp, err := p.GetIamPolicy(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	etag, _ := resp.Data["etag"].(string)
	if etag == "" {
		t.Fatal("default policy has no etag")
	}

	// First member.
	resp, err = p.SetIamPolicy(ctx, newNR(map[string]any{
		"project": "proj",
		"body": map[string]any{"policy": map[string]any{
			"etag":     etag,
			"bindings": []any{binding("roles/pubsub.publisher", "serviceAccount:sa@proj.iam.gserviceaccount.com")},
		}},
	}))
	if err != nil {
		t.Fatalf("setIamPolicy #1: %v", err)
	}
	etag2, _ := resp.Data["etag"].(string)
	if etag2 == "" || etag2 == etag {
		t.Fatalf("etag must rotate on write: old=%q new=%q", etag, etag2)
	}

	// Second member, merged on top of the first using the fresh etag — both
	// grants must survive (this is exactly the provider's read-modify-write).
	first := resp.Data["bindings"].([]any)
	merged := append(append([]any{}, first...), binding("roles/pubsub.viewer", "user:compat-viewer@example.com"))
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{
		"project": "proj",
		"body": map[string]any{"policy": map[string]any{
			"etag":     etag2,
			"bindings": merged,
		}},
	})); err != nil {
		t.Fatalf("setIamPolicy #2: %v", err)
	}

	resp, err = p.GetIamPolicy(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("getIamPolicy after merges: %v", err)
	}
	bindings, _ := resp.Data["bindings"].([]any)
	if len(bindings) != 2 {
		t.Fatalf("bindings = %d, want 2 (both grants survive): %v", len(bindings), bindings)
	}

	// A stale etag is rejected with ABORTED/409.
	_, err = p.SetIamPolicy(ctx, newNR(map[string]any{
		"project": "proj",
		"body": map[string]any{"policy": map[string]any{
			"etag":     "stale-etag",
			"bindings": []any{},
		}},
	}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("stale etag error = %T (%v), want *model.ProviderError", err, err)
	}
	if pe.HTTPStatus != 409 || pe.Status != "ABORTED" {
		t.Fatalf("stale etag = %+v, want 409 ABORTED", pe)
	}
}

func TestMissingProjectIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	handlers := map[string]provider.HandlerFunc{
		"getProject":         p.GetProject,
		"getIamPolicy":       p.GetIamPolicy,
		"setIamPolicy":       p.SetIamPolicy,
		"testIamPermissions": p.TestIamPermissions,
	}
	for name, fn := range handlers {
		_, err := fn(ctx, newNR(nil))
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Errorf("%s: err = %v, want 400 InvalidArgument", name, err)
		}
	}
}

func TestTestIamPermissions(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.TestIamPermissions(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    map[string]any{"permissions": []any{"resourcemanager.projects.get", "resourcemanager.projects.setIamPolicy"}},
	}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	perms, _ := resp.Data["permissions"].([]string)
	if len(perms) != 2 {
		t.Fatalf("permissions = %v, want 2", resp.Data["permissions"])
	}
}
