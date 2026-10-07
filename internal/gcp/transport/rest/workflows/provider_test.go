package workflows

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
)

const source = "main:\n  steps:\n    - r:\n        return: 1\n"

func newProvider(t *testing.T) *Provider {
	t.Helper()
	return NewProvider(core.NewService(workflowsstore.NewMemoryStore()), "default-proj")
}

func nr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

// TestProviderRoundTrip exercises the REST handlers over the shared core:
// create (LRO) → get → list → update (masked) → getOperation → delete.
func TestProviderRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	create := nr(map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1",
		"body": map[string]any{
			"description":    "hello",
			"sourceContents": source,
			"labels":         map[string]any{"k": "v"},
		},
	})
	cresp, err := p.CreateWorkflow(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if cresp.Data["done"] != true {
		t.Fatalf("create not done: %v", cresp.Data)
	}
	opName, _ := cresp.Data["name"].(string)
	if !strings.HasPrefix(opName, "projects/proj/locations/us-central1/operations/") {
		t.Fatalf("operation name = %q", opName)
	}
	response, _ := cresp.Data["response"].(map[string]any)
	if response["name"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Fatalf("operation response name = %v", response["name"])
	}
	wantSA := "projects/proj/serviceAccounts/" + resource.ProjectNumber("proj") + "-compute@developer.gserviceaccount.com"
	if response["serviceAccount"] != wantSA {
		t.Fatalf("serviceAccount = %v, want %v", response["serviceAccount"], wantSA)
	}

	// Duplicate → AlreadyExists.
	if _, err := p.CreateWorkflow(ctx, create); err == nil {
		t.Fatalf("expected AlreadyExists on duplicate create")
	}

	get := nr(map[string]any{"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	gresp, err := p.GetWorkflow(ctx, get)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gresp.Data["sourceContents"] != source || gresp.Data["state"] != "ACTIVE" {
		t.Fatalf("unexpected get data: %v", gresp.Data)
	}

	list := nr(map[string]any{"project": "proj", "location": "us-central1"})
	lresp, err := p.ListWorkflows(ctx, list)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := lresp.Data["workflows"].([]any)
	if len(items) != 1 {
		t.Fatalf("list len = %d, want 1", len(items))
	}

	upd := nr(map[string]any{
		"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1",
		"updateMask": "description",
		"body":       map[string]any{"description": "new"},
	})
	uresp, err := p.UpdateWorkflow(ctx, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	updResp, _ := uresp.Data["response"].(map[string]any)
	if updResp["description"] != "new" {
		t.Fatalf("update description = %v", updResp["description"])
	}
	if updResp["sourceContents"] != source {
		t.Fatalf("update should preserve sourceContents: %v", updResp["sourceContents"])
	}

	getOp := nr(map[string]any{"project": "proj", "location": "us-central1", "name": opName})
	oresp, err := p.GetOperation(ctx, getOp)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if oresp.Data["name"] != opName {
		t.Fatalf("get operation name = %v, want %v", oresp.Data["name"], opName)
	}

	del := nr(map[string]any{"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	dresp, err := p.DeleteWorkflow(ctx, del)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if dresp.Data["done"] != true {
		t.Fatalf("delete not done: %v", dresp.Data)
	}

	if _, err := p.GetWorkflow(ctx, get); err == nil {
		t.Fatalf("expected NotFound after delete")
	}
}

const source2 = "main:\n  steps:\n    - r:\n        return: 2\n"

// TestProviderListWorkflowRevisions exercises the :listRevisions handler and
// the revisionId-scoped GetWorkflow over the shared core.
func TestProviderListWorkflowRevisions(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	create := nr(map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1",
		"body": map[string]any{"sourceContents": source},
	})
	cresp, err := p.CreateWorkflow(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	first, _ := cresp.Data["response"].(map[string]any)
	firstRev, _ := first["revisionId"].(string)
	if firstRev == "" {
		t.Fatalf("create response missing revisionId: %v", first)
	}

	upd := nr(map[string]any{
		"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1",
		"updateMask": "sourceContents",
		"body":       map[string]any{"sourceContents": source2},
	})
	if _, err := p.UpdateWorkflow(ctx, upd); err != nil {
		t.Fatalf("update source: %v", err)
	}

	list := nr(map[string]any{"project": "proj", "location": "us-central1",
		"name": "locations/us-central1/workflows/wf1"})
	lresp, err := p.ListWorkflowRevisions(ctx, list)
	if err != nil {
		t.Fatalf("listRevisions: %v", err)
	}
	items, _ := lresp.Data["workflows"].([]any)
	if len(items) != 2 {
		t.Fatalf("revisions = %d, want 2", len(items))
	}
	newest, _ := items[0].(map[string]any)
	oldest, _ := items[1].(map[string]any)
	if newest["revisionId"] == firstRev {
		t.Fatalf("newest revision = oldest %q", newest["revisionId"])
	}
	if newest["sourceContents"] != source2 {
		t.Fatalf("newest source = %v, want updated", newest["sourceContents"])
	}
	if oldest["sourceContents"] != source {
		t.Fatalf("oldest source = %v, want original preserved", oldest["sourceContents"])
	}

	// revisionId selects the historical revision.
	get := nr(map[string]any{"project": "proj", "location": "us-central1",
		"name": "locations/us-central1/workflows/wf1", "revisionId": firstRev})
	gresp, err := p.GetWorkflow(ctx, get)
	if err != nil {
		t.Fatalf("get revision: %v", err)
	}
	if gresp.Data["sourceContents"] != source || gresp.Data["revisionId"] != firstRev {
		t.Fatalf("get revision data = %v", gresp.Data)
	}

	// An unknown revision is NotFound.
	get.Params["revisionId"] = "000999-zzz"
	if _, err := p.GetWorkflow(ctx, get); err == nil || !core.IsNotFound(err) {
		t.Fatalf("expected NotFound for unknown revision, got %v", err)
	}
}

// TestProviderUsesDefaultProjectWhenAbsent verifies the configured default is
// used when neither the path nor the account scope names a project.
func TestProviderUsesDefaultProjectWhenAbsent(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	_, err := p.ListWorkflows(ctx, &model.NormalizedRequest{Params: map[string]any{"location": "us-central1"}})
	if err != nil {
		t.Fatalf("list with default project: %v", err)
	}
}

// stubOperationResolver reports handled when opID matches want, returning body.
type stubOperationResolver struct {
	want    string
	body    map[string]any
	handled bool
}

func (s stubOperationResolver) ResolveOperation(_ context.Context, _, _, opID string) (map[string]any, bool, error) {
	if s.handled && opID == s.want {
		return s.body, true, nil
	}
	return nil, false, nil
}

// TestProviderResolvesForeignLocationOperation verifies that an unknown
// Workflows operation name falls through to the wired cross-service resolvers
// (the opt-in async path for Metastore/Managed Kafka) and that a resolver that
// declines still yields the canonical NotFound.
func TestProviderResolvesForeignLocationOperation(t *testing.T) {
	ctx := context.Background()
	get := nr(map[string]any{
		"project": "proj", "location": "us-central1",
		"name": "projects/proj/locations/us-central1/operations/abc123",
	})

	// No resolvers → the Workflows NotFound stands.
	p := newProvider(t)
	if _, err := p.GetOperation(ctx, get); err == nil || !core.IsNotFound(err) {
		t.Fatalf("expected NotFound without resolvers, got %v", err)
	}

	// A declining resolver does not change that.
	declining := newProvider(t)
	declining.SetOperationResolvers(stubOperationResolver{want: "abc123", handled: false})
	if _, err := declining.GetOperation(ctx, get); err == nil || !core.IsNotFound(err) {
		t.Fatalf("expected NotFound with a declining resolver, got %v", err)
	}

	// An owning resolver serves its own JSON.
	owning := newProvider(t)
	owning.SetOperationResolvers(stubOperationResolver{
		want:    "abc123",
		handled: true,
		body:    map[string]any{"name": get.Params["name"], "done": true},
	})
	resp, err := owning.GetOperation(ctx, get)
	if err != nil {
		t.Fatalf("resolved operation: %v", err)
	}
	if resp.Data["done"] != true || resp.Data["name"] != get.Params["name"] {
		t.Fatalf("resolved data = %v", resp.Data)
	}
}
