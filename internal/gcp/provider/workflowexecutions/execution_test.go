package workflowexecutions

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func newProvider(t *testing.T, source string) *Provider {
	t.Helper()
	s := workflowsstore.NewMemoryStore()
	w := workflowsstore.Workflow{ID: "wf1", Location: "us-central1", SourceContents: source,
		State: "ACTIVE", RevisionID: "000001-a4d"}
	if err := s.CreateWorkflow(context.Background(), "proj", "us-central1", "wf1", w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	return New(s, workflowengine.New())
}

const assignReturnSource = "main:\n  params: [a, b]\n  steps:\n    - init:\n        assign:\n          - sum: ${a + b}\n    - done:\n        return: ${sum}\n"

func TestCreateAndGetExecution(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, assignReturnSource)

	nr := newNR(map[string]any{
		"name": "locations/us-central1/workflows/wf1/executions",
		"body": map[string]any{"argument": `{"a": 20, "b": 22}`},
	})
	resp, err := p.CreateExecution(ctx, nr)
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if resp.Data["state"] != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %v", resp.Data["state"])
	}
	if resp.Data["result"] != "42" {
		t.Fatalf("expected result 42, got %v", resp.Data["result"])
	}
	if resp.Data["workflowRevisionId"] != "000001-a4d" {
		t.Fatalf("expected revision, got %v", resp.Data["workflowRevisionId"])
	}
	name, _ := resp.Data["name"].(string)
	if name != "projects/proj/locations/us-central1/workflows/wf1/executions/"+execIDFromName(name) {
		t.Fatalf("unexpected execution name: %q", name)
	}

	// Get by name.
	relName := name[len("projects/proj/"):]
	get := newNR(map[string]any{"name": relName})
	got, err := p.GetExecution(ctx, get)
	if err != nil {
		t.Fatalf("get execution: %v", err)
	}
	if got.Data["result"] != "42" || got.Data["state"] != "SUCCEEDED" {
		t.Fatalf("unexpected get result: %v", got.Data)
	}

	// List.
	list := newNR(map[string]any{"name": "locations/us-central1/workflows/wf1/executions"})
	lr, err := p.ListExecutions(ctx, list)
	if err != nil {
		t.Fatalf("list executions: %v", err)
	}
	items, _ := lr.Data["executions"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 execution, got %d", len(items))
	}

	// Cancel is a no-op on a terminal execution.
	cancel := newNR(map[string]any{"name": relName + ""})
	_ = cancel
	cancelNR := newNR(map[string]any{"name": relName})
	cres, err := p.CancelExecution(ctx, cancelNR)
	if err != nil {
		t.Fatalf("cancel execution: %v", err)
	}
	if cres.Data["state"] != "SUCCEEDED" {
		t.Fatalf("cancel should not change a terminal execution, got %v", cres.Data["state"])
	}
}

func TestCreateExecutionMissingWorkflow(t *testing.T) {
	ctx := context.Background()
	p := New(workflowsstore.NewMemoryStore(), workflowengine.New())
	nr := newNR(map[string]any{"name": "locations/us-central1/workflows/missing/executions", "body": map[string]any{}})
	if _, err := p.CreateExecution(ctx, nr); err == nil {
		t.Fatal("expected NotFound for missing workflow")
	}
}

func TestCreateExecutionBuiltinEnvVars(t *testing.T) {
	ctx := context.Background()
	source := `main:
  steps:
    - done:
        return: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID") + "/" + sys.get_env("GOOGLE_CLOUD_LOCATION") + "/" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_ID") + "/" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_REVISION_ID")}
`
	p := newProvider(t, source)
	nr := newNR(map[string]any{"name": "locations/us-central1/workflows/wf1/executions", "body": map[string]any{}})
	resp, err := p.CreateExecution(ctx, nr)
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if resp.Data["state"] != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %v", resp.Data["state"])
	}
	// proj (project) / us-central1 (location) / wf1 (workflow) / 000001-a4d (revision).
	if resp.Data["result"] != `"proj/us-central1/wf1/000001-a4d"` {
		t.Fatalf("unexpected built-in resolution: %v", resp.Data["result"])
	}
}

func TestCreateExecutionBadArgument(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, assignReturnSource)
	nr := newNR(map[string]any{"name": "locations/us-central1/workflows/wf1/executions", "body": map[string]any{"argument": "not json"}})
	if _, err := p.CreateExecution(ctx, nr); err == nil {
		t.Fatal("expected InvalidArgument for malformed argument JSON")
	}
}

func TestCreateExecutionFailsLoud(t *testing.T) {
	ctx := context.Background()
	// A workflow referencing an unknown stdlib function must FAIL (not no-op).
	p := newProvider(t, "main:\n  steps:\n    - x:\n        call: http.delete\n        args:\n          url: \"http://x\"\n")
	nr := newNR(map[string]any{"name": "locations/us-central1/workflows/wf1/executions", "body": map[string]any{}})
	resp, err := p.CreateExecution(ctx, nr)
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if resp.Data["state"] != "FAILED" {
		t.Fatalf("expected FAILED, got %v", resp.Data["state"])
	}
	errObj, _ := resp.Data["error"].(map[string]any)
	if errObj == nil || errObj["payload"] == "" {
		t.Fatalf("expected error payload, got %v", resp.Data["error"])
	}
}

func TestCreateExecutionInvalidYamlReturns400(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, "main: [unclosed")
	nr := newNR(map[string]any{"name": "locations/us-central1/workflows/wf1/executions", "body": map[string]any{}})
	if _, err := p.CreateExecution(ctx, nr); err == nil {
		t.Fatal("expected InvalidArgument for invalid workflow YAML")
	}
}

func execIDFromName(name string) string {
	const prefix = "projects/proj/locations/us-central1/workflows/wf1/executions/"
	if len(name) > len(prefix) {
		return name[len(prefix):]
	}
	return name
}
