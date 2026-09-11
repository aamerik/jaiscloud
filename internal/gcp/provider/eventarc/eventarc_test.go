package eventarc

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func newProvider() (*Provider, *store.MemoryResourceStore, *workflowsstore.MemoryStore) {
	resources := store.NewMemoryResourceStore()
	workflows := workflowsstore.NewMemoryStore()
	return New(eventarcstore.NewMemoryStore(), resources, workflows), resources, workflows
}

func createWorkflow(t *testing.T, workflows *workflowsstore.MemoryStore, location, id string) {
	t.Helper()
	if err := workflows.CreateWorkflow(context.Background(), "proj", location, id, workflowsstore.Workflow{}); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
}

func createTopic(t *testing.T, resources *store.MemoryResourceStore, topicID string) {
	t.Helper()
	if err := resources.Create(context.Background(), "proj", store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: topicID, Data: []byte(`{}`)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

func triggerBody(workflowName string) map[string]any {
	return map[string]any{
		"destination":  map[string]any{"workflow": workflowName},
		"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.workflows.workflow.v1.executed"}},
	}
}

func TestTriggerCRUD(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")

	wfName := "projects/proj/locations/us-central1/workflows/w1"
	createResp, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "triggerId": "t1", "body": triggerBody(wfName),
	}))
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if createResp.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", createResp.Data["done"])
	}
	created, _ := createResp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/triggers/t1"
	if created["name"] != wantName {
		t.Errorf("name = %v, want %v", created["name"], wantName)
	}
	if created["uid"] == "" || created["etag"] == "" {
		t.Errorf("uid/etag not populated: %v", created)
	}

	// Get echoes the trigger with a stable uid/etag.
	got, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if got.Data["name"] != wantName || got.Data["uid"] != created["uid"] || got.Data["etag"] != created["etag"] {
		t.Errorf("get = %v, want name=%v uid=%v etag=%v", got.Data, wantName, created["uid"], created["etag"])
	}

	// List.
	listResp, err := p.ListTriggers(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if items, _ := listResp.Data["triggers"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(items))
	}

	// Update (labels merge).
	updResp, err := p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"body": map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("UpdateTrigger: %v", err)
	}
	updated, _ := updResp.Data["response"].(map[string]any)
	if labels, _ := updated["labels"].(map[string]string); labels["env"] != "prod" {
		t.Errorf("updated labels = %v, want env=prod", updated["labels"])
	}

	// Delete.
	delResp, err := p.DeleteTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	if delResp.Data["done"] != true {
		t.Errorf("delete done = %v, want true", delResp.Data["done"])
	}
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

func TestTriggerValidation(t *testing.T) {
	ctx := context.Background()
	p, resources, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	createTopic(t, resources, "my-topic")

	cases := []struct {
		name string
		body map[string]any
		code string
	}{
		{"missing eventFilters", map[string]any{"destination": map[string]any{"workflow": "projects/proj/locations/us-central1/workflows/w1"}}, "InvalidArgument"},
		{"missing destination", map[string]any{"eventFilters": []any{map[string]any{"attribute": "type", "value": "x"}}}, "InvalidArgument"},
		{"workflow not found", triggerBody("projects/proj/locations/us-central1/workflows/missing"), "NotFound"},
		{"topic not found", map[string]any{
			"destination":  map[string]any{"cloudRun": map[string]any{"service": "svc", "region": "us-central1"}},
			"eventFilters": []any{map[string]any{"attribute": "type", "value": "x"}},
			"transport":    map[string]any{"pubsub": map[string]any{"topic": "projects/proj/topics/missing"}},
		}, "NotFound"},
	}
	for _, tc := range cases {
		_, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t", "body": tc.body}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != tc.code {
			t.Errorf("%s: expected %s, got %v", tc.name, tc.code, err)
		}
	}
}

func TestTriggerWithPubsubTopicSucceeds(t *testing.T) {
	ctx := context.Background()
	p, resources, _ := newProvider()
	createTopic(t, resources, "my-topic")

	body := map[string]any{
		"destination":  map[string]any{"cloudRun": map[string]any{"service": "svc", "region": "us-central1"}},
		"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
		"transport":    map[string]any{"pubsub": map[string]any{"topic": "projects/proj/topics/my-topic"}},
	}
	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t", "body": body})); err != nil {
		t.Fatalf("CreateTrigger with existing topic: %v", err)
	}
}

func TestTriggerMissingParams(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1"})); err == nil {
		t.Fatal("expected InvalidArgument for missing triggerId, got nil")
	}
}

func TestTriggerAlreadyExists(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")

	params := map[string]any{"location": "us-central1", "triggerId": "t1", "body": triggerBody("projects/proj/locations/us-central1/workflows/w1")}
	if _, err := p.CreateTrigger(ctx, newNR(params)); err != nil {
		t.Fatalf("first CreateTrigger: %v", err)
	}
	_, err := p.CreateTrigger(ctx, newNR(params))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "AlreadyExists" {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestChannelCRUD(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	createResp, err := p.CreateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "channelId": "c1",
		"body": map[string]any{"provider": "projects/proj/locations/us-central1/providers/pubsub.googleapis.com"},
	}))
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if createResp.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", createResp.Data["done"])
	}
	created, _ := createResp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/channels/c1"
	if created["name"] != wantName {
		t.Errorf("name = %v, want %v", created["name"], wantName)
	}
	if created["uid"] == "" || created["activationToken"] == "" || created["pubsubTopic"] == "" || created["state"] != "ACTIVE" {
		t.Errorf("output-only channel fields missing: %v", created)
	}

	got, err := p.GetChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"}))
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if got.Data["activationToken"] != created["activationToken"] {
		t.Errorf("activationToken not stable across reads: %v", got.Data)
	}

	listResp, err := p.ListChannels(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListChannels: %v", err)
	}
	if items, _ := listResp.Data["channels"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(items))
	}

	if _, err := p.DeleteChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"})); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}
	if _, err := p.GetChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

func TestProviders(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	listResp, err := p.ListProviders(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	providers, _ := listResp.Data["providers"].([]any)
	if len(providers) < 2 {
		t.Fatalf("expected at least 2 providers, got %d", len(providers))
	}
	first, _ := providers[0].(map[string]any)
	if first["name"] != "projects/proj/locations/us-central1/providers/pubsub.googleapis.com" {
		t.Errorf("first provider name = %v", first["name"])
	}
	if first["displayName"] != "Cloud Pub/Sub" {
		t.Errorf("first provider displayName = %v", first["displayName"])
	}

	got, err := p.GetProvider(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/providers/storage.googleapis.com"}))
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	if got.Data["displayName"] != "Cloud Storage" {
		t.Errorf("displayName = %v, want Cloud Storage", got.Data["displayName"])
	}

	if _, err := p.GetProvider(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/providers/does-not-exist.googleapis.com"})); err == nil {
		t.Fatal("expected NotFound for unknown provider, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestUnimplementedIAM(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	routes := p.Routes()
	for _, name := range []string{
		"Eventarc.TriggerGetIamPolicy", "Eventarc.TriggerSetIamPolicy", "Eventarc.TriggerTestIamPermissions",
		"Eventarc.ChannelGetIamPolicy", "Eventarc.ChannelSetIamPolicy", "Eventarc.ChannelTestIamPermissions",
	} {
		_, err := routes[name](ctx, newNR(nil))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "Unimplemented" {
			t.Errorf("%s: expected Unimplemented, got %v", name, err)
		}
	}
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")

	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t1", "body": triggerBody("projects/proj/locations/us-central1/workflows/w1")})); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	p.Reset(ctx)
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err == nil {
		t.Fatal("expected NotFound after Reset, got nil error")
	}
}

func TestRoutes_AllHandlersRegistered(t *testing.T) {
	p, _, _ := newProvider()
	routes := p.Routes()
	want := []string{
		"Eventarc.CreateTrigger", "Eventarc.GetTrigger", "Eventarc.ListTriggers",
		"Eventarc.UpdateTrigger", "Eventarc.DeleteTrigger",
		"Eventarc.CreateChannel", "Eventarc.GetChannel", "Eventarc.ListChannels",
		"Eventarc.UpdateChannel", "Eventarc.DeleteChannel",
		"Eventarc.ListProviders", "Eventarc.GetProvider",
		"Eventarc.TriggerGetIamPolicy", "Eventarc.TriggerSetIamPolicy", "Eventarc.TriggerTestIamPermissions",
		"Eventarc.ChannelGetIamPolicy", "Eventarc.ChannelSetIamPolicy", "Eventarc.ChannelTestIamPermissions",
	}
	for _, k := range want {
		if routes[k] == nil {
			t.Errorf("missing route %q", k)
		}
	}
	if len(routes) != len(want) {
		t.Errorf("got %d routes, want %d", len(routes), len(want))
	}
}
