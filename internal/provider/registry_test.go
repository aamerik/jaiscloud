package provider_test

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

func makeHandler(tag string) provider.HandlerFunc {
	return func(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return &model.ProviderResponse{Data: map[string]any{"handler": tag}}, nil
	}
}

func TestRegistry_Dispatch_ExactMatch(t *testing.T) {
	r := provider.NewRegistry()
	r.RegisterAll(map[string]provider.HandlerFunc{
		"EMR.RunJobFlow": makeHandler("builtin"),
	})

	resp, err := r.Dispatch(context.Background(), "EMR.RunJobFlow", &model.NormalizedRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["handler"] != "builtin" {
		t.Errorf("expected builtin handler, got %v", resp.Data["handler"])
	}
}

func TestRegistry_Dispatch_PluginFallback(t *testing.T) {
	r := provider.NewRegistry()
	r.RegisterPlugin("EMR", makeHandler("plugin"))

	resp, err := r.Dispatch(context.Background(), "EMR.DescribeCluster", &model.NormalizedRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["handler"] != "plugin" {
		t.Errorf("expected plugin handler, got %v", resp.Data["handler"])
	}
}

func TestRegistry_Dispatch_PluginOverridesBuiltin(t *testing.T) {
	r := provider.NewRegistry()
	r.RegisterAll(map[string]provider.HandlerFunc{
		"EMR.RunJobFlow": makeHandler("builtin"),
	})
	// RegisterPlugin overrides ALL built-in handlers for the same prefix so
	// that the plugin is the single source of truth for the entire service.
	r.RegisterPlugin("EMR", makeHandler("plugin"))

	// Plugin wins for actions that had a built-in handler.
	resp, err := r.Dispatch(context.Background(), "EMR.RunJobFlow", &model.NormalizedRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["handler"] != "plugin" {
		t.Errorf("plugin should override builtin, got %v", resp.Data["handler"])
	}

	// Plugin also handles unknown actions.
	resp, err = r.Dispatch(context.Background(), "EMR.DescribeCluster", &model.NormalizedRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["handler"] != "plugin" {
		t.Errorf("plugin fallback expected for unknown action, got %v", resp.Data["handler"])
	}
}

func TestRegistry_Dispatch_UnknownAction(t *testing.T) {
	r := provider.NewRegistry()
	_, err := r.Dispatch(context.Background(), "EMR.Nonexistent", &model.NormalizedRequest{})
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *ProviderError, got %T", err)
	}
	if pe.Code != "UnknownAction" {
		t.Errorf("expected UnknownAction, got %s", pe.Code)
	}
}

func TestRegistry_Dispatch_NoPrefix(t *testing.T) {
	r := provider.NewRegistry()
	r.RegisterAll(map[string]provider.HandlerFunc{
		"Queue.SendMessage": makeHandler("sqs"),
	})

	resp, err := r.Dispatch(context.Background(), "Queue.SendMessage", &model.NormalizedRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["handler"] != "sqs" {
		t.Errorf("expected sqs handler, got %v", resp.Data["handler"])
	}
}

// mockProvider is a minimal Provider implementation for testing.
type mockProvider struct {
	routes map[string]provider.HandlerFunc
}

func (m *mockProvider) Routes() map[string]provider.HandlerFunc { return m.routes }

func TestRegistry_Register_DelegatesRoutes(t *testing.T) {
	p := &mockProvider{routes: map[string]provider.HandlerFunc{
		"Queue.SendMessage":    makeHandler("sqs-send"),
		"Queue.ReceiveMessage": makeHandler("sqs-recv"),
	}}

	// Verify chaining works and all routes are accessible.
	r := provider.NewRegistry().Register(p)
	for _, tc := range []struct{ key, want string }{
		{"Queue.SendMessage", "sqs-send"},
		{"Queue.ReceiveMessage", "sqs-recv"},
	} {
		resp, err := r.Dispatch(context.Background(), tc.key, &model.NormalizedRequest{})
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.key, err)
		}
		if resp.Data["handler"] != tc.want {
			t.Errorf("%s: expected %s, got %v", tc.key, tc.want, resp.Data["handler"])
		}
	}
}

func TestRegistry_Dispatch_MultiplePlugins(t *testing.T) {
	r := provider.NewRegistry()
	r.RegisterPlugin("EMR", makeHandler("emr-plugin"))
	r.RegisterPlugin("EMRContainers", makeHandler("emrc-plugin"))

	for _, tc := range []struct {
		key  string
		want string
	}{
		{"EMR.ListClusters", "emr-plugin"},
		{"EMRContainers.StartJobRun", "emrc-plugin"},
	} {
		resp, err := r.Dispatch(context.Background(), tc.key, &model.NormalizedRequest{})
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.key, err)
			continue
		}
		if resp.Data["handler"] != tc.want {
			t.Errorf("%s: expected %s, got %v", tc.key, tc.want, resp.Data["handler"])
		}
	}
}
