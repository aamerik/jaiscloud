package sdk_test

import (
	"context"
	"testing"

	"github.com/jaiscloud/plugin-sdk"
)

// ─── interface compliance stubs ───────────────────────────────────────────────
// These compile-time checks verify that stub types satisfy the SDK interfaces.
// If the SDK interface changes and a stub no longer satisfies it, this file
// will fail to compile — surfacing the break immediately.

// stubPlugin implements SparkPlugin.
type stubPlugin struct{}

func (s *stubPlugin) Init(_ context.Context, _ sdk.ResourceManager, _ sdk.ResourceStore) error {
	return nil
}
func (s *stubPlugin) Manifest() sdk.ManifestInfo { return sdk.ManifestInfo{} }
func (s *stubPlugin) Handle(_ context.Context, _ sdk.HandleRequest) sdk.HandleResponse {
	return sdk.HandleResponse{}
}
func (s *stubPlugin) Shutdown(_ context.Context) error { return nil }
func (s *stubPlugin) Reset()                           {}

var _ sdk.SparkPlugin = (*stubPlugin)(nil)

// stubResourceStore implements ResourceStore.
type stubResourceStore struct{}

func (s *stubResourceStore) Exists(_ context.Context, _, _ string) (bool, error) { return false, nil }
func (s *stubResourceStore) Get(_ context.Context, _, _ string) (sdk.ResourceEntry, error) {
	return sdk.ResourceEntry{}, nil
}
func (s *stubResourceStore) List(_ context.Context, _, _ string) ([]sdk.ResourceEntry, error) {
	return nil, nil
}
func (s *stubResourceStore) Create(_ context.Context, _ sdk.ResourceEntry) error  { return nil }
func (s *stubResourceStore) Update(_ context.Context, _ sdk.ResourceEntry) error  { return nil }
func (s *stubResourceStore) Delete(_ context.Context, _, _ string) error          { return nil }

var _ sdk.ResourceStore = (*stubResourceStore)(nil)

// stubResourceManager implements ResourceManager.
type stubResourceManager struct{}

func (s *stubResourceManager) CheckParent(_ context.Context, _, _, _, _ string, _ int) error {
	return nil
}
func (s *stubResourceManager) AcquireDelete(_ context.Context, _, _ string) (sdk.DeletionHandle, error) {
	return &stubDeletionHandle{}, nil
}
func (s *stubResourceManager) RegisterRules(_ []sdk.DeleteGuardRule) {}

var _ sdk.ResourceManager = (*stubResourceManager)(nil)

// stubDeletionHandle implements DeletionHandle.
type stubDeletionHandle struct{}

func (s *stubDeletionHandle) Release() {}

var _ sdk.DeletionHandle = (*stubDeletionHandle)(nil)

// stubEventBus implements EventBus.
type stubEventBus struct{ published []sdk.Event }

func (s *stubEventBus) Publish(_ context.Context, e sdk.Event) error {
	s.published = append(s.published, e)
	return nil
}

var _ sdk.EventBus = (*stubEventBus)(nil)

// NoopEventBus satisfies EventBus.
var _ sdk.EventBus = sdk.NoopEventBus{}

// ─── behavioural tests ────────────────────────────────────────────────────────

func TestPluginError_Error(t *testing.T) {
	err := &sdk.PluginError{Code: "TestCode", Message: "test message", HTTPStatus: 400}
	if err.Error() != "test message" {
		t.Errorf("Error() = %q, want %q", err.Error(), "test message")
	}
}

func TestNoopEventBus_Publish(t *testing.T) {
	bus := sdk.NoopEventBus{}
	err := bus.Publish(context.Background(), sdk.Event{Source: "test", Type: "Noop"})
	if err != nil {
		t.Errorf("NoopEventBus.Publish should never return an error, got: %v", err)
	}
}

func TestHandleResponse_ZeroHTTPStatus(t *testing.T) {
	resp := sdk.HandleResponse{}
	if resp.HTTPStatus != 0 {
		t.Errorf("zero value HTTPStatus should be 0 (host treats as 200), got %d", resp.HTTPStatus)
	}
}

func TestManifestInfo_MultipleServices(t *testing.T) {
	p := &stubPlugin{}
	info := p.Manifest()
	_ = info.Services // must be a slice of strings — compiler-enforced
}

func TestDeletionPolicy_Values(t *testing.T) {
	if sdk.PolicyFail != 0 {
		t.Error("PolicyFail must be 0 (highest priority)")
	}
	if sdk.PolicyForceTerminate != 1 {
		t.Error("PolicyForceTerminate must be 1")
	}
	if sdk.PolicyCascade != 2 {
		t.Error("PolicyCascade must be 2")
	}
}

func TestDeleteGuardRule_Fields(t *testing.T) {
	rule := sdk.DeleteGuardRule{
		ParentType: "emrc_virtual_cluster",
		FindChildren: func(_ context.Context, _ sdk.ResourceStore, _ string) ([]sdk.ChildRef, error) {
			return []sdk.ChildRef{{Type: "job_run", ID: "jr-1"}}, nil
		},
		Policy:   sdk.PolicyFail,
		FailCode: "ValidationException",
	}

	children, err := rule.FindChildren(context.Background(), &stubResourceStore{}, "vc-1")
	if err != nil {
		t.Fatalf("FindChildren returned error: %v", err)
	}
	if len(children) != 1 || children[0].ID != "jr-1" {
		t.Errorf("unexpected children: %v", children)
	}
}

func TestEventBus_PublishAndCollect(t *testing.T) {
	bus := &stubEventBus{}
	events := []sdk.Event{
		{Source: "aws-emr-spark", Type: "EMRJobStateChange", Detail: map[string]any{"state": "RUNNING"}},
		{Source: "aws-emr-spark", Type: "EMRJobStateChange", Detail: map[string]any{"state": "COMPLETED"}},
	}
	for _, e := range events {
		if err := bus.Publish(context.Background(), e); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if len(bus.published) != 2 {
		t.Errorf("expected 2 events, got %d", len(bus.published))
	}
}
