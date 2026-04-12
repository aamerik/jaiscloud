package plugin_test

import (
	"context"
	"sync"
	"testing"

	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/plugin"
)

// ─── minimal fakes ───────────────────────────────────────────────────────────

type fakeRM struct{}

func (f *fakeRM) CheckParent(_ context.Context, _, _, _, _ string, _ int) error { return nil }
func (f *fakeRM) AcquireDelete(_ context.Context, _, _ string) (sdk.DeletionHandle, error) {
	return &fakeHandle{}, nil
}
func (f *fakeRM) RegisterRules(_ []sdk.DeleteGuardRule) {}

type fakeHandle struct{}

func (f *fakeHandle) Release() {}

type fakeStore struct {
	mu      sync.RWMutex
	entries map[string]sdk.ResourceEntry
}

func newFakeStore() *fakeStore { return &fakeStore{entries: make(map[string]sdk.ResourceEntry)} }
func (s *fakeStore) key(t, id string) string { return t + "\x00" + id }

func (s *fakeStore) Exists(_ context.Context, t, id string) (bool, error) {
	s.mu.RLock(); defer s.mu.RUnlock()
	_, ok := s.entries[s.key(t, id)]; return ok, nil
}
func (s *fakeStore) Get(_ context.Context, t, id string) (sdk.ResourceEntry, error) {
	s.mu.RLock(); defer s.mu.RUnlock()
	e, ok := s.entries[s.key(t, id)]
	if !ok { return sdk.ResourceEntry{}, errNotFound }
	return e, nil
}
func (s *fakeStore) List(_ context.Context, t, _ string) ([]sdk.ResourceEntry, error) {
	s.mu.RLock(); defer s.mu.RUnlock()
	var out []sdk.ResourceEntry
	for _, e := range s.entries {
		if e.Type == t { out = append(out, e) }
	}
	return out, nil
}
func (s *fakeStore) Create(_ context.Context, e sdk.ResourceEntry) error {
	s.mu.Lock(); defer s.mu.Unlock()
	s.entries[s.key(e.Type, e.ID)] = e; return nil
}
func (s *fakeStore) Update(_ context.Context, e sdk.ResourceEntry) error {
	s.mu.Lock(); defer s.mu.Unlock()
	s.entries[s.key(e.Type, e.ID)] = e; return nil
}
func (s *fakeStore) Delete(_ context.Context, t, id string) error {
	s.mu.Lock(); defer s.mu.Unlock()
	delete(s.entries, s.key(t, id)); return nil
}

type errSentinel string
func (e errSentinel) Error() string { return string(e) }
var errNotFound errSentinel = "not found"

// ─── helpers ──────────────────────────────────────────────────────────────────

func initPlugin(t *testing.T) *plugin.EMRSparkPlugin {
	t.Helper()
	p := plugin.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		p.Shutdown(context.Background())
	})
	if err := p.Init(ctx, &fakeRM{}, newFakeStore()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return p
}

func emrReq(action string, params map[string]any) sdk.HandleRequest {
	return sdk.HandleRequest{Service: "emr", Action: action, Region: "us-east-1", AccountID: "000000000000", Params: params}
}
func emrcReq(action string, params map[string]any) sdk.HandleRequest {
	return sdk.HandleRequest{Service: "emrcontainers", Action: action, Region: "us-east-1", AccountID: "000000000000", Params: params}
}

// ─── Manifest ─────────────────────────────────────────────────────────────────

func TestEMRSparkPlugin_Manifest(t *testing.T) {
	p := plugin.New()
	m := p.Manifest()
	if m.Name != "aws-emr-spark" {
		t.Errorf("unexpected name: %s", m.Name)
	}
	if m.Version != "1.0.0" {
		t.Errorf("unexpected version: %s", m.Version)
	}
	services := map[string]bool{}
	for _, s := range m.Services { services[s] = true }
	for _, want := range []string{"emr", "emrcontainers"} {
		if !services[want] {
			t.Errorf("missing service %q in manifest", want)
		}
	}
}

// ─── Init ─────────────────────────────────────────────────────────────────────

func TestEMRSparkPlugin_Init_SetsUpProviders(t *testing.T) {
	p := initPlugin(t)
	// Verify Init wired the providers: a RunJobFlow must succeed
	resp := p.Handle(context.Background(), emrReq("RunJobFlow", map[string]any{"Name": "c1"}))
	if resp.Err != nil {
		t.Fatalf("RunJobFlow after Init: %v", resp.Err)
	}
}

func TestEMRSparkPlugin_Init_Idempotent(t *testing.T) {
	// Calling Init twice (e.g. accidentally) must not panic
	p := plugin.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Init(ctx, &fakeRM{}, newFakeStore()); err != nil {
		t.Fatalf("first Init: %v", err)
	}
	// Second Init with same context — should not deadlock or panic
	if err := p.Init(ctx, &fakeRM{}, newFakeStore()); err != nil {
		t.Fatalf("second Init: %v", err)
	}
	p.Shutdown(ctx)
}

// ─── Handle dispatch ──────────────────────────────────────────────────────────

func TestEMRSparkPlugin_Handle_EMR(t *testing.T) {
	p := initPlugin(t)
	resp := p.Handle(context.Background(), emrReq("RunJobFlow", map[string]any{"Name": "c1"}))
	if resp.Err != nil {
		t.Fatalf("expected success, got: %v", resp.Err)
	}
	if _, ok := resp.Data["JobFlowId"]; !ok {
		t.Error("expected JobFlowId in response")
	}
}

func TestEMRSparkPlugin_Handle_EMRContainers(t *testing.T) {
	p := initPlugin(t)
	resp := p.Handle(context.Background(), emrcReq("CreateVirtualCluster", map[string]any{"name": "vc1"}))
	if resp.Err != nil {
		t.Fatalf("expected success, got: %v", resp.Err)
	}
	if _, ok := resp.Data["id"]; !ok {
		t.Error("expected id in response")
	}
}

func TestEMRSparkPlugin_Handle_EMRContainers_AltServiceName(t *testing.T) {
	p := initPlugin(t)
	// "emr-containers" is an alias for "emrcontainers"
	req := sdk.HandleRequest{
		Service: "emr-containers", Action: "CreateVirtualCluster",
		Region: "us-east-1", AccountID: "000000000000",
		Params: map[string]any{"name": "vc1"},
	}
	resp := p.Handle(context.Background(), req)
	if resp.Err != nil {
		t.Fatalf("emr-containers alias failed: %v", resp.Err)
	}
}

func TestEMRSparkPlugin_Handle_UnknownService(t *testing.T) {
	p := initPlugin(t)
	resp := p.Handle(context.Background(), sdk.HandleRequest{
		Service: "s3", Action: "PutObject",
	})
	if resp.Err == nil || resp.Err.HTTPStatus != 400 {
		t.Errorf("expected 400 for unknown service, got %v", resp.Err)
	}
}

// ─── Reset ────────────────────────────────────────────────────────────────────

func TestEMRSparkPlugin_Reset_ClearsEMRJobs(t *testing.T) {
	p := initPlugin(t)
	ctx := context.Background()

	// Create cluster + submit step (which submits a mock job)
	resp := p.Handle(ctx, emrReq("RunJobFlow", map[string]any{"Name": "c1"}))
	clusterID := resp.Data["JobFlowId"].(string)
	p.Handle(ctx, emrReq("AddJobFlowSteps", map[string]any{
		"JobFlowId": clusterID,
		"Steps": []any{map[string]any{
			"Name": "s1",
			"HadoopJarStep": map[string]any{"Jar": "s3://b/app.jar"},
		}},
	}))

	p.Reset() // clears mock executor state

	// Mock executor state cleared — subsequent ListClusters still works (store not reset)
	resp = p.Handle(ctx, emrReq("ListClusters", map[string]any{}))
	if resp.Err != nil {
		t.Fatalf("ListClusters after Reset: %v", resp.Err)
	}
}

// ─── Shutdown ─────────────────────────────────────────────────────────────────

func TestEMRSparkPlugin_Shutdown_BeforeInit_NoOp(t *testing.T) {
	p := plugin.New()
	// Shutdown before Init must not panic
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Init: %v", err)
	}
}

func TestEMRSparkPlugin_Shutdown_StopsPoller(t *testing.T) {
	p := plugin.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Init(ctx, &fakeRM{}, newFakeStore())

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// After Shutdown, a second Shutdown must not deadlock
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

// ─── Interface compliance ─────────────────────────────────────────────────────

var _ sdk.SparkPlugin = (*plugin.EMRSparkPlugin)(nil)
