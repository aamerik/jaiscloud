package emrcontainers_test

import (
	"context"
	"sync"
	"testing"

	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/provider/emrcontainers"
)

// ─── fake store ───────────────────────────────────────────────────────────────

type fakeStore struct {
	mu      sync.RWMutex
	entries map[string]sdk.ResourceEntry
}

func newFakeStore() *fakeStore { return &fakeStore{entries: make(map[string]sdk.ResourceEntry)} }

func (s *fakeStore) key(t, id string) string { return t + "\x00" + id }

func (s *fakeStore) Exists(_ context.Context, t, id string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[s.key(t, id)]
	return ok, nil
}

func (s *fakeStore) Get(_ context.Context, t, id string) (sdk.ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[s.key(t, id)]
	if !ok {
		return sdk.ResourceEntry{}, errNotFound
	}
	return e, nil
}

func (s *fakeStore) List(_ context.Context, t, _ string) ([]sdk.ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []sdk.ResourceEntry
	for _, e := range s.entries {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) Create(_ context.Context, e sdk.ResourceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.key(e.Type, e.ID)] = e
	return nil
}

func (s *fakeStore) Update(_ context.Context, e sdk.ResourceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.key(e.Type, e.ID)] = e
	return nil
}

func (s *fakeStore) Delete(_ context.Context, t, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, s.key(t, id))
	return nil
}

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

var errNotFound sentinelError = "not found"

// ─── helpers ──────────────────────────────────────────────────────────────────

func newProvider() *emrcontainers.EMRContainersProvider {
	return emrcontainers.New(newFakeStore(), spark.NewMockExecutor(), nil, sdk.NoopEventBus{})
}

func req(action string, params map[string]any) sdk.HandleRequest {
	return sdk.HandleRequest{
		Service:   "emrcontainers",
		Action:    action,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Params:    params,
	}
}

// ─── tests ────────────────────────────────────────────────────────────────────

func TestEMRC_CreateDescribeVirtualCluster(t *testing.T) {
	p := newProvider()
	ctx := context.Background()

	resp := p.Handle(ctx, req("CreateVirtualCluster", map[string]any{
		"name": "vc1",
		"containerProvider": map[string]any{
			"id":   "my-eks-cluster",
			"type": "EKS",
			"info": map[string]any{
				"eksInfo": map[string]any{"namespace": "spark"},
			},
		},
	}))
	if resp.Err != nil {
		t.Fatalf("CreateVirtualCluster: %v", resp.Err)
	}
	id, _ := resp.Data["id"].(string)
	if id == "" {
		t.Fatal("expected id in response")
	}

	resp = p.Handle(ctx, req("DescribeVirtualCluster", map[string]any{"id": id}))
	if resp.Err != nil {
		t.Fatalf("DescribeVirtualCluster: %v", resp.Err)
	}
	vc := resp.Data["virtualCluster"].(map[string]any)
	if vc["id"] != id {
		t.Errorf("unexpected id: %v", vc["id"])
	}
	if vc["name"] != "vc1" {
		t.Errorf("unexpected name: %v", vc["name"])
	}
}

func TestEMRC_CreateVirtualCluster_RequiresName(t *testing.T) {
	p := newProvider()
	resp := p.Handle(context.Background(), req("CreateVirtualCluster", map[string]any{}))
	if resp.Err == nil || resp.Err.HTTPStatus != 400 {
		t.Errorf("expected 400 for missing name")
	}
}

func TestEMRC_StartJobRun_DescribeJobRun(t *testing.T) {
	p := newProvider()
	ctx := context.Background()

	// Create VC
	resp := p.Handle(ctx, req("CreateVirtualCluster", map[string]any{"name": "vc1"}))
	vcID := resp.Data["id"].(string)

	// Start job run
	resp = p.Handle(ctx, req("StartJobRun", map[string]any{
		"virtualClusterId": vcID,
		"name":             "job1",
		"releaseLabel":     "emr-6.6.0-latest",
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint":          "s3://bucket/app.jar",
				"entryPointArguments": []any{"--arg1", "val1"},
			},
		},
	}))
	if resp.Err != nil {
		t.Fatalf("StartJobRun: %v", resp.Err)
	}
	jobID, _ := resp.Data["id"].(string)
	if jobID == "" {
		t.Fatal("expected job run id in response")
	}

	// Describe job run
	resp = p.Handle(ctx, req("DescribeJobRun", map[string]any{
		"virtualClusterId": vcID,
		"id":               jobID,
	}))
	if resp.Err != nil {
		t.Fatalf("DescribeJobRun: %v", resp.Err)
	}
	jr := resp.Data["jobRun"].(map[string]any)
	if jr["id"] != jobID {
		t.Errorf("unexpected job run id: %v", jr["id"])
	}
}

func TestEMRC_DeleteVirtualCluster(t *testing.T) {
	p := newProvider()
	ctx := context.Background()

	resp := p.Handle(ctx, req("CreateVirtualCluster", map[string]any{"name": "vc1"}))
	id := resp.Data["id"].(string)

	resp = p.Handle(ctx, req("DeleteVirtualCluster", map[string]any{"id": id}))
	if resp.Err != nil {
		t.Fatalf("DeleteVirtualCluster: %v", resp.Err)
	}

	resp = p.Handle(ctx, req("DescribeVirtualCluster", map[string]any{"id": id}))
	vc := resp.Data["virtualCluster"].(map[string]any)
	if vc["state"] != "TERMINATING" {
		t.Errorf("expected TERMINATING, got %v", vc["state"])
	}
}

func TestEMRC_ListVirtualClusters(t *testing.T) {
	p := newProvider()
	ctx := context.Background()

	for _, name := range []string{"vc1", "vc2", "vc3"} {
		p.Handle(ctx, req("CreateVirtualCluster", map[string]any{"name": name}))
	}

	resp := p.Handle(ctx, req("ListVirtualClusters", map[string]any{}))
	if resp.Err != nil {
		t.Fatalf("ListVirtualClusters: %v", resp.Err)
	}
	vcs, _ := resp.Data["virtualClusters"].([]map[string]any)
	if len(vcs) != 3 {
		t.Errorf("expected 3 virtual clusters, got %d", len(vcs))
	}
}

func TestEMRC_UnknownAction(t *testing.T) {
	p := newProvider()
	resp := p.Handle(context.Background(), req("UnknownAction", map[string]any{}))
	if resp.Err == nil || resp.Err.HTTPStatus != 501 {
		t.Errorf("expected 501 for unknown action, got %v", resp.Err)
	}
}
