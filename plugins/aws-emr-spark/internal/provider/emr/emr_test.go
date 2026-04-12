package emr_test

import (
	"context"
	"sync"
	"testing"

	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/provider/emr"
)

// ─── fake ResourceStore ───────────────────────────────────────────────────────

type fakeStore struct {
	mu      sync.RWMutex
	entries map[string]sdk.ResourceEntry
}

func newFakeStore() *fakeStore {
	return &fakeStore{entries: make(map[string]sdk.ResourceEntry)}
}

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

func (s *fakeStore) List(_ context.Context, t, prefix string) ([]sdk.ResourceEntry, error) {
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

func newProvider() (*emr.EMRProvider, *spark.MockExecutor) {
	store := newFakeStore()
	executor := spark.NewMockExecutor()
	p := emr.New(store, executor, nil)
	return p, executor
}

func baseReq(action string, params map[string]any) sdk.HandleRequest {
	return sdk.HandleRequest{
		Service:   "emr",
		Action:    action,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Params:    params,
	}
}

// ─── tests ────────────────────────────────────────────────────────────────────

func TestEMR_RunJobFlow_DescribeCluster(t *testing.T) {
	p, _ := newProvider()
	ctx := context.Background()

	resp := p.Handle(ctx, baseReq("RunJobFlow", map[string]any{"Name": "my-cluster"}))
	if resp.Err != nil {
		t.Fatalf("RunJobFlow: %v", resp.Err)
	}
	clusterID, _ := resp.Data["JobFlowId"].(string)
	if clusterID == "" {
		t.Fatal("expected JobFlowId in response")
	}

	resp = p.Handle(ctx, baseReq("DescribeCluster", map[string]any{"ClusterId": clusterID}))
	if resp.Err != nil {
		t.Fatalf("DescribeCluster: %v", resp.Err)
	}
	c, _ := resp.Data["Cluster"].(map[string]any)
	if c["Id"] != clusterID {
		t.Errorf("unexpected cluster ID: %v", c["Id"])
	}
}

func TestEMR_RunJobFlow_RequiresName(t *testing.T) {
	p, _ := newProvider()
	resp := p.Handle(context.Background(), baseReq("RunJobFlow", map[string]any{}))
	if resp.Err == nil || resp.Err.HTTPStatus != 400 {
		t.Errorf("expected 400 error for missing Name")
	}
}

func TestEMR_AddJobFlowSteps_DescribeStep(t *testing.T) {
	p, _ := newProvider()
	ctx := context.Background()

	// Create cluster
	resp := p.Handle(ctx, baseReq("RunJobFlow", map[string]any{"Name": "c1"}))
	clusterID := resp.Data["JobFlowId"].(string)

	// Add a step
	resp = p.Handle(ctx, baseReq("AddJobFlowSteps", map[string]any{
		"JobFlowId": clusterID,
		"Steps": []any{
			map[string]any{
				"Name": "my-step",
				"HadoopJarStep": map[string]any{
					"Jar":       "s3://bucket/app.jar",
					"MainClass": "com.example.Main",
					"Args":      []any{"--input", "s3://bucket/data"},
				},
			},
		},
	}))
	if resp.Err != nil {
		t.Fatalf("AddJobFlowSteps: %v", resp.Err)
	}
	stepIDs, _ := resp.Data["StepIds"].([]string)
	if len(stepIDs) == 0 {
		t.Fatal("expected StepIds in response")
	}
	stepID := stepIDs[0]

	// Describe step
	resp = p.Handle(ctx, baseReq("DescribeStep", map[string]any{
		"ClusterId": clusterID,
		"StepId":    stepID,
	}))
	if resp.Err != nil {
		t.Fatalf("DescribeStep: %v", resp.Err)
	}
	step, _ := resp.Data["Step"].(map[string]any)
	if step["Id"] != stepID {
		t.Errorf("unexpected step ID: %v", step["Id"])
	}
}

func TestEMR_ListClusters(t *testing.T) {
	p, _ := newProvider()
	ctx := context.Background()

	p.Handle(ctx, baseReq("RunJobFlow", map[string]any{"Name": "c1"}))
	p.Handle(ctx, baseReq("RunJobFlow", map[string]any{"Name": "c2"}))

	resp := p.Handle(ctx, baseReq("ListClusters", map[string]any{}))
	if resp.Err != nil {
		t.Fatalf("ListClusters: %v", resp.Err)
	}
	clusters, _ := resp.Data["Clusters"].([]map[string]any)
	if len(clusters) != 2 {
		t.Errorf("expected 2 clusters, got %d", len(clusters))
	}
}

func TestEMR_TerminateJobFlows(t *testing.T) {
	p, _ := newProvider()
	ctx := context.Background()

	resp := p.Handle(ctx, baseReq("RunJobFlow", map[string]any{"Name": "c1"}))
	id := resp.Data["JobFlowId"].(string)

	p.Handle(ctx, baseReq("TerminateJobFlows", map[string]any{
		"JobFlowIds": []any{id},
	}))

	resp = p.Handle(ctx, baseReq("DescribeCluster", map[string]any{"ClusterId": id}))
	c := resp.Data["Cluster"].(map[string]any)
	status := c["Status"].(map[string]any)
	if status["State"] != "TERMINATING" {
		t.Errorf("expected TERMINATING, got %v", status["State"])
	}
}

func TestEMR_ListSteps(t *testing.T) {
	p, _ := newProvider()
	ctx := context.Background()

	resp := p.Handle(ctx, baseReq("RunJobFlow", map[string]any{"Name": "c1"}))
	clusterID := resp.Data["JobFlowId"].(string)

	for i := 0; i < 3; i++ {
		p.Handle(ctx, baseReq("AddJobFlowSteps", map[string]any{
			"JobFlowId": clusterID,
			"Steps":     []any{map[string]any{"Name": "s", "HadoopJarStep": map[string]any{"Jar": "s3://b/app.jar"}}},
		}))
	}

	resp = p.Handle(ctx, baseReq("ListSteps", map[string]any{"ClusterId": clusterID}))
	steps, _ := resp.Data["Steps"].([]map[string]any)
	if len(steps) != 3 {
		t.Errorf("expected 3 steps, got %d", len(steps))
	}
}

func TestEMR_DescribeCluster_NotFound(t *testing.T) {
	p, _ := newProvider()
	resp := p.Handle(context.Background(), baseReq("DescribeCluster", map[string]any{"ClusterId": "nonexistent"}))
	if resp.Err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestEMR_UnknownAction(t *testing.T) {
	p, _ := newProvider()
	resp := p.Handle(context.Background(), baseReq("UnknownAction", map[string]any{}))
	if resp.Err == nil || resp.Err.HTTPStatus != 501 {
		t.Errorf("expected 501 for unknown action, got %v", resp.Err)
	}
}
