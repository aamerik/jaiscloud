package managedkafka

import (
	"context"
	"errors"
	"testing"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// fakeBroker records the lifecycle calls the core makes and returns a fixed
// endpoint, so the core↔broker wiring can be asserted without a real broker.
type fakeBroker struct {
	endpoint string
	err      error
	ensured  []string
	stopped  []string
}

func (f *fakeBroker) key(project, location, cluster string) string {
	return project + "/" + location + "/" + cluster
}

func (f *fakeBroker) EnsureCluster(_ context.Context, project, location, cluster string) (string, error) {
	f.ensured = append(f.ensured, f.key(project, location, cluster))
	if f.err != nil {
		return "", f.err
	}
	return f.endpoint, nil
}

func (f *fakeBroker) Endpoint(_, _, _ string) string { return f.endpoint }

func (f *fakeBroker) StopCluster(_ context.Context, project, location, cluster string) error {
	f.stopped = append(f.stopped, f.key(project, location, cluster))
	return nil
}

func TestClusterRendersLiveBrokerEndpoint(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker-1.jaiscloud.svc.cluster.local:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))

	c, op, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.BootstrapAddress != fb.endpoint {
		t.Errorf("cluster bootstrapAddress = %q, want %q", c.BootstrapAddress, fb.endpoint)
	}
	if len(fb.ensured) != 1 || fb.ensured[0] != "proj/us-central1/c1" {
		t.Fatalf("EnsureCluster calls = %v", fb.ensured)
	}
	opJSON := OperationJSON(op, "proj")
	if resp, _ := opJSON["response"].(map[string]any); resp["bootstrapAddress"] != fb.endpoint {
		t.Errorf("operation response bootstrapAddress = %v, want %q", resp["bootstrapAddress"], fb.endpoint)
	}

	// Reads resolve the live endpoint too.
	got, err := s.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if js := ClusterJSON(got, "proj"); js["bootstrapAddress"] != fb.endpoint {
		t.Errorf("GetCluster bootstrapAddress = %v, want %q", js["bootstrapAddress"], fb.endpoint)
	}
	if js := ClusterJSON(got, "proj"); js["state"] != "ACTIVE" {
		t.Errorf("state = %v, want ACTIVE", js["state"])
	}

	list, _, err := s.ListClusters(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list) != 1 || list[0].BootstrapAddress != fb.endpoint {
		t.Fatalf("ListClusters = %+v, want live endpoint", list)
	}

	// Delete reaps the broker.
	if _, err := s.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if len(fb.stopped) != 1 || fb.stopped[0] != "proj/us-central1/c1" {
		t.Fatalf("StopCluster calls = %v", fb.stopped)
	}
}

func TestClusterFallsBackWhenBrokerFails(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{err: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))

	c, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.BootstrapAddress != "" {
		t.Errorf("bootstrapAddress = %q, want empty on broker failure", c.BootstrapAddress)
	}
	if js := ClusterJSON(c, "proj"); js["bootstrapAddress"] != BootstrapAddress("proj", "us-central1", "c1") {
		t.Errorf("rendered bootstrapAddress = %v, want synthesized fallback", js["bootstrapAddress"])
	}
}

func TestWithBrokerNilIsMock(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore(), WithBroker(nil))
	c, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.BootstrapAddress != "" {
		t.Errorf("bootstrapAddress = %q, want empty for mock topology", c.BootstrapAddress)
	}
}
