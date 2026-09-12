package managedkafka

import (
	"bytes"
	"context"
	"testing"
)

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	if _, err := s.GetCluster(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchCluster {
		t.Fatalf("expected ErrNoSuchCluster, got %v", err)
	}

	c := Cluster{
		Name:   "my-cluster",
		Config: []byte(`{"capacityConfig":{"vcpuCount":3,"memoryBytes":3221225472},"gcpConfig":{"accessConfig":{"networkConfigs":[]}}}`),
		Labels: map[string]string{"env": "dev"},
	}
	if err := s.CreateCluster(ctx, "proj", "us-central1", c); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if err := s.CreateCluster(ctx, "proj", "us-central1", c); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetCluster(ctx, "proj", "us-central1", "my-cluster")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Labels["env"] != "dev" {
		t.Fatalf("cluster labels lost: %+v", got)
	}
	if string(got.Config) != `{"capacityConfig":{"vcpuCount":3,"memoryBytes":3221225472},"gcpConfig":{"accessConfig":{"networkConfigs":[]}}}` {
		t.Fatalf("config not verbatim: %s", got.Config)
	}

	got.Config = []byte(`{"capacityConfig":{"vcpuCount":6,"memoryBytes":6442450944}}`)
	if err := s.UpdateCluster(ctx, "proj", "us-central1", got); err != nil {
		t.Fatalf("update cluster: %v", err)
	}
	list, err := s.ListClusters(ctx, "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list clusters: %v %d", err, len(list))
	}
	if string(list[0].Config) != `{"capacityConfig":{"vcpuCount":6,"memoryBytes":6442450944}}` {
		t.Fatalf("update not persisted: %+v", list[0])
	}

	// Topics
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "my-cluster", "nope"); err != ErrNoSuchTopic {
		t.Fatalf("expected ErrNoSuchTopic, got %v", err)
	}
	tp := Topic{Name: "t1", PartitionCount: 3, ReplicationFactor: 1}
	if err := s.CreateTopic(ctx, "proj", "us-central1", "my-cluster", tp); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := s.CreateTopic(ctx, "proj", "us-central1", "my-cluster", tp); err != ErrAlreadyExists {
		t.Fatalf("expected topic ErrAlreadyExists, got %v", err)
	}
	gotTopic, err := s.GetTopic(ctx, "proj", "us-central1", "my-cluster", "t1")
	if err != nil || gotTopic.PartitionCount != 3 {
		t.Fatalf("get topic: %v %+v", err, gotTopic)
	}
	gotTopic.PartitionCount = 6
	if err := s.UpdateTopic(ctx, "proj", "us-central1", "my-cluster", gotTopic); err != nil {
		t.Fatalf("update topic: %v", err)
	}
	tlist, err := s.ListTopics(ctx, "proj", "us-central1", "my-cluster")
	if err != nil || len(tlist) != 1 || tlist[0].PartitionCount != 6 {
		t.Fatalf("list topics: %v %+v", err, tlist)
	}
	if err := s.DeleteTopic(ctx, "proj", "us-central1", "my-cluster", "t1"); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	// Delete cluster
	if err := s.DeleteCluster(ctx, "proj", "us-central1", "my-cluster"); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "my-cluster"); err != ErrNoSuchCluster {
		t.Fatalf("expected ErrNoSuchCluster after delete, got %v", err)
	}

	// Operations
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}
	op := Operation{ID: "op1", Done: true, Metadata: `{"@type":"x"}`, Response: `{}`, Verb: "create", Target: "t"}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil || gotOp.Verb != "create" || gotOp.Metadata != `{"@type":"x"}` {
		t.Fatalf("get operation: %v %+v", err, gotOp)
	}
	ops, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil || len(ops) != 1 || ops[0].ID != "op1" {
		t.Fatalf("list operations: %v %+v", err, ops)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateCluster(ctx, "p", "l", Cluster{Name: "c", Labels: map[string]string{"k": "v"}, Config: []byte(`{"capacityConfig":{"vcpuCount":3}}`)})
	_ = s.CreateTopic(ctx, "p", "l", "c", Topic{Name: "t", PartitionCount: 3, ReplicationFactor: 1})
	_ = s.CreateOperation(ctx, "p", "l", Operation{ID: "op1", Done: true, Metadata: `{"@type":"x"}`, Response: `{}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := s2.GetCluster(ctx, "p", "l", "c")
	if err != nil || got.Labels["k"] != "v" {
		t.Fatalf("cluster lost after restore: %v %+v", err, got)
	}
	gotTopic, err := s2.GetTopic(ctx, "p", "l", "c", "t")
	if err != nil || gotTopic.PartitionCount != 3 || gotTopic.ReplicationFactor != 1 {
		t.Fatalf("topic lost after restore: %v %+v", err, gotTopic)
	}
	gotOp, err := s2.GetOperation(ctx, "p", "l", "op1")
	if err != nil || !gotOp.Done || gotOp.Metadata != `{"@type":"x"}` {
		t.Fatalf("operation lost after restore: %v %+v", err, gotOp)
	}
}
