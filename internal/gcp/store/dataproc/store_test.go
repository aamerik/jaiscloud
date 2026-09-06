package dataproc

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
		Name:        "my-cluster",
		Config:      []byte(`{"gceClusterConfig":{"zoneUri":"us-central1-a"},"softwareConfig":{"imageVersion":"2.2"}}`),
		Labels:      map[string]string{"env": "dev"},
		Status:      ClusterStatus{State: "CREATING"},
		ClusterUUID: "uuid-1",
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
	if got.Status.State != "CREATING" || got.ClusterUUID != "uuid-1" {
		t.Fatalf("cluster fields lost: %+v", got)
	}
	if string(got.Config) != `{"gceClusterConfig":{"zoneUri":"us-central1-a"},"softwareConfig":{"imageVersion":"2.2"}}` {
		t.Fatalf("config not verbatim: %s", got.Config)
	}

	got.Status.State = "RUNNING"
	if err := s.UpdateCluster(ctx, "proj", "us-central1", got); err != nil {
		t.Fatalf("update cluster: %v", err)
	}
	list, err := s.ListClusters(ctx, "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list clusters: %v %d", err, len(list))
	}
	if list[0].Status.State != "RUNNING" {
		t.Fatalf("update not persisted: %+v", list[0])
	}

	// Jobs
	if _, err := s.GetJob(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchJob {
		t.Fatalf("expected ErrNoSuchJob, got %v", err)
	}
	j := Job{
		JobID:                "job-1",
		PlacementClusterName: "my-cluster",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		Status:               JobStatus{State: "DONE"},
	}
	if err := s.CreateJob(ctx, "proj", "us-central1", j); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := s.CreateJob(ctx, "proj", "us-central1", j); err != ErrAlreadyExists {
		t.Fatalf("expected job ErrAlreadyExists, got %v", err)
	}
	gotJob, err := s.GetJob(ctx, "proj", "us-central1", "job-1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if gotJob.Status.State != "DONE" || gotJob.Type != "pysparkJob" {
		t.Fatalf("job fields lost: %+v", gotJob)
	}
	jlist, err := s.ListJobs(ctx, "proj", "us-central1")
	if err != nil || len(jlist) != 1 {
		t.Fatalf("list jobs: %v %d", err, len(jlist))
	}
	if err := s.DeleteJob(ctx, "proj", "us-central1", "job-1"); err != nil {
		t.Fatalf("delete job: %v", err)
	}

	// Operations
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}
	op := Operation{ID: "op-1", Done: true, Metadata: `{"@type":"x"}`, Response: `{}`}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, "proj", "us-central1", "op-1")
	if err != nil || gotOp.Metadata != `{"@type":"x"}` {
		t.Fatalf("get op: %v %+v", err, gotOp)
	}

	// Delete cluster
	if err := s.DeleteCluster(ctx, "proj", "us-central1", "my-cluster"); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "my-cluster"); err != ErrNoSuchCluster {
		t.Fatalf("expected ErrNoSuchCluster after delete, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateCluster(ctx, "p", "r", Cluster{Name: "c"})
	s.Reset(ctx)
	empty, _ := s.IsEmpty(ctx)
	if !empty {
		t.Fatal("expected empty after reset")
	}
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateCluster(ctx, "p", "r", Cluster{Name: "c", Labels: map[string]string{"k": "v"}, Status: ClusterStatus{State: "RUNNING"}})
	_ = s.CreateJob(ctx, "p", "r", Job{JobID: "j", Type: "sparkJob", TypeJob: []byte(`{"mainJarFileUri":"gs://b/a.jar"}`)})
	_ = s.CreateOperation(ctx, "p", "r", Operation{ID: "op", Metadata: `{"@type":"m"}`, Response: `{"x":1}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := s2.GetCluster(ctx, "p", "r", "c")
	if err != nil || got.Status.State != "RUNNING" {
		t.Fatalf("cluster lost after restore: %v %+v", err, got)
	}
	gotJob, err := s2.GetJob(ctx, "p", "r", "j")
	if err != nil || string(gotJob.TypeJob) != `{"mainJarFileUri":"gs://b/a.jar"}` {
		t.Fatalf("job lost after restore: %v %+v", err, gotJob)
	}
	gotOp, err := s2.GetOperation(ctx, "p", "r", "op")
	if err != nil || gotOp.Metadata != `{"@type":"m"}` {
		t.Fatalf("op lost after restore: %v %+v", err, gotOp)
	}
}
