package managedkafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// fakeBroker records the lifecycle calls the core makes and returns a fixed
// endpoint, so the core↔broker wiring can be asserted without a real broker.
type fakeBroker struct {
	endpoint string
	err      error
	ensured  []string
	stopped  []string

	// Topic data-plane behaviour + call recording.
	ensureTopicErr   error
	addPartitionsErr error
	deleteTopicErr   error
	ensuredTopics    []string // "project/location/cluster/topic:partitions"
	addedPartitions  []string // "project/location/cluster/topic:total"
	deletedTopics    []string // "project/location/cluster/topic"

	// Optional hooks fire while the broker call is "in flight", so tests can
	// simulate a concurrent mutation landing before the call fails.
	beforeEnsureTopic   func()
	beforeAddPartitions func()
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

func (f *fakeBroker) EnsureTopic(_ context.Context, project, location, cluster, topic string, partitions int) error {
	f.ensuredTopics = append(f.ensuredTopics, fmt.Sprintf("%s/%s:%d", f.key(project, location, cluster), topic, partitions))
	err := f.ensureTopicErr
	if f.beforeEnsureTopic != nil {
		f.beforeEnsureTopic()
	}
	return err
}

func (f *fakeBroker) AddTopicPartitions(_ context.Context, project, location, cluster, topic string, totalPartitions int) error {
	f.addedPartitions = append(f.addedPartitions, fmt.Sprintf("%s/%s/%s/%s:%d", project, location, cluster, topic, totalPartitions))
	err := f.addPartitionsErr
	if f.beforeAddPartitions != nil {
		f.beforeAddPartitions()
	}
	return err
}

func (f *fakeBroker) DeleteBrokerTopic(_ context.Context, project, location, cluster, topic string) error {
	f.deletedTopics = append(f.deletedTopics, f.key(project, location, cluster)+"/"+topic)
	return f.deleteTopicErr
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

// --- Topic data plane ---

func withCluster(t *testing.T, s *Service) {
	t.Helper()
	if _, _, err := s.CreateCluster(context.Background(), "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
}

func TestTopicProvisionsOnBroker(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker-1.jaiscloud.svc.cluster.local:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 2)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if len(fb.ensuredTopics) != 1 || fb.ensuredTopics[0] != "proj/us-central1/c1/t1:3" {
		t.Fatalf("EnsureTopic calls = %v", fb.ensuredTopics)
	}
	// The broker replica count is fixed at 1 (single-node); the caller's
	// replicationFactor stays metadata.
	if got, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil || got.ReplicationFactor != 2 {
		t.Fatalf("stored replicationFactor = %d, %v; want 2, nil", got.ReplicationFactor, err)
	}

	// Growth is pushed to the broker.
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(6, 0)); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if len(fb.addedPartitions) != 1 || fb.addedPartitions[0] != "proj/us-central1/c1/t1:6" {
		t.Fatalf("AddTopicPartitions calls = %v", fb.addedPartitions)
	}

	// Delete removes the broker topic before the metadata record.
	if err := s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if len(fb.deletedTopics) != 1 || fb.deletedTopics[0] != "proj/us-central1/c1/t1" {
		t.Fatalf("DeleteBrokerTopic calls = %v", fb.deletedTopics)
	}
}

func TestTopicCreateRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", ensureTopicErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1))
	assertInternal(t, err)
	// The store write was rolled back: an API-visible topic is guaranteed to
	// exist on the broker, and here the broker call failed.
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err == nil {
		t.Fatal("topic survived a failed broker provision")
	}
}

func TestTopicUpdateIsIncreaseOnly(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(2, 0))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("decrease error = %v, want InvalidArgument", err)
	}
	if len(fb.addedPartitions) != 0 {
		t.Fatalf("broker contacted on a rejected decrease: %v", fb.addedPartitions)
	}
	if got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); got.PartitionCount != 3 {
		t.Fatalf("partitionCount = %d, want 3 after rejected decrease", got.PartitionCount)
	}
}

func TestTopicUpdateRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", addPartitionsErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(5, 0))
	assertInternal(t, err)
	if got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); got.PartitionCount != 3 {
		t.Fatalf("partitionCount = %d, want 3 after rollback", got.PartitionCount)
	}
}

func TestTopicDeleteBrokerFailureKeepsMetadata(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", deleteTopicErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	assertInternal(t, s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"))
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("metadata removed despite broker delete failure: %v", err)
	}
}

func TestTopicNoBrokerIsMetadataOnly(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)

	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(6, 0)); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if err := s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func assertInternal(t *testing.T, err error) {
	t.Helper()
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "Internal" || perr.HTTPStatus != 500 {
		t.Fatalf("error = %v, want Internal(500)", err)
	}
}

func TestCreateTopicRequiresPartitionsAndReplication(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)

	for name, in := range map[string]TopicInput{
		"zero partitions":  topicIn(0, 1),
		"zero replication": topicIn(1, 0),
	} {
		_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in)
		if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
			t.Errorf("%s: error = %v, want InvalidArgument", name, err)
		}
	}
}

func TestUpdateTopicRejectsReplicationFactorChange(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 2)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// replication_factor is Immutable: a change is rejected, an unchanged value
	// is fine.
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(0, 3)); err == nil {
		t.Fatal("changing replicationFactor was accepted")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("error = %v, want InvalidArgument", err)
	}
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(0, 2)); err != nil {
		t.Fatalf("unchanged replicationFactor rejected: %v", err)
	}
}

// TestUpdateRollbackDoesNotClobberConcurrentGrowth proves the partition-count
// rollback only applies when the metadata still holds the count this request
// wrote; a concurrent successful growth is preserved.
func TestUpdateRollbackDoesNotClobberConcurrentGrowth(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", addPartitionsErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	fb.beforeAddPartitions = func() {
		fb.beforeAddPartitions = nil
		fb.addPartitionsErr = nil // the concurrent request succeeds
		if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(7, 0)); err != nil {
			t.Errorf("concurrent UpdateTopic: %v", err)
		}
	}

	// This request grows 3→5; its broker call fails after the concurrent
	// 3→7 committed.
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(5, 0)); err == nil {
		t.Fatal("expected the failed broker growth to surface an error")
	} else {
		assertInternal(t, err)
	}
	if got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); got.PartitionCount != 7 {
		t.Fatalf("partitionCount = %d, want the concurrent 7 preserved", got.PartitionCount)
	}
}

// TestCreateRollbackSkipsConcurrentlyModifiedTopic proves a failed broker
// provision does not delete metadata a concurrent request already updated.
func TestCreateRollbackSkipsConcurrentlyModifiedTopic(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", ensureTopicErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	fb.beforeEnsureTopic = func() {
		fb.beforeEnsureTopic = nil
		fb.ensureTopicErr = nil
		if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(5, 0)); err != nil {
			t.Errorf("concurrent UpdateTopic: %v", err)
		}
	}

	_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1))
	assertInternal(t, err)
	got, gerr := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if gerr != nil {
		t.Fatalf("concurrently-modified topic was rolled back: %v", gerr)
	}
	if got.PartitionCount != 5 {
		t.Fatalf("partitionCount = %d, want 5", got.PartitionCount)
	}
}
