package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"k8s.io/client-go/kubernetes/fake"
)

// fakeTopicAdmin records the admin calls made through the pool.
type fakeTopicAdmin struct {
	mu      sync.Mutex
	created []string
	added   []string
	deleted []string
	closed  bool
	err     error
}

func (f *fakeTopicAdmin) EnsureTopic(_ context.Context, topic string, partitions int32, replicationFactor int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, fmt.Sprintf("%s:%d:%d", topic, partitions, replicationFactor))
	return f.err
}

func (f *fakeTopicAdmin) AddPartitions(_ context.Context, topic string, totalPartitions int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, fmt.Sprintf("%s:%d", topic, totalPartitions))
	return f.err
}

func (f *fakeTopicAdmin) DeleteTopic(_ context.Context, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, topic)
	return f.err
}

func (f *fakeTopicAdmin) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// fakeAdminAPI is a minimal stand-in for *kadm.Client so the franzAdmin mapping
// can be tested at the kadm boundary without a broker.
type fakeAdminAPI struct {
	createResp kadm.CreateTopicResponse
	createErr  error
	updateResp kadm.CreatePartitionsResponses
	updateErr  error
	deleteResp kadm.DeleteTopicResponse
	deleteErr  error
}

func (f *fakeAdminAPI) CreateTopic(context.Context, int32, int16, map[string]*string, string) (kadm.CreateTopicResponse, error) {
	return f.createResp, f.createErr
}

func (f *fakeAdminAPI) UpdatePartitions(context.Context, int, ...string) (kadm.CreatePartitionsResponses, error) {
	return f.updateResp, f.updateErr
}

func (f *fakeAdminAPI) DeleteTopic(context.Context, string) (kadm.DeleteTopicResponse, error) {
	return f.deleteResp, f.deleteErr
}

func (f *fakeAdminAPI) Close() {}

func TestFranzAdminAddPartitionsSurfacesResponseError(t *testing.T) {
	// kadm's top-level error is request-level only; a broker rejection lives in
	// the per-topic response and must be surfaced.
	api := &fakeAdminAPI{updateResp: kadm.CreatePartitionsResponses{
		"t1": {Topic: "t1", Err: kerr.UnknownTopicOrPartition},
	}}
	a := &franzAdmin{client: api}
	err := a.AddPartitions(context.Background(), "t1", 6)
	if !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("AddPartitions error = %v, want UnknownTopicOrPartition", err)
	}

	api2 := &fakeAdminAPI{updateErr: errors.New("network")}
	if err := (&franzAdmin{client: api2}).AddPartitions(context.Background(), "t1", 6); err == nil {
		t.Fatal("AddPartitions swallowed a request-level error")
	}
}

func TestFranzAdminIdempotentCreateAndDelete(t *testing.T) {
	ctx := context.Background()

	api := &fakeAdminAPI{createErr: kerr.TopicAlreadyExists}
	if err := (&franzAdmin{client: api}).EnsureTopic(ctx, "t1", 0, 0); err != nil {
		t.Fatalf("EnsureTopic(already exists) = %v, want nil", err)
	}
	api.createErr = errors.New("boom")
	if err := (&franzAdmin{client: api}).EnsureTopic(ctx, "t1", 3, 2); err == nil {
		t.Fatal("EnsureTopic swallowed a real error")
	}

	api.deleteErr = kerr.UnknownTopicOrPartition
	if err := (&franzAdmin{client: api}).DeleteTopic(ctx, "absent"); err != nil {
		t.Fatalf("DeleteTopic(absent) = %v, want nil", err)
	}
	api.deleteErr = errors.New("boom")
	if err := (&franzAdmin{client: api}).DeleteTopic(ctx, "t1"); err == nil {
		t.Fatal("DeleteTopic swallowed a real error")
	}
}

// recordingFactory returns a factory that hands out fake admins keyed by
// endpoint and counts how many were created.
func recordingFactory() (topicAdminFactory, func() int) {
	var (
		mu      sync.Mutex
		created int
	)
	factory := func(string) (topicAdmin, error) {
		mu.Lock()
		defer mu.Unlock()
		created++
		return &fakeTopicAdmin{}, nil
	}
	return factory, func() int {
		mu.Lock()
		defer mu.Unlock()
		return created
	}
}

func TestAdminPoolCachesPerEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	p := newAdminPool(factory)

	a1, err := p.get("broker-a:9092")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	a2, err := p.get("broker-a:9092")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if a1 != a2 {
		t.Error("same endpoint returned distinct admins")
	}
	if _, err := p.get("broker-b:9092"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if created() != 2 {
		t.Fatalf("admins created = %d, want 2", created())
	}

	// close drops the cached client and closes it once.
	p.close("broker-a:9092")
	if created() != 2 {
		t.Fatalf("close re-created an admin: %d", created())
	}
	if _, err := p.get("broker-a:9092"); err != nil {
		t.Fatalf("get after close: %v", err)
	}
	if created() != 3 {
		t.Fatalf("admins created after close = %d, want 3", created())
	}
}

func TestAdminPoolCloseClosesClient(t *testing.T) {
	fa := &fakeTopicAdmin{}
	p := newAdminPool(func(string) (topicAdmin, error) { return fa, nil })
	if _, err := p.get("broker:9092"); err != nil {
		t.Fatalf("get: %v", err)
	}
	p.closeAll()
	if !fa.closed {
		t.Error("closeAll did not close the admin client")
	}
}

func TestTopicHelpersNoOpWithoutEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	p := newAdminPool(factory)
	ctx := context.Background()

	if err := ensureTopic(ctx, p, "", "t1", 3); err != nil {
		t.Fatalf("ensureTopic: %v", err)
	}
	if err := addPartitions(ctx, p, "", "t1", 6); err != nil {
		t.Fatalf("addPartitions: %v", err)
	}
	if err := deleteBrokerTopic(ctx, p, "", "t1"); err != nil {
		t.Fatalf("deleteBrokerTopic: %v", err)
	}
	if created() != 0 {
		t.Fatalf("no-op calls built %d admin clients, want 0", created())
	}
}

func TestTopicHelpersDelegateToAdmin(t *testing.T) {
	fa := &fakeTopicAdmin{}
	p := newAdminPool(func(string) (topicAdmin, error) { return fa, nil })
	ctx := context.Background()

	if err := ensureTopic(ctx, p, "broker:9092", "t1", 0); err != nil {
		t.Fatalf("ensureTopic: %v", err)
	}
	if err := ensureTopic(ctx, p, "broker:9092", "t2", 3); err != nil {
		t.Fatalf("ensureTopic: %v", err)
	}
	if err := addPartitions(ctx, p, "broker:9092", "t2", 6); err != nil {
		t.Fatalf("addPartitions: %v", err)
	}
	if err := deleteBrokerTopic(ctx, p, "broker:9092", "t2"); err != nil {
		t.Fatalf("deleteBrokerTopic: %v", err)
	}

	// Partition count is floored at 1 and the single-node replica factor is 1.
	wantCreated := []string{"t1:1:1", "t2:3:1"}
	if fmt.Sprint(fa.created) != fmt.Sprint(wantCreated) {
		t.Errorf("created = %v, want %v", fa.created, wantCreated)
	}
	if len(fa.added) != 1 || fa.added[0] != "t2:6" {
		t.Errorf("added = %v, want [t2:6]", fa.added)
	}
	if len(fa.deleted) != 1 || fa.deleted[0] != "t2" {
		t.Errorf("deleted = %v, want [t2]", fa.deleted)
	}
}

func TestTopicHelpersSurfaceAdminError(t *testing.T) {
	fa := &fakeTopicAdmin{err: errors.New("broker down")}
	p := newAdminPool(func(string) (topicAdmin, error) { return fa, nil })
	if err := ensureTopic(context.Background(), p, "broker:9092", "t1", 1); err == nil {
		t.Fatal("ensureTopic swallowed the admin error")
	}
}

func TestK8sBrokerTopicOpsNoOpWithoutEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	b := newK8sBroker(fake.NewSimpleClientset(), "jaiscloud", "redpanda:test", discardLogger())
	b.admins = newAdminPool(factory)
	ctx := context.Background()

	if err := b.EnsureTopic(ctx, "p", "l", "c1", "t1", 3); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if err := b.AddTopicPartitions(ctx, "p", "l", "c1", "t1", 6); err != nil {
		t.Fatalf("AddTopicPartitions: %v", err)
	}
	if err := b.DeleteBrokerTopic(ctx, "p", "l", "c1", "t1"); err != nil {
		t.Fatalf("DeleteBrokerTopic: %v", err)
	}
	if created() != 0 {
		t.Fatalf("no live broker but built %d admin clients, want 0", created())
	}
}

func TestK8sBrokerDelegatesTopicOpsAndReapsAdmin(t *testing.T) {
	// Ensure a live broker so the endpoint exists, but keep the admin pool
	// faked so no Kafka connection is opened.
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	fa := &fakeTopicAdmin{}
	b.admins = newAdminPool(func(string) (topicAdmin, error) { return fa, nil })

	ctx := context.Background()
	if _, err := b.EnsureCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	if err := b.EnsureTopic(ctx, "p", "l", "c1", "t1", 3); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if err := b.AddTopicPartitions(ctx, "p", "l", "c1", "t1", 6); err != nil {
		t.Fatalf("AddTopicPartitions: %v", err)
	}
	if err := b.DeleteBrokerTopic(ctx, "p", "l", "c1", "t1"); err != nil {
		t.Fatalf("DeleteBrokerTopic: %v", err)
	}
	if fmt.Sprint(fa.created) != fmt.Sprint([]string{"t1:3:1"}) {
		t.Errorf("created = %v", fa.created)
	}
	if len(fa.added) != 1 || fa.added[0] != "t1:6" {
		t.Errorf("added = %v", fa.added)
	}
	if len(fa.deleted) != 1 || fa.deleted[0] != "t1" {
		t.Errorf("deleted = %v", fa.deleted)
	}

	// Stopping the cluster closes the pooled admin (its endpoint is gone).
	if err := b.StopCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if !fa.closed {
		t.Error("StopCluster did not close the pooled admin client")
	}
}
