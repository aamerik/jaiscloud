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

	core "jaiscloud/internal/gcp/service/managedkafka"
)

// fakeKafkaAdmin records the admin calls made through the pool.
type fakeKafkaAdmin struct {
	mu      sync.Mutex
	created []string
	added   []string
	deleted []string
	closed  bool
	err     error

	// Consumer-group state.
	groups       []string
	groupOffsets map[string][]core.ConsumerGroupOffset
	groupMembers map[string]int
	committed    map[string][]core.ConsumerGroupOffset
}

func (f *fakeKafkaAdmin) EnsureTopic(_ context.Context, topic string, partitions int32, replicationFactor int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, fmt.Sprintf("%s:%d:%d", topic, partitions, replicationFactor))
	return f.err
}

func (f *fakeKafkaAdmin) AddPartitions(_ context.Context, topic string, totalPartitions int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, fmt.Sprintf("%s:%d", topic, totalPartitions))
	return f.err
}

func (f *fakeKafkaAdmin) DeleteTopic(_ context.Context, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, topic)
	return f.err
}

func (f *fakeKafkaAdmin) ListGroups(context.Context) ([]string, error) {
	return f.groups, f.err
}

func (f *fakeKafkaAdmin) GroupOffsets(_ context.Context, group string) ([]core.ConsumerGroupOffset, bool, error) {
	offs, ok := f.groupOffsets[group]
	return offs, ok, f.err
}

func (f *fakeKafkaAdmin) GroupMembers(_ context.Context, group string) (int, bool, error) {
	n, ok := f.groupMembers[group]
	return n, ok, f.err
}

func (f *fakeKafkaAdmin) DeleteGroup(_ context.Context, group string) (bool, error) {
	_, ok := f.groupOffsets[group]
	delete(f.groupOffsets, group)
	return ok, f.err
}

func (f *fakeKafkaAdmin) CommitGroupOffsets(_ context.Context, group string, offsets []core.ConsumerGroupOffset) error {
	if f.committed == nil {
		f.committed = make(map[string][]core.ConsumerGroupOffset)
	}
	f.committed[group] = offsets
	return f.err
}

func (f *fakeKafkaAdmin) Close() error {
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

	groupsResp       kadm.ListedGroups
	groupsErr        error
	describeResp     kadm.DescribedGroups
	describeErr      error
	fetchResp        kadm.OffsetResponses
	fetchErr         error
	deleteGroupsResp kadm.DeleteGroupResponses
	deleteGroupsErr  error
	commitResp       kadm.OffsetResponses
	commitErr        error
	committed        kadm.Offsets
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

func (f *fakeAdminAPI) ListGroups(context.Context, ...string) (kadm.ListedGroups, error) {
	return f.groupsResp, f.groupsErr
}

func (f *fakeAdminAPI) DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error) {
	return f.describeResp, f.describeErr
}

func (f *fakeAdminAPI) FetchOffsets(context.Context, string) (kadm.OffsetResponses, error) {
	return f.fetchResp, f.fetchErr
}

func (f *fakeAdminAPI) DeleteGroups(context.Context, ...string) (kadm.DeleteGroupResponses, error) {
	return f.deleteGroupsResp, f.deleteGroupsErr
}

func (f *fakeAdminAPI) CommitOffsets(_ context.Context, _ string, os kadm.Offsets) (kadm.OffsetResponses, error) {
	f.committed = os
	return f.commitResp, f.commitErr
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

func TestFranzAdminGroupOffsets(t *testing.T) {
	ctx := context.Background()
	described := kadm.DescribedGroups{"g1": {Group: "g1", Members: []kadm.DescribedGroupMember{{}, {}}}}
	api := &fakeAdminAPI{
		describeResp: described,
		fetchResp: kadm.OffsetResponses{
			"t2": {0: {Offset: kadm.Offset{Topic: "t2", Partition: 0, At: 5}}},
			"t1": {1: {Offset: kadm.Offset{Topic: "t1", Partition: 1, At: 3, Metadata: "m"}}},
		},
	}
	a := &franzAdmin{client: api}

	offs, found, err := a.GroupOffsets(ctx, "g1")
	if err != nil || !found {
		t.Fatalf("GroupOffsets = %v, %v, want found", found, err)
	}
	// Sorted by topic then partition.
	if len(offs) != 2 || offs[0].Topic != "t1" || offs[0].Offset != 3 || offs[0].Metadata != "m" || offs[1].Topic != "t2" {
		t.Fatalf("offsets = %+v", offs)
	}

	members, found, err := a.GroupMembers(ctx, "g1")
	if err != nil || !found || members != 2 {
		t.Fatalf("GroupMembers = %d, %v, %v; want 2, true, nil", members, found, err)
	}

	// An unknown group is reported absent, not an error.
	api.describeResp = kadm.DescribedGroups{"g1": {Group: "g1", Err: kerr.GroupIDNotFound}}
	if _, found, err := a.GroupOffsets(ctx, "g1"); err != nil || found {
		t.Fatalf("GroupOffsets(unknown) = found %v, err %v; want absent nil", found, err)
	}
	if _, found, err := a.GroupMembers(ctx, "g1"); err != nil || found {
		t.Fatalf("GroupMembers(unknown) = found %v, err %v; want absent nil", found, err)
	}
}

func TestFranzAdminDeleteAndCommitGroup(t *testing.T) {
	ctx := context.Background()

	api := &fakeAdminAPI{deleteGroupsResp: kadm.DeleteGroupResponses{"g1": {Group: "g1"}}}
	a := &franzAdmin{client: api}
	if existed, err := a.DeleteGroup(ctx, "g1"); err != nil || !existed {
		t.Fatalf("DeleteGroup = %v, %v; want true, nil", existed, err)
	}
	api.deleteGroupsResp = kadm.DeleteGroupResponses{"g1": {Group: "g1", Err: kerr.GroupIDNotFound}}
	if existed, err := a.DeleteGroup(ctx, "g1"); err != nil || existed {
		t.Fatalf("DeleteGroup(absent) = %v, %v; want false, nil", existed, err)
	}
	// A non-empty group surfaces as the core precondition sentinel, not a raw
	// Kafka error that would encode as INTERNAL.
	api.deleteGroupsResp = kadm.DeleteGroupResponses{"g1": {Group: "g1", Err: kerr.NonEmptyGroup}}
	if _, err := a.DeleteGroup(ctx, "g1"); !errors.Is(err, core.ErrConsumerGroupNotEmpty) {
		t.Fatalf("DeleteGroup(non-empty) = %v, want ErrConsumerGroupNotEmpty", err)
	}
	api.deleteGroupsErr = errors.New("boom")
	if _, err := a.DeleteGroup(ctx, "g1"); err == nil {
		t.Fatal("DeleteGroup swallowed a request-level error")
	}

	// Commit maps bare topic ids into kadm.Offsets and surfaces per-partition
	// response errors.
	api = &fakeAdminAPI{}
	a = &franzAdmin{client: api}
	if err := a.CommitGroupOffsets(ctx, "g1", []core.ConsumerGroupOffset{{Topic: "t1", Partition: 2, Offset: 9, Metadata: "x"}}); err != nil {
		t.Fatalf("CommitGroupOffsets: %v", err)
	}
	got, ok := api.committed.Lookup("t1", 2)
	if !ok || got.At != 9 || got.Metadata != "x" {
		t.Fatalf("committed = %+v", api.committed)
	}
	api.commitResp = kadm.OffsetResponses{"t1": {2: {Offset: kadm.Offset{Topic: "t1", Partition: 2}, Err: kerr.UnknownTopicOrPartition}}}
	if err := a.CommitGroupOffsets(ctx, "g1", nil); !errors.Is(err, core.ErrConsumerGroupTopicNotFound) {
		t.Fatalf("CommitGroupOffsets error = %v, want ErrConsumerGroupTopicNotFound", err)
	}
}

// recordingFactory returns a factory that hands out fake admins keyed by
// endpoint and counts how many were created.
func recordingFactory() (kafkaAdminFactory, func() int) {
	var (
		mu      sync.Mutex
		created int
	)
	factory := func(string) (kafkaAdmin, error) {
		mu.Lock()
		defer mu.Unlock()
		created++
		return &fakeKafkaAdmin{}, nil
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
	fa := &fakeKafkaAdmin{}
	p := newAdminPool(func(string) (kafkaAdmin, error) { return fa, nil })
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
	fa := &fakeKafkaAdmin{}
	p := newAdminPool(func(string) (kafkaAdmin, error) { return fa, nil })
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
	fa := &fakeKafkaAdmin{err: errors.New("broker down")}
	p := newAdminPool(func(string) (kafkaAdmin, error) { return fa, nil })
	if err := ensureTopic(context.Background(), p, "broker:9092", "t1", 1); err == nil {
		t.Fatal("ensureTopic swallowed the admin error")
	}
}

func TestGroupHelpersNoOpWithoutEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	p := newAdminPool(factory)
	ctx := context.Background()

	groups, err := listGroups(ctx, p, "")
	if err != nil || len(groups) != 0 {
		t.Fatalf("listGroups = %v, %v; want empty nil", groups, err)
	}
	if _, found, err := groupOffsets(ctx, p, "", "g1"); err != nil || found {
		t.Fatalf("groupOffsets = found %v, err %v; want absent nil", found, err)
	}
	if _, found, err := groupMembers(ctx, p, "", "g1"); err != nil || found {
		t.Fatalf("groupMembers = found %v, err %v; want absent nil", found, err)
	}
	if existed, err := deleteGroup(ctx, p, "", "g1"); err != nil || existed {
		t.Fatalf("deleteGroup = %v, %v; want false nil", existed, err)
	}
	if err := commitGroupOffsets(ctx, p, "", "g1", nil); err != nil {
		t.Fatalf("commitGroupOffsets: %v", err)
	}
	if created() != 0 {
		t.Fatalf("no-op calls built %d admin clients, want 0", created())
	}
}

func TestGroupHelpersDelegateToAdmin(t *testing.T) {
	fa := &fakeKafkaAdmin{
		groups:       []string{"g1"},
		groupOffsets: map[string][]core.ConsumerGroupOffset{"g1": {{Topic: "t1", Partition: 0, Offset: 4}}},
		groupMembers: map[string]int{"g1": 1},
	}
	p := newAdminPool(func(string) (kafkaAdmin, error) { return fa, nil })
	ctx := context.Background()

	if groups, err := listGroups(ctx, p, "broker:9092"); err != nil || len(groups) != 1 || groups[0] != "g1" {
		t.Fatalf("listGroups = %v, %v", groups, err)
	}
	if offs, found, err := groupOffsets(ctx, p, "broker:9092", "g1"); err != nil || !found || len(offs) != 1 {
		t.Fatalf("groupOffsets = %+v, %v, %v", offs, found, err)
	}
	if n, found, err := groupMembers(ctx, p, "broker:9092", "g1"); err != nil || !found || n != 1 {
		t.Fatalf("groupMembers = %d, %v, %v", n, found, err)
	}
	if existed, err := deleteGroup(ctx, p, "broker:9092", "g1"); err != nil || !existed {
		t.Fatalf("deleteGroup = %v, %v", existed, err)
	}
	if err := commitGroupOffsets(ctx, p, "broker:9092", "g1", []core.ConsumerGroupOffset{{Topic: "t2", Partition: 0, Offset: 8}}); err != nil {
		t.Fatalf("commitGroupOffsets: %v", err)
	}
	if got := fa.committed["g1"]; len(got) != 1 || got[0].Topic != "t2" || got[0].Offset != 8 {
		t.Fatalf("committed = %+v", got)
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

func TestK8sBrokerGroupOpsNoOpWithoutEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	b := newK8sBroker(fake.NewSimpleClientset(), "jaiscloud", "redpanda:test", discardLogger())
	b.admins = newAdminPool(factory)
	ctx := context.Background()

	if groups, err := b.ListConsumerGroups(ctx, "p", "l", "c1"); err != nil || len(groups) != 0 {
		t.Fatalf("ListConsumerGroups = %v, %v", groups, err)
	}
	if _, found, err := b.ConsumerGroupOffsets(ctx, "p", "l", "c1", "g1"); err != nil || found {
		t.Fatalf("ConsumerGroupOffsets = %v, %v", found, err)
	}
	if existed, err := b.DeleteConsumerGroup(ctx, "p", "l", "c1", "g1"); err != nil || existed {
		t.Fatalf("DeleteConsumerGroup = %v, %v", existed, err)
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

	fa := &fakeKafkaAdmin{}
	b.admins = newAdminPool(func(string) (kafkaAdmin, error) { return fa, nil })

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
