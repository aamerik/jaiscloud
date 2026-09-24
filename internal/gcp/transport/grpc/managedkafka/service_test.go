package managedkafka

import (
	"context"
	"testing"

	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

func newGRPCService() *Service {
	return NewService(core.NewService(mkstore.NewMemoryStore()), "proj")
}

func TestCreateClusterLRO(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	op, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    "projects/proj/locations/us-central1",
		ClusterId: "c1",
		Cluster: &managedkafkapb.Cluster{
			Labels:         map[string]string{"env": "test"},
			CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
		},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("operation not done: %+v", op)
	}
	var meta managedkafkapb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetVerb() != "create" || meta.GetTarget() != "projects/proj/locations/us-central1/clusters/c1" {
		t.Errorf("metadata = %+v", &meta)
	}
	var cluster managedkafkapb.Cluster
	if err := op.GetResponse().UnmarshalTo(&cluster); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if cluster.GetName() != "projects/proj/locations/us-central1/clusters/c1" {
		t.Errorf("cluster name = %q", cluster.GetName())
	}
	if cluster.GetState() != managedkafkapb.Cluster_ACTIVE {
		t.Errorf("cluster state = %v, want ACTIVE", cluster.GetState())
	}
	if cluster.GetLabels()["env"] != "test" {
		t.Errorf("labels = %v", cluster.GetLabels())
	}

	got, err := s.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: cluster.GetName()})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if got.GetName() != cluster.GetName() {
		t.Errorf("get name = %q", got.GetName())
	}

	list, err := s.ListClusters(ctx, &managedkafkapb.ListClustersRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list.GetClusters()) != 1 {
		t.Fatalf("clusters = %v", list.GetClusters())
	}

	delOp, err := s.DeleteCluster(ctx, &managedkafkapb.DeleteClusterRequest{Name: cluster.GetName()})
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if !delOp.GetDone() {
		t.Error("delete op not done")
	}
	var deleted managedkafkapb.OperationMetadata
	if err := delOp.GetMetadata().UnmarshalTo(&deleted); err != nil {
		t.Fatalf("unmarshal delete metadata: %v", err)
	}
	if deleted.GetVerb() != "delete" {
		t.Errorf("delete verb = %q", deleted.GetVerb())
	}
	if _, err := s.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: cluster.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetCluster after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateClusterAlreadyExists(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	req := &managedkafkapb.CreateClusterRequest{
		Parent:    "projects/proj/locations/us-central1",
		ClusterId: "c1",
		Cluster:   &managedkafkapb.Cluster{},
	}
	if _, err := s.CreateCluster(ctx, req); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := s.CreateCluster(ctx, req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second create: code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestTopicCRUD(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	if _, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent: "projects/proj/locations/us-central1", ClusterId: "c1", Cluster: &managedkafkapb.Cluster{},
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	tp, err := s.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
		Parent:  "projects/proj/locations/us-central1/clusters/c1",
		TopicId: "t1",
		Topic:   &managedkafkapb.Topic{PartitionCount: 3, ReplicationFactor: 2},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if tp.GetName() != "projects/proj/locations/us-central1/clusters/c1/topics/t1" || tp.GetPartitionCount() != 3 {
		t.Errorf("topic = %+v", tp)
	}

	got, err := s.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: tp.GetName()})
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.GetReplicationFactor() != 2 {
		t.Errorf("replicationFactor = %d", got.GetReplicationFactor())
	}

	if _, err := s.UpdateTopic(ctx, &managedkafkapb.UpdateTopicRequest{
		Topic: &managedkafkapb.Topic{Name: tp.GetName(), PartitionCount: 6},
	}); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}

	list, err := s.ListTopics(ctx, &managedkafkapb.ListTopicsRequest{Parent: "projects/proj/locations/us-central1/clusters/c1"})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(list.GetTopics()) != 1 {
		t.Fatalf("topics = %v", list.GetTopics())
	}

	if _, err := s.DeleteTopic(ctx, &managedkafkapb.DeleteTopicRequest{Name: tp.GetName()}); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := s.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: tp.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetTopic after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestAclCRUD(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	if _, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent: "projects/proj/locations/us-central1", ClusterId: "c1", Cluster: &managedkafkapb.Cluster{},
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	entry := &managedkafkapb.AclEntry{Principal: "User:a@example.com", PermissionType: "ALLOW", Operation: "READ", Host: "*"}

	acl, err := s.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
		Parent: "projects/proj/locations/us-central1/clusters/c1",
		AclId:  "topic/orders",
		Acl:    &managedkafkapb.Acl{AclEntries: []*managedkafkapb.AclEntry{entry}},
	})
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	if acl.GetResourceType() != "TOPIC" || acl.GetResourceName() != "orders" || acl.GetEtag() == "" {
		t.Errorf("acl = %+v", acl)
	}

	// Add an entry.
	added, err := s.AddAclEntry(ctx, &managedkafkapb.AddAclEntryRequest{
		Acl:      acl.GetName(),
		AclEntry: &managedkafkapb.AclEntry{Principal: "User:b@example.com", PermissionType: "DENY", Operation: "WRITE", Host: "*"},
	})
	if err != nil {
		t.Fatalf("AddAclEntry: %v", err)
	}
	if added.GetAclCreated() {
		t.Error("expected aclCreated=false")
	}
	if len(added.GetAcl().GetAclEntries()) != 2 {
		t.Fatalf("entries = %v", added.GetAcl().GetAclEntries())
	}

	// Update with the current etag.
	upd, err := s.UpdateAcl(ctx, &managedkafkapb.UpdateAclRequest{
		Acl: &managedkafkapb.Acl{Name: acl.GetName(), Etag: added.GetAcl().GetEtag(), AclEntries: []*managedkafkapb.AclEntry{entry}},
	})
	if err != nil {
		t.Fatalf("UpdateAcl: %v", err)
	}
	// Stale etag is Aborted.
	if _, err := s.UpdateAcl(ctx, &managedkafkapb.UpdateAclRequest{
		Acl: &managedkafkapb.Acl{Name: acl.GetName(), Etag: "stale", AclEntries: []*managedkafkapb.AclEntry{entry}},
	}); status.Code(err) != codes.Aborted {
		t.Errorf("stale update: code = %v, want Aborted", status.Code(err))
	}

	// Remove the only remaining entry: acl is deleted.
	rem, err := s.RemoveAclEntry(ctx, &managedkafkapb.RemoveAclEntryRequest{Acl: upd.GetName(), AclEntry: entry})
	if err != nil {
		t.Fatalf("RemoveAclEntry: %v", err)
	}
	if !rem.GetAclDeleted() {
		t.Errorf("expected aclDeleted, got %+v", rem)
	}
	if _, err := s.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: upd.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetAcl after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestConsumerGroupsGRPC(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	if _, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent: "projects/proj/locations/us-central1", ClusterId: "c1", Cluster: &managedkafkapb.Cluster{},
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	list, err := s.ListConsumerGroups(ctx, &managedkafkapb.ListConsumerGroupsRequest{
		Parent: "projects/proj/locations/us-central1/clusters/c1",
	})
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	if len(list.GetConsumerGroups()) != 0 {
		t.Errorf("consumerGroups = %v, want empty", list.GetConsumerGroups())
	}
	name := "projects/proj/locations/us-central1/clusters/c1/consumerGroups/g"
	if _, err := s.GetConsumerGroup(ctx, &managedkafkapb.GetConsumerGroupRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Errorf("GetConsumerGroup: code = %v, want NotFound", status.Code(err))
	}
	if _, err := s.DeleteConsumerGroup(ctx, &managedkafkapb.DeleteConsumerGroupRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Errorf("DeleteConsumerGroup: code = %v, want NotFound", status.Code(err))
	}
}

// compile-time assertion that the adapter implements the generated server.
var _ managedkafkapb.ManagedKafkaServer = (*Service)(nil)
