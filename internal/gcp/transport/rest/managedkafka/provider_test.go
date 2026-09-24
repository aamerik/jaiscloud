package managedkafka

import (
	"context"
	"net/http/httptest"
	"testing"

	core "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

func TestCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"POST", "/v1/projects/p/locations/us-central1/clusters?clusterId=c", "CreateCluster"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters", "ListClusters"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c", "GetCluster"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c", "UpdateCluster"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c", "DeleteCluster"},
		{"GET", "/v1/projects/p/locations/us-central1/operations", "ListOperations"},
		{"GET", "/v1/projects/p/locations/us-central1/operations/op", "GetOperation"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/topics?topicId=t", "CreateTopic"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/topics", "ListTopics"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/topics/t", "GetTopic"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c/topics/t", "UpdateTopic"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c/topics/t", "DeleteTopic"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups", "ListConsumerGroups"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups/g", "GetConsumerGroup"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups/g", "UpdateConsumerGroup"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups/g", "DeleteConsumerGroup"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls?aclId=topic/x", "CreateAcl"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/acls", "ListAcls"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x", "GetAcl"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x", "UpdateAcl"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x", "DeleteAcl"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x:addAclEntry", "AddAclEntry"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x:removeAclEntry", "RemoveAclEntry"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/cluster:addAclEntry", "AddAclEntry"},
	}
	for _, tc := range cases {
		codec := NewCodec()
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}

func TestCodecDecodeUnsupported(t *testing.T) {
	codec := NewCodec()
	for _, tc := range []struct{ method, path string }{
		{"PUT", "/v1/projects/p/locations/us-central1/clusters/c"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x:bogus"},
		{"GET", "/v1/projects/p/locations/us-central1/nonsense"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if _, err := codec.Decode(r, nil); err == nil {
			t.Errorf("%s %s: expected error", tc.method, tc.path)
		}
	}
}

func newRESTProvider() (*Provider, *core.Service) {
	c := core.NewService(mkstore.NewMemoryStore())
	return NewProvider(c, "proj"), c
}

func nr(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

func TestProviderClusterAndTopicFlow(t *testing.T) {
	ctx := context.Background()
	p, _ := newRESTProvider()

	// Create cluster.
	resp, err := p.CreateCluster(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1",
		"body": map[string]any{"labels": map[string]any{"env": "test"}},
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("done = %v, want true", resp.Data["done"])
	}
	created, _ := resp.Data["response"].(map[string]any)
	if created["name"] != "projects/proj/locations/us-central1/clusters/c1" {
		t.Errorf("name = %v", created["name"])
	}

	// Create topic.
	if _, err := p.CreateTopic(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "topicId": "t1",
		"body": map[string]any{"partitionCount": float64(3), "replicationFactor": float64(2)},
	})); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	listResp, err := p.ListTopics(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	items, _ := listResp.Data["topics"].([]any)
	if len(items) != 1 {
		t.Fatalf("topics = %v", items)
	}

	// Consumer groups list is empty; item ops are NotFound.
	cg, err := p.ListConsumerGroups(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	groups, ok := cg.Data["consumerGroups"].([]any)
	if !ok {
		t.Fatalf("consumerGroups = %#v, want an empty JSON array (not null)", cg.Data["consumerGroups"])
	}
	if len(groups) != 0 {
		t.Errorf("consumerGroups = %v, want empty", groups)
	}
	if _, err := p.GetConsumerGroup(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1", "consumerGroupId": "g"})); err == nil {
		t.Error("expected NotFound for a consumer group")
	}
}

func TestProviderAclFlow(t *testing.T) {
	ctx := context.Background()
	p, _ := newRESTProvider()
	if _, err := p.CreateCluster(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	entry := map[string]any{"principal": "User:a@example.com", "permissionType": "ALLOW", "operation": "READ", "host": "*"}
	resp, err := p.CreateAcl(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"aclEntries": []any{entry}},
	}))
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	if resp.Data["resourceType"] != "TOPIC" || resp.Data["resourceName"] != "x" {
		t.Errorf("acl = %v", resp.Data)
	}
	etag, _ := resp.Data["etag"].(string)

	// Update requires the etag from the create response.
	if _, err := p.UpdateAcl(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"etag": "stale", "aclEntries": []any{entry}},
	})); err == nil {
		t.Error("expected etag mismatch error")
	}
	if _, err := p.UpdateAcl(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"etag": etag, "aclEntries": []any{entry}},
	})); err != nil {
		t.Fatalf("UpdateAcl: %v", err)
	}

	// Add and remove an entry.
	if _, err := p.AddAclEntry(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"principal": "User:b@example.com", "permissionType": "DENY", "operation": "WRITE", "host": "*"},
	})); err != nil {
		t.Fatalf("AddAclEntry: %v", err)
	}
	removed, err := p.RemoveAclEntry(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"principal": "User:b@example.com", "permissionType": "DENY", "operation": "WRITE", "host": "*"},
	}))
	if err != nil {
		t.Fatalf("RemoveAclEntry: %v", err)
	}
	if _, ok := removed.Data["acl"]; !ok {
		t.Errorf("remove response = %v, want acl", removed.Data)
	}

	// Remove the last entry deletes the acl.
	delResp, err := p.RemoveAclEntry(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"principal": "User:a@example.com", "permissionType": "ALLOW", "operation": "READ", "host": "*"},
	}))
	if err != nil {
		t.Fatalf("RemoveAclEntry last: %v", err)
	}
	if delResp.Data["aclDeleted"] != true {
		t.Errorf("expected aclDeleted, got %v", delResp.Data)
	}
}
