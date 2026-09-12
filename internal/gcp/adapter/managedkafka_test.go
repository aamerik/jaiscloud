package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestManagedKafkaCodecDecode(t *testing.T) {
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
	}
	for _, tc := range cases {
		codec := &ManagedKafkaCodec{Service: "managedkafka"}
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

// TestDetectManagedKafkaServiceDoesNotClaimOperations locks the
// operations-routing decision: the shared locations/{l}/operations/{id} LRO
// path is path-ambiguous with Workflows on a single host, so it stays routed to
// workflows. Managed Kafka returns its operations inline (done:true), and only
// the "clusters" segment is claimed.
func TestDetectManagedKafkaServiceDoesNotClaimOperations(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us-central1/clusters":     "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c":   "managedkafka",
		"/v1/projects/p/locations/us-central1/operations":   "workflows",
		"/v1/projects/p/locations/us-central1/operations/o": "workflows",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}

	if got := detectManagedKafkaResourceType([]string{"locations", "us-central1", "operations", "op"}); got != "" {
		t.Errorf("detectManagedKafkaResourceType claimed the shared operations path: %q", got)
	}
	if got := detectManagedKafkaResourceType([]string{"locations", "us-central1", "clusters"}); got != "clusters" {
		t.Errorf("detectManagedKafkaResourceType should claim clusters, got %q", got)
	}
}
