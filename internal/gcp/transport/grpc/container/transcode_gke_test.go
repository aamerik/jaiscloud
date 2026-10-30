package container

import (
	"testing"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"
)

// TestClusterAutopilotAndWorkloadIdentityTranscode verifies the gRPC transport
// carries the same two cluster settings the REST transport does, so a
// Dataproc-on-GKE create sees one emulated GKE cluster regardless of the client.
func TestClusterAutopilotAndWorkloadIdentityTranscode(t *testing.T) {
	pb := &containerpb.Cluster{
		Name:                   "gke-1",
		Autopilot:              &containerpb.Autopilot{Enabled: true},
		WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{WorkloadPool: "proj.svc.id.goog"},
	}
	stored := clusterFromProto(pb)
	if stored.Autopilot == nil || !stored.Autopilot.Enabled {
		t.Fatalf("autopilot not decoded: %+v", stored.Autopilot)
	}
	if stored.WorkloadIdentityConfig == nil || stored.WorkloadIdentityConfig.WorkloadPool != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not decoded: %+v", stored.WorkloadIdentityConfig)
	}

	back := clusterToProto(stored)
	if !back.GetAutopilot().GetEnabled() {
		t.Fatalf("autopilot not rendered: %v", back)
	}
	if back.GetWorkloadIdentityConfig().GetWorkloadPool() != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not rendered: %v", back)
	}
}
