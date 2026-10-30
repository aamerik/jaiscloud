package container

import (
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestClusterAutopilotAndWorkloadIdentityRoundTrip verifies the two cluster
// settings Dataproc-on-GKE validates are decoded from the request and rendered
// back on read.
func TestClusterAutopilotAndWorkloadIdentityRoundTrip(t *testing.T) {
	body := map[string]any{
		"name":    "gke-1",
		"network": "default",
		"autopilot": map[string]any{
			"enabled": true,
		},
		"workloadIdentityConfig": map[string]any{
			"workloadPool": "proj.svc.id.goog",
		},
	}
	c, err := clusterFromBody(body)
	if err != nil {
		t.Fatalf("clusterFromBody: %v", err)
	}
	if c.Autopilot == nil || !c.Autopilot.Enabled {
		t.Fatalf("autopilot not parsed: %+v", c.Autopilot)
	}
	if c.WorkloadIdentityConfig == nil || c.WorkloadIdentityConfig.WorkloadPool != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not parsed: %+v", c.WorkloadIdentityConfig)
	}

	out := clusterToJSON(containerstore.Cluster{
		Name:                   "gke-1",
		Autopilot:              c.Autopilot,
		WorkloadIdentityConfig: c.WorkloadIdentityConfig,
	})
	ap, _ := out["autopilot"].(map[string]any)
	if ap["enabled"] != true {
		t.Fatalf("autopilot not rendered: %v", out)
	}
	wi, _ := out["workloadIdentityConfig"].(map[string]any)
	if wi["workloadPool"] != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not rendered: %v", out)
	}
}

// TestClusterAutopilotAndWorkloadIdentityAbsent verifies neither field is
// invented for a cluster that carries them not at all.
func TestClusterAutopilotAndWorkloadIdentityAbsent(t *testing.T) {
	c, err := clusterFromBody(map[string]any{"name": "plain"})
	if err != nil {
		t.Fatalf("clusterFromBody: %v", err)
	}
	if c.Autopilot != nil || c.WorkloadIdentityConfig != nil {
		t.Fatalf("unexpected settings parsed: %+v %+v", c.Autopilot, c.WorkloadIdentityConfig)
	}
	out := clusterToJSON(c)
	if _, ok := out["autopilot"]; ok {
		t.Fatalf("autopilot must be absent: %v", out)
	}
	if _, ok := out["workloadIdentityConfig"]; ok {
		t.Fatalf("workloadIdentityConfig must be absent: %v", out)
	}
}
