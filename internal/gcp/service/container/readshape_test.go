package container

import (
	"context"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestWithReadDefaultsFillsResponseMessages pins the four nested messages real
// GKE always renders and clients dereference unconditionally (the
// hashicorp/google provider's google_container_cluster read aborts on any of
// them being absent), for a record stored before they were modelled.
func TestWithReadDefaultsFillsResponseMessages(t *testing.T) {
	c := WithReadDefaults(containerstore.Cluster{ProjectID: "p", Location: "us-central1", Name: "c1"})
	if c.ShieldedNodes == nil || !c.ShieldedNodes.Enabled {
		t.Fatalf("shieldedNodes = %+v, want enabled", c.ShieldedNodes)
	}
	if c.MaxPodsPerNode != DefaultMaxPodsPerNode {
		t.Fatalf("maxPodsPerNode = %d, want %d", c.MaxPodsPerNode, DefaultMaxPodsPerNode)
	}
	if c.LegacyAbac == nil || c.LegacyAbac.Enabled {
		t.Fatalf("legacyAbac = %+v, want present and disabled", c.LegacyAbac)
	}
	if c.LoggingService != LoggingServiceKubernetes {
		t.Fatalf("loggingService = %q", c.LoggingService)
	}
	if c.MonitoringService != MonitoringServiceKubernetes {
		t.Fatalf("monitoringService = %q", c.MonitoringService)
	}
}

// TestWithReadDefaultsKeepsCallerValues verifies the defaults never overwrite
// what a caller (or a setter) recorded.
func TestWithReadDefaultsKeepsCallerValues(t *testing.T) {
	c := WithReadDefaults(containerstore.Cluster{
		Name:              "c1",
		ShieldedNodes:     &containerstore.ShieldedNodes{Enabled: false},
		MaxPodsPerNode:    64,
		LegacyAbac:        &containerstore.LegacyAbac{Enabled: true},
		LoggingService:    "none",
		MonitoringService: "none",
	})
	if c.ShieldedNodes.Enabled {
		t.Fatalf("shieldedNodes rewritten: %+v", c.ShieldedNodes)
	}
	if c.MaxPodsPerNode != 64 {
		t.Fatalf("maxPodsPerNode rewritten: %d", c.MaxPodsPerNode)
	}
	if !c.LegacyAbac.Enabled {
		t.Fatalf("legacyAbac rewritten: %+v", c.LegacyAbac)
	}
	if c.LoggingService != "none" || c.MonitoringService != "none" {
		t.Fatalf("services rewritten: %q %q", c.LoggingService, c.MonitoringService)
	}
}

// TestNetworkConfigPathsDerivesResourceNames pins NetworkConfig.network and
// .subnetwork as the relative resource names Discovery documents, including the
// zone→region reduction a zonal cluster needs.
func TestNetworkConfigPathsDerivesResourceNames(t *testing.T) {
	for _, tc := range []struct {
		location string
		network  string
		subnet   string
		wantNet  string
		wantSub  string
	}{
		{"us-central1", "default", "default", "projects/p/global/networks/default", "projects/p/regions/us-central1/subnetworks/default"},
		{"us-central1-a", "vpc-1", "sn-1", "projects/p/global/networks/vpc-1", "projects/p/regions/us-central1/subnetworks/sn-1"},
		// Already-canonical inputs are passed through, not prefixed twice.
		{"us-central1", "projects/p/global/networks/vpc-2", "projects/p/regions/us-east1/subnetworks/sn-2",
			"projects/p/global/networks/vpc-2", "projects/p/regions/us-east1/subnetworks/sn-2"},
	} {
		c := containerstore.Cluster{ProjectID: "p", Location: tc.location, Network: tc.network, Subnetwork: tc.subnet}
		net, sub := NetworkConfigPaths(c)
		if net != tc.wantNet || sub != tc.wantSub {
			t.Fatalf("%s: got %q / %q, want %q / %q", tc.location, net, sub, tc.wantNet, tc.wantSub)
		}
	}
}

// TestCreateClusterStoresAndRendersReadDefaults verifies a fresh cluster
// persists the GKE defaults (so a snapshot carries them) and renders them on
// both the single read and the list.
func TestCreateClusterStoresAndRendersReadDefaults(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.ShieldedNodes == nil || !c.ShieldedNodes.Enabled || c.MaxPodsPerNode != 110 {
		t.Fatalf("stored defaults = %+v / %d", c.ShieldedNodes, c.MaxPodsPerNode)
	}
	if c.LoggingService != LoggingServiceKubernetes || c.MonitoringService != MonitoringServiceKubernetes {
		t.Fatalf("stored services = %q %q", c.LoggingService, c.MonitoringService)
	}
	list, err := s.ListClusters(ctx, "p", "us-central1")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list) != 1 || list[0].ShieldedNodes == nil {
		t.Fatalf("list = %+v", list)
	}
}
