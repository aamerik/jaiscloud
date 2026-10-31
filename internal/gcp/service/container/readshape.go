package container

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// Values real GKE renders on every cluster read. They are GKE's documented
// defaults, not emulator inventions: a new Standard cluster is VPC-native and
// carries logging.googleapis.com/kubernetes + monitoring.googleapis.com/
// kubernetes, Shielded Nodes are enabled, the pod-per-node constraint is 110,
// and legacy ABAC is off (the message is still present in the response).
const (
	// DefaultMaxPodsPerNode is MaxPodsConstraint.maxPodsPerNode for a cluster
	// created without an explicit constraint.
	DefaultMaxPodsPerNode int64 = 110
	// LoggingServiceKubernetes is the cluster logging service GKE selects when
	// the create leaves loggingService empty.
	LoggingServiceKubernetes = "logging.googleapis.com/kubernetes"
	// MonitoringServiceKubernetes is the cluster monitoring service GKE selects
	// when the create leaves monitoringService empty.
	MonitoringServiceKubernetes = "monitoring.googleapis.com/kubernetes"
)

// WithReadDefaults fills the response messages real GKE always returns and a
// client may dereference unconditionally — networkConfig is derived separately
// (NetworkConfigPaths), and masterAuth.clusterCaCertificate stays empty because
// the mock mints no CA. Records written before these fields were modelled (a
// snapshot or a --dsn import) get the same rendering as a fresh cluster, so no
// migration is needed.
//
// Applied on read only: it never changes what a caller persisted.
func WithReadDefaults(c containerstore.Cluster) containerstore.Cluster {
	if c.ShieldedNodes == nil {
		c.ShieldedNodes = &containerstore.ShieldedNodes{Enabled: true}
	}
	if c.MaxPodsPerNode <= 0 {
		c.MaxPodsPerNode = DefaultMaxPodsPerNode
	}
	if c.LoggingService == "" {
		c.LoggingService = LoggingServiceKubernetes
	}
	if c.MonitoringService == "" {
		c.MonitoringService = MonitoringServiceKubernetes
	}
	if c.LegacyAbac == nil {
		c.LegacyAbac = &containerstore.LegacyAbac{}
	}
	return c
}

// NetworkConfigPaths returns the NetworkConfig.network/subnetwork values real
// GKE reports: the relative resource names of the cluster's network and
// subnetwork (Discovery: "projects/my-project/global/networks/my-network" and
// "projects/my-project/regions/us-central1/subnetworks/my-subnet"). Cluster.
// network/subnetwork hold the short names, so both are derived from the cluster
// record rather than stored twice.
func NetworkConfigPaths(c containerstore.Cluster) (network, subnetwork string) {
	return networkPath(c.ProjectID, c.Network), subnetworkPath(c.ProjectID, c.Location, c.Subnetwork)
}

func networkPath(project, network string) string {
	if network == "" {
		network = "default"
	}
	if strings.HasPrefix(network, "projects/") {
		return network
	}
	return resource.ResourceID(project)("compute-network", network)
}

func subnetworkPath(project, location, subnetwork string) string {
	if subnetwork == "" {
		subnetwork = "default"
	}
	if strings.HasPrefix(subnetwork, "projects/") {
		return subnetwork
	}
	// compute-subnetwork takes "region/name" and owns the project prefix.
	return resource.ResourceID(project)("compute-subnetwork", regionOf(location)+"/"+subnetwork)
}

// regionOf maps a cluster location to its region: a location is either a region
// ("us-central1") or a zone ("us-central1-a"); a subnetwork path always names
// the region.
func regionOf(location string) string {
	if i := strings.LastIndex(location, "-"); i > 0 {
		if suffix := location[i+1:]; len(suffix) == 1 && suffix[0] >= 'a' && suffix[0] <= 'z' {
			return location[:i]
		}
	}
	return location
}
