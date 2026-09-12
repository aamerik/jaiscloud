package monitoring

import (
	"sort"

	labelpb "google.golang.org/genproto/googleapis/api/label"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"

	"jaiscloud/internal/gcp/resource"
)

// catalogEntry is one canonical MonitoredResourceDescriptor definition: the
// type, its human-readable display name/description, and the labels that
// identify an instance of the type.
type catalogEntry struct {
	typ         string
	displayName string
	description string
	labels      []*labelpb.LabelDescriptor
}

func strLabel(key, description string) *labelpb.LabelDescriptor {
	return &labelpb.LabelDescriptor{
		Key:         key,
		ValueType:   labelpb.LabelDescriptor_STRING,
		Description: description,
	}
}

func int64Label(key, description string) *labelpb.LabelDescriptor {
	return &labelpb.LabelDescriptor{
		Key:         key,
		ValueType:   labelpb.LabelDescriptor_INT64,
		Description: description,
	}
}

// monitoredResourceCatalog is the canonical catalog of well-known monitored
// resource types the emulator serves. The label sets mirror the descriptors
// published by Cloud Monitoring (cloud.google.com/monitoring/api/resources).
var monitoredResourceCatalog = []catalogEntry{
	{
		typ:         "api",
		displayName: "API",
		description: "An API request, such as a call to a Google Cloud service.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource, such as \"my-project\"."),
			strLabel("service", "The service name as used for authentication, e.g. \"compute.googleapis.com\"."),
			strLabel("method", "The API method name, e.g. \"compute.instances.get\"."),
			strLabel("version", "The API version, e.g. \"v1\"."),
			strLabel("location", "The location of the API request, e.g. \"us-east1\"."),
		},
	},
	{
		typ:         "cloudsql_database",
		displayName: "Cloud SQL Database",
		description: "A relational database hosted by Google Cloud SQL.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("database_id", "The name of the Cloud SQL database."),
			strLabel("region", "The region in which the database is running, e.g. \"us-central1\"."),
		},
	},
	{
		typ:         "dataproc_cluster",
		displayName: "Cloud Dataproc Cluster",
		description: "A cluster running in Google Cloud Dataproc.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("region", "The region in which the cluster is running, e.g. \"us-central1\"."),
			strLabel("cluster_name", "The name of the cluster."),
		},
	},
	{
		typ:         "gce_instance",
		displayName: "GCE VM Instance",
		description: "A virtual machine instance running in Google Compute Engine.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			int64Label("instance_id", "The numeric VM instance identifier assigned by Compute Engine."),
			strLabel("zone", "The Compute Engine zone in which the VM is running, e.g. \"us-central1-a\"."),
		},
	},
	{
		typ:         "gcs_bucket",
		displayName: "GCS Bucket",
		description: "A bucket in Google Cloud Storage.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("bucket_name", "The name of the bucket."),
			strLabel("location", "The location of the bucket, e.g. \"us-central1\" or \"US\"."),
		},
	},
	{
		typ:         "k8s_container",
		displayName: "Kubernetes Container",
		description: "A container running in a Kubernetes cluster.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("location", "The location of the cluster, e.g. \"us-central1-a\"."),
			strLabel("cluster_name", "The name of the cluster."),
			strLabel("namespace_name", "The name of the namespace."),
			strLabel("pod_name", "The name of the pod."),
			strLabel("container_name", "The name of the container."),
		},
	},
	{
		typ:         "k8s_node",
		displayName: "Kubernetes Node",
		description: "A node running in a Kubernetes cluster.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("location", "The location of the cluster, e.g. \"us-central1-a\"."),
			strLabel("cluster_name", "The name of the cluster."),
			strLabel("node_name", "The name of the node."),
		},
	},
	{
		typ:         "k8s_pod",
		displayName: "Kubernetes Pod",
		description: "A pod running in a Kubernetes cluster.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("location", "The location of the cluster, e.g. \"us-central1-a\"."),
			strLabel("cluster_name", "The name of the cluster."),
			strLabel("namespace_name", "The name of the namespace."),
			strLabel("pod_name", "The name of the pod."),
		},
	},
	{
		typ:         "pubsub_subscription",
		displayName: "Pub/Sub Subscription",
		description: "A subscription in Google Cloud Pub/Sub.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("subscription_id", "The name of the subscription."),
		},
	},
	{
		typ:         "pubsub_topic",
		displayName: "Pub/Sub Topic",
		description: "A topic in Google Cloud Pub/Sub.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("topic_id", "The name of the topic."),
		},
	},
}

// monitoredResourceDescriptors returns a fresh, type-sorted copy of the
// canonical catalog with resource names scoped to project.
func monitoredResourceDescriptors(project string) []*monitoredrespb.MonitoredResourceDescriptor {
	out := make([]*monitoredrespb.MonitoredResourceDescriptor, 0, len(monitoredResourceCatalog))
	for _, e := range monitoredResourceCatalog {
		out = append(out, descriptorFromCatalogEntry(project, e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetType() < out[j].GetType() })
	return out
}

// lookupMonitoredResourceDescriptor returns the canonical descriptor for typ,
// or ok=false when the type is not in the catalog.
func lookupMonitoredResourceDescriptor(project, typ string) (*monitoredrespb.MonitoredResourceDescriptor, bool) {
	for _, e := range monitoredResourceCatalog {
		if e.typ == typ {
			return descriptorFromCatalogEntry(project, e), true
		}
	}
	return nil, false
}

func descriptorFromCatalogEntry(project string, e catalogEntry) *monitoredrespb.MonitoredResourceDescriptor {
	return &monitoredrespb.MonitoredResourceDescriptor{
		Name:        resource.ResourceID(project)("monitored-resource-descriptor", e.typ),
		Type:        e.typ,
		DisplayName: e.displayName,
		Description: e.description,
		Labels:      e.labels,
	}
}
