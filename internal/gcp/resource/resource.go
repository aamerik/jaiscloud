// Package resource contains GCP resource-name formatting helpers.
//
// GCP identifies resources by hierarchical names rather than ARNs. A resource
// name is a slash-separated path rooted at the owning project, e.g.
// "projects/{project}/topics/{topic}". Some resources are globally named and
// carry no project prefix (GCS buckets).
package resource

import (
	"fmt"
	"log/slog"
	"strings"
)

// formatters maps abstract resource types to their GCP resource-name format
// function. To add a new resource type, add one entry here — no switch to update.
var formatters = map[string]func(project, name string) string{
	// Google Cloud Storage — bucket names are global, no project prefix.
	"gcs-bucket": func(_, n string) string { return n },
	"gcs-object": func(_, n string) string { return n },
	// GCS bucket IAM policy resourceId uses a fixed "_" project placeholder.
	"gcs-bucket-policy": func(_, n string) string { return "projects/_/buckets/" + n },
	// GCS object IAM policy resourceId: projects/_/buckets/{bucket}/objects/{object}.
	"gcs-object-policy": func(_, n string) string { return "projects/_/buckets/" + n },
	// Cloud Pub/Sub
	"pubsub-topic":        func(p, n string) string { return fmt.Sprintf("projects/%s/topics/%s", p, n) },
	"pubsub-subscription": func(p, n string) string { return fmt.Sprintf("projects/%s/subscriptions/%s", p, n) },
	// Secret Manager
	"secret": func(p, n string) string { return fmt.Sprintf("projects/%s/secrets/%s", p, n) },
	// Cloud KMS — names embed the location; callers pass "location/keyRing/cryptoKey".
	"kms-keyring": func(p, n string) string {
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", p, locOf(n), ringOf(n))
	},
	"kms-cryptokey": func(p, n string) string {
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", p, locOf(n), ringOf(n), keyOf(n))
	},
	"kms-cryptokey-version": func(p, n string) string {
		// callers pass "location/keyRing/cryptoKey/version"
		loc, kr, k, v := parts4(n)
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", p, loc, kr, k, v)
	},
	// IAM — service accounts are identified by their email in the full name.
	"service-account": func(p, n string) string { return fmt.Sprintf("projects/%s/serviceAccounts/%s", p, n) },
	// Firestore — document names: projects/{p}/databases/{db}/documents/{path}.
	// The name argument is the relative path after the project, i.e.
	// "databases/{db}/documents/{path}".
	"firestore-document": func(p, n string) string { return fmt.Sprintf("projects/%s/%s", p, n) },
	// Cloud Functions — names embed the location; callers pass "location/function".
	"cloud-function": func(p, n string) string {
		loc, fn := fnLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/functions/%s", p, loc, fn)
	},
	// Cloud Workflows — names embed the location; callers pass "location/workflow".
	"workflow": func(p, n string) string {
		loc, wf := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/workflows/%s", p, loc, wf)
	},
	// Workflow executions — callers pass "location/workflow/execution".
	"workflow-execution": func(p, n string) string {
		loc, wf, ex := wfExec(n)
		return fmt.Sprintf("projects/%s/locations/%s/workflows/%s/executions/%s", p, loc, wf, ex)
	},
	// Workflow long-running operations — callers pass "location/operation".
	"workflow-operation": func(p, n string) string {
		loc, op := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", p, loc, op)
	},
	// Cloud Dataproc — names embed the region; callers pass "region/name".
	// Dataproc uses "regions" (not "locations") and its operations are the
	// google.longrunning operations under /regions/{region}/operations/{id}.
	"dataproc-cluster": func(p, n string) string {
		reg, c := regionOf(n)
		return fmt.Sprintf("projects/%s/regions/%s/clusters/%s", p, reg, c)
	},
	"dataproc-job": func(p, n string) string {
		reg, j := regionOf(n)
		return fmt.Sprintf("projects/%s/regions/%s/jobs/%s", p, reg, j)
	},
	"dataproc-operation": func(p, n string) string {
		reg, op := regionOf(n)
		return fmt.Sprintf("projects/%s/regions/%s/operations/%s", p, reg, op)
	},
	// Managed Kafka (Apache Kafka for BigQuery) — names embed the location;
	// callers pass "location/cluster" and "location/cluster/topic".
	"managedkafka-cluster": func(p, n string) string {
		loc, c := wfLoc(n)
		return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", p, loc, c)
	},
	"managedkafka-topic": func(p, n string) string {
		loc, c, t := wfExec(n)
		return fmt.Sprintf("projects/%s/locations/%s/clusters/%s/topics/%s", p, loc, c, t)
	},
	// BigQuery — datasets/tables/jobs carry a projectId but no "name" field on
	// the REST wire (they use datasetReference/tableReference/jobReference and
	// the opaque id), so these formatters are not exercised by the v2 REST
	// provider; they exist for the resource-name surface shared with any future
	// gRPC/management-plane representation.
	"bigquery-dataset": func(p, n string) string {
		return fmt.Sprintf("projects/%s/datasets/%s", p, n)
	},
	"bigquery-table": func(p, n string) string {
		d, t := bqTableOf(n)
		return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", p, d, t)
	},
	"bigquery-job": func(p, n string) string {
		return fmt.Sprintf("projects/%s/jobs/%s", p, n)
	},
	// Cloud Logging (gRPC-only; no REST wire equivalent in this emulator).
	"log": func(p, n string) string { return fmt.Sprintf("projects/%s/logs/%s", p, n) },
	// Cloud Monitoring (gRPC-only; no REST wire equivalent in this emulator).
	"metric-descriptor": func(p, n string) string {
		return fmt.Sprintf("projects/%s/metricDescriptors/%s", p, n)
	},
	"alert-policy": func(p, n string) string { return fmt.Sprintf("projects/%s/alertPolicies/%s", p, n) },
	"monitored-resource-descriptor": func(p, n string) string {
		return fmt.Sprintf("projects/%s/monitoredResourceDescriptors/%s", p, n)
	},
}

// ResourceID returns a function that formats GCP resource names for a project.
// Inject the result into NormalizedRequest.ResourceID at the gateway layer.
func ResourceID(project string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		if f, ok := formatters[resourceType]; ok {
			return f(project, name)
		}
		slog.Warn("gcp/resource.ResourceID: unknown resource type, returning name as-is",
			"resourceType", resourceType, "name", name)
		return name
	}
}

// locOf splits a "location/keyRing[/cryptoKey]" name into its location part.
func locOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			return name[:i]
		}
	}
	return "global"
}

// ringOf returns the keyRing segment of a "location/keyRing[/cryptoKey]" name.
func ringOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			rest := name[i+1:]
			for j := 0; j < len(rest); j++ {
				if rest[j] == '/' {
					return rest[:j]
				}
			}
			return rest
		}
	}
	return name
}

// keyOf returns the cryptoKey segment of a "location/keyRing/cryptoKey" name.
func keyOf(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			rest := name[i+1:]
			for j := 0; j < len(rest); j++ {
				if rest[j] == '/' {
					return rest[j+1:]
				}
			}
			return ""
		}
	}
	return ""
}

// parts4 splits a "location/keyRing/cryptoKey/version" name.
func parts4(name string) (loc, ring, key, ver string) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 4:
		return parts[0], parts[1], parts[2], parts[3]
	case 3:
		return parts[0], parts[1], parts[2], ""
	default:
		return "global", name, "", ""
	}
}

// fnLoc splits a "location/function" name. A name with no slash defaults the
// location to "-" (the GCP wildcard region).
func fnLoc(name string) (loc, fn string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "-", name
}

// wfLoc splits a "location/workflow" name. A name with no slash defaults the
// location to "-".
func wfLoc(name string) (loc, wf string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "-", name
}

// regionOf splits a "region/name" Dataproc name. A name with no slash
// defaults the region to "global".
func regionOf(name string) (region, id string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "global", name
}

// wfExec splits a "location/workflow/execution" name.
func wfExec(name string) (loc, wf, ex string) {
	parts := strings.Split(name, "/")
	if len(parts) >= 3 {
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1], ""
	}
	return "-", name, ""
}

// bqTableOf splits a "datasetId/tableId" name into its two segments. A name
// with no slash defaults the dataset to "-".
func bqTableOf(name string) (dataset, table string) {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "-", name
}
