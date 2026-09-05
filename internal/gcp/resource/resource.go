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
	// Cloud Functions
	"cloud-function": func(p, n string) string { return fmt.Sprintf("projects/%s/locations/-/functions/%s", p, n) },
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
