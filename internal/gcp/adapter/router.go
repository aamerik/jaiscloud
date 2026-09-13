package gcp

import (
	"net/http"
	"strings"
)

// DetectionSource indicates how the service was identified.
type DetectionSource int

const (
	SourceUnknown DetectionSource = iota
	SourcePath                    // URL path prefix matched a service
)

// DetectService identifies the GCP service from the HTTP request path.
// GCP has no SigV4 scope; the path is the sole reliable discriminator.
func DetectService(r *http.Request) (service string, source DetectionSource) {
	p := r.URL.Path
	// SDK test clients concatenate an endpoint that may already end in "/" with
	// "/v1/..." paths, yielding a leading "//". Collapse redundant leading
	// slashes so path-prefix detection is stable.
	p = "/" + strings.TrimLeft(p, "/")
	for _, svc := range gcpServices {
		for _, prefix := range svc.PathPrefixes {
			if strings.HasPrefix(p, prefix) {
				return svc.ServiceName, SourcePath
			}
		}
	}
	// BigQuery's servicePath is bigquery/v2/ (not /v1/), so it is detected here
	// before the generic /v1/ resolver below. The /bigquery/v2/projects prefix
	// is unambiguous and cannot collide with dataproc/workflows/managedkafka
	// (all /v1/projects/...).
	if svc := detectBigQueryService(p); svc != "" {
		return svc, SourcePath
	}
	// /v1/projects/{project}/... services — resolve by resource type. This must
	// run before the raw-media fallback: a /v1/... path also has two or more
	// segments and would otherwise be mistaken for a GCS media download.
	if strings.HasPrefix(p, "/v1/") {
		if svc := detectV1Service(r.URL.EscapedPath()); svc != "" {
			return svc, SourcePath
		}
	}
	// GCS media downloads use the "raw" URL form /{bucket}/{object} (no JSON-API
	// prefix). The storage client derives this base from the emulator endpoint.
	// Recognise it as a storage media request when no other service prefix
	// matched and the path has at least a bucket and an object segment.
	if isRawStorageMediaPath(r) {
		return "storage", SourcePath
	}
	return "", SourceUnknown
}

// isRawStorageMediaPath reports whether r is a GCS raw media download of the
// form /{bucket}/{object} (GET/HEAD), or a V4 signed-URL upload of the same
// shape (PUT with an X-Goog-Signature query param). Admin routes and JSON-API
// prefixes are handled elsewhere; only genuine object requests reach this
// fallback.
func isRawStorageMediaPath(r *http.Request) bool {
	switch {
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
	case r.Method == http.MethodPut && hasSignedSignature(r):
	default:
		return false
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" || strings.HasPrefix(p, "_jaiscloud") {
		return false
	}
	idx := strings.IndexByte(p, '/')
	return idx > 0 && idx < len(p)-1
}

// detectBigQueryService maps a BigQuery path to the "bigquery" service name.
// Two forms are accepted: the full servicePath /bigquery/v2/projects/{project}/...
// and the WithEndpoint-stripped form /projects/{project}/{datasets|jobs|queries|serviceAccount}
// (the apiary client resolves method paths against the endpoint option, which
// drops the bigquery/v2/ servicePath). Both are unambiguous — no other GCP
// service in the emulator routes bare /projects/{project}/... without a
// version segment, so this runs before the generic /v1/ resolver and the raw
// GCS media fallback without colliding.
func detectBigQueryService(path string) string {
	if strings.HasPrefix(path, "/bigquery/v2/projects/") {
		return "bigquery"
	}
	if !strings.HasPrefix(path, "/projects/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "/projects/")
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return ""
	}
	resource := rest[idx+1:]
	for _, r := range []string{"datasets", "jobs", "queries", "serviceAccount"} {
		if resource == r || strings.HasPrefix(resource, r+"/") {
			return "bigquery"
		}
	}
	return ""
}

// detectV1Service maps a /v1/projects/{project}/... path to a service name by
// inspecting the resource-type segment(s) after the project.
func detectV1Service(path string) string {
	seg := splitEscaped(path)
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+2 >= len(seg) {
		return ""
	}
	rest := seg[pi+2:]
	// Strip a trailing custom-method suffix (":commit", ":runQuery", ...) from
	// the last segment so "documents:commit" detects as "documents" (mirrors the
	// JSONCodec.Decode custom-method handling).
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			rest[len(rest)-1] = last[:i]
		}
	}
	// Dataproc lives under /v1/projects/{project}/regions/{region}/{clusters|jobs|operations}
	// — detect it before the generic resource-type switch (its "operations"
	// segment would otherwise be mistaken for the Workflows LRO surface).
	if detectDataprocResourceType(rest) != "" {
		return "dataproc"
	}
	// Managed Kafka lives under /v1/projects/{project}/locations/{location}/clusters
	// — detect it before the generic resource-type switch (a topic path's "topics"
	// segment would otherwise be mistaken for the Pub/Sub topic surface).
	if detectManagedKafkaResourceType(rest) != "" {
		return "managedkafka"
	}
	// Dataproc Metastore lives under /v1/projects/{project}/locations/{location}/services
	// — detect it before the generic resource-type switch (its "services" segment
	// is otherwise unmatched; "backups"/"metadataImports"/"operations" are only
	// meaningful nested under it).
	if detectMetastoreResourceType(rest) != "" {
		return "metastore"
	}
	// Memorystore for Redis lives under /v1/projects/{project}/locations/{location}/instances
	// — detect it before the generic resource-type switch (its "locations"
	// segment would otherwise be unclaimed and fall through to the GCS raw-media
	// fallback). Location discovery (locations[/{location}]) is claimed here too:
	// no other emulator service owns those bare paths.
	if detectMemorystoreResourceType(rest) != "" {
		return "redis"
	}
	switch detectResourceType(rest) {
	case "topics", "subscriptions":
		return "pubsub"
	case "secrets":
		return "secretmanager"
	case "keyRings", "cryptoKeys", "cryptoKeyVersions":
		return "kms"
	case "serviceAccounts", "keys":
		return "iam"
	case "documents", "indexes":
		return "firestore"
	case "functions":
		return "functions"
	case "workflows", "operations":
		return "workflows"
	case "executions":
		return "workflowexecutions"
	case "triggers", "channels", "providers":
		return "eventarc"
	}
	return ""
}

// detectDataprocResourceType returns "clusters", "jobs", or "operations" when
// the segments after projects/{project} form regions/{region}/{type}, else "".
func detectDataprocResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "regions" {
		return ""
	}
	switch seg[2] {
	case "clusters", "jobs", "operations":
		return seg[2]
	}
	return ""
}

// detectManagedKafkaResourceType returns "clusters" when the segments after
// projects/{project} form locations/{location}/clusters, else "". The
// "locations" (vs Dataproc's "regions") segment is the distinguishing key. The
// shared locations/{location}/operations/{id} LRO path is intentionally NOT
// claimed here — it is path-ambiguous with Workflows' operations surface on a
// single host, so it remains routed to workflows and Managed Kafka returns its
// operations inline (done: true) from the cluster mutation that created them.
// The provider still registers GetOperation/ListOperations for direct dispatch.
func detectManagedKafkaResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "locations" {
		return ""
	}
	if seg[2] == "clusters" {
		return "clusters"
	}
	return ""
}

// detectMemorystoreResourceType returns "instances" when the segments after
// projects/{project} form locations/{location}/instances, and "locations" for
// the bare location-discovery paths locations or locations/{location}. The
// "instances" segment is unique to Memorystore for Redis among the emulator's
// /v1/projects/{project}/locations/{location}/... services (workflows uses
// "workflows", functions uses "functions", KMS uses "keyRings", Managed Kafka
// uses "clusters", Dataproc Metastore uses "services"). The shared
// locations/{location}/operations/{id} LRO path is intentionally NOT claimed
// here — it is path-ambiguous with Workflows' operations surface on a single
// host, so it remains routed to workflows and Memorystore returns its
// operations inline (done: true).
func detectMemorystoreResourceType(seg []string) string {
	if len(seg) == 0 || seg[0] != "locations" {
		return ""
	}
	switch {
	case len(seg) >= 3 && seg[2] == "instances":
		return "instances"
	case len(seg) <= 2:
		return "locations"
	}
	return ""
}

// detectMetastoreResourceType returns "services" when the segments after
// projects/{project} form locations/{location}/services, else "". The
// "services" segment is unique to Dataproc Metastore among the emulator's
// /v1/projects/{project}/locations/{location}/... services (workflows uses
// "workflows", functions uses "functions", KMS uses "keyRings", Managed Kafka
// uses "clusters"). The shared locations/{location}/operations/{id} LRO path is
// intentionally NOT claimed here — it is path-ambiguous with Workflows'
// operations surface on a single host, so it remains routed to workflows.
func detectMetastoreResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "locations" {
		return ""
	}
	if seg[2] == "services" {
		return "services"
	}
	return ""
}
