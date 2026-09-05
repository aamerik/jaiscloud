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
	for _, svc := range gcpServices {
		for _, prefix := range svc.PathPrefixes {
			if strings.HasPrefix(p, prefix) {
				return svc.ServiceName, SourcePath
			}
		}
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
// form /{bucket}/{object} (GET/HEAD). Admin routes and JSON-API prefixes are
// handled elsewhere; only genuine object downloads reach this fallback.
func isRawStorageMediaPath(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" || strings.HasPrefix(p, "_jaiscloud") {
		return false
	}
	idx := strings.IndexByte(p, '/')
	return idx > 0 && idx < len(p)-1
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
	}
	return ""
}
