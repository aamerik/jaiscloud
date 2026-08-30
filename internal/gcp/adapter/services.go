package gcp

import (
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
)

// ServiceDescriptor captures the per-service metadata needed by the router and
// the gateway routing layer. GCP services are identified by URL path prefix
// rather than a SigV4 scope or target header.
type ServiceDescriptor struct {
	// ServiceName is the wire service name set on NormalizedRequest.Service
	// (e.g. "storage", "pubsub").
	ServiceName string

	// PathPrefixes are URL path prefixes that unambiguously identify this
	// service (e.g. "/storage/v1/" and "/upload/storage/v1/" for GCS).
	PathPrefixes []string

	// ProviderPrefix is the prefix used in the provider Registry dispatch key
	// (e.g. "Storage" → "Storage.BucketsInsert").
	ProviderPrefix string

	// Codec is a factory function that returns a new Codec for this service.
	Codec func() adapter.Codec
}

// gcpServices is the authoritative list of GCP services known to JaisCloud.
// GCS uses path-prefix detection; the /v1/projects/{project}/... services use
// segment-based detection (see detectV1Service) since they share the /v1/ prefix.
var gcpServices = []ServiceDescriptor{
	{
		ServiceName:    "storage",
		PathPrefixes:   []string{"/storage/v1/", "/upload/storage/v1/", "/download/storage/v1/"},
		ProviderPrefix: "Storage",
		Codec:          func() adapter.Codec { return &GCSCodec{} },
	},
	{
		ServiceName:    "pubsub",
		ProviderPrefix: "PubSub",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "pubsub"} },
	},
	{
		ServiceName:    "secretmanager",
		ProviderPrefix: "Secret",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "secretmanager"} },
	},
	{
		ServiceName:    "kms",
		ProviderPrefix: "KMS",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "kms"} },
	},
	{
		ServiceName:    "iam",
		ProviderPrefix: "IAM",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "iam"} },
	},
}

// serviceProviderMap maps wire service name → provider registry prefix.
// Built once at init time from gcpServices. Do not modify directly.
var serviceProviderMap map[string]string

func init() {
	serviceProviderMap = make(map[string]string, len(gcpServices))
	for _, svc := range gcpServices {
		serviceProviderMap[svc.ServiceName] = svc.ProviderPrefix
	}
}

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
	switch detectResourceType(seg[pi+2:]) {
	case "topics", "subscriptions":
		return "pubsub"
	case "secrets":
		return "secretmanager"
	case "keyRings", "cryptoKeys":
		return "kms"
	case "serviceAccounts":
		return "iam"
	}
	return ""
}

// DetectionSource indicates how the service was identified.
type DetectionSource int

const (
	SourceUnknown DetectionSource = iota
	SourcePath                    // URL path prefix matched a service
)
