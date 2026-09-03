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
// Add one entry here when wiring in a new service — nothing else needs changing.
var gcpServices = []ServiceDescriptor{
	{
		ServiceName:    "storage",
		PathPrefixes:   []string{"/storage/v1/", "/upload/storage/v1/", "/download/storage/v1/"},
		ProviderPrefix: "Storage",
		Codec:          func() adapter.Codec { return &GCSCodec{} },
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

// DetectionSource indicates how the service was identified.
type DetectionSource int

const (
	SourceUnknown DetectionSource = iota
	SourcePath                    // URL path prefix matched a service
)
