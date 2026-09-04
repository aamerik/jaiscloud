package gcp

import (
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
