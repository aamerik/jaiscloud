package adapter

import (
	"net/http"

	"jaiscloud/internal/model"
)

// Codec handles encode/decode for one AWS service (e.g. SQS).
type Codec interface {
	ServiceName() string
	Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error)
	Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (status int, headers http.Header, body []byte)
	EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (status int, headers http.Header, body []byte)
}

// CloudAdapter is implemented by each cloud-specific adapter (AWS, Azure, GCP).
// The adapter to use is chosen once at startup from Config.Cloud.
type CloudAdapter interface {
	// Cloud returns the identifier for this adapter (aws, azure, gcp).
	Cloud() model.Cloud

	// DetectAndDecode identifies the service, selects the codec, and decodes the request.
	DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, Codec, error)

	// ServiceToProvider maps a wire service name (e.g. "sqs") to the provider registry
	// prefix (e.g. "Queue") used to build the dispatch key "Queue.CreateQueue".
	// Each cloud adapter owns this mapping for its own wire protocol.
	// Returns the service name unchanged if no mapping is found.
	ServiceToProvider(service string) string

	// EnrichRequest extracts per-request identity (region, accountID, accessKey) from r.
	// defaultRegion and defaultAccountID are the config-level fallbacks used when the
	// request carries no credential (e.g. anonymous access or stub clouds).
	EnrichRequest(r *http.Request, defaultRegion, defaultAccountID string) (region, accountID, accessKey string)

	// ResourceIDFor returns a function that formats cloud-specific resource identifiers
	// (ARNs for AWS, resource paths for Azure/GCP) for the given region and account.
	// Inject the returned function into NormalizedRequest.ResourceID at the gateway.
	ResourceIDFor(region, accountID string) func(resourceType, name string) string
}
