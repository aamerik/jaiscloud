// Package azure provides a stub CloudAdapter for Azure ARM/REST requests.
// Detection heuristics: Authorization header with "Bearer" + Azure-specific headers,
// or URL paths starting with /subscriptions/.
//
// Full decoding is not yet implemented — the foundation is in place for Phase 6 work.
package azure

import (
	"net/http"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// AzureAdapter detects Azure ARM/REST requests.
// Implements adapter.CloudAdapter.
type AzureAdapter struct{}

// New returns a new AzureAdapter.
func New() *AzureAdapter { return &AzureAdapter{} }

// Cloud implements adapter.CloudAdapter.
func (a *AzureAdapter) Cloud() model.Cloud { return model.CloudAzure }

// ServiceToProvider implements adapter.CloudAdapter.
// Stub: Azure wire service names are not yet mapped; returns the service name unchanged.
func (a *AzureAdapter) ServiceToProvider(service string) string { return service }

// DetectAndDecode implements adapter.CloudAdapter.
// Not yet fully implemented — returns UnsupportedOperation.
func (a *AzureAdapter) DetectAndDecode(_ *http.Request, _ []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	return nil, nil, model.NewProviderError(
		"UnsupportedOperation",
		"Azure adapter not yet implemented",
		501,
	)
}

// EnrichRequest implements adapter.CloudAdapter.
// Stub: returns the config-level defaults unchanged until Azure identity parsing is implemented.
func (a *AzureAdapter) EnrichRequest(_ *http.Request, defaultRegion, defaultAccountID string) (region, accountID, accessKey string) {
	return defaultRegion, defaultAccountID, ""
}

// ResourceIDFor implements adapter.CloudAdapter.
// Stub: returns name unchanged until Azure resource ID formatting is implemented.
func (a *AzureAdapter) ResourceIDFor(_, _ string) func(resourceType, name string) string {
	return func(_, name string) string { return name }
}
