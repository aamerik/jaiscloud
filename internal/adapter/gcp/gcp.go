// Package gcp provides a stub CloudAdapter for Google Cloud API requests.
// Detection heuristics: x-goog-* headers or URL paths starting with /v1/projects/.
//
// Full decoding is not yet implemented — the foundation is in place for Phase 6 work.
package gcp

import (
	"net/http"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// GCPAdapter detects Google Cloud API requests.
// Implements adapter.CloudAdapter.
type GCPAdapter struct{}

// New returns a new GCPAdapter.
func New() *GCPAdapter { return &GCPAdapter{} }

// Cloud implements adapter.CloudAdapter.
func (a *GCPAdapter) Cloud() model.Cloud { return model.CloudGCP }

// ServiceToProvider implements adapter.CloudAdapter.
// Stub: GCP wire service names are not yet mapped; returns the service name unchanged.
func (a *GCPAdapter) ServiceToProvider(service string) string { return service }

// DetectAndDecode implements adapter.CloudAdapter.
// Not yet fully implemented — returns UnsupportedOperation.
func (a *GCPAdapter) DetectAndDecode(_ *http.Request, _ []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	return nil, nil, model.NewProviderError(
		"UnsupportedOperation",
		"GCP adapter not yet implemented",
		501,
	)
}
