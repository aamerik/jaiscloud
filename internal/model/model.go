// Package model holds the shared request/response types that flow between the
// gateway, adapter, and provider layers. It has no dependencies on any other
// internal package, which prevents import cycles.
package model

import (
	"fmt"
	"net/http"

	"jaiscloud/internal/clock"
)

// Cloud identifies which cloud provider handled a request.
type Cloud string

const (
	CloudAWS   Cloud = "aws"
	CloudAzure Cloud = "azure"
	CloudGCP   Cloud = "gcp"
)

// NormalizedRequest is the cloud-agnostic representation of an API call.
// Each cloud adapter decodes the wire-format request into this struct.
type NormalizedRequest struct {
	Service string         // "sqs", "s3", ...
	Action  string         // "CreateQueue", "SendMessage", ...
	Params  map[string]any // decoded action-specific parameters

	// Injected by the gateway after codec decode
	Clock     clock.Clock
	Region    string
	AccountID string
	Port      int
	Cloud     Cloud // which cloud adapter handled this request (aws, azure, gcp)

	// ResourceID constructs a cloud-specific resource identifier from an abstract
	// resource type and logical name. Injected by the gateway; providers must call
	// this instead of formatting cloud-specific IDs (ARNs, Azure resource IDs, etc.)
	// directly. Example: nr.ResourceID("dynamodb-table", "my-table")
	ResourceID func(resourceType, name string) string

	// Per-request metadata (e.g. protocol variant)
	meta map[string]string

	// Original HTTP request (for headers, remote addr, etc.)
	Raw *http.Request
}

func (nr *NormalizedRequest) SetMeta(key, value string) {
	if nr.meta == nil {
		nr.meta = make(map[string]string)
	}
	nr.meta[key] = value
}

func (nr *NormalizedRequest) GetMeta(key string) string {
	if nr.meta == nil {
		return ""
	}
	return nr.meta[key]
}

// ProviderResponse is the cloud-agnostic response from a provider operation.
// The codec layer serialises it into the cloud-specific wire format.
type ProviderResponse struct {
	HTTPStatus int
	Data       map[string]any
}

// ProviderError is a structured error returned by a provider operation.
// The codec layer translates it into the cloud-specific wire format.
type ProviderError struct {
	Code       string // canonical code, e.g. "NotFound", "InvalidParameter"
	Message    string
	HTTPStatus int
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.HTTPStatus)
}

func NewProviderError(code, message string, httpStatus int) *ProviderError {
	return &ProviderError{Code: code, Message: message, HTTPStatus: httpStatus}
}
