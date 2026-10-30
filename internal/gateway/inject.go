package gateway

import (
	"context"
	"net/http"

	"jaiscloud/internal/model"
)

// AccountObserver is an optional interface a CloudAdapter may implement to be
// told, for every request it serves, which account (AWS) or project (GCP) and
// which service were addressed. It follows the same optional-capability pattern
// as RequestFilter: the gateway type-asserts the adapter, so a cloud that does
// not implement it is unaffected.
//
// GCP uses it for console project discovery (internal/gcp/service/
// resourcemanager): a client that writes to a project the console never
// registered would otherwise leave no trace, and the operator could not find
// where the workload's resources went.
type AccountObserver interface {
	// ObserveAccount records a served request. It runs after EnrichRequest, so
	// the account/project and service are already resolved on nr.
	ObserveAccount(ctx context.Context, account, service string)
}

// RequestFilter is an optional interface a CloudAdapter may implement to refuse
// requests before dispatch and to adjust the encoded error response. It follows
// the same optional-capability pattern as BatchHandler: the gateway type-asserts
// the adapter, so a cloud that does not implement it is completely unaffected.
//
// GCP uses it for the opt-in throttle/quota injector (internal/gcp/throttle):
// FilterRequest returns a retryable error for matching requests, and
// DecorateError attaches the service-appropriate retry hints to that response.
type RequestFilter interface {
	// FilterRequest returns a non-nil ProviderError to fail the request before
	// it reaches the registry, or nil to allow it. It runs after EnrichRequest,
	// so identity (e.g. the GCP project) is available on nr.
	FilterRequest(nr *model.NormalizedRequest) *model.ProviderError

	// DecorateError adjusts the response encoded for a ProviderError returned by
	// FilterRequest (for example to add a Retry-After header and a
	// google.rpc.RetryInfo detail). It returns the final status, headers, body.
	DecorateError(nr *model.NormalizedRequest, perr *model.ProviderError, status int, headers http.Header, body []byte) (int, http.Header, []byte)
}
