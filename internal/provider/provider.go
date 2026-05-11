package provider

import (
	"context"
	"errors"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// HandlerFunc is the signature every provider operation must implement.
type HandlerFunc func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

// ServiceDispatcher allows one provider to invoke another provider's handler.
// Used by the Step Functions execution engine to call Lambda, DynamoDB, SQS, etc.
type ServiceDispatcher interface {
	// Dispatch invokes a registered provider action and returns its response data.
	// providerPrefix is the registry prefix (e.g. "Function", "Table", "Queue").
	// action is the operation name (e.g. "Invoke", "PutItem", "SendMessage").
	Dispatch(ctx context.Context, providerPrefix, action string, params map[string]any) (map[string]any, error)
}

// OK is a convenience constructor for a successful 200 response.
func OK(data map[string]any) *model.ProviderResponse {
	return &model.ProviderResponse{HTTPStatus: 200, Data: data}
}

// StoreNotFoundError returns a 400 ProviderError when the store reports the
// resource is absent.  For any other error (including storage unavailability)
// it returns the error unchanged so the gateway can emit a 500.
//
// Usage:
//
//	entry, err := p.resources.Get(ctx, rt, id)
//	if err != nil {
//	    return nil, provider.StoreNotFoundError(err, "NotFound", "thing does not exist")
//	}
func StoreNotFoundError(err error, code, message string) error {
	if errors.Is(err, store.ErrNotFound) {
		return model.NewProviderError(code, message, 400)
	}
	return err // non-NotFound errors (e.g. ErrStorageUnavailable) become 500 at the gateway
}
