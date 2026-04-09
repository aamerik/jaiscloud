package provider

import (
	"context"
	"fmt"
	"jaiscloud/internal/model"
)

// Registry dispatches NormalizedRequests to the registered HandlerFunc.
// Key format: "ProviderPrefix.ActionName" e.g. "Queue.SendMessage".
type Registry struct {
	handlers map[string]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

// RegisterAll bulk-registers a provider's route map.
func (r *Registry) RegisterAll(routes map[string]HandlerFunc) {
	for k, v := range routes {
		r.handlers[k] = v
	}
}

// Dispatch routes a request to the matching handler.
// Returns a ProviderError (wrapped as error) if the handler is not found.
func (r *Registry) Dispatch(ctx context.Context, key string, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	h, ok := r.handlers[key]
	if !ok {
		return nil, model.NewProviderError("UnknownAction", fmt.Sprintf("no handler for %q", key), 400)
	}
	return h(ctx, nr)
}
