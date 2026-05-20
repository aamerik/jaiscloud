package provider

import (
	"context"
	"fmt"
	"log/slog"

	"jaiscloud/internal/model"
)

// Registry dispatches NormalizedRequests to the registered HandlerFunc.
// Key format: "ProviderPrefix.ActionName" e.g. "Queue.SendMessage".
//
// Plugins register a wildcard handler via RegisterPlugin("EMR", fn).
// When Dispatch looks up "EMR.RunJobFlow" and finds no exact match,
// it falls back to the wildcard handler registered for prefix "EMR".
type Registry struct {
	handlers map[string]HandlerFunc
	plugins  map[string]HandlerFunc // prefix → wildcard handler (plugin fallback)
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
		plugins:  make(map[string]HandlerFunc),
	}
}

// RegisterAll bulk-registers a route map. Returns the registry for chaining.
func (r *Registry) RegisterAll(routes map[string]HandlerFunc) *Registry {
	for k, v := range routes {
		r.handlers[k] = v
	}
	return r
}

// Register registers all routes returned by p.Routes(). Returns the registry
// for chaining so callers can write:
//
//	registry := provider.NewRegistry().
//	    Register(keyProv).
//	    Register(funcP).
//	    Register(queueP)
//
// Use RegisterAll directly only when registering a bare route map that is not
// the primary Routes() return of a provider (e.g., tableProvider.StreamRoutes()).
func (r *Registry) Register(p Provider) *Registry {
	return r.RegisterAll(p.Routes())
}

// RegisterPlugin registers a plugin as the wildcard handler for a provider prefix.
// When Dispatch finds no exact key match, it tries the plugin handler for the prefix.
// A plugin handler takes precedence over the built-in wildcard fallback.
func (r *Registry) RegisterPlugin(prefix string, h HandlerFunc) {
	// Override any built-in exact-match handlers whose prefix matches,
	// so the plugin receives all actions for this service.
	for key := range r.handlers {
		p := key
		for i, c := range key {
			if c == '.' {
				p = key[:i]
				break
			}
		}
		if p == prefix {
			r.handlers[key] = h
		}
	}
	r.plugins[prefix] = h
}

// Dispatch routes a request to the matching handler.
// Lookup order:
//  1. Exact match in handlers (built-in providers).
//  2. Plugin wildcard for the provider prefix (full-mode plugins).
//  3. ProviderError(UnknownAction).
func (r *Registry) Dispatch(ctx context.Context, key string, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if h, ok := r.handlers[key]; ok {
		resp, err := h(ctx, nr)
		if err != nil {
			if _, isProvider := err.(*model.ProviderError); !isProvider {
				slog.Error("provider handler error",
					"key", key,
					"err", err,
				)
			}
		}
		return resp, err
	}

	// Extract prefix from "Prefix.Action"
	prefix := key
	for i, c := range key {
		if c == '.' {
			prefix = key[:i]
			break
		}
	}
	if h, ok := r.plugins[prefix]; ok {
		resp, err := h(ctx, nr)
		if err != nil {
			if _, isProvider := err.(*model.ProviderError); !isProvider {
				slog.Error("plugin handler error",
					"key", key,
					"prefix", prefix,
					"err", err,
				)
			}
		}
		return resp, err
	}

	slog.Error("no handler registered",
		"key", key,
		"service", nr.Service,
		"action", nr.Action,
	)
	return nil, model.NewProviderError("UnknownAction", fmt.Sprintf("no handler for %q", key), 400)
}
