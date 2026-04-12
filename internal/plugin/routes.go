package plugin

import (
	"context"
	"net/http"

	sdk "github.com/jaiscloud/plugin-sdk"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// registerPluginRoutes registers the plugin as the handler for every action
// under the given service. The provider key format is "ProviderPrefix.Action".
//
// Since a plugin handles multiple actions internally, we register a wildcard
// by overriding the registry's catch-all for the service prefix.
// The gateway calls registry.Dispatch("EMR.RunJobFlow", nr); we register
// "EMR.*" by inserting a per-action wrapper that delegates to plugin.Handle.
//
// In practice the plugin is registered as the handler for the service provider
// prefix via a special catch-all key. The Registry's Dispatch already tries
// exact match; we use the same key format "Prefix.Action" but defer action
// routing to the plugin itself.
//
// We register a single catch-all handler per service that wraps plugin.Handle.
// The registry recognises the special key "EMR.*" and falls through to it when
// no exact key matches.
func registerPluginRoutes(registry *provider.Registry, service string, sp sdk.SparkPlugin) {
	prefix := serviceToProviderPrefix(service)

	// Register a wildcard handler for this provider prefix.
	// provider.Registry.Dispatch is exact-match; to support wildcard fallback
	// we register the plugin handler for the wildcard sentinel key.
	registry.RegisterPlugin(prefix, func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		req := sdk.HandleRequest{
			Service:    nr.Service,
			Action:     nr.Action,
			Params:     nr.Params,
			Region:     nr.Region,
			AccountID:  nr.AccountID,
			ResourceID: nr.ResourceID,
		}
		resp := sp.Handle(ctx, req)
		if resp.Err != nil {
			return nil, model.NewProviderError(resp.Err.Code, resp.Err.Message, resp.Err.HTTPStatus)
		}
		status := resp.HTTPStatus
		if status == 0 {
			status = http.StatusOK
		}
		return &model.ProviderResponse{HTTPStatus: status, Data: resp.Data}, nil
	})
}

// serviceToProviderPrefix maps a service name (as used in ManifestInfo.Services)
// to the provider prefix used as the registry key prefix (e.g. "emr" → "EMR").
func serviceToProviderPrefix(service string) string {
	switch service {
	case "emr":
		return "EMR"
	case "emrcontainers", "emr-containers":
		return "EMRContainers"
	case "sqs":
		return "Queue"
	case "sns":
		return "Notification"
	case "dynamodb":
		return "Table"
	case "s3":
		return "Object"
	case "lambda":
		return "Function"
	default:
		return service
	}
}
