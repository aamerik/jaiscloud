// Package sdk defines the JaisCloud plugin contract.
// Plugins are compiled as Go .so files (go build -buildmode=plugin) and loaded
// at runtime by the host using plugin.Open + symbol lookup.
//
// A plugin must export a package-level symbol:
//
//	var Plugin sdk.SparkPlugin = &MyPlugin{}
//
// The host calls Init once at startup, then routes matching requests to Handle.
package sdk

import "context"

// SparkPlugin is the interface every aws-emr-spark compatible plugin must implement.
// The host creates one instance per loaded .so and calls the lifecycle methods in order:
//
//	Init → (Manifest to register routes) → Handle* → Shutdown
//	Reset may be called any time while the plugin is running.
type SparkPlugin interface {
	// Init is called once after the plugin is loaded.
	// The host passes a ResourceManager, ResourceStore, and EventBus so the plugin
	// can register deletion-guard rules, mutate resources, and publish events.
	// Init must return within a reasonable timeout (host may cancel ctx).
	Init(ctx context.Context, rm ResourceManager, store ResourceStore, bus EventBus) error

	// Manifest returns metadata the host uses to register the plugin's routes.
	Manifest() ManifestInfo

	// Handle processes a single request routed to this plugin.
	// Returns a HandleResponse; the host converts it to an HTTP response.
	Handle(ctx context.Context, req HandleRequest) HandleResponse

	// Shutdown is called on graceful server stop. The plugin must stop
	// background goroutines (pollers, watchers) before returning.
	Shutdown(ctx context.Context) error

	// Reset wipes all in-memory plugin state (e.g. MockExecutor jobs).
	// Called from POST /_jaiscloud/reset during integration tests.
	Reset()
}

// ManifestInfo describes the routes a plugin handles.
type ManifestInfo struct {
	// Name is a human-readable plugin identifier, e.g. "aws-emr-spark".
	Name string

	// Version is the plugin's semver string, e.g. "1.0.0".
	Version string

	// Services lists the service names this plugin handles, e.g. ["emr", "emrcontainers"].
	// The host uses this list to route NormalizedRequests to the plugin.
	Services []string
}

// HandleRequest is the plugin-facing view of a normalised cloud API request.
// It mirrors the host's model.NormalizedRequest but uses only stdlib types
// so plugins do not need to import the host module.
type HandleRequest struct {
	// Service is the cloud service name, e.g. "emr".
	Service string

	// Action is the API action name, e.g. "RunJobFlow".
	Action string

	// Params contains all decoded request parameters (query params, JSON body fields).
	Params map[string]any

	// Region is the AWS region from the request, e.g. "us-east-1".
	Region string

	// AccountID is the AWS account ID, e.g. "000000000000".
	AccountID string

	// ResourceID returns a cloud-specific resource identifier (e.g. AWS ARN).
	// The host injects the appropriate formatter; plugins must not call
	// fmt.Sprintf("arn:aws:...") directly.
	// May be nil in unit tests that do not go through the gateway.
	ResourceID func(resourceType, name string) string
}

// HandleResponse is the result returned by a plugin's Handle method.
type HandleResponse struct {
	// HTTPStatus is the HTTP status code to return, e.g. 200.
	// Zero is treated as 200 by the host.
	HTTPStatus int

	// Data is the response body. The host encodes it to XML or JSON
	// using the same codec used to decode the request.
	Data map[string]any

	// Err, if non-nil, causes the host to return an error response.
	// The host reads Err.Code, Err.Message, and Err.HTTPStatus.
	Err *PluginError
}

// PluginError is a structured error returned inside HandleResponse.
type PluginError struct {
	// Code is the cloud-specific error code, e.g. "InvalidRequestException".
	Code string

	// Message is the human-readable error description.
	Message string

	// HTTPStatus is the HTTP status code, e.g. 400.
	HTTPStatus int
}

func (e *PluginError) Error() string { return e.Message }
