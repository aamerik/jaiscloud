// Package functions is the REST transport for Cloud Functions v1 and v2
// (cloudfunctions.googleapis.com/v1 and /v2). It is a thin adapter: every
// handler resolves the NormalizedRequest params into the shared core's typed
// API (internal/gcp/service/functions) and encodes the result as
// Discovery-shaped JSON. No business logic and no state live here — the REST
// and gRPC transports call the SAME core Service (one store, one Lambda
// executor), so they cannot drift.
//
// Unlike the other /v1/projects/{project}/... services, Cloud Functions is
// detected by the generic JSONCodec{Service: "functions"} plus v2 path
// detection in the adapter router; this package therefore has no Codec of its
// own.
package functions

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Functions REST data plane.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Functions REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Function.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Function.CreateFunction":             p.CreateFunction,
		"Function.GetFunction":                p.GetFunction,
		"Function.ListFunctions":              p.ListFunctions,
		"Function.UpdateFunction":             p.UpdateFunction,
		"Function.DeleteFunction":             p.DeleteFunction,
		"Function.CallFunction":               p.CallFunction,
		"Function.GenerateUploadUrl":          p.GenerateUploadUrl,
		"Function.GenerateDownloadUrl":        p.GenerateDownloadUrl,
		"Function.ListLocations":              p.ListLocations,
		"Function.GetLocation":                p.GetLocation,
		"Function.FunctionGetIamPolicy":       p.FunctionGetIamPolicy,
		"Function.FunctionSetIamPolicy":       p.FunctionSetIamPolicy,
		"Function.FunctionTestIamPermissions": p.FunctionTestIamPermissions,
		"Function.GetOperation":               p.GetOperation,
		"Function.ListOperations":             p.ListOperations,
		"Function.CancelOperation":            p.CancelOperation,
		"Function.DeleteOperation":            p.DeleteOperation,
	}
}

// version returns the wire API version of the request (v2 when the decoded
// path carried an apiVersion of "v2", v1 otherwise).
func (p *Provider) version(nr *model.NormalizedRequest) core.Version {
	return core.VersionFromAPI(strParam(nr, "apiVersion"))
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// --- Function CRUD ---

func (p *Provider) CreateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	v := p.version(nr)
	body := bodyOf(nr)
	_, op, err := p.core.CreateFunction(ctx, project, strParam(nr, "location"), strParam(nr, "functionId"), core.FunctionInputFromMap(body, v), v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	project := p.project(nr)
	f, err := p.core.GetFunction(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.FunctionJSON(p.version(nr), project, f)), nil
}

func (p *Provider) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	v := p.version(nr)
	page, next, err := p.core.ListFunctions(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, f := range page {
		items = append(items, core.FunctionJSON(v, project, f))
	}
	resp := map[string]any{"functions": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	project := p.project(nr)
	v := p.version(nr)
	mask := splitMask(strParam(nr, "updateMask"))
	if len(mask) == 0 {
		mask = splitMask(strParam(nr, "update_mask"))
	}
	_, op, err := p.core.UpdateFunction(ctx, project, location, id, core.FunctionInputFromMap(bodyOf(nr), v), mask, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	project := p.project(nr)
	op, err := p.core.DeleteFunction(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(p.version(nr), project, op)), nil
}

// CallFunction invokes a function synchronously via the core's Lambda executor.
// An executor error is returned in-band (HTTP 200 with an "error" field),
// matching real Cloud Functions.
func (p *Provider) CallFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	executionID, result, invokeErr, err := p.core.CallFunction(ctx, p.project(nr), location, id, bodyString(bodyOf(nr), "data"))
	if err != nil {
		return nil, err
	}
	if invokeErr != "" {
		return provider.OK(map[string]any{"executionId": executionID, "error": invokeErr}), nil
	}
	return provider.OK(map[string]any{"executionId": executionID, "result": result}), nil
}

// GenerateUploadUrl returns a fake signed upload URL for source deployment.
func (p *Provider) GenerateUploadUrl(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"uploadUrl": p.core.GenerateUploadURL(p.project(nr), strParam(nr, "location"))}), nil
}

// GenerateDownloadUrl returns a fake signed download URL for a function's
// source archive. The function must exist (NotFound otherwise).
func (p *Provider) GenerateDownloadUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	url, err := p.core.GenerateDownloadURL(ctx, p.project(nr), location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"downloadUrl": url}), nil
}

// --- Locations ---

func (p *Provider) GetLocation(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, err := p.core.GetLocation(p.project(nr), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	return provider.OK(loc), nil
}

func (p *Provider) ListLocations(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	page, next := p.core.ListLocations(p.project(nr), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	items := make([]any, 0, len(page))
	for _, l := range page {
		items = append(items, l)
	}
	resp := map[string]any{"locations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// --- IAM ---

func (p *Provider) FunctionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, location, id, err := functionNameParts(nr)
	if err != nil {
		return nil, err
	}
	pol, err := p.core.GetIamPolicy(ctx, p.project(nr), location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) FunctionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, location, id, err := functionNameParts(nr)
	if err != nil {
		return nil, err
	}
	pol, err := p.core.SetIamPolicy(ctx, p.project(nr), location, id, bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) FunctionTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, location, id, err := functionNameParts(nr)
	if err != nil {
		return nil, err
	}
	perms, err := p.core.TestIamPermissions(ctx, p.project(nr), location, id, policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// functionNameParts parses the request's function name into project, location,
// and id (project is returned for symmetry; callers use p.project for the
// account/project scope).
func functionNameParts(nr *model.NormalizedRequest) (project, location, id string, err error) {
	name, err := resourceName(nr)
	if err != nil {
		return "", "", "", err
	}
	return core.ParseFunctionName(name)
}

// --- Operations ---

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.GetOperationJSON(p.project(nr), name, p.version(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) ListOperations(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"operations": p.core.ListOperations()}), nil
}

func (p *Provider) CancelOperation(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.core.CancelOperation(name); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DeleteOperation(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.core.DeleteOperation(name); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}
