package serviceusage

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/serviceusage"
)

// Provider handles the Service Usage v1 REST data plane. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Service Usage REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "ServiceUsage.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ServiceUsage.ServicesList":        p.ListServices,
		"ServiceUsage.ServicesGet":         p.GetService,
		"ServiceUsage.ServicesBatchEnable": p.BatchEnableServices,
		"ServiceUsage.ServicesEnable":      p.EnableService,
		"ServiceUsage.ServicesDisable":     p.DisableService,
	}
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

func (p *Provider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filter, err := core.ParseFilter(strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListAPIs(ctx, p.project(nr), filter, intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, a := range page {
		items = append(items, serviceToJSON(a))
	}
	resp := map[string]any{"services": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	api, err := p.core.GetAPI(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(serviceToJSON(api)), nil
}

func (p *Provider) EnableService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	api, op, err := p.core.EnableAPI(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op, typedResponse(enableResponseType,
		map[string]any{"service": serviceToJSON(api)}))), nil
}

func (p *Provider) DisableService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	api, op, err := p.core.DisableAPI(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op, typedResponse(disableResponseType,
		map[string]any{"service": serviceToJSON(api)}))), nil
}

func (p *Provider) BatchEnableServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apis, op, err := p.core.BatchEnableAPIs(ctx, p.project(nr), stringSlice(bodyOf(nr)["serviceIds"]))
	if err != nil {
		return nil, err
	}
	services := make([]any, 0, len(apis))
	for _, a := range apis {
		services = append(services, serviceToJSON(a))
	}
	return provider.OK(operationToJSON(op, typedResponse(batchEnableRespType,
		map[string]any{"services": services}))), nil
}
