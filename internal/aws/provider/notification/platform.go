package notification

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtPlatformApp      = "sns_platform_app"
	rtPlatformEndpoint = "sns_platform_endpoint"
)

type platformApp struct {
	PlatformApplicationArn string            `json:"PlatformApplicationArn"`
	Platform               string            `json:"Platform"`
	Name                   string            `json:"Name"`
	Attributes             map[string]string `json:"Attributes"`
}

type platformEndpoint struct {
	EndpointArn            string            `json:"EndpointArn"`
	PlatformApplicationArn string            `json:"PlatformApplicationArn"`
	Token                  string            `json:"Token"`
	Attributes             map[string]string `json:"Attributes"`
}

func (p *SNSProvider) CreatePlatformApplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	platform := strParam(nr.Params, "Platform")
	name := strParam(nr.Params, "Name")
	arn := nr.ResourceID("sns-platform-app", platform+"/"+name)
	attrs := map[string]string{}
	if m, ok := nr.Params["Attributes"].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				attrs[k] = s
			}
		}
	}
	app := platformApp{PlatformApplicationArn: arn, Platform: platform, Name: name, Attributes: attrs}
	if err := saveEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformApp, arn, app); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"PlatformApplicationArn": arn}), nil
}

func (p *SNSProvider) GetPlatformApplicationAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PlatformApplicationArn")
	var app platformApp
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformApp, arn, &app); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Platform application not found")
	}
	return provider.OK(map[string]any{"Attributes": app.Attributes}), nil
}

func (p *SNSProvider) SetPlatformApplicationAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PlatformApplicationArn")
	var app platformApp
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformApp, arn, &app); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Platform application not found")
	}
	if m, ok := nr.Params["Attributes"].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				app.Attributes[k] = s
			}
		}
	}
	return provider.OK(map[string]any{}), saveEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformApp, arn, app)
}

func (p *SNSProvider) DeletePlatformApplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PlatformApplicationArn")
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPlatformApp, arn)
	return provider.OK(map[string]any{}), nil
}

func (p *SNSProvider) ListPlatformApplications(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtPlatformApp, "")
	apps := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var app platformApp
		if json.Unmarshal(e.Data, &app) == nil {
			apps = append(apps, map[string]any{
				"PlatformApplicationArn": app.PlatformApplicationArn,
				"Attributes":             app.Attributes,
			})
		}
	}
	return provider.OK(map[string]any{"PlatformApplications": apps}), nil
}

func (p *SNSProvider) CreatePlatformEndpoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	appArn := strParam(nr.Params, "PlatformApplicationArn")
	token := strParam(nr.Params, "Token")
	endpointArn := nr.ResourceID("sns-platform-endpoint", appArn+"/"+token)
	attrs := map[string]string{"Enabled": "true", "Token": token}
	if m, ok := nr.Params["Attributes"].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				attrs[k] = s
			}
		}
	}
	ep := platformEndpoint{
		EndpointArn:            endpointArn,
		PlatformApplicationArn: appArn,
		Token:                  token,
		Attributes:             attrs,
	}
	entry := store.ResourceEntry{Type: rtPlatformEndpoint, ID: endpointArn}
	data, _ := json.Marshal(ep)
	entry.Data = data
	if err := p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"EndpointArn": endpointArn}), nil
}

func (p *SNSProvider) GetEndpointAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "EndpointArn")
	var ep platformEndpoint
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformEndpoint, arn, &ep); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Endpoint not found")
	}
	return provider.OK(map[string]any{"Attributes": ep.Attributes}), nil
}

func (p *SNSProvider) SetEndpointAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "EndpointArn")
	var ep platformEndpoint
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformEndpoint, arn, &ep); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Endpoint not found")
	}
	if m, ok := nr.Params["Attributes"].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				ep.Attributes[k] = s
			}
		}
	}
	return provider.OK(map[string]any{}), saveEntry(ctx, p.resources, nr.AccountID, nr.Region, rtPlatformEndpoint, arn, ep)
}

func (p *SNSProvider) DeleteEndpoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "EndpointArn")
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPlatformEndpoint, arn)
	return provider.OK(map[string]any{}), nil
}

func (p *SNSProvider) ListEndpointsByPlatformApplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	appArn := strParam(nr.Params, "PlatformApplicationArn")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtPlatformEndpoint, "")
	eps := make([]map[string]any, 0)
	for _, e := range entries {
		var ep platformEndpoint
		if json.Unmarshal(e.Data, &ep) == nil && ep.PlatformApplicationArn == appArn {
			eps = append(eps, map[string]any{
				"EndpointArn": ep.EndpointArn,
				"Attributes":  ep.Attributes,
			})
		}
	}
	return provider.OK(map[string]any{"Endpoints": eps}), nil
}
