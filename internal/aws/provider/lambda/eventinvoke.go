package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const resTypeEventInvoke = "lambda_event_invoke_cfg"

type eventInvokeConfig struct {
	FunctionName             string         `json:"FunctionName"`
	Qualifier                string         `json:"Qualifier"`
	MaximumRetryAttempts     int            `json:"MaximumRetryAttempts"`
	MaximumEventAgeInSeconds int            `json:"MaximumEventAgeInSeconds"`
	DestinationConfig        map[string]any `json:"DestinationConfig,omitempty"`
	LastModified             time.Time      `json:"LastModified"`
}

func eiKey(funcName, qualifier string) string {
	if qualifier == "" {
		qualifier = "$LATEST"
	}
	return fmt.Sprintf("%s:%s", funcName, qualifier)
}

// unmarshal populates the eventInvokeConfig from raw JSON bytes.
func (c *eventInvokeConfig) unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

func (p *FunctionProvider) PutFunctionEventInvokeConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	qualifier := strParam(nr.Params, "Qualifier")
	cfg := eventInvokeConfig{
		FunctionName: funcName,
		Qualifier:    qualifier,
		LastModified: clock.Now(),
	}
	if v, ok := nr.Params["MaximumRetryAttempts"].(float64); ok {
		cfg.MaximumRetryAttempts = int(v)
	}
	if v, ok := nr.Params["MaximumEventAgeInSeconds"].(float64); ok {
		cfg.MaximumEventAgeInSeconds = int(v)
	}
	if d, ok := nr.Params["DestinationConfig"].(map[string]any); ok {
		cfg.DestinationConfig = d
	}
	data, _ := json.Marshal(cfg)
	entry := store.ResourceEntry{Type: resTypeEventInvoke, ID: eiKey(funcName, qualifier), Data: data}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(eiToWire(cfg)), nil
}

func (p *FunctionProvider) GetFunctionEventInvokeConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	qualifier := strParam(nr.Params, "Qualifier")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, resTypeEventInvoke, eiKey(funcName, qualifier))
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Event invoke config not found")
	}
	var cfg eventInvokeConfig
	json.Unmarshal(e.Data, &cfg)
	return provider.OK(eiToWire(cfg)), nil
}

func (p *FunctionProvider) UpdateFunctionEventInvokeConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// UpdateFunctionEventInvokeConfig has same semantics as Put for our purposes.
	return p.PutFunctionEventInvokeConfig(ctx, nr)
}

func (p *FunctionProvider) DeleteFunctionEventInvokeConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	qualifier := strParam(nr.Params, "Qualifier")
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, resTypeEventInvoke, eiKey(funcName, qualifier))
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListFunctionEventInvokeConfigs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeEventInvoke, funcName+":")
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var cfg eventInvokeConfig
		if json.Unmarshal(e.Data, &cfg) == nil {
			items = append(items, eiToWire(cfg))
		}
	}
	return provider.OK(map[string]any{"FunctionEventInvokeConfigs": items}), nil
}

func eiToWire(cfg eventInvokeConfig) map[string]any {
	return map[string]any{
		"FunctionArn":              cfg.FunctionName,
		"MaximumRetryAttempts":     cfg.MaximumRetryAttempts,
		"MaximumEventAgeInSeconds": cfg.MaximumEventAgeInSeconds,
		"DestinationConfig":        cfg.DestinationConfig,
		"LastModified":             float64(cfg.LastModified.UnixMilli()) / 1000.0,
	}
}
