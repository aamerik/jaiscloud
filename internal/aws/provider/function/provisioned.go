package function

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const resTypeProvisioned = "lambda_provisioned"

type provisionedEntry struct {
	FunctionName                    string `json:"FunctionName"`
	Qualifier                       string `json:"Qualifier"`
	RequestedProvisionedConcurrentExecutions int `json:"RequestedProvisionedConcurrentExecutions"`
	AllocatedProvisionedConcurrentExecutions int `json:"AllocatedProvisionedConcurrentExecutions"`
	AvailableProvisionedConcurrentExecutions int `json:"AvailableProvisionedConcurrentExecutions"`
	StatusReason                    string `json:"StatusReason"`
	Status                          string `json:"Status"`
	LastModified                    string `json:"LastModified"`
}

func (p *FunctionProvider) PutProvisionedConcurrencyConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	qualifier := strParam(nr.Params, "Qualifier")
	requested := 0
	if v, ok := nr.Params["ProvisionedConcurrentExecutions"].(float64); ok {
		requested = int(v)
	}
	key := fmt.Sprintf("%s:%s", funcName, qualifier)
	pe := provisionedEntry{
		FunctionName:                    funcName,
		Qualifier:                       qualifier,
		RequestedProvisionedConcurrentExecutions: requested,
		AllocatedProvisionedConcurrentExecutions: requested,
		AvailableProvisionedConcurrentExecutions: requested,
		Status:       "READY",
		LastModified: time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(pe)
	entry := store.ResourceEntry{Type: resTypeProvisioned, ID: key, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		}
	}
	return &model.ProviderResponse{HTTPStatus: 202, Data: provisionedToWire(pe)}, nil
}

func (p *FunctionProvider) GetProvisionedConcurrencyConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	qualifier := strParam(nr.Params, "Qualifier")
	key := fmt.Sprintf("%s:%s", funcName, qualifier)
	e, err := p.resources.Get(ctx, resTypeProvisioned, key)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ProvisionedConcurrencyConfigNotFoundException", "No provisioned concurrency config for "+key)
	}
	var pe provisionedEntry
	json.Unmarshal(e.Data, &pe)
	return provider.OK(provisionedToWire(pe)), nil
}

func (p *FunctionProvider) DeleteProvisionedConcurrencyConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	qualifier := strParam(nr.Params, "Qualifier")
	key := fmt.Sprintf("%s:%s", funcName, qualifier)
	_ = p.resources.Delete(ctx, resTypeProvisioned, key)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListProvisionedConcurrencyConfigs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	entries, _ := p.resources.List(ctx, resTypeProvisioned, funcName+":")
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var pe provisionedEntry
		if json.Unmarshal(e.Data, &pe) == nil {
			items = append(items, provisionedToWire(pe))
		}
	}
	return provider.OK(map[string]any{"ProvisionedConcurrencyConfigs": items}), nil
}

func provisionedToWire(pe provisionedEntry) map[string]any {
	return map[string]any{
		"RequestedProvisionedConcurrentExecutions": pe.RequestedProvisionedConcurrentExecutions,
		"AllocatedProvisionedConcurrentExecutions": pe.AllocatedProvisionedConcurrentExecutions,
		"AvailableProvisionedConcurrentExecutions": pe.AvailableProvisionedConcurrentExecutions,
		"Status":       pe.Status,
		"StatusReason": pe.StatusReason,
		"LastModified": pe.LastModified,
	}
}
