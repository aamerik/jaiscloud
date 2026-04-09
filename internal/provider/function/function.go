// Package function implements the Lambda provider (FunctionProvider).
// All functions are stored in the ResourceStore; Invoke echoes the payload.
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

const resourceType = "lambda_functions"

// FunctionProvider handles all Lambda operations.
type FunctionProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *FunctionProvider {
	return &FunctionProvider{resources: resources}
}

func (p *FunctionProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Function.CreateFunction":             p.CreateFunction,
		"Function.GetFunction":                p.GetFunction,
		"Function.GetFunctionConfiguration":   p.GetFunctionConfiguration,
		"Function.DeleteFunction":             p.DeleteFunction,
		"Function.ListFunctions":              p.ListFunctions,
		"Function.UpdateFunctionConfiguration": p.UpdateFunctionConfiguration,
		"Function.UpdateFunctionCode":         p.UpdateFunctionCode,
		"Function.InvokeFunction":             p.InvokeFunction,
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type functionConfig struct {
	FunctionName string `json:"FunctionName"`
	FunctionArn  string `json:"FunctionArn"`
	Runtime      string `json:"Runtime"`
	Role         string `json:"Role"`
	Handler      string `json:"Handler"`
	Description  string `json:"Description"`
	Timeout      int    `json:"Timeout"`
	MemorySize   int    `json:"MemorySize"`
	State        string `json:"State"`
	LastModified string `json:"LastModified"`
	RevisionId   string `json:"RevisionId"`
	CodeSize     int64  `json:"CodeSize"`
}

func (p *FunctionProvider) functionARN(nr *model.NormalizedRequest, name string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", nr.Region, nr.AccountID, name)
}

func (p *FunctionProvider) saveConfig(ctx context.Context, cfg functionConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	entry := store.ResourceEntry{Type: resourceType, ID: cfg.FunctionName, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			return p.resources.Update(ctx, entry)
		}
		return err
	}
	return nil
}

func (p *FunctionProvider) loadConfig(ctx context.Context, name string) (functionConfig, error) {
	entry, err := p.resources.Get(ctx, resourceType, name)
	if err != nil {
		return functionConfig{}, err
	}
	var cfg functionConfig
	return cfg, json.Unmarshal(entry.Data, &cfg)
}

// ─── CRUD ─────────────────────────────────────────────────────────────────────

func (p *FunctionProvider) CreateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "FunctionName")
	if name == "" {
		name = strParam(nr.Params, "_function_name")
	}
	if name == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "FunctionName is required", 400)
	}

	runtime := strParam(nr.Params, "Runtime")
	if runtime == "" {
		runtime = "provided"
	}
	timeout := 3
	if t, ok := nr.Params["Timeout"]; ok {
		switch v := t.(type) {
		case float64:
			timeout = int(v)
		case int:
			timeout = v
		}
	}
	memSize := 128
	if m, ok := nr.Params["MemorySize"]; ok {
		switch v := m.(type) {
		case float64:
			memSize = int(v)
		case int:
			memSize = v
		}
	}

	cfg := functionConfig{
		FunctionName: name,
		FunctionArn:  p.functionARN(nr, name),
		Runtime:      runtime,
		Role:         strParam(nr.Params, "Role"),
		Handler:      strParam(nr.Params, "Handler"),
		Description:  strParam(nr.Params, "Description"),
		Timeout:      timeout,
		MemorySize:   memSize,
		State:        "Active",
		LastModified: time.Now().UTC().Format(time.RFC3339),
		RevisionId:   "1",
	}

	if err := p.saveConfig(ctx, cfg); err != nil {
		return nil, model.NewProviderError("ResourceConflictException", "Function already exists", 409)
	}

	var data map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &data)
	return &model.ProviderResponse{HTTPStatus: 201, Data: data}, nil
}

func (p *FunctionProvider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function not found: "+name, 404)
	}
	var cfgMap map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &cfgMap)
	return provider.OK(map[string]any{
		"Configuration": cfgMap,
		"Code":          map[string]any{"Location": ""},
		"Tags":          map[string]any{},
	}), nil
}

func (p *FunctionProvider) GetFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function not found: "+name, 404)
	}
	var data map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &data)
	return provider.OK(data), nil
}

func (p *FunctionProvider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	if err := p.resources.Delete(ctx, resourceType, name); err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function not found: "+name, 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, resourceType, "")
	if err != nil {
		return nil, err
	}
	functions := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var cfg functionConfig
		if json.Unmarshal(e.Data, &cfg) == nil {
			var m map[string]any
			b, _ := json.Marshal(cfg)
			json.Unmarshal(b, &m)
			functions = append(functions, m)
		}
	}
	return provider.OK(map[string]any{"Functions": functions}), nil
}

func (p *FunctionProvider) UpdateFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function not found: "+name, 404)
	}
	if r := strParam(nr.Params, "Role"); r != "" {
		cfg.Role = r
	}
	if h := strParam(nr.Params, "Handler"); h != "" {
		cfg.Handler = h
	}
	if d := strParam(nr.Params, "Description"); d != "" {
		cfg.Description = d
	}
	cfg.LastModified = time.Now().UTC().Format(time.RFC3339)

	if err := p.saveConfig(ctx, cfg); err != nil {
		return nil, err
	}
	var data map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &data)
	return provider.OK(data), nil
}

func (p *FunctionProvider) UpdateFunctionCode(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function not found: "+name, 404)
	}
	cfg.LastModified = time.Now().UTC().Format(time.RFC3339)
	if err := p.saveConfig(ctx, cfg); err != nil {
		return nil, err
	}
	var data map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &data)
	return provider.OK(data), nil
}

// ─── Invoke ───────────────────────────────────────────────────────────────────

// InvokeFunction echoes the payload back — mock Lambda behaviour.
func (p *FunctionProvider) InvokeFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	if _, err := p.loadConfig(ctx, name); err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function not found: "+name, 404)
	}
	payload, _ := nr.Params["_payload"].([]byte)
	return &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"_payload": payload,
		},
	}, nil
}
