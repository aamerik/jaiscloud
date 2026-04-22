// Package function implements the Lambda provider (FunctionProvider).
// All functions are stored in the ResourceStore; Invoke echoes the payload.
package function

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const resourceType = "lambda_functions"

// FunctionProvider handles all Lambda operations.
type FunctionProvider struct {
	resources store.ResourceStore
	executor  lambdaexec.LambdaExecutor // nil → MockExecutor behaviour (echo)
}

// New constructs a FunctionProvider with a mock (echo) executor.
func New(resources store.ResourceStore) *FunctionProvider {
	return &FunctionProvider{resources: resources, executor: &lambdaexec.MockExecutor{}}
}

// NewWithExecutor constructs a FunctionProvider with the given executor.
func NewWithExecutor(resources store.ResourceStore, exec lambdaexec.LambdaExecutor) *FunctionProvider {
	return &FunctionProvider{resources: resources, executor: exec}
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
	FunctionName string            `json:"FunctionName"`
	FunctionArn  string            `json:"FunctionArn"`
	Runtime      string            `json:"Runtime"`
	Role         string            `json:"Role"`
	Handler      string            `json:"Handler"`
	Description  string            `json:"Description"`
	Timeout      int               `json:"Timeout"`
	MemorySize   int               `json:"MemorySize"`
	State        string            `json:"State"`
	LastModified string            `json:"LastModified"`
	RevisionId   string            `json:"RevisionId"`
	CodeSize     int64             `json:"CodeSize"`
	Environment  map[string]string `json:"Environment,omitempty"`
}

// parseEnvVars extracts Environment.Variables from the Lambda request params.
// AWS SDK sends: {"Environment": {"Variables": {"KEY": "val"}}}
func parseEnvVars(params map[string]any) map[string]string {
	env, ok := params["Environment"].(map[string]any)
	if !ok {
		return nil
	}
	vars, ok := env["Variables"].(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]string, len(vars))
	for k, v := range vars {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}

func (p *FunctionProvider) functionARN(nr *model.NormalizedRequest, name string) string {
	return nr.ResourceID(model.RTLambdaFunction, name)
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
		Environment:  parseEnvVars(nr.Params),
	}

	if err := p.saveConfig(ctx, cfg); err != nil {
		return nil, model.NewProviderError("ResourceConflictException", "Function already exists", 409)
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: cfgToWire(cfg)}, nil
}

func (p *FunctionProvider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	return provider.OK(map[string]any{
		"Configuration": cfgToWire(cfg),
		"Code":          map[string]any{"Location": ""},
		"Tags":          map[string]any{},
	}), nil
}

func (p *FunctionProvider) GetFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	return provider.OK(cfgToWire(cfg)), nil
}

// cfgToWire converts a functionConfig to the wire map, shaping Environment as
// {"Variables": {...}} so the AWS SDK deserialises it correctly.
func cfgToWire(cfg functionConfig) map[string]any {
	var m map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &m)
	// Reshape Environment: stored as flat map, SDK expects {"Variables":{...}}
	if cfg.Environment != nil {
		m["Environment"] = map[string]any{"Variables": cfg.Environment}
	}
	return m
}

func (p *FunctionProvider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	if err := p.resources.Delete(ctx, resourceType, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	p.executor.DeleteFunction(ctx, name)
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
			functions = append(functions, cfgToWire(cfg))
		}
	}
	return provider.OK(map[string]any{"Functions": functions}), nil
}

func (p *FunctionProvider) UpdateFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
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
	if env := parseEnvVars(nr.Params); env != nil {
		cfg.Environment = env
	}
	cfg.LastModified = time.Now().UTC().Format(time.RFC3339)

	if err := p.saveConfig(ctx, cfg); err != nil {
		return nil, err
	}
	return provider.OK(cfgToWire(cfg)), nil
}

func (p *FunctionProvider) UpdateFunctionCode(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	cfg.LastModified = time.Now().UTC().Format(time.RFC3339)
	if err := p.saveConfig(ctx, cfg); err != nil {
		return nil, err
	}
	return provider.OK(cfgToWire(cfg)), nil
}

// ─── Invoke ───────────────────────────────────────────────────────────────────

// InvokeFunction invokes the function via the configured executor.
// When InvocationType=Event (async), returns 202 without waiting for a result.
func (p *FunctionProvider) InvokeFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "_function_name")
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	if strings.EqualFold(strParam(nr.Params, "_invocation_type"), "Event") {
		return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{}}, nil
	}
	payload, _ := nr.Params["_payload"].([]byte)
	req := lambdaexec.InvokeRequest{
		FunctionName: cfg.FunctionName,
		Runtime:      cfg.Runtime,
		Handler:      cfg.Handler,
		MemoryMB:     cfg.MemorySize,
		TimeoutSecs:  cfg.Timeout,
		EnvVars:      cfg.Environment,
		Payload:      payload,
	}
	result, err := p.executor.Invoke(ctx, req)
	if err != nil {
		return nil, model.NewProviderError("ServiceException", "invocation failed: "+err.Error(), 500)
	}
	return &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"_payload": result},
	}, nil
}
