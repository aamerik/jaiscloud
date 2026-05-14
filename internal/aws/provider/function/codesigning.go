package function

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	resTypeCodeSigning = "lambda_code_signing"
	resTypeFuncCSC     = "lambda_func_csc"
)

type codeSigningConfig struct {
	CodeSigningConfigID  string         `json:"CodeSigningConfigId"`
	CodeSigningConfigArn string         `json:"CodeSigningConfigArn"`
	Description          string         `json:"Description"`
	AllowedPublishers    map[string]any `json:"AllowedPublishers"`
	CodeSigningPolicies  map[string]any `json:"CodeSigningPolicies"`
	LastModified         string         `json:"LastModified"`
}

func newCSCID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return "csc-" + hex.EncodeToString(b)
}

func (p *FunctionProvider) CreateCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := newCSCID()
	arn := nr.ResourceID("lambda-code-signing-config", id)
	csc := codeSigningConfig{
		CodeSigningConfigID:  id,
		CodeSigningConfigArn: arn,
		Description:          strParam(nr.Params, "Description"),
		LastModified:         time.Now().UTC().Format(time.RFC3339),
	}
	if ap, ok := nr.Params["AllowedPublishers"].(map[string]any); ok {
		csc.AllowedPublishers = ap
	}
	if cp, ok := nr.Params["CodeSigningPolicies"].(map[string]any); ok {
		csc.CodeSigningPolicies = cp
	}
	data, _ := json.Marshal(csc)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: resTypeCodeSigning, ID: arn, Data: data}); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"CodeSigningConfig": cscToWire(csc)}}, nil
}

func (p *FunctionProvider) GetCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "CodeSigningConfigArn")
	var csc codeSigningConfig
	if err := p.loadCSC(ctx, arn, &csc); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Code signing config not found: "+arn)
	}
	return provider.OK(map[string]any{"CodeSigningConfig": cscToWire(csc)}), nil
}

func (p *FunctionProvider) UpdateCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "CodeSigningConfigArn")
	var csc codeSigningConfig
	if err := p.loadCSC(ctx, arn, &csc); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Code signing config not found: "+arn)
	}
	if d := strParam(nr.Params, "Description"); d != "" {
		csc.Description = d
	}
	if ap, ok := nr.Params["AllowedPublishers"].(map[string]any); ok {
		csc.AllowedPublishers = ap
	}
	if cp, ok := nr.Params["CodeSigningPolicies"].(map[string]any); ok {
		csc.CodeSigningPolicies = cp
	}
	csc.LastModified = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.Marshal(csc)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeCodeSigning, ID: arn, Data: data})
	return provider.OK(map[string]any{"CodeSigningConfig": cscToWire(csc)}), nil
}

func (p *FunctionProvider) DeleteCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "CodeSigningConfigArn")
	_ = p.resources.Delete(ctx, resTypeCodeSigning, arn)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListCodeSigningConfigs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, resTypeCodeSigning, "")
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var csc codeSigningConfig
		if json.Unmarshal(e.Data, &csc) == nil {
			items = append(items, cscToWire(csc))
		}
	}
	return provider.OK(map[string]any{"CodeSigningConfigs": items}), nil
}

func (p *FunctionProvider) PutFunctionCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	arn := strParam(nr.Params, "CodeSigningConfigArn")
	data, _ := json.Marshal(map[string]string{"CodeSigningConfigArn": arn})
	entry := store.ResourceEntry{Type: resTypeFuncCSC, ID: funcName, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		}
	}
	return provider.OK(map[string]any{"CodeSigningConfigArn": arn, "FunctionName": funcName}), nil
}

func (p *FunctionProvider) GetFunctionCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	e, err := p.resources.Get(ctx, resTypeFuncCSC, funcName)
	if err != nil {
		return provider.OK(map[string]any{"FunctionName": funcName}), nil
	}
	var m map[string]string
	json.Unmarshal(e.Data, &m)
	return provider.OK(map[string]any{"CodeSigningConfigArn": m["CodeSigningConfigArn"], "FunctionName": funcName}), nil
}

func (p *FunctionProvider) DeleteFunctionCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	funcName := extractFunctionName(strParam(nr.Params, "FunctionName"))
	_ = p.resources.Delete(ctx, resTypeFuncCSC, funcName)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListFunctionsByCodeSigningConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "CodeSigningConfigArn")
	entries, _ := p.resources.List(ctx, resTypeFuncCSC, "")
	funcs := make([]string, 0)
	for _, e := range entries {
		var m map[string]string
		if json.Unmarshal(e.Data, &m) == nil && m["CodeSigningConfigArn"] == arn {
			funcs = append(funcs, e.ID)
		}
	}
	return provider.OK(map[string]any{"FunctionArns": funcs}), nil
}

func (p *FunctionProvider) loadCSC(ctx context.Context, arn string, out *codeSigningConfig) error {
	e, err := p.resources.Get(ctx, resTypeCodeSigning, arn)
	if err != nil {
		return err
	}
	return json.Unmarshal(e.Data, out)
}

func cscToWire(csc codeSigningConfig) map[string]any {
	return map[string]any{
		"CodeSigningConfigId":  csc.CodeSigningConfigID,
		"CodeSigningConfigArn": csc.CodeSigningConfigArn,
		"Description":          csc.Description,
		"AllowedPublishers":    csc.AllowedPublishers,
		"CodeSigningPolicies":  csc.CodeSigningPolicies,
		"LastModified":         csc.LastModified,
	}
}
