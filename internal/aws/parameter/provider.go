package parameter

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ParameterProvider handles SSM Parameter Store API operations.
type ParameterProvider struct {
	store ParameterStore
	kms   model.KeyEncryptor // nil → plaintext (lite/dev mode)
}

// New constructs a ParameterProvider.
func New(store ParameterStore, kms model.KeyEncryptor) *ParameterProvider {
	return &ParameterProvider{store: store, kms: kms}
}

// Routes returns all SSM handler registrations.
func (p *ParameterProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Parameter.PutParameter":           p.PutParameter,
		"Parameter.GetParameter":           p.GetParameter,
		"Parameter.GetParameters":          p.GetParameters,
		"Parameter.GetParametersByPath":    p.GetParametersByPath,
		"Parameter.DeleteParameter":        p.DeleteParameter,
		"Parameter.DeleteParameters":       p.DeleteParameters,
		"Parameter.DescribeParameters":     p.DescribeParameters,
		"Parameter.GetParameterHistory":    p.GetParameterHistory,
		"Parameter.AddTagsToResource":       p.AddTagsToResource,
		"Parameter.RemoveTagsFromResource":  p.RemoveTagsFromResource,
		"Parameter.ListTagsForResource":     p.ListTagsForResource,
		"Parameter.LabelParameterVersion":   p.LabelParameterVersion,
		"Parameter.UnlabelParameterVersion": p.UnlabelParameterVersion,
	}
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (p *ParameterProvider) PutParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "Name is required", 400)
	}
	paramType, _ := nr.Params["Type"].(string)
	if paramType == "" {
		paramType = "String"
	}
	valueStr, _ := nr.Params["Value"].(string)
	desc, _ := nr.Params["Description"].(string)
	kmsKeyID, _ := nr.Params["KeyId"].(string)
	overwrite := false
	if v, ok := nr.Params["Overwrite"].(bool); ok {
		overwrite = v
	}
	tags := extractTags(nr.Params)

	raw, err := p.encryptValue(ctx, paramType, kmsKeyID, name, []byte(valueStr))
	if err != nil {
		return nil, err
	}

	e := ParameterEntry{
		Name:        name,
		Type:        paramType,
		Description: desc,
		KMSKeyID:    kmsKeyID,
		Value:       raw,
		Tags:        tags,
	}
	if err := p.store.PutParameter(ctx, e, overwrite); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, model.NewProviderError("ParameterAlreadyExists",
				"parameter already exists; use Overwrite=true to update: "+name, 400)
		}
		return nil, fmt.Errorf("ssm: put parameter: %w", err)
	}
	return provider.OK(map[string]any{"Version": e.Version, "Tier": "Standard"}), nil
}

func (p *ParameterProvider) GetParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	withDecryption, _ := nr.Params["WithDecryption"].(bool)

	e, err := p.store.GetParameter(ctx, name)
	if errors.Is(err, ErrParameterNotFound) {
		return nil, model.NewProviderError("ParameterNotFound", "parameter not found: "+name, 400)
	}
	if err != nil {
		return nil, fmt.Errorf("ssm: get parameter: %w", err)
	}

	value, err := p.decryptValue(ctx, e, withDecryption)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"Parameter": paramDetail(e, value, nr),
	}), nil
}

func (p *ParameterProvider) GetParameters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names := extractStringList(nr.Params, "Names")
	withDecryption, _ := nr.Params["WithDecryption"].(bool)

	var found []map[string]any
	var invalid []string
	for _, name := range names {
		e, err := p.store.GetParameter(ctx, name)
		if errors.Is(err, ErrParameterNotFound) {
			invalid = append(invalid, name)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("ssm: get parameters: %w", err)
		}
		value, err := p.decryptValue(ctx, e, withDecryption)
		if err != nil {
			return nil, err
		}
		found = append(found, paramDetail(e, value, nr))
	}
	return provider.OK(map[string]any{
		"Parameters":        found,
		"InvalidParameters": invalid,
	}), nil
}

func (p *ParameterProvider) GetParametersByPath(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	path, _ := nr.Params["Path"].(string)
	recursive, _ := nr.Params["Recursive"].(bool)
	withDecryption, _ := nr.Params["WithDecryption"].(bool)

	entries, err := p.store.ListParameters(ctx, path, recursive)
	if err != nil {
		return nil, fmt.Errorf("ssm: get parameters by path: %w", err)
	}
	var items []map[string]any
	for _, e := range entries {
		value, err := p.decryptValue(ctx, e, withDecryption)
		if err != nil {
			return nil, err
		}
		items = append(items, paramDetail(e, value, nr))
	}
	return provider.OK(map[string]any{"Parameters": items}), nil
}

func (p *ParameterProvider) DeleteParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if err := p.store.DeleteParameter(ctx, name); err != nil {
		if errors.Is(err, ErrParameterNotFound) {
			return nil, model.NewProviderError("ParameterNotFound", "parameter not found: "+name, 400)
		}
		return nil, fmt.Errorf("ssm: delete parameter: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ParameterProvider) DeleteParameters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names := extractStringList(nr.Params, "Names")
	var deleted, invalid []string
	for _, name := range names {
		if err := p.store.DeleteParameter(ctx, name); err != nil {
			invalid = append(invalid, name)
		} else {
			deleted = append(deleted, name)
		}
	}
	return provider.OK(map[string]any{
		"DeletedParameters": deleted,
		"InvalidParameters": invalid,
	}), nil
}

func (p *ParameterProvider) DescribeParameters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.store.ListParameters(ctx, "", true)
	if err != nil {
		return nil, fmt.Errorf("ssm: describe parameters: %w", err)
	}
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		items = append(items, map[string]any{
			"Name":        e.Name,
			"Type":        e.Type,
			"Description": e.Description,
			"Version":     e.Version,
		})
	}
	return provider.OK(map[string]any{"Parameters": items}), nil
}

func (p *ParameterProvider) GetParameterHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	history, err := p.store.GetParameterHistory(ctx, name)
	if errors.Is(err, ErrParameterNotFound) {
		return nil, model.NewProviderError("ParameterNotFound", "parameter not found: "+name, 400)
	}
	if err != nil {
		return nil, fmt.Errorf("ssm: get parameter history: %w", err)
	}
	withDecryption, _ := nr.Params["WithDecryption"].(bool)
	items := make([]map[string]any, 0, len(history))
	for _, h := range history {
		e := ParameterEntry{Name: h.Name, Type: h.Type, KMSKeyID: h.KMSKeyID, Value: h.Value}
		value, err := p.decryptValue(ctx, e, withDecryption)
		if err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"Name":    h.Name,
			"Type":    h.Type,
			"Value":   value,
			"Version": h.Version,
		})
	}
	return provider.OK(map[string]any{"Parameters": items}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *ParameterProvider) AddTagsToResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["ResourceId"].(string)
	e, err := p.store.GetParameter(ctx, name)
	if errors.Is(err, ErrParameterNotFound) {
		return nil, model.NewProviderError("InvalidResourceId", "parameter not found: "+name, 400)
	}
	if err != nil {
		return nil, fmt.Errorf("ssm: add tags: %w", err)
	}
	newTags := extractTags(nr.Params)
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	for k, v := range newTags {
		e.Tags[k] = v
	}
	if err := p.store.PutParameter(ctx, e, true); err != nil {
		return nil, fmt.Errorf("ssm: save tags: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ParameterProvider) RemoveTagsFromResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["ResourceId"].(string)
	e, err := p.store.GetParameter(ctx, name)
	if errors.Is(err, ErrParameterNotFound) {
		return nil, model.NewProviderError("InvalidResourceId", "parameter not found: "+name, 400)
	}
	if err != nil {
		return nil, fmt.Errorf("ssm: remove tags: %w", err)
	}
	for _, k := range extractStringList(nr.Params, "TagKeys") {
		delete(e.Tags, k)
	}
	if err := p.store.PutParameter(ctx, e, true); err != nil {
		return nil, fmt.Errorf("ssm: save tags: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ParameterProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["ResourceId"].(string)
	e, err := p.store.GetParameter(ctx, name)
	if errors.Is(err, ErrParameterNotFound) {
		return nil, model.NewProviderError("InvalidResourceId", "parameter not found: "+name, 400)
	}
	if err != nil {
		return nil, fmt.Errorf("ssm: list tags: %w", err)
	}
	tags := make([]map[string]string, 0, len(e.Tags))
	for k, v := range e.Tags {
		tags = append(tags, map[string]string{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"TagList": tags}), nil
}

// ─── Label operations ─────────────────────────────────────────────────────────

func (p *ParameterProvider) LabelParameterVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "Name is required", 400)
	}
	version := int64(0)
	switch v := nr.Params["ParameterVersion"].(type) {
	case float64:
		version = int64(v)
	case int64:
		version = v
	}
	labels := extractStringList(nr.Params, "Labels")
	invalid, err := p.store.LabelParameterVersion(ctx, name, version, labels)
	if errors.Is(err, ErrParameterNotFound) {
		return nil, model.NewProviderError("ParameterNotFound", "parameter not found: "+name, 400)
	}
	if err != nil {
		return nil, fmt.Errorf("ssm: label parameter version: %w", err)
	}
	return provider.OK(map[string]any{
		"InvalidLabels":    invalid,
		"ParameterVersion": version,
	}), nil
}

func (p *ParameterProvider) UnlabelParameterVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "Name is required", 400)
	}
	version := int64(0)
	switch v := nr.Params["ParameterVersion"].(type) {
	case float64:
		version = int64(v)
	case int64:
		version = v
	}
	labels := extractStringList(nr.Params, "Labels")
	if err := p.store.UnlabelParameterVersion(ctx, name, version, labels); err != nil {
		return nil, fmt.Errorf("ssm: unlabel parameter version: %w", err)
	}
	return provider.OK(map[string]any{
		"RemovedLabels":    labels,
		"ParameterVersion": version,
	}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (p *ParameterProvider) encryptValue(ctx context.Context, paramType, kmsKeyID, name string, raw []byte) ([]byte, error) {
	if paramType != "SecureString" || p.kms == nil {
		return raw, nil
	}
	ct, err := p.kms.Encrypt(ctx, kmsKeyID, raw, map[string]string{"ParameterName": name})
	if err != nil {
		return nil, fmt.Errorf("ssm: encrypt parameter: %w", err)
	}
	return ct, nil
}

func (p *ParameterProvider) decryptValue(ctx context.Context, e ParameterEntry, withDecryption bool) (string, error) {
	if e.Type != "SecureString" || !withDecryption || p.kms == nil {
		if e.Type == "SecureString" && !withDecryption {
			return "****", nil // masked
		}
		return string(e.Value), nil
	}
	pt, err := p.kms.Decrypt(ctx, e.KMSKeyID, e.Value, map[string]string{"ParameterName": e.Name})
	if err != nil {
		return "", fmt.Errorf("ssm: decrypt parameter: %w", err)
	}
	return string(pt), nil
}

func paramDetail(e ParameterEntry, value string, nr *model.NormalizedRequest) map[string]any {
	arn := nr.ResourceID(model.RTSSMParameter, strings.TrimPrefix(e.Name, "/"))
	return map[string]any{
		"Name":    e.Name,
		"Type":    e.Type,
		"Value":   value,
		"Version": e.Version,
		"ARN":     arn,
	}
}

func extractTags(params map[string]any) map[string]string {
	raw, ok := params["Tags"]
	if !ok {
		return nil
	}
	out := make(map[string]string)
	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				val, _ := m["Value"].(string)
				if k != "" {
					out[k] = val
				}
			}
		}
	case map[string]any:
		for k, val := range v {
			out[k] = fmt.Sprint(val)
		}
	}
	return out
}

func extractStringList(params map[string]any, key string) []string {
	raw, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		out = append(out, fmt.Sprint(v))
	}
	return out
}
