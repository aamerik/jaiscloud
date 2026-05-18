package parameter

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
)

// ─── Cross-service interfaces ─────────────────────────────────────────────────

// ParameterEventPublisher is the narrow interface used to emit SSM change events
// into EventBridge without creating an import cycle.
type ParameterEventPublisher interface {
	InternalPutEvents(ctx context.Context, entries []map[string]any) error
}

// SecretValueGetter is the narrow interface used to resolve
// /aws/reference/secretsmanager/ references without creating an import cycle.
type SecretValueGetter interface {
	InternalGetSecretValue(ctx context.Context, secretID string) (string, error)
}

// ─── Provider ─────────────────────────────────────────────────────────────────

// ParameterProvider handles SSM Parameter Store API operations.
type ParameterProvider struct {
	store        ParameterStore
	kms          model.KeyEncryptor // nil → plaintext (lite/dev mode)
	eventPub     ParameterEventPublisher
	secretGetter SecretValueGetter
}

// New constructs a ParameterProvider.
func New(store ParameterStore, kms model.KeyEncryptor) *ParameterProvider {
	return &ParameterProvider{store: store, kms: kms}
}

// SetEventPublisher wires the EventBridge publisher for SSM change events (second-pass wiring).
func (p *ParameterProvider) SetEventPublisher(pub ParameterEventPublisher) { p.eventPub = pub }

// SetSecretGetter wires the SecretsManager getter for /aws/reference/secretsmanager/ bridge.
func (p *ParameterProvider) SetSecretGetter(sg SecretValueGetter) { p.secretGetter = sg }

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
	name = Normalize(name)

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
	// Respect explicit Tier param or infer it.
	explicitTier, _ := nr.Params["Tier"].(string)
	tier := inferTier(len(valueStr), false, false)
	if explicitTier == "Advanced" {
		tier = "Advanced"
	}

	tags := extractTags(nr.Params)

	raw, err := p.encryptValue(ctx, paramType, kmsKeyID, name, []byte(valueStr))
	if err != nil {
		return nil, err
	}

	// Determine if this is a create or update (for event emission).
	_, existsErr := p.store.GetParameter(ctx, name)
	isCreate := errors.Is(existsErr, ErrParameterNotFound)

	e := ParameterEntry{
		AccountID:   nr.AccountID,
		Region:      nr.Region,
		Name:        name,
		Type:        paramType,
		Description: desc,
		KMSKeyID:    kmsKeyID,
		Value:       raw,
		Tier:        tier,
		Tags:        tags,
	}
	if err := p.store.PutParameter(ctx, &e, overwrite); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, model.NewProviderError("ParameterAlreadyExists",
				"parameter already exists; use Overwrite=true to update: "+name, 400)
		}
		return nil, fmt.Errorf("ssm: put parameter: %w", err)
	}

	// Emit EventBridge event.
	if p.eventPub != nil {
		op := "Update"
		if isCreate {
			op = "Create"
		}
		_ = p.eventPub.InternalPutEvents(ctx, []map[string]any{{
			"Source":     "aws.ssm",
			"DetailType": "Parameter Store Change",
			"Detail": map[string]any{
				"name":      name,
				"type":      paramType,
				"operation": op,
			},
		}})
	}

	return provider.OK(map[string]any{"Version": e.Version, "Tier": tier}), nil
}

func (p *ParameterProvider) GetParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	rawName, _ := nr.Params["Name"].(string)
	withDecryption, _ := nr.Params["WithDecryption"].(bool)

	// Check for /aws/reference/secretsmanager/ bridge.
	normalized := Normalize(rawName)
	const smPrefix = "/aws/reference/secretsmanager/"
	if strings.HasPrefix(normalized, smPrefix) && p.secretGetter != nil {
		secretName := strings.TrimPrefix(normalized, smPrefix)
		val, err := p.secretGetter.InternalGetSecretValue(ctx, secretName)
		if err != nil {
			return nil, model.NewProviderError("ParameterNotFound", "secret reference not found: "+secretName, 400)
		}
		return provider.OK(map[string]any{
			"Parameter": map[string]any{
				"Name":    normalized,
				"Type":    "String",
				"Value":   val,
				"Version": int64(0),
				"ARN":     "",
			},
		}), nil
	}

	base, version, label := ParseSelector(normalized)

	e, err := p.resolveParameterWithSelector(ctx, base, version, label)
	if err != nil {
		if errors.Is(err, ErrParameterNotFound) || errors.Is(err, ErrVersionNotFound) {
			return nil, model.NewProviderError("ParameterNotFound", "parameter not found: "+rawName, 400)
		}
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

	const smPrefix = "/aws/reference/secretsmanager/"

	var found []map[string]any
	var invalid []string
	for _, rawName := range names {
		normalized := Normalize(rawName)

		// /aws/reference/secretsmanager/ bridge.
		if strings.HasPrefix(normalized, smPrefix) && p.secretGetter != nil {
			secretName := strings.TrimPrefix(normalized, smPrefix)
			val, err := p.secretGetter.InternalGetSecretValue(ctx, secretName)
			if err != nil {
				invalid = append(invalid, rawName)
				continue
			}
			found = append(found, map[string]any{
				"Name":    normalized,
				"Type":    "String",
				"Value":   val,
				"Version": int64(0),
				"ARN":     "",
			})
			continue
		}

		base, version, label := ParseSelector(normalized)
		e, err := p.resolveParameterWithSelector(ctx, base, version, label)
		if errors.Is(err, ErrParameterNotFound) || errors.Is(err, ErrVersionNotFound) {
			invalid = append(invalid, rawName)
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
	path = Normalize(path)
	recursive, _ := nr.Params["Recursive"].(bool)
	withDecryption, _ := nr.Params["WithDecryption"].(bool)

	maxResults := 0
	switch v := nr.Params["MaxResults"].(type) {
	case float64:
		maxResults = int(v)
	case int:
		maxResults = v
	case int64:
		maxResults = int(v)
	}
	nextToken, _ := nr.Params["NextToken"].(string)

	entries, err := p.store.ListParameters(ctx, path, recursive)
	if err != nil {
		return nil, fmt.Errorf("ssm: get parameters by path: %w", err)
	}

	page, newToken, err := pagination.Paginate(entries, maxResults, nextToken, "GetParametersByPath")
	if err != nil {
		return nil, model.NewProviderError("InvalidNextToken", err.Error(), 400)
	}

	var items []map[string]any
	for _, e := range page {
		value, err := p.decryptValue(ctx, e, withDecryption)
		if err != nil {
			return nil, err
		}
		items = append(items, paramDetail(e, value, nr))
	}
	resp := map[string]any{"Parameters": items}
	if newToken != "" {
		resp["NextToken"] = newToken
	}
	return provider.OK(resp), nil
}

func (p *ParameterProvider) DeleteParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	name = Normalize(name)
	if err := p.store.DeleteParameter(ctx, name); err != nil {
		if errors.Is(err, ErrParameterNotFound) {
			return nil, model.NewProviderError("ParameterNotFound", "parameter not found: "+name, 400)
		}
		return nil, fmt.Errorf("ssm: delete parameter: %w", err)
	}

	// Emit EventBridge event.
	if p.eventPub != nil {
		_ = p.eventPub.InternalPutEvents(ctx, []map[string]any{{
			"Source":     "aws.ssm",
			"DetailType": "Parameter Store Change",
			"Detail": map[string]any{
				"name":      name,
				"operation": "Delete",
			},
		}})
	}

	return provider.OK(map[string]any{}), nil
}

func (p *ParameterProvider) DeleteParameters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names := extractStringList(nr.Params, "Names")
	var deleted, invalid []string
	for _, name := range names {
		name = Normalize(name)
		if err := p.store.DeleteParameter(ctx, name); err != nil {
			invalid = append(invalid, name)
		} else {
			deleted = append(deleted, name)
			// Emit EventBridge event per deletion.
			if p.eventPub != nil {
				_ = p.eventPub.InternalPutEvents(ctx, []map[string]any{{
					"Source":     "aws.ssm",
					"DetailType": "Parameter Store Change",
					"Detail": map[string]any{
						"name":      name,
						"operation": "Delete",
					},
				}})
			}
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

	// Apply ParameterFilters if present.
	filters := parseParameterFilters(nr.Params)
	entries = ApplyFilters(entries, filters)

	maxResults := 0
	switch v := nr.Params["MaxResults"].(type) {
	case float64:
		maxResults = int(v)
	case int:
		maxResults = v
	case int64:
		maxResults = int(v)
	}
	nextToken, _ := nr.Params["NextToken"].(string)

	page, newToken, err := pagination.Paginate(entries, maxResults, nextToken, "DescribeParameters")
	if err != nil {
		return nil, model.NewProviderError("InvalidNextToken", err.Error(), 400)
	}

	items := make([]map[string]any, 0, len(page))
	for _, e := range page {
		items = append(items, map[string]any{
			"Name":        e.Name,
			"Type":        e.Type,
			"Description": e.Description,
			"Version":     e.Version,
			"Tier":        e.Tier,
		})
	}
	resp := map[string]any{"Parameters": items}
	if newToken != "" {
		resp["NextToken"] = newToken
	}
	return provider.OK(resp), nil
}

func (p *ParameterProvider) GetParameterHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	name = Normalize(name)
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
	name = Normalize(name)
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
	if err := p.store.PutParameter(ctx, &e, true); err != nil {
		return nil, fmt.Errorf("ssm: save tags: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ParameterProvider) RemoveTagsFromResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["ResourceId"].(string)
	name = Normalize(name)
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
	if err := p.store.PutParameter(ctx, &e, true); err != nil {
		return nil, fmt.Errorf("ssm: save tags: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ParameterProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["ResourceId"].(string)
	name = Normalize(name)
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
	name = Normalize(name)
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

	// Emit EventBridge event for label operation.
	if p.eventPub != nil {
		_ = p.eventPub.InternalPutEvents(ctx, []map[string]any{{
			"Source":     "aws.ssm",
			"DetailType": "Parameter Store Change",
			"Detail": map[string]any{
				"name":      name,
				"operation": "LabelParameterVersion",
			},
		}})
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
	name = Normalize(name)
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

// inferTier returns "Standard" or "Advanced" based on value size and features.
func inferTier(valueLen int, hasPolicies, hasLabels bool) string {
	if valueLen > 4096 || hasPolicies || hasLabels {
		return "Advanced"
	}
	return "Standard"
}

// resolveParameterWithSelector fetches a parameter, optionally by version or label.
// When both version==0 and label=="", it returns the current version.
func (p *ParameterProvider) resolveParameterWithSelector(ctx context.Context, name string, version int64, label string) (ParameterEntry, error) {
	if version == 0 && label == "" {
		return p.store.GetParameter(ctx, name)
	}

	// Fetch current first.
	current, err := p.store.GetParameter(ctx, name)
	if errors.Is(err, ErrParameterNotFound) {
		return ParameterEntry{}, ErrParameterNotFound
	}
	if err != nil {
		return ParameterEntry{}, err
	}

	if version > 0 {
		if current.Version == version {
			return current, nil
		}
		history, err := p.store.GetParameterHistory(ctx, name)
		if err != nil {
			return ParameterEntry{}, err
		}
		for _, h := range history {
			if h.Version == version {
				return ParameterEntry{
					Name:     h.Name,
					Type:     h.Type,
					KMSKeyID: h.KMSKeyID,
					Value:    h.Value,
					Version:  h.Version,
				}, nil
			}
		}
		return ParameterEntry{}, ErrVersionNotFound
	}

	// Resolve by label — check current version's labels first.
	currentLabels, err := p.store.GetLabelsByVersion(ctx, name, current.Version)
	if err == nil {
		for _, lbl := range currentLabels {
			if lbl == label {
				return current, nil
			}
		}
	}
	// Check history versions (newest first).
	history, err := p.store.GetParameterHistory(ctx, name)
	if err != nil {
		return ParameterEntry{}, err
	}
	for i := len(history) - 1; i >= 0; i-- {
		h := history[i]
		lbls, err := p.store.GetLabelsByVersion(ctx, name, h.Version)
		if err != nil {
			continue
		}
		for _, lbl := range lbls {
			if lbl == label {
				return ParameterEntry{
					Name:     h.Name,
					Type:     h.Type,
					KMSKeyID: h.KMSKeyID,
					Value:    h.Value,
					Version:  h.Version,
				}, nil
			}
		}
	}
	return ParameterEntry{}, ErrVersionNotFound
}

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

// decryptValue returns the string representation of a parameter's value.
//
//   - Non-SecureString: returns string(e.Value).
//   - SecureString + WithDecryption=true + kms present: decrypts and returns plaintext.
//   - SecureString + WithDecryption=false + kms present: returns base64-encoded ciphertext.
//   - SecureString + kms==nil: value was stored as plaintext bytes; return as-is.
func (p *ParameterProvider) decryptValue(ctx context.Context, e ParameterEntry, withDecryption bool) (string, error) {
	if e.Type != "SecureString" {
		return string(e.Value), nil
	}
	if p.kms == nil {
		// No KMS configured — value stored as plaintext bytes.
		return string(e.Value), nil
	}
	if !withDecryption {
		// Return raw ciphertext bytes as base64 instead of literal "****".
		return base64.StdEncoding.EncodeToString(e.Value), nil
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

// parseParameterFilters parses ParameterFilters from nr.Params.
// Accepts []any of maps with "Key", "Option", "Values".
func parseParameterFilters(params map[string]any) []ParameterFilter {
	raw, ok := params["ParameterFilters"]
	if !ok {
		return nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	var out []ParameterFilter
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["Key"].(string)
		option, _ := m["Option"].(string)
		var values []string
		if vs, ok := m["Values"].([]any); ok {
			for _, v := range vs {
				values = append(values, fmt.Sprint(v))
			}
		}
		out = append(out, ParameterFilter{Key: key, Option: option, Values: values})
	}
	return out
}
