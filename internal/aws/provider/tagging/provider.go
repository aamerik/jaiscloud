// Package tagging implements the AWS Resource Groups Tagging API provider.
package tagging

import (
	"context"
	"encoding/json"
	"net/http"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtTaggingResource = "tagging_resource"

// Provider handles the Tagging API operations.
type Provider struct {
	resources store.ResourceStore
}

// New creates a new Tagging API provider.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes returns all Tagging API handler registrations.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Tagging.GetResources":     p.GetResources,
		"Tagging.GetTagKeys":       p.GetTagKeys,
		"Tagging.GetTagValues":     p.GetTagValues,
		"Tagging.TagResources":     p.TagResources,
		"Tagging.UntagResources":   p.UntagResources,
	}
}

// ─── Types ────────────────────────────────────────────────────────────────────

type taggedResource struct {
	ARN  string            `json:"ARN"`
	Tags map[string]string `json:"Tags"`
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (p *Provider) GetResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Build tag filter from TagFilters list
	tagFilters := extractTagFilters(nr.Params)

	entries, err := p.resources.List(ctx, rtTaggingResource, "")
	if err != nil {
		return nil, err
	}

	var resourceTagMappings []any
	for _, e := range entries {
		var res taggedResource
		json.Unmarshal(e.Data, &res)

		if !matchesTagFilters(res.Tags, tagFilters) {
			continue
		}

		var tags []any
		for k, v := range res.Tags {
			tags = append(tags, map[string]any{"Key": k, "Value": v})
		}
		if tags == nil {
			tags = []any{}
		}
		resourceTagMappings = append(resourceTagMappings, map[string]any{
			"ResourceARN": res.ARN,
			"Tags":        tags,
		})
	}
	if resourceTagMappings == nil {
		resourceTagMappings = []any{}
	}

	return provider.OK(map[string]any{
		"ResourceTagMappingList": resourceTagMappings,
		"PaginationToken":        "",
	}), nil
}

func (p *Provider) GetTagKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtTaggingResource, "")
	if err != nil {
		return nil, err
	}

	keySet := map[string]bool{}
	for _, e := range entries {
		var res taggedResource
		json.Unmarshal(e.Data, &res)
		for k := range res.Tags {
			keySet[k] = true
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	return provider.OK(map[string]any{
		"TagKeys":         keys,
		"PaginationToken": "",
	}), nil
}

func (p *Provider) GetTagValues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterKey := strParam(nr.Params, "Key")

	entries, err := p.resources.List(ctx, rtTaggingResource, "")
	if err != nil {
		return nil, err
	}

	valueSet := map[string]bool{}
	for _, e := range entries {
		var res taggedResource
		json.Unmarshal(e.Data, &res)
		for k, v := range res.Tags {
			if filterKey == "" || k == filterKey {
				valueSet[v] = true
			}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}

	return provider.OK(map[string]any{
		"TagValues":       values,
		"PaginationToken": "",
	}), nil
}

func (p *Provider) TagResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arns := extractStringList(nr.Params, "ResourceARNList")
	tags := extractTagMap(nr.Params, "Tags")

	for _, arn := range arns {
		res := taggedResource{ARN: arn, Tags: map[string]string{}}
		if e, err := p.resources.Get(ctx, rtTaggingResource, arn); err == nil {
			json.Unmarshal(e.Data, &res)
		}
		for k, v := range tags {
			res.Tags[k] = v
		}
		data, _ := json.Marshal(res)
		_ = p.resources.Delete(ctx, rtTaggingResource, arn)
		_ = p.resources.Create(ctx, store.ResourceEntry{Type: rtTaggingResource, ID: arn, Data: data})
	}

	return provider.OK(map[string]any{"FailedResourcesMap": map[string]any{}}), nil
}

func (p *Provider) UntagResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arns := extractStringList(nr.Params, "ResourceARNList")
	keys := extractStringList(nr.Params, "TagKeys")

	for _, arn := range arns {
		res := taggedResource{ARN: arn, Tags: map[string]string{}}
		if e, err := p.resources.Get(ctx, rtTaggingResource, arn); err == nil {
			json.Unmarshal(e.Data, &res)
		}
		for _, k := range keys {
			delete(res.Tags, k)
		}
		data, _ := json.Marshal(res)
		_ = p.resources.Delete(ctx, rtTaggingResource, arn)
		if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtTaggingResource, ID: arn, Data: data}); err != nil {
			return nil, err
		}
	}

	return provider.OK(map[string]any{"FailedResourcesMap": map[string]any{}}), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

type tagFilter struct {
	Key    string
	Values []string
}

func extractTagFilters(params map[string]any) []tagFilter {
	var filters []tagFilter
	for i := 1; ; i++ {
		keyParam := paramForIndex(params, "TagFilters", i, "Key")
		if keyParam == "" {
			break
		}
		var values []string
		for j := 1; ; j++ {
			v := paramForIndex2(params, "TagFilters", i, "Values", j)
			if v == "" {
				break
			}
			values = append(values, v)
		}
		filters = append(filters, tagFilter{Key: keyParam, Values: values})
	}
	return filters
}

func paramForIndex(params map[string]any, list string, i int, field string) string {
	// Try both "List.N.Field" and "List.member.N.Field" patterns
	key1 := listKey(list, i, field)
	if v, ok := params[key1]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func paramForIndex2(params map[string]any, list string, i int, sub string, j int) string {
	k := listKey2(list, i, sub, j)
	if v, ok := params[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func listKey(list string, i int, field string) string {
	return list + "." + itoa(i) + "." + field
}

func listKey2(list string, i int, sub string, j int) string {
	return list + "." + itoa(i) + "." + sub + "." + itoa(j)
}

func itoa(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return string(digits[i])
	}
	b := make([]byte, 0, 3)
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}

func matchesTagFilters(tags map[string]string, filters []tagFilter) bool {
	for _, f := range filters {
		v, ok := tags[f.Key]
		if !ok {
			return false
		}
		if len(f.Values) > 0 {
			found := false
			for _, fv := range f.Values {
				if fv == v {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func extractStringList(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case []any:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return val
	case string:
		return []string{val}
	}
	return nil
}

func extractTagMap(params map[string]any, key string) map[string]string {
	v, ok := params[key]
	if !ok {
		return map[string]string{}
	}
	if m, ok := v.(map[string]any); ok {
		result := make(map[string]string, len(m))
		for k, val := range m {
			if s, ok := val.(string); ok {
				result[k] = s
			}
		}
		return result
	}
	return map[string]string{}
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// InternalTagResource allows other providers to tag their resources in the tagging store.
func (p *Provider) InternalTagResource(ctx context.Context, arn string, tags map[string]string) error {
	if arn == "" || len(tags) == 0 {
		return nil
	}
	res := taggedResource{ARN: arn, Tags: map[string]string{}}
	if e, err := p.resources.Get(ctx, rtTaggingResource, arn); err == nil {
		json.Unmarshal(e.Data, &res)
	}
	for k, v := range tags {
		res.Tags[k] = v
	}
	data, _ := json.Marshal(res)
	_ = p.resources.Delete(ctx, rtTaggingResource, arn)
	return p.resources.Create(ctx, store.ResourceEntry{Type: rtTaggingResource, ID: arn, Data: data})
}

// InternalUntagResource allows other providers to remove tags from their resources.
func (p *Provider) InternalUntagResource(ctx context.Context, arn string, keys []string) error {
	if arn == "" || len(keys) == 0 {
		return nil
	}
	res := taggedResource{ARN: arn, Tags: map[string]string{}}
	if e, err := p.resources.Get(ctx, rtTaggingResource, arn); err == nil {
		json.Unmarshal(e.Data, &res)
	}
	for _, k := range keys {
		delete(res.Tags, k)
	}
	data, _ := json.Marshal(res)
	_ = p.resources.Delete(ctx, rtTaggingResource, arn)
	return p.resources.Create(ctx, store.ResourceEntry{Type: rtTaggingResource, ID: arn, Data: data})
}

// ErrorFailedResource represents a resource that couldn't be tagged.
type ErrorFailedResource struct {
	StatusCode  int    `json:"StatusCode"`
	ErrorCode   string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// validateARN checks if an ARN is non-empty.
func validateARN(arn string) error {
	if arn == "" {
		return &model.ProviderError{Code: "InvalidParameterException", Message: "ARN cannot be empty", HTTPStatus: http.StatusBadRequest}
	}
	return nil
}
