// Package resourcegroups implements the AWS Resource Groups provider (metadata-only).
package resourcegroups

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtGroup    = "resourcegroup"
	rtGroupTag = "resourcegroup_tag"
)

// Provider handles Resource Groups operations.
type Provider struct {
	resources store.ResourceStore
}

// New creates a new Resource Groups provider.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes returns all Resource Groups handler registrations.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ResourceGroups.CreateGroup": p.CreateGroup,
		"ResourceGroups.DeleteGroup": p.DeleteGroup,
		"ResourceGroups.GetGroup":    p.GetGroup,
		"ResourceGroups.ListGroups":  p.ListGroups,
		"ResourceGroups.UpdateGroup": p.UpdateGroup,
		"ResourceGroups.Tag":         p.Tag,
		"ResourceGroups.Untag":       p.Untag,
		"ResourceGroups.GetTags":     p.GetTags,
	}
}

// ─── Types ────────────────────────────────────────────────────────────────────

type resourceGroup struct {
	Name          string            `json:"Name"`
	GroupARN      string            `json:"GroupArn"`
	Description   string            `json:"Description"`
	ResourceQuery map[string]any    `json:"ResourceQuery"`
	Tags          map[string]string `json:"Tags"`
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (p *Provider) CreateGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	arn := nr.ResourceID("resourcegroup", name)
	g := resourceGroup{
		Name:        name,
		GroupARN:    arn,
		Description: strParam(nr.Params, "Description"),
	}
	if rq, ok := nr.Params["ResourceQuery"].(map[string]any); ok {
		g.ResourceQuery = rq
	}
	if tags, ok := nr.Params["Tags"].(map[string]any); ok {
		g.Tags = toStringMap(tags)
	}

	data, _ := json.Marshal(g)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtGroup, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "AlreadyExistsException", Message: "A resource group with this name already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}

	return provider.OK(map[string]any{
		"Group":         groupToWire(g),
		"ResourceQuery": g.ResourceQuery,
		"Tags":          g.Tags,
	}), nil
}

func (p *Provider) DeleteGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := groupNameFromRequest(nr)
	if name == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "Group name is required", HTTPStatus: http.StatusBadRequest}
	}

	e, err := p.resources.Get(ctx, rtGroup, name)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFoundException", Message: "Group not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}

	var g resourceGroup
	json.Unmarshal(e.Data, &g)
	p.resources.Delete(ctx, rtGroup, name)

	return provider.OK(map[string]any{"Group": groupToWire(g)}), nil
}

func (p *Provider) GetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := groupNameFromRequest(nr)
	if name == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "Group name is required", HTTPStatus: http.StatusBadRequest}
	}

	e, err := p.resources.Get(ctx, rtGroup, name)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFoundException", Message: "Group not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}

	var g resourceGroup
	json.Unmarshal(e.Data, &g)
	return provider.OK(map[string]any{"Group": groupToWire(g)}), nil
}

func (p *Provider) ListGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtGroup, "")
	if err != nil {
		return nil, err
	}

	var groups []any
	for _, e := range entries {
		var g resourceGroup
		json.Unmarshal(e.Data, &g)
		groups = append(groups, groupToWire(g))
	}
	if groups == nil {
		groups = []any{}
	}
	return provider.OK(map[string]any{"GroupIdentifiers": groups}), nil
}

func (p *Provider) UpdateGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := groupNameFromRequest(nr)
	if name == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "Group name is required", HTTPStatus: http.StatusBadRequest}
	}

	e, err := p.resources.Get(ctx, rtGroup, name)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFoundException", Message: "Group not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}

	var g resourceGroup
	json.Unmarshal(e.Data, &g)

	if desc := strParam(nr.Params, "Description"); desc != "" {
		g.Description = desc
	}

	data, _ := json.Marshal(g)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtGroup, ID: name, Data: data})

	return provider.OK(map[string]any{"Group": groupToWire(g)}), nil
}

// ─── Resource Tags ────────────────────────────────────────────────────────────

func (p *Provider) Tag(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := arnFromPath(nr)
	if arn == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "ARN is required", HTTPStatus: http.StatusBadRequest}
	}

	tags := toStringMap(paramMap(nr.Params, "Tags"))

	existing := map[string]string{}
	if e, err := p.resources.Get(ctx, rtGroupTag, arn); err == nil {
		json.Unmarshal(e.Data, &existing)
	}
	for k, v := range tags {
		existing[k] = v
	}

	data, _ := json.Marshal(existing)
	_ = p.resources.Delete(ctx, rtGroupTag, arn)
	_ = p.resources.Create(ctx, store.ResourceEntry{Type: rtGroupTag, ID: arn, Data: data})

	return provider.OK(map[string]any{"Arn": arn, "Tags": existing}), nil
}

func (p *Provider) Untag(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := arnFromPath(nr)
	if arn == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "ARN is required", HTTPStatus: http.StatusBadRequest}
	}

	keys, _ := nr.Params["Keys"].([]any)
	existing := map[string]string{}
	if e, err := p.resources.Get(ctx, rtGroupTag, arn); err == nil {
		json.Unmarshal(e.Data, &existing)
	}
	for _, k := range keys {
		if ks, ok := k.(string); ok {
			delete(existing, ks)
		}
	}
	data, _ := json.Marshal(existing)
	_ = p.resources.Delete(ctx, rtGroupTag, arn)
	_ = p.resources.Create(ctx, store.ResourceEntry{Type: rtGroupTag, ID: arn, Data: data})

	return provider.OK(map[string]any{"Arn": arn, "Keys": keys}), nil
}

func (p *Provider) GetTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := arnFromPath(nr)
	if arn == "" {
		return nil, &model.ProviderError{Code: "BadRequestException", Message: "ARN is required", HTTPStatus: http.StatusBadRequest}
	}

	existing := map[string]string{}
	if e, err := p.resources.Get(ctx, rtGroupTag, arn); err == nil {
		json.Unmarshal(e.Data, &existing)
	}

	return provider.OK(map[string]any{"Arn": arn, "Tags": existing}), nil
}

// ─── Wire helpers ─────────────────────────────────────────────────────────────

func groupToWire(g resourceGroup) map[string]any {
	return map[string]any{
		"Name":        g.Name,
		"GroupArn":    g.GroupARN,
		"Description": g.Description,
	}
}

// ─── Request helpers ──────────────────────────────────────────────────────────

// groupNameFromRequest extracts the group name from path params or body params.
func groupNameFromRequest(nr *model.NormalizedRequest) string {
	// REST path params: /groups/{GroupName}
	if nr.Raw != nil {
		path := nr.Raw.URL.Path
		// /groups/name or /groups/name/...
		parts := strings.SplitN(strings.TrimPrefix(path, "/groups/"), "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			return parts[0]
		}
	}
	// Fall back to body
	return strParam(nr.Params, "Group")
}

// arnFromPath extracts ARN from path /resources/{Arn}/tags
func arnFromPath(nr *model.NormalizedRequest) string {
	if nr.Raw != nil {
		path := nr.Raw.URL.Path
		// /resources/{Arn}/tags
		trimmed := strings.TrimPrefix(path, "/resources/")
		if idx := strings.LastIndex(trimmed, "/tags"); idx >= 0 {
			return trimmed[:idx]
		}
		return trimmed
	}
	return strParam(nr.Params, "Arn")
}

func toStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}

func paramMap(params map[string]any, key string) map[string]any {
	if v, ok := params[key].(map[string]any); ok {
		return v
	}
	return map[string]any{}
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
