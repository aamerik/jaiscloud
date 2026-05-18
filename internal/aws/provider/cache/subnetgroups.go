package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtCacheSubnetGroup = "elasticache_subnet_group"
	rtCacheTags        = "elasticache_tags"
)

type cacheSubnetGroup struct {
	CacheSubnetGroupName        string   `json:"CacheSubnetGroupName"`
	CacheSubnetGroupDescription string   `json:"CacheSubnetGroupDescription"`
	VpcId                       string   `json:"VpcId"`
	SubnetIds                   []string `json:"SubnetIds"`
	ARN                         string   `json:"ARN"`
}

func (g cacheSubnetGroup) toWire() map[string]any {
	subnets := make([]map[string]any, 0, len(g.SubnetIds))
	for _, id := range g.SubnetIds {
		subnets = append(subnets, map[string]any{
			"SubnetIdentifier": id,
			"SubnetAvailabilityZone": map[string]any{"Name": "us-east-1a"},
		})
	}
	return map[string]any{
		"CacheSubnetGroupName":        g.CacheSubnetGroupName,
		"CacheSubnetGroupDescription": g.CacheSubnetGroupDescription,
		"VpcId":                       g.VpcId,
		"Subnets":                     subnets,
		"ARN":                         g.ARN,
	}
}

// extractSubnetIds reads SubnetIds.SubnetIdentifier.N from Query-protocol params.
func extractSubnetIds(params map[string]any) []string {
	var ids []string
	for i := 1; ; i++ {
		key := fmt.Sprintf("SubnetIds.SubnetIdentifier.%d", i)
		v, ok := params[key]
		if !ok {
			break
		}
		if s, ok := v.(string); ok {
			ids = append(ids, s)
		}
	}
	return ids
}

func (p *CacheProvider) CreateCacheSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheSubnetGroupName")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "CacheSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	g := cacheSubnetGroup{
		CacheSubnetGroupName:        name,
		CacheSubnetGroupDescription: strParam(nr.Params, "CacheSubnetGroupDescription"),
		VpcId:                       strParam(nr.Params, "VpcId"),
		SubnetIds:                   extractSubnetIds(nr.Params),
		ARN:                         nr.ResourceID("elasticache-subnetgroup", name),
	}
	data, _ := json.Marshal(g)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtCacheSubnetGroup, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "CacheSubnetGroupAlreadyExistsFault", Message: "subnet group " + name + " already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"CacheSubnetGroup": g.toWire()}), nil
}

func (p *CacheProvider) DescribeCacheSubnetGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheSubnetGroupName")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtCacheSubnetGroup, "")
	var groups []map[string]any
	for _, e := range entries {
		var g cacheSubnetGroup
		if json.Unmarshal(e.Data, &g) != nil {
			continue
		}
		if name != "" && g.CacheSubnetGroupName != name {
			continue
		}
		groups = append(groups, g.toWire())
	}
	if groups == nil {
		groups = []map[string]any{}
	}
	maxRecords := 100
	if v := strParam(nr.Params, "MaxRecords"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxRecords = n
		}
	}
	marker, _ := nr.Params["Marker"].(string)
	page, nextMarker, pgErr := pagination.Paginate(groups, maxRecords, marker, "DescribeCacheSubnetGroups")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	resp := map[string]any{"CacheSubnetGroups": page}
	if nextMarker != "" {
		resp["Marker"] = nextMarker
	}
	return provider.OK(resp), nil
}

func (p *CacheProvider) ModifyCacheSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheSubnetGroupName")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCacheSubnetGroup, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "CacheSubnetGroupNotFoundFault", Message: "subnet group " + name + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var g cacheSubnetGroup
	_ = json.Unmarshal(e.Data, &g)
	if v := strParam(nr.Params, "CacheSubnetGroupDescription"); v != "" {
		g.CacheSubnetGroupDescription = v
	}
	if ids := extractSubnetIds(nr.Params); len(ids) > 0 {
		g.SubnetIds = ids
	}
	data, _ := json.Marshal(g)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtCacheSubnetGroup, ID: name, Data: data})
	return provider.OK(map[string]any{"CacheSubnetGroup": g.toWire()}), nil
}

func (p *CacheProvider) DeleteCacheSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheSubnetGroupName")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtCacheSubnetGroup, name); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "CacheSubnetGroupNotFoundFault", Message: "subnet group " + name + " not found", HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Tagging ──────────────────────────────────────────────────────────────────

// extractCacheTags reads Tags.Tag.N.Key / Tags.Tag.N.Value.
func extractCacheTags(params map[string]any) map[string]string {
	tags := map[string]string{}
	for i := 1; ; i++ {
		k := strParam(params, fmt.Sprintf("Tags.Tag.%d.Key", i))
		if k == "" {
			break
		}
		tags[k] = strParam(params, fmt.Sprintf("Tags.Tag.%d.Value", i))
	}
	return tags
}

// extractTagKeys reads TagKeys.member.N.
func extractTagKeys(params map[string]any) []string {
	var keys []string
	for i := 1; ; i++ {
		k := strParam(params, fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	return keys
}

func loadCacheTags(ctx context.Context, res store.ResourceStore, account, region, arn string) map[string]string {
	e, err := res.Get(ctx, account, region, rtCacheTags, arn)
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	_ = json.Unmarshal(e.Data, &m)
	return m
}

func saveCacheTags(ctx context.Context, res store.ResourceStore, account, region, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtCacheTags, ID: arn, Data: data}
	if err := res.Create(ctx, account, region, entry); err == store.ErrAlreadyExists {
		res.Update(ctx, account, region, entry)
	}
}

func (p *CacheProvider) AddTagsToResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceName")
	newTags := extractCacheTags(nr.Params)
	existing := loadCacheTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	for k, v := range newTags {
		existing[k] = v
	}
	saveCacheTags(ctx, p.resources, nr.AccountID, nr.Region, arn, existing)
	tagList := tagsToList(existing)
	return provider.OK(map[string]any{"TagList": tagList}), nil
}

func (p *CacheProvider) RemoveTagsFromResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceName")
	keys := extractTagKeys(nr.Params)
	existing := loadCacheTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	for _, k := range keys {
		delete(existing, k)
	}
	saveCacheTags(ctx, p.resources, nr.AccountID, nr.Region, arn, existing)
	tagList := tagsToList(existing)
	return provider.OK(map[string]any{"TagList": tagList}), nil
}

func (p *CacheProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceName")
	tags := loadCacheTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	return provider.OK(map[string]any{"TagList": tagsToList(tags)}), nil
}

func tagsToList(tags map[string]string) []map[string]any {
	out := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		out = append(out, map[string]any{"Key": k, "Value": v})
	}
	return out
}

// ─── Cache Parameter Groups ───────────────────────────────────────────────────

const rtCacheParameterGroup = "elasticache_parameter_group"

type cacheParameterGroup struct {
	CacheParameterGroupName   string `json:"CacheParameterGroupName"`
	CacheParameterGroupFamily string `json:"CacheParameterGroupFamily"`
	Description               string `json:"Description"`
	ARN                       string `json:"ARN"`
}

func (g cacheParameterGroup) toWire() map[string]any {
	return map[string]any{
		"CacheParameterGroupName":   g.CacheParameterGroupName,
		"CacheParameterGroupFamily": g.CacheParameterGroupFamily,
		"Description":               g.Description,
		"ARN":                       g.ARN,
	}
}

func (p *CacheProvider) CreateCacheParameterGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheParameterGroupName")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "CacheParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	grp := cacheParameterGroup{
		CacheParameterGroupName:   name,
		CacheParameterGroupFamily: strParam(nr.Params, "CacheParameterGroupFamily"),
		Description:               strParam(nr.Params, "Description"),
		ARN:                       nr.ResourceID("elasticache-parametergroup", name),
	}
	data, _ := json.Marshal(grp)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtCacheParameterGroup, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "CacheParameterGroupAlreadyExists", Message: "Cache parameter group already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"CacheParameterGroup": grp.toWire()}), nil
}

func (p *CacheProvider) DescribeCacheParameterGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheParameterGroupName")
	if name != "" {
		e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCacheParameterGroup, name)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "CacheParameterGroupNotFound", Message: "Cache parameter group not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var grp cacheParameterGroup
		json.Unmarshal(e.Data, &grp)
		return provider.OK(map[string]any{"CacheParameterGroups": []any{grp.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtCacheParameterGroup, "")
	if err != nil {
		return nil, err
	}
	groups := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var grp cacheParameterGroup
		json.Unmarshal(e.Data, &grp)
		groups = append(groups, grp.toWire())
	}
	maxRecords := 100
	if v := strParam(nr.Params, "MaxRecords"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxRecords = n
		}
	}
	marker, _ := nr.Params["Marker"].(string)
	page, nextMarker, pgErr := pagination.Paginate(groups, maxRecords, marker, "DescribeCacheParameterGroups")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	resp := map[string]any{"CacheParameterGroups": page}
	if nextMarker != "" {
		resp["Marker"] = nextMarker
	}
	return provider.OK(resp), nil
}

func (p *CacheProvider) DeleteCacheParameterGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "CacheParameterGroupName")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtCacheParameterGroup, name); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "CacheParameterGroupNotFound", Message: "Cache parameter group not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{}), nil
}
