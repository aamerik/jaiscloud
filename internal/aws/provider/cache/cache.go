// Package cache implements the ElastiCache provider (CacheProvider).
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// CacheProvider handles ElastiCache cache clusters and replication groups.
type CacheProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *CacheProvider {
	return &CacheProvider{resources: resources}
}

func (p *CacheProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ElastiCache.CreateCacheCluster":       p.CreateCacheCluster,
		"ElastiCache.DescribeCacheClusters":    p.DescribeCacheClusters,
		"ElastiCache.ModifyCacheCluster":       p.ModifyCacheCluster,
		"ElastiCache.DeleteCacheCluster":       p.DeleteCacheCluster,
		"ElastiCache.CreateReplicationGroup":   p.CreateReplicationGroup,
		"ElastiCache.DescribeReplicationGroups": p.DescribeReplicationGroups,
		"ElastiCache.ModifyReplicationGroup":   p.ModifyReplicationGroup,
		"ElastiCache.DeleteReplicationGroup":   p.DeleteReplicationGroup,
		// Subnet groups (14.6)
		"ElastiCache.CreateCacheSubnetGroup":   p.CreateCacheSubnetGroup,
		"ElastiCache.DescribeCacheSubnetGroups": p.DescribeCacheSubnetGroups,
		"ElastiCache.ModifyCacheSubnetGroup":   p.ModifyCacheSubnetGroup,
		"ElastiCache.DeleteCacheSubnetGroup":   p.DeleteCacheSubnetGroup,
		// Tagging (14.6)
		"ElastiCache.AddTagsToResource":      p.AddTagsToResource,
		"ElastiCache.RemoveTagsFromResource": p.RemoveTagsFromResource,
		"ElastiCache.ListTagsForResource":    p.ListTagsForResource,
	}
}

const (
	rtCacheCluster     = "elasticache_cluster"
	rtReplicationGroup = "elasticache_replication_group"
)

// ─── Cache Clusters ───────────────────────────────────────────────────────────

type cacheCluster struct {
	CacheClusterId     string `json:"CacheClusterId"`
	CacheClusterStatus string `json:"CacheClusterStatus"`
	CacheNodeType      string `json:"CacheNodeType"`
	Engine             string `json:"Engine"`
	EngineVersion      string `json:"EngineVersion"`
	NumCacheNodes      int    `json:"NumCacheNodes"`
}

func defaultEngineVersion(engine string) string {
	switch engine {
	case "memcached":
		return "1.6.22"
	case "redis":
		return "7.1.0"
	}
	return "7.1.0"
}

func defaultPort(engine string) int {
	if engine == "memcached" {
		return 11211
	}
	return 6379
}

func (c cacheCluster) toWire() map[string]any {
	port := defaultPort(c.Engine)
	return map[string]any{
		"CacheClusterId":     c.CacheClusterId,
		"CacheClusterStatus": c.CacheClusterStatus,
		"CacheNodeType":      c.CacheNodeType,
		"Engine":             c.Engine,
		"EngineVersion":      c.EngineVersion,
		"NumCacheNodes":      fmt.Sprintf("%d", c.NumCacheNodes),
		"ConfigurationEndpoint": map[string]any{
			"Address": c.CacheClusterId + ".jaiscloud.cache.amazonaws.com",
			"Port":    fmt.Sprintf("%d", port),
		},
	}
}

func (p *CacheProvider) CreateCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "CacheClusterId")
	if id == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "CacheClusterId is required", HTTPStatus: http.StatusBadRequest}
	}
	engine := strParam(nr.Params, "Engine")
	if engine == "" {
		engine = "redis"
	}
	numNodes := 1
	if s := strParam(nr.Params, "NumCacheNodes"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			numNodes = n
		}
	}
	c := cacheCluster{
		CacheClusterId:     id,
		CacheClusterStatus: "available",
		CacheNodeType:      strParam(nr.Params, "CacheNodeType"),
		Engine:             engine,
		EngineVersion:      strParam(nr.Params, "EngineVersion"),
		NumCacheNodes:      numNodes,
	}
	if c.EngineVersion == "" {
		c.EngineVersion = defaultEngineVersion(engine)
	}
	data, _ := json.Marshal(c)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtCacheCluster, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "CacheClusterAlreadyExists", Message: "Cache cluster already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: map[string]any{"CacheCluster": c.toWire()}}, nil
}

func (p *CacheProvider) DescribeCacheClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "CacheClusterId")
	if id != "" {
		e, err := p.resources.Get(ctx, rtCacheCluster, id)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "CacheClusterNotFound", Message: "Cache cluster not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var c cacheCluster
		json.Unmarshal(e.Data, &c)
		return provider.OK(map[string]any{"CacheClusters": []map[string]any{c.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtCacheCluster, "")
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range entries {
		var c cacheCluster
		json.Unmarshal(e.Data, &c)
		list = append(list, c.toWire())
	}
	return provider.OK(map[string]any{"CacheClusters": list}), nil
}

func (p *CacheProvider) ModifyCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "CacheClusterId")
	e, err := p.resources.Get(ctx, rtCacheCluster, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "CacheClusterNotFound", Message: "Cache cluster not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var c cacheCluster
	json.Unmarshal(e.Data, &c)
	if v := strParam(nr.Params, "CacheNodeType"); v != "" {
		c.CacheNodeType = v
	}
	data, _ := json.Marshal(c)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtCacheCluster, ID: id, Data: data})
	return provider.OK(map[string]any{"CacheClusterModified": c.toWire()}), nil
}

func (p *CacheProvider) DeleteCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "CacheClusterId")
	e, err := p.resources.Get(ctx, rtCacheCluster, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "CacheClusterNotFound", Message: "Cache cluster not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var c cacheCluster
	json.Unmarshal(e.Data, &c)
	c.CacheClusterStatus = "deleting"
	p.resources.Delete(ctx, rtCacheCluster, id)
	return provider.OK(map[string]any{"CacheClusterDeleted": c.toWire()}), nil
}

// ─── Replication Groups ───────────────────────────────────────────────────────

type replicationGroup struct {
	ReplicationGroupId string `json:"ReplicationGroupId"`
	Description        string `json:"Description"`
	Status             string `json:"Status"`
	Engine             string `json:"Engine"`
}

func (rg replicationGroup) toWire() map[string]any {
	return map[string]any{
		"ReplicationGroupId": rg.ReplicationGroupId,
		"Description":        rg.Description,
		"Status":             rg.Status,
		"ConfigurationEndpoint": map[string]any{
			"Address": rg.ReplicationGroupId + ".jaiscloud.cache.amazonaws.com",
			"Port":    "6379",
		},
	}
}

func (p *CacheProvider) CreateReplicationGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ReplicationGroupId")
	if id == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "ReplicationGroupId is required", HTTPStatus: http.StatusBadRequest}
	}
	rg := replicationGroup{
		ReplicationGroupId: id,
		Description:        strParam(nr.Params, "ReplicationGroupDescription"),
		Status:             "available",
		Engine:             "redis",
	}
	data, _ := json.Marshal(rg)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtReplicationGroup, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "ReplicationGroupAlreadyExistsFault", Message: "Replication group already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: map[string]any{"ReplicationGroup": rg.toWire()}}, nil
}

func (p *CacheProvider) DescribeReplicationGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ReplicationGroupId")
	if id != "" {
		e, err := p.resources.Get(ctx, rtReplicationGroup, id)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "ReplicationGroupNotFoundFault", Message: "Replication group not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var rg replicationGroup
		json.Unmarshal(e.Data, &rg)
		return provider.OK(map[string]any{"ReplicationGroups": []map[string]any{rg.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtReplicationGroup, "")
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range entries {
		var rg replicationGroup
		json.Unmarshal(e.Data, &rg)
		list = append(list, rg.toWire())
	}
	return provider.OK(map[string]any{"ReplicationGroups": list}), nil
}

func (p *CacheProvider) ModifyReplicationGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ReplicationGroupId")
	e, err := p.resources.Get(ctx, rtReplicationGroup, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ReplicationGroupNotFoundFault", Message: "Replication group not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var rg replicationGroup
	json.Unmarshal(e.Data, &rg)
	if v := strParam(nr.Params, "ReplicationGroupDescription"); v != "" {
		rg.Description = v
	}
	data, _ := json.Marshal(rg)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtReplicationGroup, ID: id, Data: data})
	return provider.OK(map[string]any{"ReplicationGroupModified": rg.toWire()}), nil
}

func (p *CacheProvider) DeleteReplicationGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ReplicationGroupId")
	e, err := p.resources.Get(ctx, rtReplicationGroup, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ReplicationGroupNotFoundFault", Message: "Replication group not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var rg replicationGroup
	json.Unmarshal(e.Data, &rg)
	rg.Status = "deleting"
	p.resources.Delete(ctx, rtReplicationGroup, id)
	return provider.OK(map[string]any{"ReplicationGroupDeleted": rg.toWire()}), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
