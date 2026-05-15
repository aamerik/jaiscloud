// Package eks implements a minimal EKS provider for the JaisCloud emulator.
package eks

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtCluster   = "eks_cluster"
	rtNodegroup = "eks_nodegroup"
	rtAddon     = "eks_addon"
)

// EKSProvider handles EKS cluster CRUD operations.
type EKSProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *EKSProvider {
	return &EKSProvider{resources: resources}
}

func (p *EKSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"EKS.CreateCluster":   p.CreateCluster,
		"EKS.DescribeCluster": p.DescribeCluster,
		"EKS.DeleteCluster":   p.DeleteCluster,
		"EKS.ListClusters":    p.ListClusters,
		// Nodegroups (14.2)
		"EKS.CreateNodegroup":        p.CreateNodegroup,
		"EKS.DescribeNodegroup":      p.DescribeNodegroup,
		"EKS.ListNodegroups":         p.ListNodegroups,
		"EKS.DeleteNodegroup":        p.DeleteNodegroup,
		"EKS.UpdateNodegroupConfig":  p.UpdateNodegroupConfig,
		"EKS.UpdateNodegroupVersion": p.UpdateNodegroupVersion,
		// Addons (14.3)
		"EKS.CreateAddon":   p.CreateAddon,
		"EKS.DescribeAddon": p.DescribeAddon,
		"EKS.ListAddons":    p.ListAddons,
		"EKS.DeleteAddon":   p.DeleteAddon,
		"EKS.UpdateAddon":   p.UpdateAddon,
	}
}

type eksCluster struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Arn       string    `json:"arn"`
	CreatedAt time.Time `json:"createdAt"`
}

func (p *EKSProvider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	c := eksCluster{
		Name:      name,
		Status:    "ACTIVE",
		Arn:       nr.ResourceID("eks-cluster", name),
		CreatedAt: time.Now().UTC(),
	}
	data, _ := json.Marshal(c)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtCluster, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "ResourceInUseException", Message: "cluster " + name + " already exists", HTTPStatus: http.StatusConflict}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"cluster": clusterToWire(c)}), nil
}

func (p *EKSProvider) DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	e, err := p.resources.Get(ctx, rtCluster, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "cluster " + name + " not found", HTTPStatus: http.StatusNotFound}
	}
	var c eksCluster
	_ = json.Unmarshal(e.Data, &c)
	return provider.OK(map[string]any{"cluster": clusterToWire(c)}), nil
}

func (p *EKSProvider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	e, err := p.resources.Get(ctx, rtCluster, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "cluster " + name + " not found", HTTPStatus: http.StatusNotFound}
	}
	var c eksCluster
	_ = json.Unmarshal(e.Data, &c)
	_ = p.resources.Delete(ctx, rtCluster, name)
	return provider.OK(map[string]any{"cluster": clusterToWire(c)}), nil
}

func (p *EKSProvider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtCluster, "")
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		var c eksCluster
		if json.Unmarshal(e.Data, &c) == nil {
			names = append(names, c.Name)
		}
	}
	maxResults := 100
	if v, ok := nr.Params["maxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["nextToken"].(string)
	page, next, pgErr := pagination.Paginate(names, maxResults, token, "ListClusters")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterException", pgErr.Error(), 400)
	}
	data := map[string]any{"clusters": page}
	if next != "" {
		data["nextToken"] = next
	}
	return provider.OK(data), nil
}

func clusterToWire(c eksCluster) map[string]any {
	return map[string]any{
		"name":      c.Name,
		"status":    c.Status,
		"arn":       c.Arn,
		"createdAt": c.CreatedAt.UTC().Format(time.RFC3339),
	}
}
