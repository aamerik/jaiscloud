package container

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtCapacityProvider = "ecs_capacity_provider"

type capacityProvider struct {
	Name   string           `json:"name"`
	Arn    string           `json:"capacityProviderArn"`
	Status string           `json:"status"`
	Tags   []map[string]any `json:"tags,omitempty"`
	Extra  map[string]any   `json:"extra,omitempty"`
}

func (cp capacityProvider) toWire() map[string]any {
	return map[string]any{
		"name":                cp.Name,
		"capacityProviderArn": cp.Arn,
		"status":              cp.Status,
		"tags":                cp.Tags,
	}
}

func (p *ContainerProvider) CreateCapacityProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	if name == "" {
		return nil, model.NewProviderError("InvalidParameterException", "name is required", 400)
	}
	arn := nr.ResourceID("ecs-capacity-provider", name)
	cp := capacityProvider{
		Name:   name,
		Arn:    arn,
		Status: "ACTIVE",
	}
	if extra, ok := nr.Params["autoScalingGroupProvider"].(map[string]any); ok {
		cp.Extra = extra
	}
	data, _ := json.Marshal(cp)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtCapacityProvider, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("InvalidParameterException", "Capacity provider already exists: "+name, 400)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"capacityProvider": cp.toWire()}}, nil
}

func (p *ContainerProvider) DescribeCapacityProviders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names := extractStringList(nr.Params, "capacityProviders")
	cps := []map[string]any{}
	failures := []map[string]any{}
	if len(names) == 0 {
		entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtCapacityProvider, "")
		for _, e := range entries {
			var cp capacityProvider
			if json.Unmarshal(e.Data, &cp) == nil {
				cps = append(cps, cp.toWire())
			}
		}
	} else {
		for _, name := range names {
			e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCapacityProvider, name)
			if err != nil {
				failures = append(failures, map[string]any{"reason": "MISSING", "detail": name})
				continue
			}
			var cp capacityProvider
			if json.Unmarshal(e.Data, &cp) == nil {
				cps = append(cps, cp.toWire())
			}
		}
	}
	return provider.OK(map[string]any{"capacityProviders": cps, "failures": failures}), nil
}

func (p *ContainerProvider) DeleteCapacityProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["capacityProvider"].(string)
	name = splitARN(name)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCapacityProvider, name)
	if err != nil {
		return nil, model.NewProviderError("InvalidParameterException", "Capacity provider not found: "+name, 400)
	}
	var cp capacityProvider
	json.Unmarshal(e.Data, &cp)
	cp.Status = "INACTIVE"
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtCapacityProvider, name)
	return provider.OK(map[string]any{"capacityProvider": cp.toWire()}), nil
}

func (p *ContainerProvider) PutClusterCapacityProviders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["cluster"].(string)
	clusterName = splitARN(clusterName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCluster, clusterName)
	if err != nil {
		return nil, model.NewProviderError("ClusterNotFoundException", "Cluster not found", 400)
	}
	var c cluster
	json.Unmarshal(e.Data, &c)
	data, _ := json.Marshal(c)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtCluster, ID: clusterName, Data: data})
	return provider.OK(map[string]any{"cluster": c.toWire()}), nil
}

func (p *ContainerProvider) UpdateCapacityProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCapacityProvider, name)
	if err != nil {
		return nil, model.NewProviderError("InvalidParameterException", "Capacity provider not found: "+name, 400)
	}
	var cp capacityProvider
	json.Unmarshal(e.Data, &cp)
	if extra, ok := nr.Params["autoScalingGroupProvider"].(map[string]any); ok {
		cp.Extra = extra
	}
	data, _ := json.Marshal(cp)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtCapacityProvider, ID: name, Data: data})
	return provider.OK(map[string]any{"capacityProvider": cp.toWire()}), nil
}
