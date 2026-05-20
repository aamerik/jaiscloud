package container

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtTaskSet = "ecs_task_set"

type taskSet struct {
	ID           string         `json:"id"`
	Arn          string         `json:"taskSetArn"`
	ClusterArn   string         `json:"clusterArn"`
	ServiceArn   string         `json:"serviceArn"`
	Status       string         `json:"status"`
	TaskDef      string         `json:"taskDefinition"`
	DesiredCount int            `json:"desiredCount"`
	ExternalID   string         `json:"externalId"`
	Extra        map[string]any `json:"extra,omitempty"`
}

func (t taskSet) toWire() map[string]any {
	return map[string]any{
		"id":             t.ID,
		"taskSetArn":     t.Arn,
		"clusterArn":     t.ClusterArn,
		"serviceArn":     t.ServiceArn,
		"status":         t.Status,
		"taskDefinition": t.TaskDef,
		"desiredCount":   t.DesiredCount,
		"externalId":     t.ExternalID,
	}
}

func (p *ContainerProvider) CreateTaskSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["cluster"].(string)
	clusterName = splitARN(clusterName)
	svcName, _ := nr.Params["service"].(string)
	svcName = splitARN(svcName)
	id := newID()
	arn := nr.ResourceID("ecs-task-set", clusterName+"/"+svcName+"/"+id)
	ts := taskSet{
		ID:         id,
		Arn:        arn,
		ClusterArn: nr.ResourceID("ecs-cluster", clusterName),
		ServiceArn: nr.ResourceID("ecs-service", svcName),
		Status:     "ACTIVE",
	}
	if td, ok := nr.Params["taskDefinition"].(string); ok {
		ts.TaskDef = td
	}
	if dc, ok := nr.Params["desiredCount"].(float64); ok {
		ts.DesiredCount = int(dc)
	}
	data, _ := json.Marshal(ts)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTaskSet, ID: arn, Data: data}); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"taskSet": ts.toWire()}}, nil
}

func (p *ContainerProvider) DescribeTaskSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	taskSets := []map[string]any{}
	ids := extractStringList(nr.Params, "taskSets")
	if len(ids) == 0 {
		entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtTaskSet, "")
		for _, e := range entries {
			var ts taskSet
			if json.Unmarshal(e.Data, &ts) == nil {
				taskSets = append(taskSets, ts.toWire())
			}
		}
	} else {
		for _, id := range ids {
			e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTaskSet, id)
			if err != nil {
				continue
			}
			var ts taskSet
			if json.Unmarshal(e.Data, &ts) == nil {
				taskSets = append(taskSets, ts.toWire())
			}
		}
	}
	return provider.OK(map[string]any{"taskSets": taskSets, "failures": []any{}}), nil
}

func (p *ContainerProvider) UpdateTaskSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tsArn, _ := nr.Params["taskSet"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTaskSet, tsArn)
	if err != nil {
		return nil, model.NewProviderError("TaskSetNotFoundException", "Task set not found", 400)
	}
	var ts taskSet
	json.Unmarshal(e.Data, &ts)
	if v, ok := nr.Params["scale"].(map[string]any); ok {
		_ = v // store scale info as extra
		ts.Extra = v
	}
	data, _ := json.Marshal(ts)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTaskSet, ID: tsArn, Data: data})
	return provider.OK(map[string]any{"taskSet": ts.toWire()}), nil
}

func (p *ContainerProvider) DeleteTaskSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tsArn, _ := nr.Params["taskSet"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTaskSet, tsArn)
	if err != nil {
		return nil, model.NewProviderError("TaskSetNotFoundException", "Task set not found", 400)
	}
	var ts taskSet
	json.Unmarshal(e.Data, &ts)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTaskSet, tsArn)
	return provider.OK(map[string]any{"taskSet": ts.toWire()}), nil
}

func (p *ContainerProvider) UpdateServicePrimaryTaskSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tsArn, _ := nr.Params["primaryTaskSet"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTaskSet, tsArn)
	if err != nil {
		return nil, model.NewProviderError("TaskSetNotFoundException", "Task set not found", 400)
	}
	var ts taskSet
	json.Unmarshal(e.Data, &ts)
	ts.Status = "PRIMARY"
	data, _ := json.Marshal(ts)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTaskSet, ID: tsArn, Data: data})
	return provider.OK(map[string]any{"taskSet": ts.toWire()}), nil
}
