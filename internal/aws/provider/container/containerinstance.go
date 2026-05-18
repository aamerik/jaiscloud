package container

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtContainerInstance = "ecs_container_instance"

type containerInstance struct {
	ContainerInstanceArn string `json:"containerInstanceArn"`
	EC2InstanceID        string `json:"ec2InstanceId"`
	Status               string `json:"status"`
	AgentConnected       bool   `json:"agentConnected"`
	AgentUpdateStatus    string `json:"agentUpdateStatus,omitempty"`
	ClusterName          string `json:"clusterName"`
}

func (ci containerInstance) toWire() map[string]any {
	return map[string]any{
		"containerInstanceArn": ci.ContainerInstanceArn,
		"ec2InstanceId":        ci.EC2InstanceID,
		"status":               ci.Status,
		"agentConnected":       ci.AgentConnected,
		"agentUpdateStatus":    ci.AgentUpdateStatus,
	}
}

func (p *ContainerProvider) RegisterContainerInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["cluster"].(string)
	if clusterName == "" {
		clusterName = "default"
	}
	clusterName = splitARN(clusterName)
	id := newID()
	arn := nr.ResourceID("ecs-container-instance", clusterName+"/"+id)
	ec2ID, _ := nr.Params["instanceIdentityDocument"].(string)
	ci := containerInstance{
		ContainerInstanceArn: arn,
		EC2InstanceID:        ec2ID,
		Status:               "ACTIVE",
		AgentConnected:       true,
		ClusterName:          clusterName,
	}
	data, _ := json.Marshal(ci)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtContainerInstance, ID: arn, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"containerInstance": ci.toWire()}), nil
}

func (p *ContainerProvider) DeregisterContainerInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["containerInstance"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtContainerInstance, arn)
	if err != nil {
		return nil, model.NewProviderError("InvalidParameterException", "Container instance not found", 400)
	}
	var ci containerInstance
	json.Unmarshal(e.Data, &ci)
	ci.Status = "INACTIVE"
	ci.AgentConnected = false
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtContainerInstance, arn)
	return provider.OK(map[string]any{"containerInstance": ci.toWire()}), nil
}

func (p *ContainerProvider) DescribeContainerInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arns := extractStringList(nr.Params, "containerInstances")
	instances := []map[string]any{}
	failures := []map[string]any{}
	for _, arn := range arns {
		e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtContainerInstance, arn)
		if err != nil {
			failures = append(failures, map[string]any{"arn": arn, "reason": "MISSING"})
			continue
		}
		var ci containerInstance
		if json.Unmarshal(e.Data, &ci) == nil {
			instances = append(instances, ci.toWire())
		}
	}
	if len(arns) == 0 {
		entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtContainerInstance, "")
		for _, e := range entries {
			var ci containerInstance
			if json.Unmarshal(e.Data, &ci) == nil {
				instances = append(instances, ci.toWire())
			}
		}
	}
	return provider.OK(map[string]any{"containerInstances": instances, "failures": failures}), nil
}

func (p *ContainerProvider) ListContainerInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtContainerInstance, "")
	arns := make([]string, 0, len(entries))
	for _, e := range entries {
		arns = append(arns, e.ID)
	}
	return provider.OK(map[string]any{"containerInstanceArns": arns}), nil
}

func (p *ContainerProvider) UpdateContainerInstancesState(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arns := extractStringList(nr.Params, "containerInstances")
	status, _ := nr.Params["status"].(string)
	updated := []map[string]any{}
	failures := []map[string]any{}
	for _, arn := range arns {
		e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtContainerInstance, arn)
		if err != nil {
			failures = append(failures, map[string]any{"arn": arn, "reason": "MISSING"})
			continue
		}
		var ci containerInstance
		json.Unmarshal(e.Data, &ci)
		ci.Status = status
		data, _ := json.Marshal(ci)
		_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtContainerInstance, ID: arn, Data: data})
		updated = append(updated, ci.toWire())
	}
	return provider.OK(map[string]any{"containerInstances": updated, "failures": failures}), nil
}

func (p *ContainerProvider) UpdateContainerAgent(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["containerInstance"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtContainerInstance, arn)
	if err != nil {
		return nil, model.NewProviderError("InvalidParameterException", "Container instance not found", 400)
	}
	var ci containerInstance
	json.Unmarshal(e.Data, &ci)
	ci.AgentUpdateStatus = "UPDATED"
	data, _ := json.Marshal(ci)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtContainerInstance, ID: arn, Data: data})
	return provider.OK(map[string]any{"containerInstance": ci.toWire()}), nil
}
