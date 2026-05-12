// Package container implements the ECS provider (ContainerProvider).
package container

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ContainerProvider handles ECS clusters, task definitions, services, and tasks.
type ContainerProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *ContainerProvider {
	return &ContainerProvider{resources: resources}
}

func (p *ContainerProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ECS.CreateCluster":             p.CreateCluster,
		"ECS.DescribeClusters":          p.DescribeClusters,
		"ECS.DeleteCluster":             p.DeleteCluster,
		"ECS.ListClusters":              p.ListClusters,
		"ECS.RegisterTaskDefinition":    p.RegisterTaskDefinition,
		"ECS.DescribeTaskDefinition":    p.DescribeTaskDefinition,
		"ECS.DeregisterTaskDefinition":  p.DeregisterTaskDefinition,
		"ECS.ListTaskDefinitions":       p.ListTaskDefinitions,
		"ECS.CreateService":             p.CreateService,
		"ECS.DescribeServices":          p.DescribeServices,
		"ECS.UpdateService":             p.UpdateService,
		"ECS.DeleteService":             p.DeleteService,
		"ECS.ListServices":              p.ListServices,
		"ECS.RunTask":                   p.RunTask,
		"ECS.DescribeTasks":             p.DescribeTasks,
		"ECS.StopTask":                  p.StopTask,
		"ECS.ListTasks":                 p.ListTasks,
		// Tagging (14.1)
		"ECS.TagResource":               p.TagResource,
		"ECS.UntagResource":             p.UntagResource,
		"ECS.ListTagsForResource":       p.ListTagsForResource,
		// ExecuteCommand stub (14.1)
		"ECS.ExecuteCommand":            p.ExecuteCommand,
	}
}

const (
	rtCluster        = "ecs_cluster"
	rtTaskDefinition = "ecs_task_definition"
	rtService        = "ecs_service"
	rtTask           = "ecs_task"
	rtECSTags        = "ecs_tags"
)

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%016x", b)
}

// ─── Clusters ─────────────────────────────────────────────────────────────────

type cluster struct {
	ClusterName              string `json:"clusterName"`
	ClusterArn               string `json:"clusterArn"`
	Status                   string `json:"status"`
	RunningTasksCount        int    `json:"runningTasksCount"`
	PendingTasksCount        int    `json:"pendingTasksCount"`
	ActiveServicesCount      int    `json:"activeServicesCount"`
	RegisteredContainerInstancesCount int `json:"registeredContainerInstancesCount"`
}

func (c cluster) toWire() map[string]any {
	return map[string]any{
		"clusterName":              c.ClusterName,
		"clusterArn":               c.ClusterArn,
		"status":                   c.Status,
		"runningTasksCount":        c.RunningTasksCount,
		"pendingTasksCount":        c.PendingTasksCount,
		"activeServicesCount":      c.ActiveServicesCount,
		"registeredContainerInstancesCount": c.RegisteredContainerInstancesCount,
	}
}

func (p *ContainerProvider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["clusterName"].(string)
	if name == "" {
		name = "default"
	}
	c := cluster{
		ClusterName: name,
		ClusterArn:  fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:cluster/%s", name),
		Status:      "ACTIVE",
	}
	data, _ := json.Marshal(c)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtCluster, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			// Return existing cluster (idempotent)
			e, _ := p.resources.Get(ctx, rtCluster, name)
			var existing cluster
			json.Unmarshal(e.Data, &existing)
			return provider.OK(map[string]any{"cluster": existing.toWire()}), nil
		}
		return nil, err
	}
	if rawTags, ok := nr.Params["tags"].([]any); ok && len(rawTags) > 0 {
		tags := p.loadECSTags(ctx, c.ClusterArn)
		for _, t := range rawTags {
			if m, ok := t.(map[string]any); ok {
				if k, ok := m["key"].(string); ok {
					if v, ok := m["value"].(string); ok {
						tags[k] = v
					}
				}
			}
		}
		p.saveECSTags(ctx, c.ClusterArn, tags)
	}
	return provider.OK(map[string]any{"cluster": c.toWire()}), nil
}

func (p *ContainerProvider) DescribeClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusters := []map[string]any{}
	failures := []map[string]any{}

	names := extractStringList(nr.Params, "clusters")
	if len(names) == 0 {
		// List all
		entries, err := p.resources.List(ctx, rtCluster, "")
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			var c cluster
			json.Unmarshal(e.Data, &c)
			clusters = append(clusters, c.toWire())
		}
	} else {
		for _, name := range names {
			// strip arn prefix if needed
			if len(name) > 32 {
				parts := splitARN(name)
				name = parts
			}
			e, err := p.resources.Get(ctx, rtCluster, name)
			if err == store.ErrNotFound {
				failures = append(failures, map[string]any{"arn": name, "reason": "MISSING"})
				continue
			}
			var c cluster
			json.Unmarshal(e.Data, &c)
			clusters = append(clusters, c.toWire())
		}
	}
	return provider.OK(map[string]any{"clusters": clusters, "failures": failures}), nil
}

func (p *ContainerProvider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["cluster"].(string)
	name = splitARN(name)
	e, err := p.resources.Get(ctx, rtCluster, name)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ClusterNotFoundException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	var c cluster
	json.Unmarshal(e.Data, &c)
	c.Status = "INACTIVE"
	p.resources.Delete(ctx, rtCluster, name)
	return provider.OK(map[string]any{"cluster": c.toWire()}), nil
}

func (p *ContainerProvider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtCluster, "")
	if err != nil {
		return nil, err
	}
	arns := []string{}
	for _, e := range entries {
		var c cluster
		json.Unmarshal(e.Data, &c)
		arns = append(arns, c.ClusterArn)
	}
	return provider.OK(map[string]any{"clusterArns": arns}), nil
}

// ─── Task Definitions ─────────────────────────────────────────────────────────

type taskDefinition struct {
	Family               string         `json:"family"`
	Revision             int            `json:"revision"`
	TaskDefinitionArn    string         `json:"taskDefinitionArn"`
	Status               string         `json:"status"`
	ContainerDefinitions []map[string]any `json:"containerDefinitions"`
	Cpu                  string         `json:"cpu"`
	Memory               string         `json:"memory"`
	NetworkMode          string         `json:"networkMode"`
	RequiresCompatibilities []string    `json:"requiresCompatibilities"`
}

func (td taskDefinition) toWire() map[string]any {
	return map[string]any{
		"family":               td.Family,
		"revision":             td.Revision,
		"taskDefinitionArn":    td.TaskDefinitionArn,
		"status":               td.Status,
		"containerDefinitions": td.ContainerDefinitions,
		"cpu":                  td.Cpu,
		"memory":               td.Memory,
		"networkMode":          td.NetworkMode,
		"requiresCompatibilities": td.RequiresCompatibilities,
	}
}

func (p *ContainerProvider) RegisterTaskDefinition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	family, _ := nr.Params["family"].(string)
	if family == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "family is required", HTTPStatus: http.StatusBadRequest}
	}

	// Count existing revisions
	entries, _ := p.resources.List(ctx, rtTaskDefinition, family)
	revision := len(entries) + 1

	containers := []map[string]any{}
	if c, ok := nr.Params["containerDefinitions"].([]any); ok {
		for _, item := range c {
			if m, ok := item.(map[string]any); ok {
				containers = append(containers, m)
			}
		}
	}

	cpu, _ := nr.Params["cpu"].(string)
	memory, _ := nr.Params["memory"].(string)
	networkMode, _ := nr.Params["networkMode"].(string)
	if networkMode == "" {
		networkMode = "bridge"
	}
	compat := extractStringList(nr.Params, "requiresCompatibilities")

	td := taskDefinition{
		Family:               family,
		Revision:             revision,
		TaskDefinitionArn:    fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:task-definition/%s:%d", family, revision),
		Status:               "ACTIVE",
		ContainerDefinitions: containers,
		Cpu:                  cpu,
		Memory:               memory,
		NetworkMode:          networkMode,
		RequiresCompatibilities: compat,
	}
	data, _ := json.Marshal(td)
	id := fmt.Sprintf("%s:%d", family, revision)
	p.resources.Create(ctx, store.ResourceEntry{Type: rtTaskDefinition, ID: id, Data: data})
	return provider.OK(map[string]any{"taskDefinition": td.toWire()}), nil
}

func (p *ContainerProvider) DescribeTaskDefinition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tdParam, _ := nr.Params["taskDefinition"].(string)
	id := resolveTaskDefID(tdParam)
	if id == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "taskDefinition is required", HTTPStatus: http.StatusBadRequest}
	}
	e, err := p.resources.Get(ctx, rtTaskDefinition, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ClientException", Message: "Unable to describe task definition", HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return nil, err
	}
	var td taskDefinition
	json.Unmarshal(e.Data, &td)
	return provider.OK(map[string]any{"taskDefinition": td.toWire()}), nil
}

func (p *ContainerProvider) DeregisterTaskDefinition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tdParam, _ := nr.Params["taskDefinition"].(string)
	id := resolveTaskDefID(tdParam)
	e, err := p.resources.Get(ctx, rtTaskDefinition, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ClientException", Message: "Task definition not found", HTTPStatus: http.StatusBadRequest}
	}
	var td taskDefinition
	json.Unmarshal(e.Data, &td)
	td.Status = "INACTIVE"
	data, _ := json.Marshal(td)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtTaskDefinition, ID: id, Data: data})
	return provider.OK(map[string]any{"taskDefinition": td.toWire()}), nil
}

func (p *ContainerProvider) ListTaskDefinitions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	familyPrefix, _ := nr.Params["familyPrefix"].(string)
	entries, err := p.resources.List(ctx, rtTaskDefinition, familyPrefix)
	if err != nil {
		return nil, err
	}
	arns := []string{}
	for _, e := range entries {
		var td taskDefinition
		json.Unmarshal(e.Data, &td)
		arns = append(arns, td.TaskDefinitionArn)
	}
	return provider.OK(map[string]any{"taskDefinitionArns": arns}), nil
}

// ─── Services ─────────────────────────────────────────────────────────────────

type service struct {
	ServiceName    string `json:"serviceName"`
	ServiceArn     string `json:"serviceArn"`
	ClusterArn     string `json:"clusterArn"`
	TaskDefinition string `json:"taskDefinition"`
	DesiredCount   int    `json:"desiredCount"`
	RunningCount   int    `json:"runningCount"`
	PendingCount   int    `json:"pendingCount"`
	Status         string `json:"status"`
}

func (s service) toWire() map[string]any {
	return map[string]any{
		"serviceName":    s.ServiceName,
		"serviceArn":     s.ServiceArn,
		"clusterArn":     s.ClusterArn,
		"taskDefinition": s.TaskDefinition,
		"desiredCount":   s.DesiredCount,
		"runningCount":   s.RunningCount,
		"pendingCount":   s.PendingCount,
		"status":         s.Status,
	}
}

func (p *ContainerProvider) CreateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	svcName, _ := nr.Params["serviceName"].(string)
	if svcName == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "serviceName is required", HTTPStatus: http.StatusBadRequest}
	}
	td, _ := nr.Params["taskDefinition"].(string)
	desired := intParam(nr.Params, "desiredCount")

	svc := service{
		ServiceName:    svcName,
		ServiceArn:     fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:service/%s/%s", clusterName, svcName),
		ClusterArn:     fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:cluster/%s", clusterName),
		TaskDefinition: td,
		DesiredCount:   desired,
		RunningCount:   desired,
		PendingCount:   0,
		Status:         "ACTIVE",
	}
	id := clusterName + "/" + svcName
	data, _ := json.Marshal(svc)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtService, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "Service already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"service": svc.toWire()}), nil
}

func (p *ContainerProvider) DescribeServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	names := extractStringList(nr.Params, "services")
	services := []map[string]any{}
	failures := []map[string]any{}
	for _, name := range names {
		name = splitARN(name)
		id := clusterName + "/" + name
		e, err := p.resources.Get(ctx, rtService, id)
		if err == store.ErrNotFound {
			failures = append(failures, map[string]any{"arn": name, "reason": "MISSING"})
			continue
		}
		var svc service
		json.Unmarshal(e.Data, &svc)
		services = append(services, svc.toWire())
	}
	return provider.OK(map[string]any{"services": services, "failures": failures}), nil
}

func (p *ContainerProvider) UpdateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	svcName, _ := nr.Params["service"].(string)
	svcName = splitARN(svcName)
	id := clusterName + "/" + svcName
	e, err := p.resources.Get(ctx, rtService, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ServiceNotFoundException", Message: "Service not found", HTTPStatus: http.StatusBadRequest}
	}
	var svc service
	json.Unmarshal(e.Data, &svc)
	if v, ok := nr.Params["desiredCount"]; ok {
		svc.DesiredCount = intParam(nr.Params, "desiredCount")
		svc.RunningCount = svc.DesiredCount
		_ = v
	}
	if td, ok := nr.Params["taskDefinition"].(string); ok && td != "" {
		svc.TaskDefinition = td
	}
	data, _ := json.Marshal(svc)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtService, ID: id, Data: data})
	return provider.OK(map[string]any{"service": svc.toWire()}), nil
}

func (p *ContainerProvider) DeleteService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	svcName, _ := nr.Params["service"].(string)
	svcName = splitARN(svcName)
	id := clusterName + "/" + svcName
	e, err := p.resources.Get(ctx, rtService, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ServiceNotFoundException", Message: "Service not found", HTTPStatus: http.StatusBadRequest}
	}
	var svc service
	json.Unmarshal(e.Data, &svc)
	svc.Status = "INACTIVE"
	svc.DesiredCount = 0
	svc.RunningCount = 0
	p.resources.Delete(ctx, rtService, id)
	return provider.OK(map[string]any{"service": svc.toWire()}), nil
}

func (p *ContainerProvider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	entries, err := p.resources.List(ctx, rtService, clusterName)
	if err != nil {
		return nil, err
	}
	arns := []string{}
	for _, e := range entries {
		var svc service
		json.Unmarshal(e.Data, &svc)
		if svc.Status == "ACTIVE" {
			arns = append(arns, svc.ServiceArn)
		}
	}
	return provider.OK(map[string]any{"serviceArns": arns}), nil
}

// ─── Tasks ────────────────────────────────────────────────────────────────────

type task struct {
	TaskArn        string `json:"taskArn"`
	ClusterArn     string `json:"clusterArn"`
	TaskDefinition string `json:"taskDefinition"`
	LastStatus     string `json:"lastStatus"`
	DesiredStatus  string `json:"desiredStatus"`
	Group          string `json:"group"`
}

func (t task) toWire() map[string]any {
	return map[string]any{
		"taskArn":        t.TaskArn,
		"clusterArn":     t.ClusterArn,
		"taskDefinitionArn": t.TaskDefinition,
		"lastStatus":     t.LastStatus,
		"desiredStatus":  t.DesiredStatus,
		"group":          t.Group,
	}
}

func (p *ContainerProvider) RunTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	td, _ := nr.Params["taskDefinition"].(string)
	count := intParam(nr.Params, "count")
	if count == 0 {
		count = 1
	}
	tasks := []map[string]any{}
	for i := 0; i < count; i++ {
		id := newID()
		t := task{
			TaskArn:        fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:task/%s/%s", clusterName, id),
			ClusterArn:     fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:cluster/%s", clusterName),
			TaskDefinition: td,
			LastStatus:     "RUNNING",
			DesiredStatus:  "RUNNING",
		}
		data, _ := json.Marshal(t)
		p.resources.Create(ctx, store.ResourceEntry{Type: rtTask, ID: id, Data: data})
		tasks = append(tasks, t.toWire())
	}
	return provider.OK(map[string]any{"tasks": tasks, "failures": []any{}}), nil
}

func (p *ContainerProvider) DescribeTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	taskIDs := extractStringList(nr.Params, "tasks")
	tasks := []map[string]any{}
	failures := []map[string]any{}
	for _, tid := range taskIDs {
		shortID := splitARN(tid)
		e, err := p.resources.Get(ctx, rtTask, shortID)
		if err == store.ErrNotFound {
			failures = append(failures, map[string]any{"arn": tid, "reason": "MISSING"})
			continue
		}
		var t task
		json.Unmarshal(e.Data, &t)
		tasks = append(tasks, t.toWire())
	}
	return provider.OK(map[string]any{"tasks": tasks, "failures": failures}), nil
}

func (p *ContainerProvider) StopTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	taskID, _ := nr.Params["task"].(string)
	shortID := splitARN(taskID)
	e, err := p.resources.Get(ctx, rtTask, shortID)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "Task not found", HTTPStatus: http.StatusBadRequest}
	}
	var t task
	json.Unmarshal(e.Data, &t)
	t.LastStatus = "STOPPED"
	t.DesiredStatus = "STOPPED"
	data, _ := json.Marshal(t)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtTask, ID: shortID, Data: data})
	return provider.OK(map[string]any{"task": t.toWire()}), nil
}

func (p *ContainerProvider) ListTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	entries, err := p.resources.List(ctx, rtTask, "")
	if err != nil {
		return nil, err
	}
	arns := []string{}
	for _, e := range entries {
		var t task
		json.Unmarshal(e.Data, &t)
		if clusterName == "" || t.ClusterArn == fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:cluster/%s", clusterName) {
			if t.LastStatus != "STOPPED" {
				arns = append(arns, t.TaskArn)
			}
		}
	}
	return provider.OK(map[string]any{"taskArns": arns}), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func clusterParam(params map[string]any) string {
	if v, ok := params["cluster"].(string); ok && v != "" {
		return splitARN(v)
	}
	return "default"
}

func splitARN(s string) string {
	// arn:aws:ecs:...:cluster/name → name
	if idx := lastSlash(s); idx >= 0 {
		return s[idx+1:]
	}
	return s
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func extractStringList(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case []any:
		result := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return t
	case string:
		return []string{t}
	}
	return nil
}

func intParam(params map[string]any, key string) int {
	v, ok := params[key]
	if !ok {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	}
	return 0
}

func resolveTaskDefID(s string) string {
	// Could be "family:revision" or ARN "arn:...:task-definition/family:revision"
	s = splitARN(s)
	return s
}

// ─── Tagging (14.1) ───────────────────────────────────────────────────────────

func (p *ContainerProvider) loadECSTags(ctx context.Context, arn string) map[string]string {
	tags := map[string]string{}
	if e, err := p.resources.Get(ctx, rtECSTags, arn); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	return tags
}

func (p *ContainerProvider) saveECSTags(ctx context.Context, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtECSTags, ID: arn, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		}
	}
}

func (p *ContainerProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "resourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	tags := p.loadECSTags(ctx, arn)
	if rawTags, ok := nr.Params["tags"].([]any); ok {
		for _, t := range rawTags {
			if m, ok := t.(map[string]any); ok {
				k, _ := m["key"].(string)
				v, _ := m["value"].(string)
				if k != "" {
					tags[k] = v
				}
			}
		}
	}
	p.saveECSTags(ctx, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *ContainerProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "resourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	tags := p.loadECSTags(ctx, arn)
	if keys, ok := nr.Params["tagKeys"].([]any); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				delete(tags, s)
			}
		}
	}
	p.saveECSTags(ctx, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *ContainerProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "resourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	tags := p.loadECSTags(ctx, arn)
	tagList := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"key": k, "value": v})
	}
	return provider.OK(map[string]any{"tags": tagList}), nil
}

// ─── ExecuteCommand stub (14.1) ───────────────────────────────────────────────

func (p *ContainerProvider) ExecuteCommand(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	cluster, _ := nr.Params["cluster"].(string)
	task, _ := nr.Params["task"].(string)
	container, _ := nr.Params["container"].(string)
	interactive, _ := nr.Params["interactive"].(bool)

	sessionID := fmt.Sprintf("ecs-exec-%x", randBytes(8))
	return provider.OK(map[string]any{
		"clusterArn":    nr.ResourceID("ecs-cluster", cluster),
		"containerArn":  nr.ResourceID("ecs-container-instance", container),
		"containerName": container,
		"interactive":   interactive,
		"session": map[string]any{
			"sessionId":  sessionID,
			"streamUrl":  "wss://localhost:4566/ecs-exec-stub",
			"tokenValue": "synthetic-token-" + sessionID,
		},
		"taskArn": nr.ResourceID("ecs-task", task),
	}), nil
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
