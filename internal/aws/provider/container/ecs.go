// Package container implements the ECS provider (ContainerProvider).
package container

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	ecsexec "jaiscloud/internal/executor/ecs"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ContainerProvider handles ECS clusters, task definitions, services, and tasks.
type ContainerProvider struct {
	resources         store.ResourceStore
	executor          ecsexec.Executor
	jaisCloudEndpoint string
	mu                sync.Mutex
	handles           map[string]ecsexec.TaskHandle // taskShortID → handle
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
}

func New(resources store.ResourceStore) *ContainerProvider {
	ctx, cancel := context.WithCancel(context.Background())
	return &ContainerProvider{
		resources: resources,
		executor:  &ecsexec.MockExecutor{},
		handles:   make(map[string]ecsexec.TaskHandle),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// SetExecutor replaces the container backend used for RunTask/StopTask.
func (p *ContainerProvider) SetExecutor(e ecsexec.Executor) {
	p.executor = e
}

// SetJaisCloudEndpoint sets the endpoint injected as AWS_ENDPOINT_URL in task containers.
func (p *ContainerProvider) SetJaisCloudEndpoint(ep string) {
	p.jaisCloudEndpoint = ep
}

// Shutdown waits for all background task-watcher goroutines to finish.
func (p *ContainerProvider) Shutdown() {
	p.cancel()
	p.wg.Wait()
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
		// UpdateCluster / StartTask
		"ECS.UpdateCluster": p.UpdateCluster,
		"ECS.StartTask":     p.StartTask,
		// TaskSet CRUD
		"ECS.CreateTaskSet":                p.CreateTaskSet,
		"ECS.DescribeTaskSets":             p.DescribeTaskSets,
		"ECS.UpdateTaskSet":                p.UpdateTaskSet,
		"ECS.DeleteTaskSet":                p.DeleteTaskSet,
		"ECS.UpdateServicePrimaryTaskSet":  p.UpdateServicePrimaryTaskSet,
		// ContainerInstance
		"ECS.RegisterContainerInstance":     p.RegisterContainerInstance,
		"ECS.DeregisterContainerInstance":   p.DeregisterContainerInstance,
		"ECS.DescribeContainerInstances":    p.DescribeContainerInstances,
		"ECS.ListContainerInstances":        p.ListContainerInstances,
		"ECS.UpdateContainerInstancesState": p.UpdateContainerInstancesState,
		"ECS.UpdateContainerAgent":          p.UpdateContainerAgent,
		// CapacityProvider
		"ECS.CreateCapacityProvider":        p.CreateCapacityProvider,
		"ECS.DescribeCapacityProviders":     p.DescribeCapacityProviders,
		"ECS.DeleteCapacityProvider":        p.DeleteCapacityProvider,
		"ECS.PutClusterCapacityProviders":   p.PutClusterCapacityProviders,
		"ECS.UpdateCapacityProvider":        p.UpdateCapacityProvider,
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
		ClusterArn:  nr.ResourceID("ecs-cluster", name),
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
		TaskDefinitionArn:    nr.ResourceID("ecs-task-definition", fmt.Sprintf("%s:%d", family, revision)),
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
	if max := intParam(nr.Params, "maxResults"); max > 0 && max < len(arns) {
		arns = arns[:max]
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
		ServiceArn:     nr.ResourceID("ecs-service", clusterName+"/"+svcName),
		ClusterArn:     nr.ResourceID("ecs-cluster", clusterName),
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
	TaskArn        string           `json:"taskArn"`
	ClusterArn     string           `json:"clusterArn"`
	TaskDefinition string           `json:"taskDefinition"`
	LastStatus     string           `json:"lastStatus"`
	DesiredStatus  string           `json:"desiredStatus"`
	Group          string           `json:"group"`
	Containers     []taskContainer  `json:"containers,omitempty"`
}

type taskContainer struct {
	Name       string `json:"name"`
	LastStatus string `json:"lastStatus"`
	ExitCode   *int   `json:"exitCode,omitempty"`
}

func (t task) toWire() map[string]any {
	containers := make([]map[string]any, 0, len(t.Containers))
	for _, c := range t.Containers {
		m := map[string]any{
			"name":       c.Name,
			"lastStatus": c.LastStatus,
		}
		if c.ExitCode != nil {
			m["exitCode"] = *c.ExitCode
		}
		containers = append(containers, m)
	}
	return map[string]any{
		"taskArn":           t.TaskArn,
		"clusterArn":        t.ClusterArn,
		"taskDefinitionArn": t.TaskDefinition,
		"lastStatus":        t.LastStatus,
		"desiredStatus":     t.DesiredStatus,
		"group":             t.Group,
		"containers":        containers,
	}
}

func (p *ContainerProvider) RunTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName := clusterParam(nr.Params)
	tdParam, _ := nr.Params["taskDefinition"].(string)
	count := intParam(nr.Params, "count")
	if count == 0 {
		count = 1
	}

	// Resolve task definition.
	tdID := resolveTaskDefID(tdParam)
	var td taskDefinition
	if e, err := p.resources.Get(ctx, rtTaskDefinition, tdID); err == nil {
		json.Unmarshal(e.Data, &td)
	}

	tasks := []map[string]any{}
	for i := 0; i < count; i++ {
		id := newID()
		taskARN := nr.ResourceID("ecs-task", clusterName+"/"+id)

		var containers []taskContainer
		for _, cd := range td.ContainerDefinitions {
			name, _ := cd["name"].(string)
			containers = append(containers, taskContainer{Name: name, LastStatus: "PROVISIONING"})
		}

		t := task{
			TaskArn:        taskARN,
			ClusterArn:     nr.ResourceID("ecs-cluster", clusterName),
			TaskDefinition: tdParam,
			LastStatus:     "PROVISIONING",
			DesiredStatus:  "RUNNING",
			Containers:     containers,
		}
		data, _ := json.Marshal(t)
		p.resources.Create(ctx, store.ResourceEntry{Type: rtTask, ID: id, Data: data})

		spec := p.buildTaskSpec(nr, clusterName, taskARN, td)
		handle, err := p.executor.Run(ctx, spec)
		if err != nil {
			t.LastStatus = "STOPPED"
			t.DesiredStatus = "STOPPED"
			data, _ = json.Marshal(t)
			p.resources.Update(ctx, store.ResourceEntry{Type: rtTask, ID: id, Data: data})
			slog.Warn("ecs: run task failed", "task", id, "err", err)
		} else {
			// Mock handles advance immediately to RUNNING so callers see a stable
			// pre-STOPPED state; real executors start at PROVISIONING and the
			// watcher advances the status as containers start.
			if handle.Mode == ecsexec.ModeMock {
				t.LastStatus = "RUNNING"
				for i := range t.Containers {
					t.Containers[i].LastStatus = "RUNNING"
				}
				data, _ = json.Marshal(t)
				p.resources.Update(ctx, store.ResourceEntry{Type: rtTask, ID: id, Data: data})
			}
			p.mu.Lock()
			p.handles[id] = handle
			p.mu.Unlock()
			p.wg.Add(1)
			go p.watchTask(id, taskARN, handle)
		}

		tasks = append(tasks, t.toWire())
	}
	return provider.OK(map[string]any{"tasks": tasks, "failures": []any{}}), nil
}

// buildTaskSpec constructs an ecsexec.TaskSpec from a resolved task definition.
func (p *ContainerProvider) buildTaskSpec(nr *model.NormalizedRequest, clusterName, taskARN string, td taskDefinition) ecsexec.TaskSpec {
	awsCreds := map[string]string{
		"AWS_ACCESS_KEY_ID":     nr.AccountID,
		"AWS_SECRET_ACCESS_KEY": "test",
		"AWS_SESSION_TOKEN":     "test",
		"AWS_REGION":            nr.Region,
	}
	if p.jaisCloudEndpoint != "" {
		awsCreds["AWS_ENDPOINT_URL"] = p.jaisCloudEndpoint
	}

	var containers []ecsexec.ContainerSpec
	for _, cd := range td.ContainerDefinitions {
		name, _ := cd["name"].(string)
		image, _ := cd["image"].(string)
		env := make(map[string]string)
		// Inject AWS creds first; user env vars can override below.
		for k, v := range awsCreds {
			env[k] = v
		}
		if rawEnv, ok := cd["environment"].([]any); ok {
			for _, item := range rawEnv {
				if m, ok := item.(map[string]any); ok {
					k, _ := m["name"].(string)
					v, _ := m["value"].(string)
					if k != "" {
						env[k] = v
					}
				}
			}
		}

		var mem, cpu int64
		if v, ok := cd["memory"]; ok {
			mem = toMB(v) * 1024 * 1024
		}
		if v, ok := cd["cpu"]; ok {
			cpu = toInt64(v)
		}

		var portMappings []ecsexec.PortMapping
		if raw, ok := cd["portMappings"].([]any); ok {
			for _, pm := range raw {
				if m, ok := pm.(map[string]any); ok {
					portMappings = append(portMappings, ecsexec.PortMapping{
						ContainerPort: int(toInt64(m["containerPort"])),
						HostPort:      int(toInt64(m["hostPort"])),
						Protocol:      stringOrDefault(m["protocol"], "tcp"),
					})
				}
			}
		}

		var logCfg ecsexec.LogConfig
		if rawLog, ok := cd["logConfiguration"].(map[string]any); ok {
			logCfg.LogDriver, _ = rawLog["logDriver"].(string)
			if opts, ok := rawLog["options"].(map[string]any); ok {
				logCfg.Options = make(map[string]string)
				for k, v := range opts {
					logCfg.Options[k], _ = v.(string)
				}
			}
		}

		containers = append(containers, ecsexec.ContainerSpec{
			Name:         name,
			Image:        image,
			Env:          env,
			Memory:       mem,
			CPU:          cpu,
			PortMappings: portMappings,
			LogConfig:    logCfg,
		})
	}

	return ecsexec.TaskSpec{
		ClusterName:       clusterName,
		TaskARN:           taskARN,
		TaskDefName:       td.Family,
		Containers:        containers,
		AccountID:         nr.AccountID,
		Region:            nr.Region,
		JaisCloudEndpoint: p.jaisCloudEndpoint,
	}
}

// watchTask polls the executor every 2s and updates the persisted task record.
func (p *ContainerProvider) watchTask(shortID, taskARN string, handle ecsexec.TaskHandle) {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}

		st, err := p.executor.StatusOf(p.ctx, handle)
		if err != nil {
			slog.Warn("ecs: status poll error", "task", shortID, "err", err)
			continue
		}

		e, getErr := p.resources.Get(p.ctx, rtTask, shortID)
		if getErr != nil {
			return
		}
		var t task
		json.Unmarshal(e.Data, &t)
		t.LastStatus = st.LastStatus

		// Sync per-container statuses.
		for i := range t.Containers {
			for _, cst := range st.Containers {
				if cst.Name == t.Containers[i].Name {
					t.Containers[i].LastStatus = cst.LastStatus
					t.Containers[i].ExitCode = cst.ExitCode
					break
				}
			}
		}

		data, _ := json.Marshal(t)
		p.resources.Update(p.ctx, store.ResourceEntry{Type: rtTask, ID: shortID, Data: data})

		if st.LastStatus == "STOPPED" {
			p.mu.Lock()
			delete(p.handles, shortID)
			p.mu.Unlock()
			return
		}
	}
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

	p.mu.Lock()
	handle, hasHandle := p.handles[shortID]
	p.mu.Unlock()
	if hasHandle {
		p.executor.Stop(ctx, handle) //nolint:errcheck
	}

	t.LastStatus = "STOPPED"
	t.DesiredStatus = "STOPPED"
	data, _ := json.Marshal(t)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtTask, ID: shortID, Data: data})
	return provider.OK(map[string]any{"task": t.toWire()}), nil
}

// ─── numeric helpers ──────────────────────────────────────────────────────────

func toInt64(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int:
		return int64(t)
	case int64:
		return t
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	}
	return 0
}

func toMB(v any) int64 { return toInt64(v) }

func stringOrDefault(v any, def string) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return def
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
		if clusterName == "" || t.ClusterArn == nr.ResourceID("ecs-cluster", clusterName) {
			if t.LastStatus != "STOPPED" {
				arns = append(arns, t.TaskArn)
			}
		}
	}
	return provider.OK(map[string]any{"taskArns": arns}), nil
}

func (p *ContainerProvider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["cluster"].(string)
	name = splitARN(name)
	e, err := p.resources.Get(ctx, rtCluster, name)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ClusterNotFoundException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	var c cluster
	json.Unmarshal(e.Data, &c)
	// Apply any settings updates (stub — persist unchanged).
	data, _ := json.Marshal(c)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtCluster, ID: name, Data: data})
	return provider.OK(map[string]any{"cluster": c.toWire()}), nil
}

func (p *ContainerProvider) StartTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// StartTask is like RunTask but for specific container instances.
	return p.RunTask(ctx, nr)
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
