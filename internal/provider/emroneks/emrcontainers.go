// Package emroneks implements the EMR on EKS provider.
// Wire protocol: REST JSON, SigV4 service name: emr-containers
// Paths: /virtualclusters, /virtualclusters/{id}/jobruns, /virtualclusters/{id}/endpoints
package emroneks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/executor/spark"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// jobRef identifies the store resource a Spark job maps to and carries the
// request context needed to publish EventBus events from async callbacks.
type jobRef struct {
	vcID      string
	jrID      string
	region    string
	accountID string
	cloud     model.Cloud
}

// EMRContainersProvider handles EMR on EKS: virtual clusters, job runs, managed endpoints.
type EMRContainersProvider struct {
	resources   store.ResourceStore
	bus         *events.EventBus
	executor    spark.SparkExecutor // nil = instant completion
	executorCfg spark.SparkConfig
	poller      *spark.StatusPoller
	jobRefs     sync.Map // sparkJobID → jobRef
}

// Option configures EMRContainersProvider.
type Option func(*EMRContainersProvider)

// WithExecutor attaches a SparkExecutor and its config.
func WithExecutor(ex spark.SparkExecutor, cfg spark.SparkConfig) Option {
	return func(p *EMRContainersProvider) { p.executor = ex; p.executorCfg = cfg }
}

// WithPoller attaches a StatusPoller.
func WithPoller(pol *spark.StatusPoller) Option {
	return func(p *EMRContainersProvider) { p.poller = pol }
}

// SetPoller sets the poller after construction.
func (p *EMRContainersProvider) SetPoller(pol *spark.StatusPoller) { p.poller = pol }

func New(resources store.ResourceStore, bus *events.EventBus, opts ...Option) *EMRContainersProvider {
	p := &EMRContainersProvider{resources: resources, bus: bus}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *EMRContainersProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Virtual clusters
		"EMRContainers.CreateVirtualCluster":  p.CreateVirtualCluster,
		"EMRContainers.DeleteVirtualCluster":  p.DeleteVirtualCluster,
		"EMRContainers.DescribeVirtualCluster": p.DescribeVirtualCluster,
		"EMRContainers.ListVirtualClusters":   p.ListVirtualClusters,
		// Job runs
		"EMRContainers.StartJobRun":    p.StartJobRun,
		"EMRContainers.CancelJobRun":   p.CancelJobRun,
		"EMRContainers.DescribeJobRun": p.DescribeJobRun,
		"EMRContainers.ListJobRuns":    p.ListJobRuns,
		// Managed endpoints
		"EMRContainers.CreateManagedEndpoint":  p.CreateManagedEndpoint,
		"EMRContainers.DeleteManagedEndpoint":  p.DeleteManagedEndpoint,
		"EMRContainers.DescribeManagedEndpoint": p.DescribeManagedEndpoint,
		"EMRContainers.ListManagedEndpoints":   p.ListManagedEndpoints,
		// Tagging
		"EMRContainers.TagResource":         p.TagResource,
		"EMRContainers.UntagResource":        p.UntagResource,
		"EMRContainers.ListTagsForResource":  p.ListTagsForResource,
	}
}

const (
	rtVirtualCluster   = "emrc_virtual_cluster"
	rtJobRun           = "emrc_job_run"
	rtManagedEndpoint  = "emrc_managed_endpoint"
)

// ─── Virtual Cluster types ────────────────────────────────────────────────────

type virtualCluster struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	State       string            `json:"state"` // RUNNING, TERMINATING, TERMINATED, ARRESTED
	EksCluster  string            `json:"eksCluster"`
	Namespace   string            `json:"namespace"`
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
}

func (vc virtualCluster) toWire(resourceID func(string, string) string) map[string]any {
	return map[string]any{
		"id":    vc.ID,
		"name":  vc.Name,
		"state": vc.State,
		"arn":   resourceID("emr-virtual-cluster", vc.ID),
		"containerProvider": map[string]any{
			"type": "EKS",
			"id":   vc.EksCluster,
			"info": map[string]any{
				"eksInfo": map[string]any{
					"namespace": vc.Namespace,
				},
			},
		},
		"createdAt": vc.CreatedAt.Format(time.RFC3339),
		"tags":      vc.Tags,
	}
}

// ─── Job Run types ────────────────────────────────────────────────────────────

type jobRun struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	VirtualClusterID string            `json:"virtualClusterId"`
	State            string            `json:"state"` // PENDING, SUBMITTED, RUNNING, FAILED, CANCELLED, CANCEL_PENDING, COMPLETED
	FailureReason    string            `json:"failureReason,omitempty"`
	ReleaseLabel     string            `json:"releaseLabel"`
	ExecutionRole    string            `json:"executionRoleArn"`
	CreatedAt        time.Time         `json:"createdAt"`
	Tags             map[string]string `json:"tags"`
	JobDriver        map[string]any    `json:"jobDriver"`
}

func (jr jobRun) toWire(resourceID func(string, string) string) map[string]any {
	m := map[string]any{
		"id":               jr.ID,
		"name":             jr.Name,
		"virtualClusterId": jr.VirtualClusterID,
		"state":            jr.State,
		"arn":              resourceID("emr-job-run", jr.VirtualClusterID+"/"+jr.ID),
		"releaseLabel":     jr.ReleaseLabel,
		"executionRoleArn": jr.ExecutionRole,
		"createdAt":        jr.CreatedAt.Format(time.RFC3339),
		"tags":             jr.Tags,
		"jobDriver":        jr.JobDriver,
	}
	if jr.FailureReason != "" {
		m["failureReason"] = jr.FailureReason
	}
	return m
}

// ─── Managed Endpoint types ───────────────────────────────────────────────────

type managedEndpoint struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	VirtualClusterID string            `json:"virtualClusterId"`
	Type             string            `json:"type"`
	ReleaseLabel     string            `json:"releaseLabel"`
	ExecutionRole    string            `json:"executionRoleArn"`
	State            string            `json:"state"` // CREATING, ACTIVE, TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS
	CreatedAt        time.Time         `json:"createdAt"`
	Tags             map[string]string `json:"tags"`
}

func (me managedEndpoint) toWire(resourceID func(string, string) string) map[string]any {
	return map[string]any{
		"id":               me.ID,
		"name":             me.Name,
		"virtualClusterId": me.VirtualClusterID,
		"type":             me.Type,
		"releaseLabel":     me.ReleaseLabel,
		"executionRoleArn": me.ExecutionRole,
		"state":            me.State,
		"arn":              resourceID("emr-managed-endpoint", me.VirtualClusterID+"/"+me.ID),
		"createdAt":        me.CreatedAt.Format(time.RFC3339),
		"tags":             me.Tags,
	}
}

// ─── Virtual Cluster operations ───────────────────────────────────────────────

func (p *EMRContainersProvider) CreateVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}

	eksCluster, namespace := "", "default"
	if cp, ok := nr.Params["containerProvider"].(map[string]any); ok {
		eksCluster = strParamFromMap(cp, "id")
		if info, ok := cp["info"].(map[string]any); ok {
			if eks, ok := info["eksInfo"].(map[string]any); ok {
				namespace = strParamFromMap(eks, "namespace")
			}
		}
	}

	id := shortID()
	vc := virtualCluster{
		ID:         id,
		Name:       name,
		State:      "RUNNING",
		EksCluster: eksCluster,
		Namespace:  namespace,
		CreatedAt:  time.Now().UTC(),
		Tags:       parseTags(nr.Params),
	}
	data, _ := json.Marshal(vc)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtVirtualCluster, ID: id, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"id":   id,
		"name": name,
		"arn":  nr.ResourceID("emr-virtual-cluster", id),
		"tags": vc.Tags,
	}), nil
}

func (p *EMRContainersProvider) DeleteVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := pathID(nr, "virtualClusterId", "id")
	vc, err := p.loadVC(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Virtual cluster not found", HTTPStatus: http.StatusNotFound}
	}
	vc.State = "TERMINATING"
	p.saveVC(ctx, vc)
	return provider.OK(map[string]any{"id": id}), nil
}

func (p *EMRContainersProvider) DescribeVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := pathID(nr, "virtualClusterId", "id")
	vc, err := p.loadVC(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Virtual cluster not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{"virtualCluster": vc.toWire(nr.ResourceID)}), nil
}

func (p *EMRContainersProvider) ListVirtualClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtVirtualCluster, "")
	if err != nil {
		return nil, err
	}
	stateFilter := strParam(nr.Params, "states")
	eksFilter := strParam(nr.Params, "containerProviderId")
	list := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var vc virtualCluster
		json.Unmarshal(e.Data, &vc)
		if stateFilter != "" && vc.State != stateFilter {
			continue
		}
		if eksFilter != "" && vc.EksCluster != eksFilter {
			continue
		}
		list = append(list, vc.toWire(nr.ResourceID))
	}
	return provider.OK(map[string]any{"virtualClusters": list}), nil
}

// ─── Job Run operations ───────────────────────────────────────────────────────

func (p *EMRContainersProvider) StartJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	if _, err := p.loadVC(ctx, vcID); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Virtual cluster not found", HTTPStatus: http.StatusNotFound}
	}

	id := shortID()
	name := strParam(nr.Params, "name")

	initialState := "COMPLETED"
	if p.executor != nil {
		initialState = "PENDING"
	}

	jr := jobRun{
		ID:               id,
		Name:             name,
		VirtualClusterID: vcID,
		State:            initialState,
		ReleaseLabel:     strParam(nr.Params, "releaseLabel"),
		ExecutionRole:    strParam(nr.Params, "executionRoleArn"),
		CreatedAt:        time.Now().UTC(),
		Tags:             parseTags(nr.Params),
	}
	if jd, ok := nr.Params["jobDriver"].(map[string]any); ok {
		jr.JobDriver = jd
	}

	storeID := vcID + "/" + id
	data, _ := json.Marshal(jr)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtJobRun, ID: storeID, Data: data}); err != nil {
		return nil, err
	}

	if p.executor != nil {
		var entryPoint, sparkParams string
		var args []string
		if jd, ok := nr.Params["jobDriver"].(map[string]any); ok {
			if sc, ok := jd["sparkSubmitJobDriver"].(map[string]any); ok {
				entryPoint, _ = sc["entryPoint"].(string)
				sparkParams, _ = sc["sparkSubmitParameters"].(string)
				if raw, ok := sc["entryPointArguments"].([]any); ok {
					for _, a := range raw {
						if s, ok := a.(string); ok {
							args = append(args, s)
						}
					}
				}
			}
		}
		job := spark.BuildSparkJob(id, entryPoint, "", args, sparkParams, p.executorCfg)
		if submitErr := p.executor.Submit(ctx, job); submitErr != nil {
			jr.State = "FAILED"
			p.saveJobRun(ctx, jr)
		} else {
			p.jobRefs.Store(id, jobRef{
					vcID:      vcID,
					jrID:      id,
					region:    nr.Region,
					accountID: nr.AccountID,
					cloud:     nr.Cloud,
				})
			if p.poller != nil {
				p.poller.Track(id, spark.StatePending)
			}
		}
	} else {
		// Instant completion path — transition to COMPLETED and publish event.
		jr.State = "COMPLETED"
		p.saveJobRun(ctx, jr)
		p.bus.Publish(events.Event{
			Type: events.EventEMRJobRunState,
			Payload: events.EMRJobRunStateEvent{
				VirtualClusterID: vcID,
				JobRunID:         id,
				Name:             name,
				State:            "COMPLETED",
				Region:           nr.Region,
				AccountID:        nr.AccountID,
				Cloud:            nr.Cloud,
			},
		})
	}

	return provider.OK(map[string]any{
		"id":               id,
		"name":             name,
		"arn":              nr.ResourceID("emr-job-run", vcID+"/"+id),
		"virtualClusterId": vcID,
	}), nil
}

func (p *EMRContainersProvider) CancelJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	jobID := pathID(nr, "jobRunId", "id")
	jr, err := p.loadJobRun(ctx, vcID, jobID)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Job run not found", HTTPStatus: http.StatusNotFound}
	}
	if p.executor != nil {
		_ = p.executor.Cancel(ctx, jobID)
		p.jobRefs.Delete(jobID)
	}
	jr.State = "CANCELLED"
	p.saveJobRun(ctx, jr)
	p.bus.Publish(events.Event{
		Type: events.EventEMRJobRunState,
		Payload: events.EMRJobRunStateEvent{
			VirtualClusterID: vcID,
			JobRunID:         jobID,
			Name:             jr.Name,
			State:            "CANCELLED",
			Region:           nr.Region,
			AccountID:        nr.AccountID,
			Cloud:            nr.Cloud,
		},
	})
	return provider.OK(map[string]any{"id": jobID, "virtualClusterId": vcID}), nil
}

func (p *EMRContainersProvider) DescribeJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	jobID := pathID(nr, "jobRunId", "id")
	jr, err := p.loadJobRun(ctx, vcID, jobID)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Job run not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{"jobRun": jr.toWire(nr.ResourceID)}), nil
}

func (p *EMRContainersProvider) ListJobRuns(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	prefix := vcID + "/"
	entries, err := p.resources.List(ctx, rtJobRun, prefix)
	if err != nil {
		return nil, err
	}
	stateFilter := strParam(nr.Params, "states")
	list := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var jr jobRun
		json.Unmarshal(e.Data, &jr)
		if stateFilter != "" && jr.State != stateFilter {
			continue
		}
		list = append(list, jr.toWire(nr.ResourceID))
	}
	return provider.OK(map[string]any{"jobRuns": list}), nil
}

// ─── Managed Endpoint operations ──────────────────────────────────────────────

func (p *EMRContainersProvider) CreateManagedEndpoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	if _, err := p.loadVC(ctx, vcID); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Virtual cluster not found", HTTPStatus: http.StatusNotFound}
	}

	id := shortID()
	me := managedEndpoint{
		ID:               id,
		Name:             strParam(nr.Params, "name"),
		VirtualClusterID: vcID,
		Type:             strParam(nr.Params, "type"),
		ReleaseLabel:     strParam(nr.Params, "releaseLabel"),
		ExecutionRole:    strParam(nr.Params, "executionRoleArn"),
		State:            "ACTIVE",
		CreatedAt:        time.Now().UTC(),
		Tags:             parseTags(nr.Params),
	}
	storeID := vcID + "/" + id
	data, _ := json.Marshal(me)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtManagedEndpoint, ID: storeID, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"id":               id,
		"name":             me.Name,
		"arn":              nr.ResourceID("emr-managed-endpoint", vcID+"/"+id),
		"virtualClusterId": vcID,
	}), nil
}

func (p *EMRContainersProvider) DeleteManagedEndpoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	epID := pathID(nr, "endpointId", "id")
	me, err := p.loadEndpoint(ctx, vcID, epID)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Managed endpoint not found", HTTPStatus: http.StatusNotFound}
	}
	me.State = "TERMINATING"
	p.saveEndpoint(ctx, me)
	return provider.OK(map[string]any{"id": epID, "virtualClusterId": vcID}), nil
}

func (p *EMRContainersProvider) DescribeManagedEndpoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	epID := pathID(nr, "endpointId", "id")
	me, err := p.loadEndpoint(ctx, vcID, epID)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Managed endpoint not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{"endpoint": me.toWire(nr.ResourceID)}), nil
}

func (p *EMRContainersProvider) ListManagedEndpoints(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	prefix := vcID + "/"
	entries, err := p.resources.List(ctx, rtManagedEndpoint, prefix)
	if err != nil {
		return nil, err
	}
	stateFilter := strParam(nr.Params, "states")
	list := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var me managedEndpoint
		json.Unmarshal(e.Data, &me)
		if stateFilter != "" && me.State != stateFilter {
			continue
		}
		list = append(list, me.toWire(nr.ResourceID))
	}
	return provider.OK(map[string]any{"endpoints": list}), nil
}

// ─── Tagging operations ───────────────────────────────────────────────────────

// tagResource is a generic helper that applies tags to any stored resource by ARN lookup.
// EMR on EKS uses REST path params; the resource ARN is in the URL path.
func (p *EMRContainersProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// tags come in as {"tags": {"key":"value",...}}
	_ = nr // no-op: tags stored on creation for now
	return provider.OK(map[string]any{}), nil
}

func (p *EMRContainersProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

func (p *EMRContainersProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"tags": map[string]string{}}), nil
}

// ─── Executor lifecycle ───────────────────────────────────────────────────────

func (p *EMRContainersProvider) updateJobRunState(ctx context.Context, vcID, jrID, newState, message, region, accountID string, cloud model.Cloud) {
	jr, err := p.loadJobRun(ctx, vcID, jrID)
	if err != nil {
		return
	}
	jr.State = newState
	if newState == "FAILED" && message != "" {
		jr.FailureReason = message
	}
	p.saveJobRun(ctx, jr)
	p.bus.Publish(events.Event{
		Type: events.EventEMRJobRunState,
		Payload: events.EMRJobRunStateEvent{
			VirtualClusterID: vcID,
			JobRunID:         jrID,
			Name:             jr.Name,
			State:            newState,
			Region:           region,
			AccountID:        accountID,
			Cloud:            cloud,
		},
	})
}

// OnStateChange is called by the StatusPoller on each state transition.
func (p *EMRContainersProvider) OnStateChange(ev spark.StateChangeEvent) {
	v, ok := p.jobRefs.Load(ev.JobID)
	if !ok {
		return
	}
	jr := v.(jobRef)
	p.updateJobRunState(context.Background(), jr.vcID, jr.jrID, string(ev.NewState), ev.Message, jr.region, jr.accountID, jr.cloud)
	if ev.NewState.IsTerminal() {
		p.jobRefs.Delete(ev.JobID)
	}
}

// Reset clears in-memory state and resets the underlying executor.
func (p *EMRContainersProvider) Reset() {
	p.jobRefs.Range(func(k, _ any) bool { p.jobRefs.Delete(k); return true })
	switch ex := p.executor.(type) {
	case *spark.MockExecutor:
		ex.Reset()
	case *spark.DockerExecutor:
		ex.Reset()
	case *spark.K8sExecutor:
		ex.Reset()
	}
}

// Shutdown stops the poller and closes the executor.
func (p *EMRContainersProvider) Shutdown(ctx context.Context) {
	if p.poller != nil {
		p.poller.Stop()
	}
	if p.executor != nil {
		p.executor.Close()
	}
}

// ─── Store helpers ────────────────────────────────────────────────────────────

func (p *EMRContainersProvider) loadVC(ctx context.Context, id string) (virtualCluster, error) {
	e, err := p.resources.Get(ctx, rtVirtualCluster, id)
	if err != nil {
		return virtualCluster{}, err
	}
	var vc virtualCluster
	return vc, json.Unmarshal(e.Data, &vc)
}

func (p *EMRContainersProvider) saveVC(ctx context.Context, vc virtualCluster) {
	data, _ := json.Marshal(vc)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtVirtualCluster, ID: vc.ID, Data: data})
}

func (p *EMRContainersProvider) loadJobRun(ctx context.Context, vcID, jobID string) (jobRun, error) {
	e, err := p.resources.Get(ctx, rtJobRun, vcID+"/"+jobID)
	if err != nil {
		return jobRun{}, err
	}
	var jr jobRun
	return jr, json.Unmarshal(e.Data, &jr)
}

func (p *EMRContainersProvider) saveJobRun(ctx context.Context, jr jobRun) {
	data, _ := json.Marshal(jr)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtJobRun, ID: jr.VirtualClusterID + "/" + jr.ID, Data: data})
}

func (p *EMRContainersProvider) loadEndpoint(ctx context.Context, vcID, epID string) (managedEndpoint, error) {
	e, err := p.resources.Get(ctx, rtManagedEndpoint, vcID+"/"+epID)
	if err != nil {
		return managedEndpoint{}, err
	}
	var me managedEndpoint
	return me, json.Unmarshal(e.Data, &me)
}

func (p *EMRContainersProvider) saveEndpoint(ctx context.Context, me managedEndpoint) {
	data, _ := json.Marshal(me)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtManagedEndpoint, ID: me.VirtualClusterID + "/" + me.ID, Data: data})
}

// ─── Param helpers ────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func strParamFromMap(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// pathID looks up a route param (set by the REST codec) or falls back to a JSON body param.
func pathID(nr *model.NormalizedRequest, routeKey, bodyKey string) string {
	if v := strParam(nr.Params, "_path_"+routeKey); v != "" {
		return v
	}
	return strParam(nr.Params, bodyKey)
}

func parseTags(params map[string]any) map[string]string {
	out := map[string]string{}
	if tags, ok := params["tags"].(map[string]any); ok {
		for k, v := range tags {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
	}
	return out
}

func shortID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}
