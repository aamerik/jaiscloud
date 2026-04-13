// Package emrcontainers implements the EMR on EKS provider for the aws-emr-spark plugin.
package emrcontainers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
)

const (
	rtVirtualCluster = "emrc_virtual_cluster"
	rtJobRun         = "emrc_job_run"
)

// EMRContainersProvider handles virtual cluster and job run lifecycle.
type EMRContainersProvider struct {
	store       sdk.ResourceStore
	executor    spark.SparkExecutor
	executorCfg spark.SparkConfig // forwarded into every SparkJob so image/namespace/SA are never zero
	poller      *spark.StatusPoller
	bus         sdk.EventBus
	jobRunToVC  sync.Map // jobRunID → virtualClusterID
}

// New creates an EMRContainersProvider.
func New(store sdk.ResourceStore, executor spark.SparkExecutor, cfg spark.SparkConfig, poller *spark.StatusPoller, bus sdk.EventBus) *EMRContainersProvider {
	return &EMRContainersProvider{store: store, executor: executor, executorCfg: cfg, poller: poller, bus: bus}
}

// ─── Data types ───────────────────────────────────────────────────────────────

type virtualCluster struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	State      string            `json:"state"` // RUNNING, TERMINATING, TERMINATED, ARRESTED
	EKSCluster string            `json:"eksCluster"`
	Namespace  string            `json:"namespace"`
	CreatedAt  time.Time         `json:"createdAt"`
	Tags       map[string]string `json:"tags"`
}

type jobRun struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	VirtualClusterID string            `json:"virtualClusterId"`
	State            string            `json:"state"` // PENDING, SUBMITTED, RUNNING, FAILED, CANCELLED, CANCEL_PENDING, COMPLETED
	ReleaseLabel     string            `json:"releaseLabel"`
	ExecutionRole    string            `json:"executionRoleArn"`
	CreatedAt        time.Time         `json:"createdAt"`
	Tags             map[string]string `json:"tags"`
	Region           string            `json:"region"`
	AccountID        string            `json:"accountId"`
	Cloud            string            `json:"cloud"`
}

// ─── Handle dispatch ──────────────────────────────────────────────────────────

func (p *EMRContainersProvider) Handle(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	switch req.Action {
	case "CreateVirtualCluster":
		return p.CreateVirtualCluster(ctx, req)
	case "DeleteVirtualCluster":
		return p.DeleteVirtualCluster(ctx, req)
	case "DescribeVirtualCluster":
		return p.DescribeVirtualCluster(ctx, req)
	case "ListVirtualClusters":
		return p.ListVirtualClusters(ctx, req)
	case "StartJobRun":
		return p.StartJobRun(ctx, req)
	case "CancelJobRun":
		return p.CancelJobRun(ctx, req)
	case "DescribeJobRun":
		return p.DescribeJobRun(ctx, req)
	case "ListJobRuns":
		return p.ListJobRuns(ctx, req)
	default:
		return errResponse("UnsupportedOperation",
			fmt.Sprintf("EMRContainers action %q not implemented by plugin", req.Action), http.StatusNotImplemented)
	}
}

// ─── Virtual Cluster operations ───────────────────────────────────────────────

func (p *EMRContainersProvider) CreateVirtualCluster(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	name := strParam(req.Params, "name")
	if name == "" {
		return errResponse("ValidationException", "name is required", http.StatusBadRequest)
	}
	eksCluster, namespace := "", "default"
	if cp, ok := req.Params["containerProvider"].(map[string]any); ok {
		eksCluster, _ = cp["id"].(string)
		if info, ok := cp["info"].(map[string]any); ok {
			if eks, ok := info["eksInfo"].(map[string]any); ok {
				if ns, ok := eks["namespace"].(string); ok {
					namespace = ns
				}
			}
		}
	}

	id := shortID()
	vc := virtualCluster{
		ID:         id,
		Name:       name,
		State:      "RUNNING",
		EKSCluster: eksCluster,
		Namespace:  namespace,
		CreatedAt:  time.Now().UTC(),
	}
	if err := p.saveVC(ctx, vc); err != nil {
		return internalError(err)
	}
	return okResponse(map[string]any{
		"id":   id,
		"name": name,
		"arn":  ridFn(req)("emr-virtual-cluster", id),
	})
}

func (p *EMRContainersProvider) DeleteVirtualCluster(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	id := pathOrParam(req.Params, "virtualClusterId", "id")
	vc, err := p.loadVC(ctx, id)
	if err != nil {
		return errResponse("ResourceNotFoundException", "Virtual cluster not found", http.StatusNotFound)
	}
	vc.State = "TERMINATING"
	p.saveVC(ctx, vc)
	return okResponse(map[string]any{"id": id})
}

func (p *EMRContainersProvider) DescribeVirtualCluster(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	id := pathOrParam(req.Params, "virtualClusterId", "id")
	vc, err := p.loadVC(ctx, id)
	if err != nil {
		return errResponse("ResourceNotFoundException", "Virtual cluster not found", http.StatusNotFound)
	}
	return okResponse(map[string]any{"virtualCluster": vcToWire(vc, ridFn(req))})
}

func (p *EMRContainersProvider) ListVirtualClusters(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	entries, err := p.store.List(ctx, rtVirtualCluster, "")
	if err != nil {
		return internalError(err)
	}
	list := make([]map[string]any, 0)
	for _, e := range entries {
		var vc virtualCluster
		if json.Unmarshal(e.Data, &vc) != nil {
			continue
		}
		list = append(list, vcToWire(vc, ridFn(req)))
	}
	return okResponse(map[string]any{"virtualClusters": list})
}

// ─── Job Run operations ───────────────────────────────────────────────────────

func (p *EMRContainersProvider) StartJobRun(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	vcID := pathOrParam(req.Params, "virtualClusterId", "virtualClusterId")
	if _, err := p.loadVC(ctx, vcID); err != nil {
		return errResponse("ResourceNotFoundException", "Virtual cluster not found", http.StatusNotFound)
	}

	id := shortID()
	jr := jobRun{
		ID:               id,
		Name:             strParam(req.Params, "name"),
		VirtualClusterID: vcID,
		State:            "RUNNING",
		ReleaseLabel:     strParam(req.Params, "releaseLabel"),
		ExecutionRole:    strParam(req.Params, "executionRoleArn"),
		CreatedAt:        time.Now().UTC(),
		Region:           req.Region,
		AccountID:        req.AccountID,
		Cloud:            req.Cloud,
	}
	if err := p.saveJobRun(ctx, jr); err != nil {
		return internalError(err)
	}
	p.jobRunToVC.Store(id, vcID)

	// Submit via executor — parse jobDriver fields and forward executorCfg so
	// image/namespace/SA are never the zero value (Bug 3 fix). Also parse
	// sparkSubmitParameters into SparkConf overrides (Bug 4 fix).
	var jar, sparkParams string
	var args []string
	if jd, ok := req.Params["jobDriver"].(map[string]any); ok {
		if sc, ok := jd["sparkSubmitJobDriver"].(map[string]any); ok {
			jar, _ = sc["entryPoint"].(string)
			sparkParams, _ = sc["sparkSubmitParameters"].(string)
			if rawArgs, ok := sc["entryPointArguments"].([]any); ok {
				for _, a := range rawArgs {
					if s, ok := a.(string); ok {
						args = append(args, s)
					}
				}
			}
		}
	}
	job := spark.BuildSparkJob(id, jar, "", args, sparkParams, p.executorCfg)
	if err := p.executor.Submit(ctx, job); err == nil && p.poller != nil {
		p.poller.Track(id, spark.StateRunning)
	}

	return okResponse(map[string]any{
		"id":               id,
		"name":             jr.Name,
		"virtualClusterId": vcID,
		"arn":              ridFn(req)("emr-job-run", vcID+"/jobruns/"+id),
	})
}

func (p *EMRContainersProvider) CancelJobRun(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	vcID := pathOrParam(req.Params, "virtualClusterId", "virtualClusterId")
	jobID := pathOrParam(req.Params, "jobRunId", "id")
	jr, err := p.loadJobRun(ctx, vcID, jobID)
	if err != nil {
		return errResponse("ResourceNotFoundException", "Job run not found", http.StatusNotFound)
	}
	p.executor.Cancel(ctx, jobID)
	jr.State = "CANCEL_PENDING"
	p.saveJobRun(ctx, jr)
	return okResponse(map[string]any{"id": jobID, "virtualClusterId": vcID})
}

func (p *EMRContainersProvider) DescribeJobRun(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	vcID := pathOrParam(req.Params, "virtualClusterId", "virtualClusterId")
	jobID := pathOrParam(req.Params, "jobRunId", "id")

	// Sync state from poller
	if p.poller != nil {
		if state := p.poller.CurrentState(jobID); state != "" {
			if jr, err := p.loadJobRun(ctx, vcID, jobID); err == nil {
				jr.State = string(state)
				p.saveJobRun(ctx, jr)
			}
		}
	}

	jr, err := p.loadJobRun(ctx, vcID, jobID)
	if err != nil {
		return errResponse("ResourceNotFoundException", "Job run not found", http.StatusNotFound)
	}
	return okResponse(map[string]any{"jobRun": jobRunToWire(jr, ridFn(req))})
}

func (p *EMRContainersProvider) ListJobRuns(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	vcID := pathOrParam(req.Params, "virtualClusterId", "virtualClusterId")
	entries, err := p.store.List(ctx, rtJobRun, vcID+"/")
	if err != nil {
		return internalError(err)
	}
	list := make([]map[string]any, 0)
	for _, e := range entries {
		var jr jobRun
		if json.Unmarshal(e.Data, &jr) != nil {
			continue
		}
		list = append(list, jobRunToWire(jr, ridFn(req)))
	}
	return okResponse(map[string]any{"jobRuns": list})
}

// SetPoller sets the status poller after construction.
func (p *EMRContainersProvider) SetPoller(poller *spark.StatusPoller) {
	p.poller = poller
}

// OnStateChange is called by the StatusPoller when a job run changes state.
func (p *EMRContainersProvider) OnStateChange(ev spark.StateChangeEvent) {
	vcIDRaw, ok := p.jobRunToVC.Load(ev.JobID)
	if !ok {
		return
	}
	vcID, _ := vcIDRaw.(string)
	ctx := context.Background()
	jr, err := p.loadJobRun(ctx, vcID, ev.JobID)
	if err != nil {
		return
	}
	jr.State = string(ev.NewState)
	p.saveJobRun(ctx, jr)
	if p.bus != nil {
		p.bus.Publish(ctx, sdk.Event{
			Source: "aws-emr-spark",
			Type:   sdk.EventTypeEMRJobRunStateChange,
			Detail: map[string]any{
				"virtualClusterId": jr.VirtualClusterID,
				"jobRunId":         jr.ID,
				"name":             jr.Name,
				"state":            jr.State,
				"failureReason":    "",
				"region":           jr.Region,
				"accountId":        jr.AccountID,
				"cloud":            jr.Cloud,
			},
		})
	}
}

// ─── Store helpers ────────────────────────────────────────────────────────────

func (p *EMRContainersProvider) loadVC(ctx context.Context, id string) (virtualCluster, error) {
	e, err := p.store.Get(ctx, rtVirtualCluster, id)
	if err != nil {
		return virtualCluster{}, err
	}
	var vc virtualCluster
	return vc, json.Unmarshal(e.Data, &vc)
}

func (p *EMRContainersProvider) saveVC(ctx context.Context, vc virtualCluster) error {
	data, _ := json.Marshal(vc)
	entry := sdk.ResourceEntry{Type: rtVirtualCluster, ID: vc.ID, Data: data}
	exists, _ := p.store.Exists(ctx, rtVirtualCluster, vc.ID)
	if exists {
		return p.store.Update(ctx, entry)
	}
	return p.store.Create(ctx, entry)
}

func (p *EMRContainersProvider) loadJobRun(ctx context.Context, vcID, jobID string) (jobRun, error) {
	e, err := p.store.Get(ctx, rtJobRun, vcID+"/"+jobID)
	if err != nil {
		return jobRun{}, err
	}
	var jr jobRun
	return jr, json.Unmarshal(e.Data, &jr)
}

func (p *EMRContainersProvider) saveJobRun(ctx context.Context, jr jobRun) error {
	data, _ := json.Marshal(jr)
	key := jr.VirtualClusterID + "/" + jr.ID
	entry := sdk.ResourceEntry{Type: rtJobRun, ID: key, Data: data}
	exists, _ := p.store.Exists(ctx, rtJobRun, key)
	if exists {
		return p.store.Update(ctx, entry)
	}
	return p.store.Create(ctx, entry)
}

// ─── Wire format helpers ──────────────────────────────────────────────────────

func vcToWire(vc virtualCluster, rid func(string, string) string) map[string]any {
	return map[string]any{
		"id":    vc.ID,
		"name":  vc.Name,
		"state": vc.State,
		"arn":   rid("emr-virtual-cluster", vc.ID),
		"containerProvider": map[string]any{
			"type": "EKS",
			"id":   vc.EKSCluster,
			"info": map[string]any{
				"eksInfo": map[string]any{"namespace": vc.Namespace},
			},
		},
		"createdAt": vc.CreatedAt.Format(time.RFC3339),
	}
}

func jobRunToWire(jr jobRun, rid func(string, string) string) map[string]any {
	return map[string]any{
		"id":               jr.ID,
		"name":             jr.Name,
		"virtualClusterId": jr.VirtualClusterID,
		"state":            jr.State,
		"arn":              rid("emr-job-run", jr.VirtualClusterID+"/jobruns/"+jr.ID),
		"releaseLabel":     jr.ReleaseLabel,
		"executionRoleArn": jr.ExecutionRole,
		"createdAt":        jr.CreatedAt.Format(time.RFC3339),
	}
}

// ─── Param helpers ────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return ""
}

func pathOrParam(params map[string]any, pathKey, bodyKey string) string {
	if v := strParam(params, "_path_"+pathKey); v != "" {
		return v
	}
	return strParam(params, bodyKey)
}

func shortID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

// ridFn returns a nil-safe resource-ID function from req.ResourceID.
// Falls back to returning the name unchanged for unit tests where ResourceID is nil.
func ridFn(req sdk.HandleRequest) func(string, string) string {
	if req.ResourceID != nil {
		return req.ResourceID
	}
	return func(_, name string) string { return name }
}

func okResponse(data map[string]any) sdk.HandleResponse {
	return sdk.HandleResponse{HTTPStatus: http.StatusOK, Data: data}
}

func errResponse(code, msg string, status int) sdk.HandleResponse {
	return sdk.HandleResponse{Err: &sdk.PluginError{Code: code, Message: msg, HTTPStatus: status}}
}

func internalError(err error) sdk.HandleResponse {
	return errResponse("InternalFailure", err.Error(), http.StatusInternalServerError)
}
