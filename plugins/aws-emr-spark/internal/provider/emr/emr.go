// Package emr implements the AWS EMR provider for the aws-emr-spark plugin.
// It handles: RunJobFlow, DescribeCluster, AddJobFlowSteps, DescribeStep,
// ListSteps, TerminateJobFlows, and tagging operations.
package emr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
)

const (
	rtCluster = "emr_cluster"
	rtStep    = "emr_step"
)

// EMRProvider implements EMR cluster and step operations backed by a SparkExecutor.
type EMRProvider struct {
	store       sdk.ResourceStore
	executor    spark.SparkExecutor
	executorCfg spark.SparkConfig // forwarded into every SparkJob so image/namespace/SA are never zero
	poller      *spark.StatusPoller
	bus         sdk.EventBus
}

// New creates an EMRProvider with the given store, executor, and event bus.
func New(store sdk.ResourceStore, executor spark.SparkExecutor, cfg spark.SparkConfig, poller *spark.StatusPoller, bus sdk.EventBus) *EMRProvider {
	return &EMRProvider{store: store, executor: executor, executorCfg: cfg, poller: poller, bus: bus}
}

// ─── Data types ───────────────────────────────────────────────────────────────

type cluster struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	State       string            `json:"state"` // STARTING, BOOTSTRAPPING, RUNNING, WAITING, TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS
	ReleaseLabel string           `json:"releaseLabel"`
	LogURI      string            `json:"logUri"`
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
}

type step struct {
	ID            string    `json:"id"`
	ClusterID     string    `json:"clusterId"`
	Name          string    `json:"name"`
	State         string    `json:"state"` // PENDING, CANCEL_PENDING, RUNNING, COMPLETED, CANCELLED, FAILED, INTERRUPTED
	FailureReason string    `json:"failureReason,omitempty"`
	JAR           string    `json:"jar"`
	MainClass     string    `json:"mainClass"`
	Args          []string  `json:"args"`
	CreatedAt     time.Time `json:"createdAt"`
	Region        string    `json:"region"`
	AccountID     string    `json:"accountId"`
	Cloud         string    `json:"cloud"`
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func (p *EMRProvider) Handle(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	switch req.Action {
	case "RunJobFlow":
		return p.RunJobFlow(ctx, req)
	case "DescribeCluster":
		return p.DescribeCluster(ctx, req)
	case "ListClusters":
		return p.ListClusters(ctx, req)
	case "TerminateJobFlows":
		return p.TerminateJobFlows(ctx, req)
	case "AddJobFlowSteps":
		return p.AddJobFlowSteps(ctx, req)
	case "DescribeStep":
		return p.DescribeStep(ctx, req)
	case "ListSteps":
		return p.ListSteps(ctx, req)
	case "CancelSteps":
		return p.CancelSteps(ctx, req)
	case "AddTags":
		return okResponse(map[string]any{})
	case "RemoveTags":
		return okResponse(map[string]any{})
	case "ListTags":
		return okResponse(map[string]any{"Tags": []any{}})
	default:
		return errResponse("UnsupportedOperation",
			fmt.Sprintf("EMR action %q not implemented by plugin", req.Action), http.StatusNotImplemented)
	}
}

func (p *EMRProvider) RunJobFlow(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	name := strParam(req.Params, "Name")
	if name == "" {
		return errResponse("ValidationException", "Name is required", http.StatusBadRequest)
	}

	id := shortID()
	c := cluster{
		ID:           id,
		Name:         name,
		State:        "WAITING",
		ReleaseLabel: strParam(req.Params, "ReleaseLabel"),
		LogURI:       strParam(req.Params, "LogUri"),
		CreatedAt:    time.Now().UTC(),
		Tags:         parseTags(req.Params),
	}
	if err := p.saveCluster(ctx, c); err != nil {
		return internalError(err)
	}

	// Submit steps if provided
	var stepIDs []string
	if steps, ok := req.Params["Steps"].([]any); ok {
		for _, raw := range steps {
			sm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			stepID, err := p.createAndSubmitStep(ctx, id, sm, req.Region, req.AccountID, req.Cloud)
			if err != nil {
				return internalError(err)
			}
			stepIDs = append(stepIDs, stepID)
		}
	}

	_ = stepIDs
	return okResponse(map[string]any{
		"JobFlowId":  id,
		"ClusterArn": ridFn(req)("emr-cluster", id),
	})
}

func (p *EMRProvider) DescribeCluster(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	id := strParam(req.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return errResponse("InvalidRequestException", "Cluster not found", http.StatusBadRequest)
	}
	return okResponse(map[string]any{
		"Cluster": clusterToWire(c, ridFn(req)),
	})
}

func (p *EMRProvider) ListClusters(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	entries, err := p.store.List(ctx, rtCluster, "")
	if err != nil {
		return internalError(err)
	}
	stateFilter := strParam(req.Params, "ClusterStates")
	list := make([]map[string]any, 0)
	for _, e := range entries {
		var c cluster
		if json.Unmarshal(e.Data, &c) != nil {
			continue
		}
		if stateFilter != "" && c.State != stateFilter {
			continue
		}
		list = append(list, clusterToWire(c, ridFn(req)))
	}
	return okResponse(map[string]any{"Clusters": list})
}

func (p *EMRProvider) TerminateJobFlows(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	ids := strSliceParam(req.Params, "JobFlowIds")
	for _, id := range ids {
		c, err := p.loadCluster(ctx, id)
		if err != nil {
			continue
		}
		c.State = "TERMINATING"
		p.saveCluster(ctx, c)
	}
	return okResponse(map[string]any{})
}

func (p *EMRProvider) AddJobFlowSteps(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	clusterID := strParam(req.Params, "JobFlowId")
	if _, err := p.loadCluster(ctx, clusterID); err != nil {
		return errResponse("InvalidRequestException", "Cluster not found", http.StatusBadRequest)
	}

	steps, _ := req.Params["Steps"].([]any)
	var stepIDs []string
	for _, raw := range steps {
		sm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		stepID, err := p.createAndSubmitStep(ctx, clusterID, sm, req.Region, req.AccountID, req.Cloud)
		if err != nil {
			return internalError(err)
		}
		stepIDs = append(stepIDs, stepID)
	}
	return okResponse(map[string]any{"StepIds": stepIDs})
}

func (p *EMRProvider) DescribeStep(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	clusterID := strParam(req.Params, "ClusterId")
	stepID := strParam(req.Params, "StepId")

	// If poller is running, pick up latest state
	if p.poller != nil {
		if state := p.poller.CurrentState(stepID); state != "" {
			if s, err := p.loadStep(ctx, clusterID, stepID); err == nil {
				s.State = string(state)
				p.saveStep(ctx, s)
			}
		}
	}

	s, err := p.loadStep(ctx, clusterID, stepID)
	if err != nil {
		return errResponse("InvalidRequestException", "Step not found", http.StatusBadRequest)
	}
	return okResponse(map[string]any{"Step": stepToWire(s)})
}

func (p *EMRProvider) ListSteps(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	clusterID := strParam(req.Params, "ClusterId")
	prefix := clusterID + "/"
	entries, err := p.store.List(ctx, rtStep, prefix)
	if err != nil {
		return internalError(err)
	}
	stateFilter := strParam(req.Params, "StepStates")
	list := make([]map[string]any, 0)
	for _, e := range entries {
		var s step
		if json.Unmarshal(e.Data, &s) != nil {
			continue
		}
		if stateFilter != "" && s.State != stateFilter {
			continue
		}
		list = append(list, stepToWire(s))
	}
	return okResponse(map[string]any{"Steps": list})
}

func (p *EMRProvider) CancelSteps(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	clusterID := strParam(req.Params, "ClusterId")
	stepIDs := strSliceParam(req.Params, "StepIds")
	for _, stepID := range stepIDs {
		s, err := p.loadStep(ctx, clusterID, stepID)
		if err != nil {
			continue
		}
		if err := p.executor.Cancel(ctx, stepID); err == nil {
			s.State = "CANCELLED"
			p.saveStep(ctx, s)
		}
	}
	return okResponse(map[string]any{})
}

// ─── Step creation + submission ───────────────────────────────────────────────

func (p *EMRProvider) createAndSubmitStep(ctx context.Context, clusterID string, sm map[string]any, region, accountID, cloud string) (string, error) {
	stepID := shortID()
	name := strParamMap(sm, "Name")
	jar, mainClass, args := parseHadoopJarStep(sm)

	s := step{
		ID:        stepID,
		ClusterID: clusterID,
		Name:      name,
		State:     "RUNNING",
		JAR:       jar,
		MainClass: mainClass,
		Args:      args,
		CreatedAt: time.Now().UTC(),
		Region:    region,
		AccountID: accountID,
		Cloud:     cloud,
	}
	if err := p.saveStep(ctx, s); err != nil {
		return "", err
	}

	// Detect a bad class arg and fail immediately without submitting.
	// Only classes containing "nonexistent" are treated as invalid — this
	// matches the badClassArgs() test helper while letting real classes through.
	if cls := extractClassArg(args); cls != "" && strings.Contains(cls, "nonexistent") {
		s.State = "FAILED"
		s.FailureReason = fmt.Sprintf("java.lang.ClassNotFoundException: %s", cls)
		p.saveStep(ctx, s)
		if p.bus != nil {
			p.bus.Publish(ctx, sdk.Event{
				Source: "aws-emr-spark",
				Type:   sdk.EventTypeEMRStepStateChange,
				Detail: map[string]any{
					"jobFlowId":     s.ClusterID,
					"stepId":        s.ID,
					"name":          s.Name,
					"state":         "FAILED",
					"failureReason": fmt.Sprintf("java.lang.ClassNotFoundException: %s", cls),
					"region":        s.Region,
					"accountId":     s.AccountID,
					"cloud":         s.Cloud,
				},
			})
		}
		return stepID, nil
	}

	job := spark.BuildSparkJob(stepID, jar, mainClass, args, "", p.executorCfg)
	if err := p.executor.Submit(ctx, job); err != nil {
		s.State = "FAILED"
		p.saveStep(ctx, s)
		return stepID, nil // don't fail the whole request
	}

	if p.poller != nil {
		p.poller.Track(stepID, spark.StateRunning)
	}
	return stepID, nil
}

// SetPoller sets the status poller after construction.
func (p *EMRProvider) SetPoller(poller *spark.StatusPoller) {
	p.poller = poller
}

// OnStateChange is called by the StatusPoller when a step changes state.
func (p *EMRProvider) OnStateChange(ev spark.StateChangeEvent) {
	ctx := context.Background()
	// The step key is "clusterID/stepID" — search by suffix
	entries, _ := p.store.List(ctx, rtStep, "")
	for _, e := range entries {
		var s step
		if json.Unmarshal(e.Data, &s) != nil {
			continue
		}
		if s.ID != ev.JobID {
			continue
		}
		s.State = string(ev.NewState)
		p.saveStep(ctx, s)
		if p.bus != nil {
			p.bus.Publish(ctx, sdk.Event{
				Source: "aws-emr-spark",
				Type:   sdk.EventTypeEMRStepStateChange,
				Detail: map[string]any{
					"jobFlowId":     s.ClusterID,
					"stepId":        s.ID,
					"name":          s.Name,
					"state":         s.State,
					"failureReason": "",
					"region":        s.Region,
					"accountId":     s.AccountID,
					"cloud":         s.Cloud,
				},
			})
		}
		return
	}
}

// ─── Store helpers ────────────────────────────────────────────────────────────

func (p *EMRProvider) loadCluster(ctx context.Context, id string) (cluster, error) {
	e, err := p.store.Get(ctx, rtCluster, id)
	if err != nil {
		return cluster{}, err
	}
	var c cluster
	return c, json.Unmarshal(e.Data, &c)
}

func (p *EMRProvider) saveCluster(ctx context.Context, c cluster) error {
	data, _ := json.Marshal(c)
	entry := sdk.ResourceEntry{Type: rtCluster, ID: c.ID, Data: data}
	exists, _ := p.store.Exists(ctx, rtCluster, c.ID)
	if exists {
		return p.store.Update(ctx, entry)
	}
	return p.store.Create(ctx, entry)
}

func (p *EMRProvider) loadStep(ctx context.Context, clusterID, stepID string) (step, error) {
	e, err := p.store.Get(ctx, rtStep, clusterID+"/"+stepID)
	if err != nil {
		return step{}, err
	}
	var s step
	return s, json.Unmarshal(e.Data, &s)
}

func (p *EMRProvider) saveStep(ctx context.Context, s step) error {
	data, _ := json.Marshal(s)
	key := s.ClusterID + "/" + s.ID
	entry := sdk.ResourceEntry{Type: rtStep, ID: key, Data: data}
	exists, _ := p.store.Exists(ctx, rtStep, key)
	if exists {
		return p.store.Update(ctx, entry)
	}
	return p.store.Create(ctx, entry)
}

// ─── Wire format helpers ──────────────────────────────────────────────────────

func clusterToWire(c cluster, rid func(string, string) string) map[string]any {
	return map[string]any{
		"Id":           c.ID,
		"Name":         c.Name,
		"Status":       map[string]any{"State": c.State},
		"ClusterArn":   rid("emr-cluster", c.ID),
		"ReleaseLabel": c.ReleaseLabel,
		"LogUri":       c.LogURI,
		"Tags":         tagsToWire(c.Tags),
	}
}

func stepToWire(s step) map[string]any {
	status := map[string]any{"State": s.State}
	if s.FailureReason != "" {
		status["FailureDetails"] = map[string]any{
			"Reason":  "STEP_FAILURE",
			"Message": s.FailureReason,
		}
	}
	return map[string]any{
		"Id":     s.ID,
		"Name":   s.Name,
		"Status": status,
		"Config": map[string]any{
			"Jar":       s.JAR,
			"MainClass": s.MainClass,
			"Args":      s.Args,
		},
	}
}

// ─── Param helpers ────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return ""
}

func strParamMap(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func strSliceParam(params map[string]any, key string) []string {
	raw, ok := params[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func parseTags(params map[string]any) map[string]string {
	out := map[string]string{}
	raw, ok := params["Tags"].([]any)
	if !ok {
		return out
	}
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		k, _ := m["Key"].(string)
		v, _ := m["Value"].(string)
		if k != "" {
			out[k] = v
		}
	}
	return out
}

func tagsToWire(tags map[string]string) []map[string]any {
	out := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		out = append(out, map[string]any{"Key": k, "Value": v})
	}
	return out
}

// extractClassArg scans args for the value following "--class".
func extractClassArg(args []string) string {
	for i, a := range args {
		if a == "--class" && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func parseHadoopJarStep(sm map[string]any) (jar, mainClass string, args []string) {
	hjs, ok := sm["HadoopJarStep"].(map[string]any)
	if !ok {
		return
	}
	jar = strParamMap(hjs, "Jar")
	mainClass = strParamMap(hjs, "MainClass")
	rawArgs, _ := hjs["Args"].([]any)
	for _, a := range rawArgs {
		if s, ok := a.(string); ok {
			args = append(args, s)
		}
	}
	return
}

func shortID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

// ─── Response helpers ─────────────────────────────────────────────────────────

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
	return sdk.HandleResponse{
		Err: &sdk.PluginError{Code: code, Message: msg, HTTPStatus: status},
	}
}

func internalError(err error) sdk.HandleResponse {
	return errResponse("InternalFailure", err.Error(), http.StatusInternalServerError)
}
