// Package emr implements the EMR provider (EMRProvider).
// JSON protocol via X-Amz-Target: ElasticMapReduce.{Action}
package emr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// BootstrapAction is a typed representation of an EMR bootstrap action.
// Parsed from the RunJobFlow request and used by the bootstrap resolver.
type BootstrapAction struct {
	Name   string
	S3Path string
	Args   []string
}

// parsedBootstrapActions converts the stored []map[string]any bootstrap
// actions into typed BootstrapAction values for the resolver.
func parsedBootstrapActions(raw []map[string]any) []BootstrapAction {
	out := make([]BootstrapAction, 0, len(raw))
	for _, ba := range raw {
		name, _ := ba["Name"].(string)
		script, _ := ba["ScriptBootstrapAction"].(map[string]any)
		if script == nil {
			script = map[string]any{}
		}
		path, _ := script["Path"].(string)
		var args []string
		if rawArgs, ok := script["Args"].([]any); ok {
			for _, a := range rawArgs {
				if s, ok := a.(string); ok {
					args = append(args, s)
				}
			}
		}
		out = append(out, BootstrapAction{Name: name, S3Path: path, Args: args})
	}
	return out
}

// EMRProvider handles EMR clusters, job flows, steps, instance groups/fleets.
type EMRProvider struct {
	resources      store.ResourceStore
	bus            *events.EventBus
	k8sClient      kubernetes.Interface // nil = no k8s execution
	platformCfg    *platform.PlatformConfig
	namespace      string
	mockMode       bool // true = instant COMPLETED, no k8s calls
	bootstrapFetch blobfs.BlobFetcher // nil = bootstrap disabled
	bootstrapCfg   BootstrapConfig
	// ctx is the provider lifecycle context. runStep goroutines inherit it so
	// they are cancelled on Shutdown(), enabling graceful drain.
	ctx    context.Context
	cancel context.CancelFunc
}

// Option configures EMRProvider.
type Option func(*EMRProvider)

// WithK8s attaches a Kubernetes client and platform config for real Spark execution.
func WithK8s(client kubernetes.Interface, namespace string, platformCfg *platform.PlatformConfig) Option {
	return func(p *EMRProvider) {
		p.k8sClient = client
		p.namespace = namespace
		p.platformCfg = platformCfg
	}
}

// WithMockMode sets instant-COMPLETED behaviour (no k8s calls).
func WithMockMode() Option {
	return func(p *EMRProvider) { p.mockMode = true }
}

// WithBootstrap attaches a BlobFetcher and BootstrapConfig to the provider.
// When set, runSparkSubmitStep resolves bootstrap actions before submitting.
func WithBootstrap(fetcher blobfs.BlobFetcher, cfg BootstrapConfig) Option {
	return func(p *EMRProvider) { p.bootstrapFetch = fetcher; p.bootstrapCfg = cfg }
}

func New(resources store.ResourceStore, bus *events.EventBus, opts ...Option) *EMRProvider {
	ctx, cancel := context.WithCancel(context.Background())
	p := &EMRProvider{resources: resources, bus: bus, ctx: ctx, cancel: cancel}
	for _, o := range opts {
		o(p)
	}
	return p
}


func (p *EMRProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Cluster management
		"EMR.RunJobFlow":                        p.RunJobFlow,
		"EMR.DescribeCluster":                   p.DescribeCluster,
		"EMR.ListClusters":                      p.ListClusters,
		"EMR.TerminateJobFlows":                 p.TerminateJobFlows,
		"EMR.ModifyCluster":                     p.ModifyCluster,
		"EMR.SetTerminationProtection":          p.SetTerminationProtection,
		"EMR.SetVisibleToAllUsers":              p.SetVisibleToAllUsers,
		// Steps
		"EMR.AddJobFlowSteps":                   p.AddJobFlowSteps,
		"EMR.DescribeStep":                      p.DescribeStep,
		"EMR.ListSteps":                         p.ListSteps,
		"EMR.CancelSteps":                       p.CancelSteps,
		// Instance fleets
		"EMR.AddInstanceFleet":                  p.AddInstanceFleet,
		"EMR.ListInstanceFleets":                p.ListInstanceFleets,
		"EMR.ModifyInstanceFleet":               p.ModifyInstanceFleet,
		// Instance groups
		"EMR.AddInstanceGroups":                 p.AddInstanceGroups,
		"EMR.ListInstanceGroups":                p.ListInstanceGroups,
		"EMR.ModifyInstanceGroups":              p.ModifyInstanceGroups,
		// Bootstrap
		"EMR.ListBootstrapActions":              p.ListBootstrapActions,
		// Tags
		"EMR.AddTags":                           p.AddTags,
		"EMR.RemoveTags":                        p.RemoveTags,
		// Block public access
		"EMR.GetBlockPublicAccessConfiguration": p.GetBlockPublicAccessConfiguration,
		"EMR.PutBlockPublicAccessConfiguration": p.PutBlockPublicAccessConfiguration,
		// Managed scaling
		"EMR.PutManagedScalingPolicy":           p.PutManagedScalingPolicy,
		"EMR.GetManagedScalingPolicy":           p.GetManagedScalingPolicy,
		"EMR.RemoveManagedScalingPolicy":        p.RemoveManagedScalingPolicy,
	}
}

const (
	rtCluster            = "emr_cluster"
	rtBlockPublicAccess  = "emr_block_public_access"
	bpaID                = "singleton"
)

// ─── Resource shapes ──────────────────────────────────────────────────────────

// emrCluster is the full stored record.  It mirrors ministack's shape so that
// toWire() can return the record directly without field-by-field mapping.
type emrCluster struct {
	Id                    string           `json:"Id"`
	Name                  string           `json:"Name"`
	ClusterArn            string           `json:"ClusterArn"`
	Status                clusterStatus    `json:"Status"`
	Ec2InstanceAttributes map[string]any   `json:"Ec2InstanceAttributes"`
	InstanceCollectionType string          `json:"InstanceCollectionType"`
	LogUri                string           `json:"LogUri"`
	ReleaseLabel          string           `json:"ReleaseLabel"`
	AutoTerminate         bool             `json:"AutoTerminate"`
	TerminationProtected  bool             `json:"TerminationProtected"`
	VisibleToAllUsers     bool             `json:"VisibleToAllUsers"`
	Applications          []map[string]any `json:"Applications"`
	Tags                  []map[string]any `json:"Tags"` // [{Key, Value}]
	ServiceRole           string           `json:"ServiceRole"`
	JobFlowRole           string           `json:"JobFlowRole"`
	NormalizedInstanceHours int            `json:"NormalizedInstanceHours"`
	MasterPublicDnsName   string           `json:"MasterPublicDnsName"`
	StepConcurrencyLevel  int              `json:"StepConcurrencyLevel"`
	BootstrapActions      []map[string]any `json:"BootstrapActions"`
	InstanceFleets        []map[string]any `json:"InstanceFleets"`
	InstanceGroups        []map[string]any `json:"InstanceGroups"`
	Steps                 []map[string]any `json:"Steps"`
	ManagedScalingPolicy  map[string]any   `json:"ManagedScalingPolicy,omitempty"`
}

type clusterStatus struct {
	State             string         `json:"State"`
	StateChangeReason map[string]any `json:"StateChangeReason"`
	Timeline          map[string]any `json:"Timeline"`
}

func (c emrCluster) toWire() map[string]any {
	// Return the full shape like ministack — marshal then unmarshal to map
	b, _ := json.Marshal(c)
	var m map[string]any
	json.Unmarshal(b, &m)
	delete(m, "Steps") // steps not included in DescribeCluster
	return m
}

func (c emrCluster) toSummary() map[string]any {
	return map[string]any{
		"Id":                      c.Id,
		"Name":                    c.Name,
		"Status":                  map[string]any{"State": c.Status.State, "Timeline": c.Status.Timeline},
		"ClusterArn":              c.ClusterArn,
		"NormalizedInstanceHours": c.NormalizedInstanceHours,
	}
}

// ─── Cluster operations ───────────────────────────────────────────────────────

func (p *EMRProvider) RunJobFlow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	clusterID := clusterID()
	region := nr.Region
	if region == "" {
		region = "us-east-1"
	}
	arn := nr.ResourceID("emr-cluster", clusterID)
	now := nowUnix()

	instances, _ := nr.Params["Instances"].(map[string]any)
	if instances == nil {
		instances = map[string]any{}
	}
	keepAlive, _ := instances["KeepJobFlowAliveWhenNoSteps"].(bool)
	terminationProtected, _ := instances["TerminationProtected"].(bool)
	visibleToAll := true
	if v, ok := nr.Params["VisibleToAllUsers"].(bool); ok {
		visibleToAll = v
	}
	initialState := "WAITING"
	if !keepAlive {
		initialState = "TERMINATED"
	}

	// Applications
	apps := []map[string]any{}
	if raw, ok := nr.Params["Applications"].([]any); ok {
		for _, a := range raw {
			if m, ok := a.(map[string]any); ok {
				apps = append(apps, m)
			}
		}
	}

	// Tags: stored as [{Key, Value}]
	tags := []map[string]any{}
	if raw, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range raw {
			if m, ok := t.(map[string]any); ok {
				tags = append(tags, m)
			}
		}
	}

	// Bootstrap actions
	bootstrapActions := []map[string]any{}
	if raw, ok := nr.Params["BootstrapActions"].([]any); ok {
		for _, b := range raw {
			if m, ok := b.(map[string]any); ok {
				bootstrapActions = append(bootstrapActions, m)
			}
		}
	}

	// Instance fleets / groups
	instanceFleets, instanceGroups, collectionType := buildInstanceCollections(instances, now)

	// Ec2InstanceAttributes
	jobFlowRole := strParam(nr.Params, "JobFlowRole")
	ec2Attrs := map[string]any{
		"Ec2KeyName":                     strParamFromMap(instances, "Ec2KeyName"),
		"Ec2SubnetId":                    strParamFromMap(instances, "Ec2SubnetId"),
		"Ec2AvailabilityZone":            region + "a",
		"IamInstanceProfile":             jobFlowRole,
		"EmrManagedMasterSecurityGroup":  strParamFromMap(instances, "EmrManagedMasterSecurityGroup"),
		"EmrManagedSlaveSecurityGroup":   strParamFromMap(instances, "EmrManagedSlaveSecurityGroup"),
	}

	concurrency := 1
	if v, ok := nr.Params["StepConcurrencyLevel"].(float64); ok {
		concurrency = int(v)
	}

	c := emrCluster{
		Id:         clusterID,
		Name:       name,
		ClusterArn: arn,
		Status: clusterStatus{
			State:             initialState,
			StateChangeReason: map[string]any{"Code": "", "Message": ""},
			Timeline:          map[string]any{"CreationDateTime": now, "ReadyDateTime": now},
		},
		Ec2InstanceAttributes:   ec2Attrs,
		InstanceCollectionType:  collectionType,
		LogUri:                  strParam(nr.Params, "LogUri"),
		ReleaseLabel:            strParam(nr.Params, "ReleaseLabel"),
		AutoTerminate:           !keepAlive,
		TerminationProtected:    terminationProtected,
		VisibleToAllUsers:       visibleToAll,
		Applications:            apps,
		Tags:                    tags,
		ServiceRole:             strParam(nr.Params, "ServiceRole"),
		JobFlowRole:             jobFlowRole,
		NormalizedInstanceHours: 0,
		MasterPublicDnsName:     "ec2-0-0-0-0.compute-1.amazonaws.com",
		StepConcurrencyLevel:    concurrency,
		BootstrapActions:        bootstrapActions,
		InstanceFleets:          instanceFleets,
		InstanceGroups:          instanceGroups,
		Steps:                   []map[string]any{},
	}

	// Steps at creation time
	if raw, ok := nr.Params["Steps"].([]any); ok {
		for _, s := range raw {
			if m, ok := s.(map[string]any); ok {
				c.Steps = append(c.Steps, makeStep(m, "COMPLETED"))
			}
		}
	}

	data, _ := json.Marshal(c)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtCluster, ID: clusterID, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"JobFlowId": clusterID, "ClusterArn": arn}), nil
}

func (p *EMRProvider) DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(map[string]any{"Cluster": c.toWire()}), nil
}

func (p *EMRProvider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtCluster, "")
	if err != nil {
		return nil, err
	}
	// ClusterStates can be a list or single string
	stateFilter := strSliceParam(nr.Params, "ClusterStates")
	stateSet := map[string]bool{}
	for _, s := range stateFilter {
		stateSet[s] = true
	}
	summaries := []map[string]any{}
	for _, e := range entries {
		var c emrCluster
		json.Unmarshal(e.Data, &c)
		if len(stateSet) > 0 && !stateSet[c.Status.State] {
			continue
		}
		summaries = append(summaries, c.toSummary())
	}
	return provider.OK(map[string]any{"Clusters": summaries}), nil
}

func (p *EMRProvider) TerminateJobFlows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := strSliceParam(nr.Params, "JobFlowIds")
	for _, id := range ids {
		c, err := p.loadCluster(ctx, id)
		if err != nil {
			continue
		}
		if c.TerminationProtected {
			return nil, &model.ProviderError{
				Code:       "ValidationException",
				Message:    fmt.Sprintf("Cluster %s is protected from termination. Disable termination protection first.", id),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		c.Status.State = "TERMINATED"
		c.Status.StateChangeReason = map[string]any{"Code": "USER_REQUEST", "Message": "User request"}
		p.saveCluster(ctx, c)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *EMRProvider) ModifyCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	if v, ok := nr.Params["StepConcurrencyLevel"].(float64); ok {
		c.StepConcurrencyLevel = int(v)
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{"StepConcurrencyLevel": c.StepConcurrencyLevel}), nil
}

func (p *EMRProvider) SetTerminationProtection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := strSliceParam(nr.Params, "JobFlowIds")
	protect := false
	if v, ok := nr.Params["TerminationProtected"].(bool); ok {
		protect = v
	}
	for _, id := range ids {
		c, err := p.loadCluster(ctx, id)
		if err != nil {
			continue
		}
		c.TerminationProtected = protect
		p.saveCluster(ctx, c)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *EMRProvider) SetVisibleToAllUsers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := strSliceParam(nr.Params, "JobFlowIds")
	visible := true
	if v, ok := nr.Params["VisibleToAllUsers"].(bool); ok {
		visible = v
	}
	for _, id := range ids {
		c, err := p.loadCluster(ctx, id)
		if err != nil {
			continue
		}
		c.VisibleToAllUsers = visible
		p.saveCluster(ctx, c)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Step operations ──────────────────────────────────────────────────────────

func (p *EMRProvider) AddJobFlowSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "JobFlowId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	stepIDs := []string{}
	useK8s := p.k8sClient != nil && !p.mockMode
	if raw, ok := nr.Params["Steps"].([]any); ok {
		for _, s := range raw {
			m, ok := s.(map[string]any)
			if !ok {
				continue
			}
			initialState := "COMPLETED"
			if useK8s {
				initialState = "PENDING"
			}
			step := makeStep(m, initialState)
			sid := step["Id"].(string)
			c.Steps = append(c.Steps, step)
			stepIDs = append(stepIDs, sid)

			if useK8s {
				// Launch async step execution; state transitions are published via emitStepStateChange.
				// p.ctx is cancelled by Shutdown() so the goroutine stops on server shutdown.
				go p.runStep(p.ctx, id, sid, m)
			}
		}
	}
	p.saveCluster(ctx, c)
	// Publish initial state events for newly added steps
	newIDs := map[string]bool{}
	for _, sid := range stepIDs {
		newIDs[sid] = true
	}
	for _, step := range c.Steps {
		sid, _ := step["Id"].(string)
		if !newIDs[sid] {
			continue
		}
		name, _ := step["Name"].(string)
		status, _ := step["Status"].(map[string]any)
		state, _ := status["State"].(string)
		failReason := ""
		if fd, ok := status["FailureDetails"].(map[string]any); ok {
			failReason, _ = fd["Message"].(string)
		}
		p.bus.Publish(events.Event{
			Type: events.EventEMRStepState,
			Payload: events.EMRStepStateEvent{
				JobFlowID:     id,
				StepID:        sid,
				Name:          name,
				State:         state,
				FailureReason: failReason,
				Region:        nr.Region,
				AccountID:     nr.AccountID,
				Cloud:         nr.Cloud,
			},
		})
	}
	return provider.OK(map[string]any{"StepIds": stepIDs}), nil
}

func (p *EMRProvider) DescribeStep(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	stepID := strParam(nr.Params, "StepId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	for _, s := range c.Steps {
		if s["Id"] == stepID {
			return provider.OK(map[string]any{"Step": s}), nil
		}
	}
	return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Step id '%s' is not valid.", stepID), HTTPStatus: http.StatusBadRequest}
}

func (p *EMRProvider) ListSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	stateFilter := strSliceParam(nr.Params, "StepStates")
	stateSet := map[string]bool{}
	for _, s := range stateFilter {
		stateSet[s] = true
	}
	steps := []map[string]any{}
	for _, s := range c.Steps {
		if len(stateSet) > 0 {
			status, _ := s["Status"].(map[string]any)
			state, _ := status["State"].(string)
			if !stateSet[state] {
				continue
			}
		}
		steps = append(steps, s)
	}
	return provider.OK(map[string]any{"Steps": steps}), nil
}

func (p *EMRProvider) CancelSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	stepIDs := strSliceParam(nr.Params, "StepIds")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	cancelInfo := []map[string]any{}
	idSet := map[string]bool{}
	for _, sid := range stepIDs {
		idSet[sid] = true
	}
	for i := range c.Steps {
		sid, _ := c.Steps[i]["Id"].(string)
		if !idSet[sid] {
			continue
		}
		status, _ := c.Steps[i]["Status"].(map[string]any)
		state, _ := status["State"].(string)
		if state == "PENDING" || state == "RUNNING" {
			status["State"] = "CANCELLED"
			c.Steps[i]["Status"] = status
			cancelInfo = append(cancelInfo, map[string]any{"StepId": sid, "Status": "SUBMITTED"})
			stepName, _ := c.Steps[i]["Name"].(string)
			p.bus.Publish(events.Event{
				Type: events.EventEMRStepState,
				Payload: events.EMRStepStateEvent{
					JobFlowID: clusterID,
					StepID:    sid,
					Name:      stepName,
					State:     "CANCELLED",
					Region:    nr.Region,
					AccountID: nr.AccountID,
					Cloud:     nr.Cloud,
				},
			})
		} else {
			cancelInfo = append(cancelInfo, map[string]any{
				"StepId": sid,
				"Status": "FAILED_TO_CANCEL",
				"Reason": fmt.Sprintf("Step in state %s cannot be cancelled", state),
			})
		}
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{"CancelStepsInfoList": cancelInfo}), nil
}

// ─── Instance fleet operations ────────────────────────────────────────────────

func (p *EMRProvider) AddInstanceFleet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	fleet, _ := nr.Params["InstanceFleet"].(map[string]any)
	if fleet == nil {
		fleet = map[string]any{}
	}
	fid := fleetID()
	now := nowUnix()
	fleetType := strParamFromMap(fleet, "InstanceFleetType")
	if fleetType == "" {
		fleetType = "TASK"
	}
	onDemand := float64(0)
	spot := float64(0)
	if v, ok := fleet["TargetOnDemandCapacity"].(float64); ok {
		onDemand = v
	}
	if v, ok := fleet["TargetSpotCapacity"].(float64); ok {
		spot = v
	}
	record := map[string]any{
		"Id":                          fid,
		"Name":                        strParamFromMap(fleet, "Name"),
		"Status":                      map[string]any{"State": "RUNNING", "StateChangeReason": map[string]any{}, "Timeline": map[string]any{"CreationDateTime": now}},
		"InstanceFleetType":           fleetType,
		"TargetOnDemandCapacity":      onDemand,
		"TargetSpotCapacity":          spot,
		"ProvisionedOnDemandCapacity": onDemand,
		"ProvisionedSpotCapacity":     spot,
		"InstanceTypeSpecifications":  fleet["InstanceTypeConfigs"],
	}
	c.InstanceFleets = append(c.InstanceFleets, record)
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{"ClusterArn": c.ClusterArn, "InstanceFleetId": fid}), nil
}

func (p *EMRProvider) ListInstanceFleets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	fleets := c.InstanceFleets
	if fleets == nil {
		fleets = []map[string]any{}
	}
	return provider.OK(map[string]any{"InstanceFleets": fleets}), nil
}

func (p *EMRProvider) ModifyInstanceFleet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	mod, _ := nr.Params["InstanceFleet"].(map[string]any)
	if mod == nil {
		return provider.OK(map[string]any{}), nil
	}
	fleetID := strParamFromMap(mod, "InstanceFleetId")
	for i := range c.InstanceFleets {
		if c.InstanceFleets[i]["Id"] == fleetID {
			if v, ok := mod["TargetOnDemandCapacity"].(float64); ok {
				c.InstanceFleets[i]["TargetOnDemandCapacity"] = v
				c.InstanceFleets[i]["ProvisionedOnDemandCapacity"] = v
			}
			if v, ok := mod["TargetSpotCapacity"].(float64); ok {
				c.InstanceFleets[i]["TargetSpotCapacity"] = v
				c.InstanceFleets[i]["ProvisionedSpotCapacity"] = v
			}
		}
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{}), nil
}

// ─── Instance group operations ────────────────────────────────────────────────

func (p *EMRProvider) AddInstanceGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "JobFlowId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	now := nowUnix()
	groupIDs := []string{}
	if raw, ok := nr.Params["InstanceGroups"].([]any); ok {
		for _, r := range raw {
			m, ok := r.(map[string]any)
			if !ok {
				continue
			}
			gid := groupID()
			count := float64(1)
			if v, ok := m["InstanceCount"].(float64); ok {
				count = v
			}
			record := map[string]any{
				"Id":                     gid,
				"Name":                   strParamFromMap(m, "Name"),
				"Market":                 firstNonEmpty(strParamFromMap(m, "Market"), "ON_DEMAND"),
				"InstanceGroupType":      strParamFromMap(m, "InstanceRole"),
				"InstanceType":           firstNonEmpty(strParamFromMap(m, "InstanceType"), "m5.xlarge"),
				"RequestedInstanceCount": count,
				"RunningInstanceCount":   count,
				"Status":                 map[string]any{"State": "RUNNING", "StateChangeReason": map[string]any{}, "Timeline": map[string]any{"CreationDateTime": now}},
			}
			c.InstanceGroups = append(c.InstanceGroups, record)
			groupIDs = append(groupIDs, gid)
		}
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{"JobFlowId": id, "InstanceGroupIds": groupIDs}), nil
}

func (p *EMRProvider) ListInstanceGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	groups := c.InstanceGroups
	if groups == nil {
		groups = []map[string]any{}
	}
	return provider.OK(map[string]any{"InstanceGroups": groups}), nil
}

func (p *EMRProvider) ModifyInstanceGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	mods, ok := nr.Params["InstanceGroups"].([]any)
	if !ok {
		return provider.OK(map[string]any{}), nil
	}
	for _, raw := range mods {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		igID := strParamFromMap(m, "InstanceGroupId")
		for i := range c.InstanceGroups {
			if c.InstanceGroups[i]["Id"] == igID {
				if count, ok := m["InstanceCount"].(float64); ok {
					c.InstanceGroups[i]["RequestedInstanceCount"] = count
					c.InstanceGroups[i]["RunningInstanceCount"] = count
				}
			}
		}
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{}), nil
}

// ─── Bootstrap actions ────────────────────────────────────────────────────────

func (p *EMRProvider) ListBootstrapActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", clusterID), HTTPStatus: http.StatusBadRequest}
	}
	actions := []map[string]any{}
	for _, ba := range c.BootstrapActions {
		script, _ := ba["ScriptBootstrapAction"].(map[string]any)
		if script == nil {
			script = map[string]any{}
		}
		args, _ := script["Args"].([]any)
		if args == nil {
			args = []any{}
		}
		actions = append(actions, map[string]any{
			"Name":       strParamFromMap(ba, "Name"),
			"ScriptPath": strParamFromMap(script, "Path"),
			"Args":       args,
		})
	}
	return provider.OK(map[string]any{"BootstrapActions": actions}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *EMRProvider) AddTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ResourceId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Resource id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	// Upsert by key
	existing := map[string]int{}
	for i, t := range c.Tags {
		if k, ok := t["Key"].(string); ok {
			existing[k] = i
		}
	}
	if raw, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range raw {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				if idx, found := existing[k]; found {
					c.Tags[idx] = tm
				} else {
					c.Tags = append(c.Tags, tm)
					existing[k] = len(c.Tags) - 1
				}
			}
		}
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{}), nil
}

func (p *EMRProvider) RemoveTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ResourceId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Resource id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	keys := map[string]bool{}
	for _, k := range strSliceParam(nr.Params, "TagKeys") {
		keys[k] = true
	}
	filtered := c.Tags[:0]
	for _, t := range c.Tags {
		k, _ := t["Key"].(string)
		if !keys[k] {
			filtered = append(filtered, t)
		}
	}
	c.Tags = filtered
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{}), nil
}

// ─── Block public access ──────────────────────────────────────────────────────

func (p *EMRProvider) GetBlockPublicAccessConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	cfg := p.loadBPA(ctx)
	return provider.OK(map[string]any{
		"BlockPublicAccessConfiguration": cfg,
		"BlockPublicAccessConfigurationMetadata": map[string]any{
			"CreationDateTime": nowUnix(),
			"CreatedByArn":     nr.ResourceID("iam-root", ""),
		},
	}), nil
}

func (p *EMRProvider) PutBlockPublicAccessConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	config, _ := nr.Params["BlockPublicAccessConfiguration"].(map[string]any)
	if config == nil {
		config = map[string]any{}
	}
	cfg := map[string]any{
		"BlockPublicSecurityGroupRules":          config["BlockPublicSecurityGroupRules"],
		"PermittedPublicSecurityGroupRuleRanges": config["PermittedPublicSecurityGroupRuleRanges"],
	}
	if cfg["PermittedPublicSecurityGroupRuleRanges"] == nil {
		cfg["PermittedPublicSecurityGroupRuleRanges"] = []any{}
	}
	data, _ := json.Marshal(cfg)
	e := store.ResourceEntry{Type: rtBlockPublicAccess, ID: bpaID, Data: data}
	if _, err := p.resources.Get(ctx, rtBlockPublicAccess, bpaID); err != nil {
		if err := p.resources.Create(ctx, e); err != nil {
			slog.Warn("emr: failed to persist block public access config", "err", err)
		}
	} else {
		if err := p.resources.Update(ctx, e); err != nil {
			slog.Warn("emr: failed to update block public access config", "err", err)
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *EMRProvider) loadBPA(ctx context.Context) map[string]any {
	e, err := p.resources.Get(ctx, rtBlockPublicAccess, bpaID)
	if err != nil {
		return map[string]any{
			"BlockPublicSecurityGroupRules":          false,
			"PermittedPublicSecurityGroupRuleRanges": []any{},
		}
	}
	var cfg map[string]any
	json.Unmarshal(e.Data, &cfg)
	return cfg
}

// ─── Managed scaling ──────────────────────────────────────────────────────────

func (p *EMRProvider) PutManagedScalingPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	if policy, ok := nr.Params["ManagedScalingPolicy"].(map[string]any); ok {
		c.ManagedScalingPolicy = policy
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{}), nil
}

func (p *EMRProvider) GetManagedScalingPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	policy := c.ManagedScalingPolicy
	if policy == nil {
		policy = map[string]any{}
	}
	return provider.OK(map[string]any{"ManagedScalingPolicy": policy}), nil
}

func (p *EMRProvider) RemoveManagedScalingPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: fmt.Sprintf("Cluster id '%s' is not valid.", id), HTTPStatus: http.StatusBadRequest}
	}
	c.ManagedScalingPolicy = nil
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{}), nil
}

// ─── Step state record ────────────────────────────────────────────────────────

// updateStepRecord loads the cluster, finds the step, updates its state record,
// and saves back. It does NOT publish bus events — callers do that separately.
func (p *EMRProvider) updateStepRecord(ctx context.Context, clusterID, stepID, newState, message string) string {
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return ""
	}
	now := nowUnix()
	stepName := ""
	for i, s := range c.Steps {
		if s["Id"] != stepID {
			continue
		}
		stepName, _ = s["Name"].(string)
		status, _ := s["Status"].(map[string]any)
		status["State"] = newState
		timeline, _ := status["Timeline"].(map[string]any)
		if timeline == nil {
			timeline = map[string]any{}
			status["Timeline"] = timeline
		}
		switch newState {
		case "RUNNING":
			timeline["StartDateTime"] = now
		case "COMPLETED", "FAILED", "CANCELLED":
			timeline["EndDateTime"] = now
		}
		if newState == "FAILED" && message != "" {
			status["FailureDetails"] = map[string]any{"Message": message}
		}
		c.Steps[i] = s
		break
	}
	p.saveCluster(ctx, c)
	return stepName
}

// Reset wipes the resource store state (no executor to reset).
func (p *EMRProvider) Reset() {}

// Shutdown cancels the provider context, signalling all in-flight runStep
// goroutines to stop after their current K8s operation completes.
func (p *EMRProvider) Shutdown(_ context.Context) {
	p.cancel()
}

// ─── Store helpers ────────────────────────────────────────────────────────────

func (p *EMRProvider) loadCluster(ctx context.Context, id string) (emrCluster, error) {
	e, err := p.resources.Get(ctx, rtCluster, id)
	if err != nil {
		return emrCluster{}, err
	}
	var c emrCluster
	return c, json.Unmarshal(e.Data, &c)
}

func (p *EMRProvider) saveCluster(ctx context.Context, c emrCluster) {
	data, _ := json.Marshal(c)
	if err := p.resources.Update(ctx, store.ResourceEntry{Type: rtCluster, ID: c.Id, Data: data}); err != nil {
		slog.Warn("emr: failed to persist cluster state", "cluster", c.Id, "state", c.Status.State, "err", err)
	}
}

// ─── Build helpers ────────────────────────────────────────────────────────────

func buildInstanceCollections(instances map[string]any, now float64) (fleets, groups []map[string]any, collectionType string) {
	if rawFleets, ok := instances["InstanceFleets"].([]any); ok && len(rawFleets) > 0 {
		for _, f := range rawFleets {
			m, ok := f.(map[string]any)
			if !ok {
				continue
			}
			ftype := strParamFromMap(m, "InstanceFleetType")
			onDemand := float64(0)
			spot := float64(0)
			if v, ok := m["TargetOnDemandCapacity"].(float64); ok {
				onDemand = v
			}
			if v, ok := m["TargetSpotCapacity"].(float64); ok {
				spot = v
			}
			fleets = append(fleets, map[string]any{
				"Id":                          fleetID(),
				"Name":                        firstNonEmpty(strParamFromMap(m, "Name"), ftype),
				"Status":                      map[string]any{"State": "RUNNING", "StateChangeReason": map[string]any{}, "Timeline": map[string]any{"CreationDateTime": now}},
				"InstanceFleetType":           ftype,
				"TargetOnDemandCapacity":      onDemand,
				"TargetSpotCapacity":          spot,
				"ProvisionedOnDemandCapacity": onDemand,
				"ProvisionedSpotCapacity":     spot,
				"InstanceTypeSpecifications":  m["InstanceTypeConfigs"],
			})
		}
		return fleets, nil, "INSTANCE_FLEET"
	}

	if rawGroups, ok := instances["InstanceGroups"].([]any); ok && len(rawGroups) > 0 {
		for _, r := range rawGroups {
			m, ok := r.(map[string]any)
			if !ok {
				continue
			}
			count := float64(1)
			if v, ok := m["InstanceCount"].(float64); ok {
				count = v
			}
			groups = append(groups, map[string]any{
				"Id":                     groupID(),
				"Name":                   firstNonEmpty(strParamFromMap(m, "Name"), strParamFromMap(m, "InstanceRole")),
				"Market":                 firstNonEmpty(strParamFromMap(m, "Market"), "ON_DEMAND"),
				"InstanceGroupType":      strParamFromMap(m, "InstanceRole"),
				"InstanceType":           firstNonEmpty(strParamFromMap(m, "InstanceType"), "m5.xlarge"),
				"RequestedInstanceCount": count,
				"RunningInstanceCount":   count,
				"Status":                 map[string]any{"State": "RUNNING", "StateChangeReason": map[string]any{}, "Timeline": map[string]any{"CreationDateTime": now}},
			})
		}
		return nil, groups, "INSTANCE_GROUP"
	}

	// Simple mode
	masterType := firstNonEmpty(strParamFromMap(instances, "MasterInstanceType"), "m5.xlarge")
	slaveType := firstNonEmpty(strParamFromMap(instances, "SlaveInstanceType"), "m5.xlarge")
	instanceCount := float64(1)
	if v, ok := instances["InstanceCount"].(float64); ok {
		instanceCount = v
	}
	groups = []map[string]any{
		{
			"Id": groupID(), "Name": "Master", "Market": "ON_DEMAND",
			"InstanceGroupType": "MASTER", "InstanceType": masterType,
			"RequestedInstanceCount": float64(1), "RunningInstanceCount": float64(1),
			"Status": map[string]any{"State": "RUNNING", "StateChangeReason": map[string]any{}, "Timeline": map[string]any{"CreationDateTime": now}},
		},
	}
	if instanceCount > 1 {
		groups = append(groups, map[string]any{
			"Id": groupID(), "Name": "Core", "Market": "ON_DEMAND",
			"InstanceGroupType": "CORE", "InstanceType": slaveType,
			"RequestedInstanceCount": instanceCount - 1, "RunningInstanceCount": instanceCount - 1,
			"Status": map[string]any{"State": "RUNNING", "StateChangeReason": map[string]any{}, "Timeline": map[string]any{"CreationDateTime": now}},
		})
	}
	return nil, groups, "INSTANCE_GROUP"
}

func extractClassArg(args []any) string {
	for i, a := range args {
		if s, ok := a.(string); ok && s == "--class" && i+1 < len(args) {
			if next, ok := args[i+1].(string); ok {
				return next
			}
		}
	}
	return ""
}

// makeStep builds the step wire shape. initialState is "COMPLETED" for the
// instant-completion path (no executor) and "PENDING" for the executor path.
// Bad-class detection is only applied on the instant-completion path.
func makeStep(cfg map[string]any, initialState string) map[string]any {
	now := nowUnix()
	jar := ""
	args := []any{}
	mainClass := ""
	props := map[string]any{}
	if hj, ok := cfg["HadoopJarStep"].(map[string]any); ok {
		jar = strParamFromMap(hj, "Jar")
		mainClass = strParamFromMap(hj, "MainClass")
		if a, ok := hj["Args"].([]any); ok {
			args = a
		}
		if p, ok := hj["Properties"].([]any); ok {
			for _, pi := range p {
				if pm, ok := pi.(map[string]any); ok {
					k, _ := pm["Key"].(string)
					v, _ := pm["Value"].(string)
					props[k] = v
				}
			}
		}
	}
	actionOnFailure := strParamFromMap(cfg, "ActionOnFailure")
	if actionOnFailure == "" {
		actionOnFailure = "CONTINUE"
	}

	stepState := initialState
	var failureDetails map[string]any
	// Bad-class detection only applies on the instant-completion path.
	if initialState != "PENDING" {
		classArg := extractClassArg(args)
		if classArg != "" && strings.Contains(classArg, "nonexistent") {
			stepState = "FAILED"
			failureDetails = map[string]any{
				"Reason":  "STEP_FAILURE",
				"Message": fmt.Sprintf("java.lang.ClassNotFoundException: %s", classArg),
			}
		}
	}

	timeline := map[string]any{"CreationDateTime": now}
	if stepState != "PENDING" {
		timeline["StartDateTime"] = now
		timeline["EndDateTime"] = now
	}
	status := map[string]any{
		"State":             stepState,
		"StateChangeReason": map[string]any{},
		"Timeline":          timeline,
	}
	if failureDetails != nil {
		status["FailureDetails"] = failureDetails
	}

	return map[string]any{
		"Id":   stepID(),
		"Name": strParamFromMap(cfg, "Name"),
		"Config": map[string]any{
			"Jar":        jar,
			"Properties": props,
			"MainClass":  mainClass,
			"Args":       args,
		},
		"ActionOnFailure": actionOnFailure,
		"Status":          status,
	}
}

// extractHadoopJarStep pulls jar, mainClass, and args out of a step config map.
func extractHadoopJarStep(cfg map[string]any) (jar, mainClass string, args []string) {
	hj, ok := cfg["HadoopJarStep"].(map[string]any)
	if !ok {
		return
	}
	jar = strParamFromMap(hj, "Jar")
	mainClass = strParamFromMap(hj, "MainClass")
	if raw, ok := hj["Args"].([]any); ok {
		for _, a := range raw {
			if s, ok := a.(string); ok {
				args = append(args, s)
			}
		}
	}
	return
}

// ─── Param helpers ─────────────────────────────────────────────────────────────

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

func strSliceParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return val
	case string:
		if val != "" {
			return []string{val}
		}
	}
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ─── ID generators ────────────────────────────────────────────────────────────

const idChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randID(prefix string, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = idChars[rand.Intn(len(idChars))]
	}
	return prefix + string(b)
}

func clusterID() string { return randID("j-", 13) }
func stepID() string    { return randID("s-", 13) }
func groupID() string   { return randID("ig-", 13) }
func fleetID() string   { return randID("if-", 13) }

func nowUnix() float64 {
	return float64(time.Now().Unix())
}

// rewriteYARNToK8s substitutes "--master yarn" with "--master k8s://kubernetes.default.svc"
// in EMR-on-EC2 step args. This is an emulation lie — EMR classic uses YARN but JaisCloud
// routes through the K8s executor. If --master is already k8s://, local[*], or absent, no change.
// The caller must also set AllowClusterMode=true so the K8s executor accepts the k8s:// master.
func rewriteYARNToK8s(args []string) ([]string, bool) {
	out := make([]string, 0, len(args))
	rewrote := false
	for i := 0; i < len(args); i++ {
		if args[i] == "--master" && i+1 < len(args) && args[i+1] == "yarn" {
			out = append(out, "--master", "k8s://kubernetes.default.svc")
			i++
			rewrote = true
			continue
		}
		if args[i] == "--master=yarn" {
			out = append(out, "--master=k8s://kubernetes.default.svc")
			rewrote = true
			continue
		}
		out = append(out, args[i])
	}
	return out, rewrote
}

