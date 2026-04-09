// Package emr implements the EMR provider (EMRProvider).
package emr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// EMRProvider handles EMR clusters, job flows, and steps.
type EMRProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *EMRProvider {
	return &EMRProvider{resources: resources}
}

func (p *EMRProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"EMR.RunJobFlow":            p.RunJobFlow,
		"EMR.DescribeCluster":       p.DescribeCluster,
		"EMR.ListClusters":          p.ListClusters,
		"EMR.TerminateJobFlows":     p.TerminateJobFlows,
		"EMR.AddJobFlowSteps":       p.AddJobFlowSteps,
		"EMR.ListSteps":             p.ListSteps,
		"EMR.DescribeStep":          p.DescribeStep,
		"EMR.SetTerminationProtection": p.SetTerminationProtection,
		"EMR.ModifyCluster":         p.ModifyCluster,
		"EMR.ListInstanceGroups":    p.ListInstanceGroups,
		"EMR.AddInstanceGroups":     p.AddInstanceGroups,
	}
}

const rtCluster = "emr_cluster"

type emrStep struct {
	Id     string `json:"Id"`
	Name   string `json:"Name"`
	Status string `json:"Status"` // PENDING, RUNNING, COMPLETED, FAILED
	Config struct {
		Jar        string            `json:"Jar"`
		Args       []string          `json:"Args"`
		Properties map[string]string `json:"Properties"`
	} `json:"Config"`
}

type instanceGroup struct {
	Id            string `json:"Id"`
	Name          string `json:"Name"`
	Market        string `json:"Market"`
	InstanceType  string `json:"InstanceType"`
	InstanceRole  string `json:"InstanceRole"`
	RequestedCount int   `json:"RequestedCount"`
	RunningCount   int   `json:"RunningCount"`
	Status        string `json:"Status"`
}

type emrCluster struct {
	Id                    string            `json:"Id"`
	Name                  string            `json:"Name"`
	Status                string            `json:"Status"`
	LogUri                string            `json:"LogUri"`
	ReleaseLabel          string            `json:"ReleaseLabel"`
	Applications          []string          `json:"Applications"`
	ServiceRole           string            `json:"ServiceRole"`
	JobFlowRole           string            `json:"JobFlowRole"`
	TerminationProtected  bool              `json:"TerminationProtected"`
	VisibleToAllUsers     bool              `json:"VisibleToAllUsers"`
	CreationTime          time.Time         `json:"CreationTime"`
	Tags                  map[string]string `json:"Tags"`
	Steps                 []emrStep         `json:"Steps"`
	InstanceGroups        []instanceGroup   `json:"InstanceGroups"`
	MasterPublicDnsName   string            `json:"MasterPublicDnsName"`
	StepConcurrencyLevel  int               `json:"StepConcurrencyLevel"`
}

func (c emrCluster) toWire() map[string]any {
	apps := make([]map[string]any, len(c.Applications))
	for i, a := range c.Applications {
		apps[i] = map[string]any{"Name": a}
	}
	tags := make([]map[string]any, 0, len(c.Tags))
	for k, v := range c.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	return map[string]any{
		"Id":   c.Id,
		"Name": c.Name,
		"Status": map[string]any{
			"State": c.Status,
			"StateChangeReason": map[string]any{
				"Code":    "",
				"Message": "",
			},
			"Timeline": map[string]any{
				"CreationDateTime": float64(c.CreationTime.Unix()),
			},
		},
		"LogUri":               c.LogUri,
		"ReleaseLabel":         c.ReleaseLabel,
		"Applications":         apps,
		"ServiceRole":          c.ServiceRole,
		"JobFlowRole":          c.JobFlowRole,
		"TerminationProtected": c.TerminationProtected,
		"VisibleToAllUsers":    c.VisibleToAllUsers,
		"MasterPublicDnsName":  c.MasterPublicDnsName,
		"StepConcurrencyLevel": c.StepConcurrencyLevel,
		"Tags":                 tags,
	}
}

func (c emrCluster) toSummary() map[string]any {
	return map[string]any{
		"Id":   c.Id,
		"Name": c.Name,
		"Status": map[string]any{
			"State": c.Status,
			"Timeline": map[string]any{
				"CreationDateTime": float64(c.CreationTime.Unix()),
			},
		},
		"ClusterArn": fmt.Sprintf("arn:aws:elasticmapreduce:us-east-1:000000000000:cluster/%s", c.Id),
	}
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (p *EMRProvider) RunJobFlow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	clusterID := fmt.Sprintf("j-%s", shortID())
	c := emrCluster{
		Id:                   clusterID,
		Name:                 name,
		Status:               "WAITING",
		LogUri:               strParam(nr.Params, "LogUri"),
		ReleaseLabel:         strParam(nr.Params, "ReleaseLabel"),
		ServiceRole:          strParam(nr.Params, "ServiceRole"),
		JobFlowRole:          strParam(nr.Params, "JobFlowRole"),
		TerminationProtected: false,
		VisibleToAllUsers:    true,
		CreationTime:         time.Now().UTC(),
		Tags:                 map[string]string{},
		MasterPublicDnsName:  fmt.Sprintf("%s-master.emr.localhost", clusterID),
		StepConcurrencyLevel: 1,
	}

	// Parse applications
	if apps, ok := nr.Params["Applications"].([]any); ok {
		for _, a := range apps {
			if m, ok := a.(map[string]any); ok {
				if n, ok := m["Name"].(string); ok {
					c.Applications = append(c.Applications, n)
				}
			}
		}
	}

	// Parse steps
	c.Steps = parseSteps(nr.Params, clusterID)

	// Parse instance groups from Instances.InstanceGroups
	c.InstanceGroups = parseInstanceGroups(nr.Params, clusterID)
	if len(c.InstanceGroups) == 0 {
		// Default groups from Instances block
		c.InstanceGroups = defaultInstanceGroups(nr.Params, clusterID)
	}

	data, _ := json.Marshal(c)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtCluster, ID: clusterID, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"JobFlowId": clusterID}), nil
}

func (p *EMRProvider) DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(map[string]any{"Cluster": c.toWire()}), nil
}

func (p *EMRProvider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtCluster, "")
	if err != nil {
		return nil, err
	}
	summaries := []map[string]any{}
	stateFilter := strParam(nr.Params, "ClusterStates")
	for _, e := range entries {
		var c emrCluster
		json.Unmarshal(e.Data, &c)
		if stateFilter != "" && c.Status != stateFilter {
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
		c.Status = "TERMINATED"
		p.saveCluster(ctx, c)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *EMRProvider) AddJobFlowSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "JobFlowId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	newSteps := parseSteps(nr.Params, id)
	stepIDs := make([]string, len(newSteps))
	for i, s := range newSteps {
		c.Steps = append(c.Steps, s)
		stepIDs[i] = s.Id
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{"StepIds": stepIDs}), nil
}

func (p *EMRProvider) ListSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	steps := make([]map[string]any, len(c.Steps))
	for i, s := range c.Steps {
		steps[i] = stepToWire(s)
	}
	return provider.OK(map[string]any{"Steps": steps}), nil
}

func (p *EMRProvider) DescribeStep(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterID := strParam(nr.Params, "ClusterId")
	stepID := strParam(nr.Params, "StepId")
	c, err := p.loadCluster(ctx, clusterID)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	for _, s := range c.Steps {
		if s.Id == stepID {
			return provider.OK(map[string]any{"Step": stepToWire(s)}), nil
		}
	}
	return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Step not found", HTTPStatus: http.StatusBadRequest}
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

func (p *EMRProvider) ModifyCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	if v, ok := nr.Params["StepConcurrencyLevel"].(float64); ok {
		c.StepConcurrencyLevel = int(v)
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{"StepConcurrencyLevel": c.StepConcurrencyLevel}), nil
}

func (p *EMRProvider) ListInstanceGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "ClusterId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	groups := make([]map[string]any, len(c.InstanceGroups))
	for i, g := range c.InstanceGroups {
		groups[i] = instanceGroupToWire(g)
	}
	return provider.OK(map[string]any{"InstanceGroups": groups}), nil
}

func (p *EMRProvider) AddInstanceGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "JobFlowId")
	c, err := p.loadCluster(ctx, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRequestException", Message: "Cluster not found", HTTPStatus: http.StatusBadRequest}
	}
	newGroups := parseInstanceGroups(nr.Params, id)
	groupIDs := make([]string, len(newGroups))
	for i, g := range newGroups {
		c.InstanceGroups = append(c.InstanceGroups, g)
		groupIDs[i] = g.Id
	}
	p.saveCluster(ctx, c)
	return provider.OK(map[string]any{
		"JobFlowId":        id,
		"InstanceGroupIds": groupIDs,
		"ClusterArn":       fmt.Sprintf("arn:aws:elasticmapreduce:us-east-1:000000000000:cluster/%s", id),
	}), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

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
	p.resources.Update(ctx, store.ResourceEntry{Type: rtCluster, ID: c.Id, Data: data})
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
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
	}
	return nil
}

func parseSteps(params map[string]any, clusterID string) []emrStep {
	stepsRaw, ok := params["Steps"].([]any)
	if !ok {
		return nil
	}
	steps := make([]emrStep, 0, len(stepsRaw))
	for _, raw := range stepsRaw {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		s := emrStep{
			Id:     fmt.Sprintf("s-%s", shortID()),
			Name:   strParamFromMap(m, "Name"),
			Status: "COMPLETED",
		}
		if hc, ok := m["HadoopJarStep"].(map[string]any); ok {
			s.Config.Jar = strParamFromMap(hc, "Jar")
			if args, ok := hc["Args"].([]any); ok {
				for _, a := range args {
					if as, ok := a.(string); ok {
						s.Config.Args = append(s.Config.Args, as)
					}
				}
			}
		}
		steps = append(steps, s)
	}
	return steps
}

func parseInstanceGroups(params map[string]any, clusterID string) []instanceGroup {
	raw, ok := params["InstanceGroups"].([]any)
	if !ok {
		return nil
	}
	groups := make([]instanceGroup, 0, len(raw))
	for _, r := range raw {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		count := 1
		if c, ok := m["InstanceCount"].(float64); ok {
			count = int(c)
		}
		g := instanceGroup{
			Id:             fmt.Sprintf("ig-%s", shortID()),
			Name:           strParamFromMap(m, "Name"),
			Market:         strParamFromMap(m, "Market"),
			InstanceType:   strParamFromMap(m, "InstanceType"),
			InstanceRole:   strParamFromMap(m, "InstanceRole"),
			RequestedCount: count,
			RunningCount:   count,
			Status:         "RUNNING",
		}
		if g.Market == "" {
			g.Market = "ON_DEMAND"
		}
		groups = append(groups, g)
	}
	return groups
}

func defaultInstanceGroups(params map[string]any, clusterID string) []instanceGroup {
	// Build default MASTER+CORE groups from Instances block
	inst, ok := params["Instances"].(map[string]any)
	if !ok {
		return []instanceGroup{
			{Id: fmt.Sprintf("ig-%s", shortID()), Name: "Master", Market: "ON_DEMAND", InstanceType: "m5.xlarge", InstanceRole: "MASTER", RequestedCount: 1, RunningCount: 1, Status: "RUNNING"},
			{Id: fmt.Sprintf("ig-%s", shortID()), Name: "Core", Market: "ON_DEMAND", InstanceType: "m5.xlarge", InstanceRole: "CORE", RequestedCount: 2, RunningCount: 2, Status: "RUNNING"},
		}
	}
	masterType := strParamFromMap(inst, "MasterInstanceType")
	if masterType == "" {
		masterType = "m5.xlarge"
	}
	coreType := strParamFromMap(inst, "SlaveInstanceType")
	if coreType == "" {
		coreType = "m5.xlarge"
	}
	coreCount := 1
	if c, ok := inst["InstanceCount"].(float64); ok && int(c) > 1 {
		coreCount = int(c) - 1
	}
	return []instanceGroup{
		{Id: fmt.Sprintf("ig-%s", shortID()), Name: "Master", Market: "ON_DEMAND", InstanceType: masterType, InstanceRole: "MASTER", RequestedCount: 1, RunningCount: 1, Status: "RUNNING"},
		{Id: fmt.Sprintf("ig-%s", shortID()), Name: "Core", Market: "ON_DEMAND", InstanceType: coreType, InstanceRole: "CORE", RequestedCount: coreCount, RunningCount: coreCount, Status: "RUNNING"},
	}
}

func stepToWire(s emrStep) map[string]any {
	return map[string]any{
		"Id":   s.Id,
		"Name": s.Name,
		"Status": map[string]any{
			"State": s.Status,
			"Timeline": map[string]any{
				"CreationDateTime": float64(time.Now().Unix()),
			},
		},
		"Config": map[string]any{
			"Jar":  s.Config.Jar,
			"Args": s.Config.Args,
		},
	}
}

func instanceGroupToWire(g instanceGroup) map[string]any {
	return map[string]any{
		"Id":                     g.Id,
		"Name":                   g.Name,
		"Market":                 g.Market,
		"InstanceType":           g.InstanceType,
		"InstanceGroupType":      g.InstanceRole,
		"RequestedInstanceCount": g.RequestedCount,
		"RunningInstanceCount":   g.RunningCount,
		"Status": map[string]any{
			"State": g.Status,
		},
	}
}

func strParamFromMap(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func shortID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}
