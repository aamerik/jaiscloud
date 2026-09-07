// Package dataproc implements the Cloud Dataproc v1 provider
// (dataproc.googleapis.com/v1): Clusters, Jobs, and the long-running
// Operations returned by Create/Update/Delete/Start/Stop/SubmitJobAsOperation.
//
// A cluster is a logical record only — the emulator never stands up a real
// multi-node cluster, exactly as EMR-on-EC2 never stands up a real YARN
// cluster. Jobs, however, run real Spark through the shared client-mode engine
// (internal/sparkhelpers + internal/k8shelpers) under JAISCLOUD_EXECUTOR_MODE.
package dataproc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/sparkgcp"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/sparkhelpers"
	"jaiscloud/internal/store"
)

// Provider handles Cloud Dataproc v1 cluster and job resources.
type Provider struct {
	store     dataprocstore.Store
	resources store.ResourceStore // terminal snapshots (rehydrate after k8s Job GC)

	k8sClient   kubernetes.Interface // nil = mock execution
	platformCfg *platform.PlatformConfig
	namespace   string
	sparkImage  string
	gcpEmulator *sparkgcp.GCPEmulatorConfig

	sparkSubmitPath string

	instanceID         string
	serviceAccountName string
	projectID          string // default project (from cfg.ProjectID), used as WI fallback

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	cancelsMu   sync.Mutex
	cancels     map[string]context.CancelFunc
	patcherStop func()
}

// Option configures Provider.
type Option func(*Provider)

// WithK8s attaches a Kubernetes client and platform config for real Spark
// execution (mock mode when nil).
func WithK8s(client kubernetes.Interface, namespace string, platformCfg *platform.PlatformConfig) Option {
	return func(p *Provider) {
		p.k8sClient = client
		p.namespace = namespace
		p.platformCfg = platformCfg
	}
}

// WithSparkImage sets the container image used for spark-submit driver pods.
func WithSparkImage(image string) Option {
	return func(p *Provider) { p.sparkImage = image }
}

// WithSparkSubmitPath overrides the spark-submit binary path inside the driver
// image (default "spark-submit"; the apache/spark image keeps it at
// /opt/spark/bin/spark-submit, off PATH).
func WithSparkSubmitPath(path string) Option {
	return func(p *Provider) { p.sparkSubmitPath = path }
}

// WithGCPEmulator wires GCP emulator endpoint config into Spark driver pods.
func WithGCPEmulator(cfg *sparkgcp.GCPEmulatorConfig) Option {
	return func(p *Provider) { p.gcpEmulator = cfg }
}

// WithInstanceID sets the instance ID stamped on Spark driver pod labels.
func WithInstanceID(id string) Option {
	return func(p *Provider) { p.instanceID = id }
}

// WithServiceAccountName sets the Kubernetes service account for Spark driver
// pods (fallback when the Workload Identity mutator does not set one).
func WithServiceAccountName(sa string) Option {
	return func(p *Provider) { p.serviceAccountName = sa }
}

// WithProjectID sets the default GCP project (used by the Workload Identity
// mutator; per-job requests override with their own project).
func WithProjectID(project string) Option {
	return func(p *Provider) { p.projectID = project }
}

// New returns a Provider backed by the given store.
func New(s dataprocstore.Store, resources store.ResourceStore, opts ...Option) *Provider {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Provider{
		store:      s,
		resources:  resources,
		ctx:        ctx,
		cancel:     cancel,
		sparkImage: "spark-dataproc:devbox",
		cancels:    make(map[string]context.CancelFunc),
	}
	for _, o := range opts {
		o(p)
	}
	if p.k8sClient != nil {
		ns := p.namespace
		if ns == "" {
			ns = "jaiscloud"
		}
		stop, err := k8shelpers.StartOwnershipPatcher(p.ctx, p.k8sClient, k8shelpers.PatcherConfig{
			Namespace:     ns,
			LabelSelector: "spark-role=executor",
			ResolveOwner:  sparkhelpers.MakeExecutorOwnerResolver(p.k8sClient, ns),
		})
		if err != nil {
			slog.Warn("dataproc: failed to start ownership patcher", "err", err)
		} else {
			p.patcherStop = stop
		}
		if err := k8shelpers.CleanupOrphans(p.ctx, p.k8sClient, k8shelpers.CleanupConfig{
			Namespace:       ns,
			InstanceID:      p.instanceID,
			OrphanSelectors: []string{"spark-role in (driver,executor)"},
		}); err != nil {
			slog.Warn("dataproc: CleanupOrphans failed", "err", err)
		}
	}
	return p
}

// Shutdown cancels the provider context and drains in-flight job goroutines.
func (p *Provider) Shutdown(_ context.Context) {
	if p.patcherStop != nil {
		p.patcherStop()
	}
	p.cancel()
	p.wg.Wait()
}

// Reset wipes the store (no executor to reset).
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Dataproc.CreateCluster":        p.CreateCluster,
		"Dataproc.GetCluster":           p.GetCluster,
		"Dataproc.ListClusters":         p.ListClusters,
		"Dataproc.UpdateCluster":        p.UpdateCluster,
		"Dataproc.DeleteCluster":        p.DeleteCluster,
		"Dataproc.StartCluster":         p.StartCluster,
		"Dataproc.StopCluster":          p.StopCluster,
		"Dataproc.DiagnoseCluster":      p.DiagnoseCluster,
		"Dataproc.SubmitJob":            p.SubmitJob,
		"Dataproc.SubmitJobAsOperation": p.SubmitJobAsOperation,
		"Dataproc.GetJob":               p.GetJob,
		"Dataproc.ListJobs":             p.ListJobs,
		"Dataproc.DeleteJob":            p.DeleteJob,
		"Dataproc.CancelJob":            p.CancelJob,
		"Dataproc.GetOperation":         p.GetOperation,
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	if body == nil {
		return nil
	}
	m, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

func bodyStringSlice(body map[string]any, key string) []string {
	if body == nil {
		return nil
	}
	raw, ok := body[key].([]any)
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

func mapErr(err error) error {
	switch {
	case errors.Is(err, dataprocstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, dataprocstore.ErrNoSuchJob):
		return model.NewProviderError("NotFound", "job not found", 404)
	case errors.Is(err, dataprocstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	}
	return err
}

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// --- Wire rendering ---

func clusterStatusMap(s dataprocstore.ClusterStatus) map[string]any {
	out := map[string]any{
		"state":          s.State,
		"stateStartTime": formatTimestamp(s.StateStartTime),
	}
	if s.Detail != "" {
		out["detail"] = s.Detail
	}
	return out
}

// clusterToMap renders a store Cluster as a dataproc.v1.Cluster wire map.
func clusterToMap(c dataprocstore.Cluster) map[string]any {
	out := map[string]any{
		"projectId":   c.ProjectID,
		"clusterName": c.Name,
		"status":      clusterStatusMap(c.Status),
	}
	if c.ClusterUUID != "" {
		out["clusterUuid"] = c.ClusterUUID
	}
	if len(c.Config) > 0 {
		var config map[string]any
		if json.Unmarshal(c.Config, &config) == nil && config != nil {
			out["config"] = config
		}
	}
	if c.Labels != nil {
		out["labels"] = c.Labels
	}
	if len(c.StatusHistory) > 0 {
		history := make([]any, 0, len(c.StatusHistory))
		for _, s := range c.StatusHistory {
			history = append(history, clusterStatusMap(s))
		}
		out["statusHistory"] = history
	}
	return out
}

func jobStatusMap(s dataprocstore.JobStatus) map[string]any {
	out := map[string]any{
		"state":          s.State,
		"stateStartTime": formatTimestamp(s.StateStartTime),
	}
	if s.Details != "" {
		out["details"] = s.Details
	}
	return out
}

// jobToMap renders a store Job as a dataproc.v1.Job wire map.
func jobToMap(j dataprocstore.Job) map[string]any {
	out := map[string]any{
		"reference": map[string]any{"projectId": j.ProjectID, "jobId": j.JobID},
		"placement": map[string]any{"clusterName": j.PlacementClusterName},
		"status":    jobStatusMap(j.Status),
		"done":      jobTerminal(j.Status.State),
	}
	if j.JobUUID != "" {
		out["jobUuid"] = j.JobUUID
	}
	if j.Type != "" && len(j.TypeJob) > 0 {
		var typeJob map[string]any
		if json.Unmarshal(j.TypeJob, &typeJob) == nil && typeJob != nil {
			out[j.Type] = typeJob
		}
	}
	if j.DriverOutputResourceURI != "" {
		out["driverOutputResourceUri"] = j.DriverOutputResourceURI
	}
	if j.DriverControlFilesURI != "" {
		out["driverControlFilesUri"] = j.DriverControlFilesURI
	}
	if j.Labels != nil {
		out["labels"] = j.Labels
	}
	if len(j.StatusHistory) > 0 {
		history := make([]any, 0, len(j.StatusHistory))
		for _, s := range j.StatusHistory {
			history = append(history, jobStatusMap(s))
		}
		out["statusHistory"] = history
	}
	return out
}

// jobTerminal reports whether a job state is terminal (done).
func jobTerminal(state string) bool {
	switch state {
	case "DONE", "ERROR", "CANCELLED":
		return true
	}
	return false
}

// operationMap renders a stored Operation as a google.longrunning.Operation.
func (p *Provider) operationMap(nr *model.NormalizedRequest, op dataprocstore.Operation) map[string]any {
	name := nr.ResourceID("dataproc-operation", op.Region+"/"+op.ID)
	var metadata any = map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done && op.Response != "" {
		var response any = map[string]any{}
		if json.Unmarshal([]byte(op.Response), &response) == nil {
			out["response"] = response
		}
	}
	return out
}

// storeOperation persists a done operation and returns its wire map.
func (p *Provider) storeOperation(ctx context.Context, nr *model.NormalizedRequest, region, verb, target string, metadata, response map[string]any) (map[string]any, error) {
	now := clock.Now().UTC()
	op := dataprocstore.Operation{
		ID:         randomHex(12),
		ProjectID:  nr.AccountID,
		Region:     region,
		Done:       true,
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if metadata != nil {
		metaJSON, _ := json.Marshal(metadata)
		op.Metadata = string(metaJSON)
	}
	if response != nil {
		respJSON, _ := json.Marshal(response)
		op.Response = string(respJSON)
	}
	if err := p.store.CreateOperation(ctx, nr.AccountID, region, op); err != nil {
		return nil, err
	}
	return p.operationMap(nr, op), nil
}

// clusterOperationMetadata renders ClusterOperationMetadata for an operation.
func clusterOperationMetadata(clusterName, clusterUUID, operationType string) map[string]any {
	return map[string]any{
		"@type":         "type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata",
		"clusterName":   clusterName,
		"clusterUuid":   clusterUUID,
		"operationType": operationType,
		"status":        map[string]any{"state": "DONE"},
		"statusHistory": []any{map[string]any{"state": "DONE"}},
	}
}

// jobOperationMetadata renders JobMetadata for a submit-as-operation.
func jobOperationMetadata(jobID, state, operationType string, start time.Time) map[string]any {
	return map[string]any{
		"@type":         "type.googleapis.com/google.cloud.dataproc.v1.JobMetadata",
		"jobId":         jobID,
		"operationType": operationType,
		"startTime":     formatTimestamp(start),
		"status":        map[string]any{"state": state},
	}
}

// pageClusters paginates cluster records using the shared GCP paging helper.
func (p *Provider) pageClusters(nr *model.NormalizedRequest, clusters []dataprocstore.Cluster) (map[string]any, error) {
	page, next := paging.Page(clusters, func(c dataprocstore.Cluster) string { return c.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, clusterToMap(c))
	}
	resp := map[string]any{"clusters": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return resp, nil
}
