// Package emroneks implements the EMR on EKS provider.
// Wire protocol: REST JSON, SigV4 service name: emr-containers
// Paths: /virtualclusters, /virtualclusters/{id}/jobruns, /virtualclusters/{id}/endpoints
package emroneks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/events"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/provider"
	sparkaws "jaiscloud/internal/provider/aws/sparkaws"
	"jaiscloud/internal/sparkhelpers"
	"jaiscloud/internal/store"
)

// EMRContainersProvider handles EMR on EKS: virtual clusters, job runs, managed endpoints.
type EMRContainersProvider struct {
	resources   store.ResourceStore
	bus         *events.EventBus
	k8sClient   kubernetes.Interface // nil = instant completion
	platformCfg *platform.PlatformConfig
	namespace   string
	// sparkImage is the container image for spark-submit driver pods.
	// Defaults to "spark-emr-eks-7.9.0:devbox"; override via WithSparkImage or JAISCLOUD_SPARK_EMREKS_IMAGE.
	sparkImage  string
	awsEmulator *sparkaws.AWSEmulatorConfig
	instanceID  string
	// ctx is the provider lifecycle context. runJobRun goroutines inherit it so
	// they are cancelled on Shutdown(), enabling graceful drain.
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup // tracks in-flight runJobRun goroutines
	patcherStop func()         // stops the executor OwnershipPatcher; nil when no k8s

	// cancelsMu guards both cancels and cancelClaimed.
	cancelsMu sync.Mutex
	// cancels maps jobRunID → CancelFunc that signals the per-jobrun context in runJobRun.
	cancels map[string]context.CancelFunc
	// cancelClaimed is the set of jobRunIDs for which a CancelJobRun is already
	// executing. Exactly one goroutine may own a cancel for any given job run;
	// concurrent callers see the job as "CANCEL_PENDING" and return idempotent OK.
	cancelClaimed map[string]bool
}

// Option configures EMRContainersProvider.
type Option func(*EMRContainersProvider)

// WithK8s attaches a Kubernetes client and platform config for real Spark execution.
func WithK8s(client kubernetes.Interface, namespace string, platformCfg *platform.PlatformConfig) Option {
	return func(p *EMRContainersProvider) {
		p.k8sClient = client
		p.namespace = namespace
		p.platformCfg = platformCfg
	}
}

// WithSparkImage sets the container image used for spark-submit driver pods.
// For real EMR-on-EKS the image is derived from the releaseLabel; in devbox use the local build.
func WithSparkImage(image string) Option {
	return func(p *EMRContainersProvider) { p.sparkImage = image }
}

// WithAWSEmulator wires AWS emulator endpoint config into Spark driver pods.
func WithAWSEmulator(cfg *sparkaws.AWSEmulatorConfig) Option {
	return func(p *EMRContainersProvider) { p.awsEmulator = cfg }
}

// WithInstanceID sets the instance ID stamped on Spark driver pod labels.
func WithInstanceID(id string) Option {
	return func(p *EMRContainersProvider) { p.instanceID = id }
}

func New(resources store.ResourceStore, bus *events.EventBus, opts ...Option) *EMRContainersProvider {
	ctx, cancel := context.WithCancel(context.Background())
	p := &EMRContainersProvider{
		resources:  resources,
		bus:        bus,
		ctx:        ctx,
		cancel:     cancel,
		sparkImage: "spark-emr-eks-7.9.0:devbox",
		cancels:       make(map[string]context.CancelFunc),
		cancelClaimed: make(map[string]bool),
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
			slog.Warn("emroneks: failed to start ownership patcher", "err", err)
		} else {
			p.patcherStop = stop
		}
	}
	return p
}

func (p *EMRContainersProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Virtual clusters
		"EMRContainers.CreateVirtualCluster":   p.CreateVirtualCluster,
		"EMRContainers.DeleteVirtualCluster":   p.DeleteVirtualCluster,
		"EMRContainers.DescribeVirtualCluster": p.DescribeVirtualCluster,
		"EMRContainers.ListVirtualClusters":    p.ListVirtualClusters,
		// Job runs
		"EMRContainers.StartJobRun":    p.StartJobRun,
		"EMRContainers.CancelJobRun":   p.CancelJobRun,
		"EMRContainers.DescribeJobRun": p.DescribeJobRun,
		"EMRContainers.ListJobRuns":    p.ListJobRuns,
		// Managed endpoints
		"EMRContainers.CreateManagedEndpoint":   p.CreateManagedEndpoint,
		"EMRContainers.DeleteManagedEndpoint":   p.DeleteManagedEndpoint,
		"EMRContainers.DescribeManagedEndpoint": p.DescribeManagedEndpoint,
		"EMRContainers.ListManagedEndpoints":    p.ListManagedEndpoints,
		// Tagging
		"EMRContainers.TagResource":         p.TagResource,
		"EMRContainers.UntagResource":       p.UntagResource,
		"EMRContainers.ListTagsForResource": p.ListTagsForResource,
	}
}

const (
	rtVirtualCluster  = "emrc_virtual_cluster"
	rtJobRun          = "emrc_job_run"
	rtManagedEndpoint = "emrc_managed_endpoint"
)

// ─── Virtual Cluster types ────────────────────────────────────────────────────

type virtualCluster struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	State              string            `json:"state"` // RUNNING, TERMINATING, TERMINATED, ARRESTED
	EksCluster         string            `json:"eksCluster"`
	Namespace          string            `json:"namespace"`
	ServiceAccountName string            `json:"serviceAccountName"` // bound SA for IRSA
	CreatedAt          time.Time         `json:"createdAt"`
	Tags               map[string]string `json:"tags"`
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
	ID               string                `json:"id"`
	Name             string                `json:"name"`
	VirtualClusterID string                `json:"virtualClusterId"`
	State            string                `json:"state"` // PENDING, SUBMITTED, RUNNING, FAILED, CANCELLED, CANCEL_PENDING, COMPLETED
	FailureReason    string                `json:"failureReason,omitempty"`
	ReleaseLabel     string                `json:"releaseLabel"`
	ExecutionRole    string                `json:"executionRoleArn"`
	CreatedAt        time.Time             `json:"createdAt"`
	Tags             map[string]string     `json:"tags"`
	JobDriver        map[string]any        `json:"jobDriver"`
	JobHandle        *k8shelpers.JobHandle `json:"jobHandle,omitempty"` // internal — excluded from toWire
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
		// JobHandle is internal state; never included in API responses.
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

	eksCluster, namespace, sa := "", "default", ""
	if cp, ok := nr.Params["containerProvider"].(map[string]any); ok {
		eksCluster = strParamFromMap(cp, "id")
		if info, ok := cp["info"].(map[string]any); ok {
			if eks, ok := info["eksInfo"].(map[string]any); ok {
				namespace = strParamFromMap(eks, "namespace")
				sa = strParamFromMap(eks, "serviceAccountName")
			}
		}
	}
	if sa == "" {
		sa = "jaiscloud-emroneks-" + sanitizeSAName(name)
	}

	id := shortID()
	vc := virtualCluster{
		ID:                 id,
		Name:               name,
		State:              "RUNNING",
		EksCluster:         eksCluster,
		Namespace:          namespace,
		ServiceAccountName: sa,
		CreatedAt:          time.Now().UTC(),
		Tags:               parseTags(nr.Params),
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
	vc, err := p.loadVC(ctx, vcID)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Virtual cluster not found", HTTPStatus: http.StatusNotFound}
	}

	id := shortID()
	name := strParam(nr.Params, "name")
	executionRoleArn := strParam(nr.Params, "executionRoleArn")

	useK8s := p.k8sClient != nil
	initialState := "COMPLETED"
	if useK8s {
		initialState = "PENDING"
	}

	jr := jobRun{
		ID:               id,
		Name:             name,
		VirtualClusterID: vcID,
		State:            initialState,
		ReleaseLabel:     strParam(nr.Params, "releaseLabel"),
		ExecutionRole:    executionRoleArn,
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

	h := newHandlerCtx(nr)
	if useK8s {
		// p.ctx is cancelled by Shutdown() so the goroutine stops on server shutdown.
		params := nr.Params
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.runJobRun(p.ctx, h, vc, id, executionRoleArn, params)
		}()
	} else {
		// Instant completion path — transition to COMPLETED and publish event.
		p.emitJobRunStateChange(h, vcID, id, "COMPLETED", "")
	}

	return provider.OK(map[string]any{
		"id":               id,
		"name":             name,
		"arn":              nr.ResourceID("emr-job-run", vcID+"/"+id),
		"virtualClusterId": vcID,
	}), nil
}

func (p *EMRContainersProvider) CancelJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	h := newHandlerCtx(nr)
	vcID := pathID(nr, "virtualClusterId", "virtualClusterId")
	jobID := pathID(nr, "jobRunId", "id")

	// Claim ownership of this cancellation under the mutex.
	// If another CancelJobRun already claimed it, return idempotent immediately —
	// this prevents N concurrent callers from all seeing State=PENDING and each
	// emitting CANCEL_PENDING+CANCELLED.
	p.cancelsMu.Lock()
	if p.cancelClaimed[jobID] {
		p.cancelsMu.Unlock()
		return provider.OK(map[string]any{"id": jobID, "virtualClusterId": vcID}), nil
	}
	p.cancelClaimed[jobID] = true
	runCancel, inFlight := p.cancels[jobID]
	p.cancelsMu.Unlock()

	defer func() {
		p.cancelsMu.Lock()
		delete(p.cancelClaimed, jobID)
		p.cancelsMu.Unlock()
	}()

	jr, err := p.loadJobRun(ctx, vcID, jobID)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Job run not found", HTTPStatus: http.StatusNotFound}
	}

	switch jr.State {
	case "COMPLETED", "FAILED", "CANCELLED":
		return nil, &model.ProviderError{
			Code:       "ValidationException",
			Message:    fmt.Sprintf("Job run is already in terminal state %s", jr.State),
			HTTPStatus: http.StatusBadRequest,
		}

	case "CANCEL_PENDING":
		// Idempotent — state already transitioning.
		return provider.OK(map[string]any{"id": jobID, "virtualClusterId": vcID}), nil

	case "PENDING", "SUBMITTED":
		// Two-phase: CANCEL_PENDING synchronously, then async delete + CANCELLED.
		p.emitJobRunStateChange(h, vcID, jobID, "CANCEL_PENDING", "User requested cancellation")
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			if inFlight {
				runCancel()
			}
			if jr.JobHandle != nil {
				if cancelErr := k8shelpers.Cancel(p.ctx, p.k8sClient, *jr.JobHandle); cancelErr != nil {
					slog.Error("emroneks: k8shelpers.Cancel failed", "jobRun", jobID, "err", cancelErr)
				}
			}
			// emitJobRunStateChange is the single-writer for jr.State.
			p.emitJobRunStateChange(h, vcID, jobID, "CANCELLED", "Job run cancelled")
		}()

	case "RUNNING":
		// Driver is up — direct transition; signal goroutine and delete Job.
		if inFlight {
			runCancel()
		}
		if jr.JobHandle != nil {
			if cancelErr := k8shelpers.Cancel(p.ctx, p.k8sClient, *jr.JobHandle); cancelErr != nil {
				slog.Error("emroneks: k8shelpers.Cancel failed", "jobRun", jobID, "err", cancelErr)
			}
		}
		p.emitJobRunStateChange(h, vcID, jobID, "CANCELLED", "Job run cancelled")

	default:
		return nil, &model.ProviderError{
			Code:       "ValidationException",
			Message:    fmt.Sprintf("Unsupported state %s", jr.State),
			HTTPStatus: http.StatusBadRequest,
		}
	}

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

// ─── Provider lifecycle ───────────────────────────────────────────────────────

// Reset is a no-op — state lives in the resource store, not in-memory maps.
func (p *EMRContainersProvider) Reset() {}

// Shutdown cancels the provider context, signalling all in-flight runJobRun
// goroutines to stop after their current K8s operation completes, then
// waits for all goroutines to exit before returning.
func (p *EMRContainersProvider) Shutdown(_ context.Context) {
	if p.patcherStop != nil {
		p.patcherStop()
	}
	p.cancel()
	p.wg.Wait()
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
	if err := p.resources.Update(ctx, store.ResourceEntry{Type: rtVirtualCluster, ID: vc.ID, Data: data}); err != nil {
		slog.Warn("emroneks: failed to persist virtual cluster", "vcID", vc.ID, "err", err)
	}
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
	if err := p.resources.Update(ctx, store.ResourceEntry{Type: rtJobRun, ID: jr.VirtualClusterID + "/" + jr.ID, Data: data}); err != nil {
		slog.Warn("emroneks: failed to persist job run", "vcID", jr.VirtualClusterID, "jobRunID", jr.ID, "err", err)
	}
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
	if err := p.resources.Update(ctx, store.ResourceEntry{Type: rtManagedEndpoint, ID: me.VirtualClusterID + "/" + me.ID, Data: data}); err != nil {
		slog.Warn("emroneks: failed to persist managed endpoint", "vcID", me.VirtualClusterID, "epID", me.ID, "err", err)
	}
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

const idChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func shortID() string {
	b := make([]byte, 12)
	for i := range b {
		b[i] = idChars[rand.Intn(len(idChars))]
	}
	return string(b)
}
