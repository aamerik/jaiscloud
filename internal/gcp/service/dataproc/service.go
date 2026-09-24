// Package dataproc is the transport-neutral core of the Cloud Dataproc v1
// service (dataproc.googleapis.com/v1). It owns all cluster, job, and
// long-running-operation business logic over internal/gcp/store/dataproc,
// including the Spark executor orchestration (mock / Kubernetes) for jobs.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/dataproc) and the REST transport
// (internal/gcp/transport/rest/dataproc) both transcode their wire format into
// this package's typed API and then call the SAME Service instance. That is the
// dual-protocol invariant: one core, one piece of state (including the
// in-flight executor registry), so the transports cannot drift.
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
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/gcp/sparkgcp"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/sparkhelpers"
	"jaiscloud/internal/store"
)

// Service is the transport-neutral Cloud Dataproc v1 core.
type Service struct {
	store     dpstore.Store
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

	// operationTTL is how long a completed operation is retained before the
	// lazy sweep removes it (keeps jc_dataproc_operations bounded; real
	// operations are GC'd after a similar TTL). Zero disables the sweep.
	operationTTL time.Duration

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	cancelsMu   sync.Mutex
	cancels     map[string]context.CancelFunc
	patcherStop func()
}

// defaultOperationTTL is how long a completed operation is retained before the
// lazy sweep removes it. Real Dataproc operations are GC'd after a TTL; without
// a sweep, jc_dataproc_operations grows unbounded.
const defaultOperationTTL = 24 * time.Hour

// Option configures Service.
type Option func(*Service)

// WithK8s attaches a Kubernetes client and platform config for real Spark
// execution (mock mode when nil).
func WithK8s(client kubernetes.Interface, namespace string, platformCfg *platform.PlatformConfig) Option {
	return func(s *Service) {
		s.k8sClient = client
		s.namespace = namespace
		s.platformCfg = platformCfg
	}
}

// WithSparkImage sets the container image used for spark-submit driver pods.
func WithSparkImage(image string) Option {
	return func(s *Service) { s.sparkImage = image }
}

// WithSparkSubmitPath overrides the spark-submit binary path inside the driver
// image (default "spark-submit"; the apache/spark image keeps it at
// /opt/spark/bin/spark-submit, off PATH).
func WithSparkSubmitPath(path string) Option {
	return func(s *Service) { s.sparkSubmitPath = path }
}

// WithGCPEmulator wires GCP emulator endpoint config into Spark driver pods.
func WithGCPEmulator(cfg *sparkgcp.GCPEmulatorConfig) Option {
	return func(s *Service) { s.gcpEmulator = cfg }
}

// WithInstanceID sets the instance ID stamped on Spark driver pod labels.
func WithInstanceID(id string) Option {
	return func(s *Service) { s.instanceID = id }
}

// WithServiceAccountName sets the Kubernetes service account for Spark driver
// pods (fallback when the Workload Identity mutator does not set one).
func WithServiceAccountName(sa string) Option {
	return func(s *Service) { s.serviceAccountName = sa }
}

// WithProjectID sets the default GCP project (used by the Workload Identity
// mutator; per-job requests override with their own project).
func WithProjectID(project string) Option {
	return func(s *Service) { s.projectID = project }
}

// WithOperationTTL overrides how long completed operations are retained before
// the lazy sweep removes them. Zero disables the sweep (tests use a short TTL).
func WithOperationTTL(d time.Duration) Option {
	return func(s *Service) { s.operationTTL = d }
}

// NewService returns a Dataproc core backed by the given store.
func NewService(s dpstore.Store, resources store.ResourceStore, opts ...Option) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	svc := &Service{
		store:        s,
		resources:    resources,
		ctx:          ctx,
		cancel:       cancel,
		sparkImage:   "spark-dataproc:devbox",
		cancels:      make(map[string]context.CancelFunc),
		operationTTL: defaultOperationTTL,
	}
	for _, o := range opts {
		o(svc)
	}
	if svc.k8sClient != nil {
		ns := svc.namespace
		if ns == "" {
			ns = "jaiscloud"
		}
		stop, err := k8shelpers.StartOwnershipPatcher(svc.ctx, svc.k8sClient, k8shelpers.PatcherConfig{
			Namespace:     ns,
			LabelSelector: "spark-role=executor",
			ResolveOwner:  sparkhelpers.MakeExecutorOwnerResolver(svc.k8sClient, ns),
		})
		if err != nil {
			slog.Warn("dataproc: failed to start ownership patcher", "err", err)
		} else {
			svc.patcherStop = stop
		}
		if err := k8shelpers.CleanupOrphans(svc.ctx, svc.k8sClient, k8shelpers.CleanupConfig{
			Namespace:       ns,
			InstanceID:      svc.instanceID,
			OrphanSelectors: []string{"spark-role in (driver,executor)"},
		}); err != nil {
			slog.Warn("dataproc: CleanupOrphans failed", "err", err)
		}
	}
	return svc
}

// Shutdown cancels the core context and drains in-flight job goroutines.
func (s *Service) Shutdown(_ context.Context) {
	if s.patcherStop != nil {
		s.patcherStop()
	}
	s.cancel()
	s.wg.Wait()
}

// Reset wipes the store. The core's own in-flight goroutines are drained by
// Shutdown; /_jaiscloud/reset does not drain them (documented limitation).
func (s *Service) Reset(ctx context.Context) { s.store.Reset(ctx) }

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// pageParams builds the shared cursor-pagination parameter map accepted by
// internal/gcp/paging.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// mapErr maps a dataproc store error onto a canonical provider error.
func mapErr(err error) error {
	switch {
	case errors.Is(err, dpstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, dpstore.ErrNoSuchJob):
		return model.NewProviderError("NotFound", "job not found", 404)
	case errors.Is(err, dpstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, dpstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}
