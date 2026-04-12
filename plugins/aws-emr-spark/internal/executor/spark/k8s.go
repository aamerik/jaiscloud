package spark

import (
	"context"
	"fmt"
	"os"
)

// K8sExecutor runs Spark jobs as Kubernetes batch/v1 Jobs using spark-submit
// in cluster deploy mode. It uses in-cluster config when running inside a pod,
// and falls back to kubeconfig for local development.
//
// Note: k8s.io/client-go is not a dependency of the core jaiscloud module.
// This executor requires the plugin to be built with --tags k8s or similar.
// For simplicity in Phase 3, K8sExecutor wraps MockExecutor and logs the
// spark-submit command that would be issued. Full k8s implementation follows
// in Phase 3.5 once client-go is added to the plugin's go.mod.
type K8sExecutor struct {
	cfg  SparkConfig
	mock *MockExecutor
}

// NewK8sExecutor creates a K8sExecutor. In the current implementation it
// delegates to MockExecutor while logging the spark-submit args.
func NewK8sExecutor(cfg SparkConfig) *K8sExecutor {
	return &K8sExecutor{cfg: cfg, mock: NewMockExecutor()}
}

func (e *K8sExecutor) Submit(ctx context.Context, job SparkJob) error {
	args := SparkSubmitArgs(job)
	fmt.Fprintf(os.Stderr, "[K8sExecutor] spark-submit %v\n", args)
	// TODO: launch batch/v1 Job via client-go
	return e.mock.Submit(ctx, job)
}

func (e *K8sExecutor) Status(ctx context.Context, jobID string) (SparkStatus, error) {
	// TODO: map Pod phase → SparkState via client-go
	return e.mock.Status(ctx, jobID)
}

func (e *K8sExecutor) Cancel(ctx context.Context, jobID string) error {
	// TODO: delete batch/v1 Job via client-go
	return e.mock.Cancel(ctx, jobID)
}

func (e *K8sExecutor) Close() error {
	return e.mock.Close()
}
