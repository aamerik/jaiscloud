// Package plugin contains the EMRSparkPlugin implementation.
// Separated from main.go so it can be unit-tested without plugin build mode.
package plugin

import (
	"context"
	"os"
	"time"

	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
	emrprovider "github.com/jaiscloud/plugin-aws-emr-spark/internal/provider/emr"
	emrcprovider "github.com/jaiscloud/plugin-aws-emr-spark/internal/provider/emrcontainers"
)

// EMRSparkPlugin implements sdk.SparkPlugin.
// The host loads it via plugin.Open + Lookup("Plugin").
type EMRSparkPlugin struct {
	executor spark.SparkExecutor
	poller   *spark.StatusPoller
	emr      *emrprovider.EMRProvider
	emrc     *emrcprovider.EMRContainersProvider
}

// New returns a new EMRSparkPlugin ready for Init.
func New() *EMRSparkPlugin { return &EMRSparkPlugin{} }

// Init is called once after the plugin is loaded.
// It wires the executor, poller, and both providers.
//
// Environment variables for k8s mode (only read when JAISCLOUD_SPARK_MODE=k8s):
//
//	JAISCLOUD_K8S_APISERVER  — K8s API server URL (default: https://kubernetes.default.svc)
//	JAISCLOUD_K8S_NAMESPACE  — Kubernetes namespace (default: "default")
//	JAISCLOUD_K8S_SA         — Service account for spark-submit pod (optional)
//	JAISCLOUD_K8S_TOKEN      — Bearer token: literal value or path to token file
//	JAISCLOUD_K8S_CA_FILE    — Path to PEM CA certificate (optional)
func (p *EMRSparkPlugin) Init(ctx context.Context, _ sdk.ResourceManager, store sdk.ResourceStore) error {
	mode := os.Getenv("JAISCLOUD_SPARK_MODE")
	if mode == "" {
		mode = "mock"
	}

	cfg := spark.SparkConfigFrom(mode, spark.SizeSmall)

	if mode == "k8s" {
		if v := os.Getenv("JAISCLOUD_K8S_APISERVER"); v != "" {
			cfg.APIServer = v
		}
		if v := os.Getenv("JAISCLOUD_K8S_NAMESPACE"); v != "" {
			cfg.Namespace = v
		}
		if v := os.Getenv("JAISCLOUD_K8S_SA"); v != "" {
			cfg.ServiceAccount = v
		}
	}

	p.executor = spark.NewExecutor(mode, cfg)
	p.poller = spark.NewStatusPoller(p.executor, 5*time.Second, nil)
	p.poller.Start(ctx)
	p.emr = emrprovider.New(store, p.executor, p.poller)
	p.emrc = emrcprovider.New(store, p.executor, p.poller)
	return nil
}

// Manifest returns plugin metadata the host uses for routing.
func (p *EMRSparkPlugin) Manifest() sdk.ManifestInfo {
	return sdk.ManifestInfo{
		Name:     "aws-emr-spark",
		Version:  "1.0.0",
		Services: []string{"emr", "emrcontainers"},
	}
}

// Handle dispatches a request to the EMR or EMRContainers provider.
func (p *EMRSparkPlugin) Handle(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
	switch req.Service {
	case "emr":
		return p.emr.Handle(ctx, req)
	case "emrcontainers", "emr-containers":
		return p.emrc.Handle(ctx, req)
	default:
		return sdk.HandleResponse{
			Err: &sdk.PluginError{
				Code:       "UnsupportedOperation",
				Message:    "service " + req.Service + " not handled by aws-emr-spark plugin",
				HTTPStatus: 400,
			},
		}
	}
}

// Shutdown stops the poller and closes the executor.
func (p *EMRSparkPlugin) Shutdown(ctx context.Context) error {
	if p.poller != nil {
		p.poller.Stop()
	}
	if p.executor != nil {
		return p.executor.Close()
	}
	return nil
}

// Reset clears all in-memory job state. Called from POST /_jaiscloud/reset.
func (p *EMRSparkPlugin) Reset() {
	if p.poller != nil {
		p.poller.Reset()
	}
	switch ex := p.executor.(type) {
	case *spark.MockExecutor:
		ex.Reset()
	case *spark.K8sExecutor:
		ex.Reset()
	}
}
