package spark

import (
	"fmt"
	"os"
	"strings"
	"time"

	"jaiscloud/internal/model"
)

// ClusterSize is a named resource profile for Spark jobs.
type ClusterSize string

const (
	SizeSmall  ClusterSize = "small"
	SizeMedium ClusterSize = "medium"
	SizeLarge  ClusterSize = "large"
)

// DefaultImage is the default Spark Docker image used by K8s and Docker executors.
const DefaultImage = "apache/spark:3.5.0"

// ResourceProfile holds CPU/memory requests for a Spark driver or executor pod.
type ResourceProfile struct {
	DriverCPU      string
	DriverMemory   string
	ExecutorCPU    string
	ExecutorMemory string
	ExecutorCount  int
}

// clusterSizeProfiles maps named sizes to resource profiles.
// CPU and memory values use Spark conf format: integer cores, and Spark memory
// strings (e.g. "1g", "512m") — NOT Kubernetes unit notation ("500m", "1Gi").
var clusterSizeProfiles = map[ClusterSize]ResourceProfile{
	SizeSmall: {
		DriverCPU:      "1",
		DriverMemory:   "1g",
		ExecutorCPU:    "1",
		ExecutorMemory: "1g",
		ExecutorCount:  1,
	},
	SizeMedium: {
		DriverCPU:      "1",
		DriverMemory:   "2g",
		ExecutorCPU:    "1",
		ExecutorMemory: "2g",
		ExecutorCount:  2,
	},
	SizeLarge: {
		DriverCPU:      "2",
		DriverMemory:   "4g",
		ExecutorCPU:    "2",
		ExecutorMemory: "4g",
		ExecutorCount:  4,
	},
}

// DefaultAPIServer is the K8s API server URL used when running inside a cluster.
const DefaultAPIServer = "https://kubernetes.default.svc"

// SparkConfig holds executor-level configuration for a Spark job.
type SparkConfig struct {
	// SparkMode selects the executor: "mock", "k8s", "local", "docker", "remote".
	SparkMode string

	// Image is the Spark Docker image (k8s / docker executors).
	Image string

	// Namespace is the Kubernetes namespace (k8s executor).
	Namespace string

	// ServiceAccount is the Kubernetes service account for the driver pod.
	ServiceAccount string

	// APIServer is the Kubernetes API server URL (k8s executor).
	// Defaults to https://kubernetes.default.svc (in-cluster).
	APIServer string

	// Size is a named resource profile. Overridden by explicit resource fields.
	Size ClusterSize

	// Resources is the resolved resource profile (populated by SparkConfigFrom).
	Resources ResourceProfile

	// S3LogURI is the S3 URI for log capture (k8s executor, optional).
	S3LogURI string

	// RoleMappings maps IAM role ARNs to K8s service accounts (k8s executor).
	RoleMappings map[string]string

	// SparkSubmitPath overrides the default spark-submit binary path in the container.
	// Defaults to "/opt/spark/bin/spark-submit". (Apacher Spark)
	// EMR images use /usr/bin/spark-submit.
	SparkSubmitPath string

	// S3Endpoint is the S3 endpoint URL injected into Spark pods as environment
	// variables so spark-submit can resolve s3:// URIs (pod templates, jars, logs).
	// In devbox this points to JaisCloud's S3 service inside the cluster.
	S3Endpoint string

	// RemoteURL is the Spark Standalone master REST API URL (remote executor).
	// Example: "http://spark-master:6066"
	RemoteURL string

	// Region is the AWS region injected into Spark pods.
	// Defaults to JAISCLOUD_REGION or "us-east-1".
	Region string

	// AWSAccessKey is the AWS access key ID injected into Spark pods.
	// Defaults to JAISCLOUD_AWS_ACCESS_KEY_ID or "test".
	AWSAccessKey string

	// AWSSecretKey is the AWS secret access key injected into Spark pods.
	// Defaults to JAISCLOUD_AWS_SECRET_ACCESS_KEY or "test".
	AWSSecretKey string

	// ExtraSparkConfs are additional --conf flags layered after cloud-transform confs.
	ExtraSparkConfs []string

	// Cloud identifies which cloud transform to apply when building K8s manifests.
	Cloud model.Cloud

	// Azure-specific
	AzureStorageAccount  string
	AzureStorageKey      string
	AzureClientID        string
	AzureClientSecret    string
	AzureTenantID        string
	AzureStorageEndpoint string

	// GCP-specific
	GCPProjectID             string
	GCPServiceAccountKeyPath string
	GCPServiceAccountSecret  string
	GCPStorageEndpoint       string

	// InstanceID uniquely identifies this JaisCloud deployment on a shared K8s
	// cluster. Stamped as the jaiscloud.io/instance-id label on all managed Jobs
	// so cleanupOrphans can skip Jobs belonging to other instances.
	// Populated by main.go from config.LoadOrCreateInstanceID.
	InstanceID string

	// ClusterRestartPolicy controls what happens to cluster-mode Jobs that are
	// still running when JaisCloud restarts. "adopt" (default) re-adopts them
	// and re-Tracks them through the poller. "reap" deletes them and dispatches
	// FAILED via OnRestartTerminal.
	ClusterRestartPolicy string // "adopt" | "reap"

	// ReconcileTimeout is how long the StatusPoller waits before declaring a
	// 404-not-found Job as FAILED. Default 10 minutes.
	ReconcileTimeout time.Duration

	// Cluster-mode config (captured from env in SparkConfigFrom; never read via os.Getenv at runtime)
	ClusterMode     string // "auto" (default) | "always" | "never"
	ClusterShutdown string // "leave" (default) | "delete" — what Close() does to cluster-mode jobs
}

// SparkConfigFrom builds a SparkConfig from the executor mode and optional overrides.
// It resolves the named size profile and fills default values.
// JAISCLOUD_K8S_SPARK_IMAGE overrides the default image; functional overrides applied
// last take precedence over the env var.
func SparkConfigFrom(mode string, size ClusterSize, overrides ...func(*SparkConfig)) SparkConfig {
	profile, ok := clusterSizeProfiles[size]
	if !ok {
		profile = clusterSizeProfiles[SizeSmall]
	}
	cfg := SparkConfig{
		SparkMode: mode,
		Image:     DefaultImage,
		Namespace: "default",
		APIServer: DefaultAPIServer,
		Size:      size,
		Resources: profile,
	}
	if v := os.Getenv("JAISCLOUD_K8S_SPARK_IMAGE"); v != "" {
		cfg.Image = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_SPARK_SUBMIT"); v != "" {
		cfg.SparkSubmitPath = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_S3_ENDPOINT"); v != "" {
		cfg.S3Endpoint = v
	}
	cfg.Region = "us-east-1"
	if v := os.Getenv("JAISCLOUD_REGION"); v != "" {
		cfg.Region = v
	}
	cfg.AWSAccessKey = "test"
	if v := os.Getenv("JAISCLOUD_AWS_ACCESS_KEY_ID"); v != "" {
		cfg.AWSAccessKey = v
	}
	cfg.AWSSecretKey = "test"
	if v := os.Getenv("JAISCLOUD_AWS_SECRET_ACCESS_KEY"); v != "" {
		cfg.AWSSecretKey = v
	}

	// Azure
	cfg.AzureStorageAccount = os.Getenv("JAISCLOUD_AZURE_STORAGE_ACCOUNT")
	cfg.AzureStorageKey = os.Getenv("JAISCLOUD_AZURE_STORAGE_KEY")
	cfg.AzureClientID = os.Getenv("JAISCLOUD_AZURE_CLIENT_ID")
	cfg.AzureClientSecret = os.Getenv("JAISCLOUD_AZURE_CLIENT_SECRET")
	cfg.AzureTenantID = os.Getenv("JAISCLOUD_AZURE_TENANT_ID")
	cfg.AzureStorageEndpoint = os.Getenv("JAISCLOUD_AZURE_STORAGE_ENDPOINT")

	// GCP
	cfg.GCPProjectID = os.Getenv("JAISCLOUD_GCP_PROJECT_ID")
	cfg.GCPServiceAccountKeyPath = os.Getenv("JAISCLOUD_GCP_SERVICE_ACCOUNT_KEY_PATH")
	cfg.GCPServiceAccountSecret = os.Getenv("JAISCLOUD_GCP_SERVICE_ACCOUNT_SECRET")
	cfg.GCPStorageEndpoint = os.Getenv("JAISCLOUD_GCP_STORAGE_ENDPOINT")

	// Cluster-mode knobs
	cfg.ClusterMode = strings.ToLower(os.Getenv("JAISCLOUD_SPARK_K8S_CLUSTER_MODE"))
	if cfg.ClusterMode != "always" && cfg.ClusterMode != "never" {
		cfg.ClusterMode = "auto"
	}
	cfg.ClusterShutdown = strings.ToLower(os.Getenv("JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN"))
	if cfg.ClusterShutdown != "delete" {
		cfg.ClusterShutdown = "leave"
	}
	cfg.ClusterRestartPolicy = strings.ToLower(os.Getenv("JAISCLOUD_SPARK_K8S_CLUSTER_RESTART_POLICY"))
	if cfg.ClusterRestartPolicy != "reap" {
		cfg.ClusterRestartPolicy = "adopt"
	}
	cfg.ReconcileTimeout = durationEnv("JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT", 10*time.Minute)

	for _, o := range overrides {
		o(&cfg)
	}
	return cfg
}

// durationEnv reads a duration from an env var, returning def on parse failure or absence.
func durationEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// ValidateSparkMode returns an error if m is not a supported Spark executor mode.
func ValidateSparkMode(m string) error {
	switch m {
	case "", "mock", "k8s":
		return nil
	default:
		return fmt.Errorf(
			"Spark does not support executor mode %q (allowed: mock, k8s); "+
				"set JAISCLOUD_SPARK_EXECUTOR_MODE to override JAISCLOUD_EXECUTOR_MODE", m)
	}
}
