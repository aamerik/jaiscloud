package spark

import "os"

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
	// Mode selects the executor: "mock", "k8s", "local", "docker", "remote".
	Mode string

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

	// RemoteURL is the Spark Standalone master REST API URL (remote executor).
	// Example: "http://spark-master:6066"
	RemoteURL string
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
		Mode:      mode,
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
	for _, o := range overrides {
		o(&cfg)
	}
	return cfg
}
