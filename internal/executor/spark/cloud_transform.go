package spark

import (
	"fmt"
	"sync"

	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/model"
)

// CloudSparkTransform encapsulates all cloud-specific contributions to a
// Spark K8s Job manifest. Implementations register themselves via init().
type CloudSparkTransform interface {
	Cloud() model.Cloud
	// ValidateURIs scans spark-submit args for storage URIs and returns a
	// user-readable error if any scheme does not match the configured cloud.
	// No rewriting or silent translation; callers receive a fail-fast error.
	ValidateURIs(args []string, cfg SparkConfig) error
	ResolveCommand(job SparkJob, cfg SparkConfig) (SparkSubmitCommand, error)
	PodEnv(cfg SparkConfig) []envVar
	PodVolumes(cfg SparkConfig) ([]volume, []volumeMount)
	SparkConfs(cfg SparkConfig) []string
}

// transformsMu guards transforms against concurrent access (e.g. parallel tests).
var transformsMu sync.RWMutex

// transforms is the global registry populated by each cloud's init().
var transforms = map[model.Cloud]CloudSparkTransform{}

// RegisterTransform registers a CloudSparkTransform for the given cloud.
// Called from init() in aws_transform.go, azure_transform.go, gcp_transform.go.
func RegisterTransform(c model.Cloud, t CloudSparkTransform) {
	transformsMu.Lock()
	defer transformsMu.Unlock()
	transforms[c] = t
}

// selectTransform returns the registered transform for cloud c.
// Returns an error for unknown clouds (defensive; config validates at startup).
func selectTransform(c model.Cloud) (CloudSparkTransform, error) {
	transformsMu.RLock()
	defer transformsMu.RUnlock()
	t, ok := transforms[c]
	if !ok {
		return nil, fmt.Errorf("spark: no transform registered for cloud %q", c)
	}
	return t, nil
}

// gcpSAVolumeName is the K8s volume name for the GCP service-account key Secret.
const gcpSAVolumeName = "jaiscloud-gcp-sa-key"

// azureIdentityVolumeName is the K8s volume name for the Azure Workload Identity projection.
const azureIdentityVolumeName = "jaiscloud-azure-identity"

// toEnvVars converts a string map to a []envVar slice.
func toEnvVars(m map[string]string) []k8stypes.EnvVar {
	out := make([]k8stypes.EnvVar, 0, len(m))
	for k, v := range m {
		out = append(out, k8stypes.EnvVar{Name: k, Value: v})
	}
	return out
}
