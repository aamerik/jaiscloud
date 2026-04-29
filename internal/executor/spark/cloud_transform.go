package spark

import (
	"fmt"
	"strings"
	"sync"

	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/model"
)

// CloudSparkTransform encapsulates all cloud-specific contributions to a
// Spark K8s Job manifest. Implementations register themselves via init().
type CloudSparkTransform interface {
	Cloud() model.Cloud
	// Rewrite rewrites a cloud-neutral URI (e.g. s3a://bucket/key) into the
	// cloud's native scheme. It receives cfg so it can use cloud-specific
	// identifiers (e.g. Azure storage account). Idempotent.
	Rewrite(uri string, cfg SparkConfig) string
	ResolveCommand(job SparkJob, cfg SparkConfig) SparkSubmitCommand
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

// rewriteURIs applies t.Rewrite(uri, cfg) to every element of args.
func rewriteURIs(t CloudSparkTransform, args []string, cfg SparkConfig) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = t.Rewrite(a, cfg)
	}
	return out
}

// rewriteS3aToABFS rewrites s3a://bucket/key → abfss://bucket@account.dfs.core.windows.net/key.
// Used by the Azure transform.
func rewriteS3aToABFS(uri, storageAccount string) string {
	const prefix = "s3a://"
	if !strings.HasPrefix(uri, prefix) {
		return uri
	}
	rest := strings.TrimPrefix(uri, prefix)
	bucket, path, _ := strings.Cut(rest, "/")
	host := bucket + "@" + storageAccount + ".dfs.core.windows.net"
	if path != "" {
		return "abfss://" + host + "/" + path
	}
	return "abfss://" + host
}

// rewriteS3aToGCS rewrites s3a://bucket/key → gs://bucket/key.
// Used by the GCP transform.
func rewriteS3aToGCS(uri string) string {
	const prefix = "s3a://"
	if !strings.HasPrefix(uri, prefix) {
		return uri
	}
	return "gs://" + strings.TrimPrefix(uri, prefix)
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
