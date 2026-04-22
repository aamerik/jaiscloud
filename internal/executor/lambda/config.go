package lambda

import "os"

// LambdaConfig holds executor-wide configuration shared by all functions.
type LambdaConfig struct {
	// Mode is "mock", "docker", or "k8s".
	Mode string
	// DefaultImage overrides the runtime→image lookup for all functions.
	DefaultImage string
	// Network is the Docker network for warm containers (docker mode only).
	Network string
	// KeepaliveSecs is the idle timeout before a warm container is stopped (docker mode).
	KeepaliveSecs int
	// Namespace is the K8s namespace for Lambda Jobs (k8s mode only).
	Namespace string
	// ServiceAccount is the K8s service account for Lambda Jobs.
	ServiceAccount string
	// APIServer is the K8s API server URL (k8s mode only).
	APIServer string
	// JaisCloudEndpoint is passed to containers so they can call back (optional).
	JaisCloudEndpoint string
	// Region is injected as AWS_DEFAULT_REGION into containers.
	Region string
}

// regionOrDefault returns r if non-empty, else "us-east-1".
func regionOrDefault(r string) string {
	if r != "" {
		return r
	}
	return "us-east-1"
}

// DefaultLambdaConfig returns a LambdaConfig with sensible defaults.
func DefaultLambdaConfig() LambdaConfig {
	cfg := LambdaConfig{
		Mode:          "mock",
		Network:       "jaiscloud-net",
		KeepaliveSecs: 300,
		Namespace:     "jaiscloud",
	}
	if v := os.Getenv("JAISCLOUD_K8S_APISERVER"); v != "" {
		cfg.APIServer = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_NAMESPACE"); v != "" {
		cfg.Namespace = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_SA"); v != "" {
		cfg.ServiceAccount = v
	}
	return cfg
}

// runtimeImages maps Lambda runtime identifiers to their default public ECR images.
var runtimeImages = map[string]string{
	"python3.12":    "public.ecr.aws/lambda/python:3.12",
	"python3.11":    "public.ecr.aws/lambda/python:3.11",
	"python3.10":    "public.ecr.aws/lambda/python:3.10",
	"python3.9":     "public.ecr.aws/lambda/python:3.9",
	"nodejs20.x":    "public.ecr.aws/lambda/nodejs:20",
	"nodejs18.x":    "public.ecr.aws/lambda/nodejs:18",
	"java21":        "public.ecr.aws/lambda/java:21",
	"java17":        "public.ecr.aws/lambda/java:17",
	"java11":        "public.ecr.aws/lambda/java:11",
	"dotnet8":       "public.ecr.aws/lambda/dotnet:8",
	"dotnet6":       "public.ecr.aws/lambda/dotnet:6",
	"go1.x":         "public.ecr.aws/lambda/provided:al2",
	"provided.al2":  "public.ecr.aws/lambda/provided:al2",
	"provided.al2023": "public.ecr.aws/lambda/provided:al2023",
}

// ImageForRuntime returns the container image for the given Lambda runtime.
// cfg.DefaultImage overrides the lookup when non-empty.
// req.Image (per-function ImageUri) takes precedence over both.
func ImageForRuntime(req InvokeRequest, cfg LambdaConfig) string {
	if req.Image != "" {
		return req.Image
	}
	if cfg.DefaultImage != "" {
		return cfg.DefaultImage
	}
	if img, ok := runtimeImages[req.Runtime]; ok {
		return img
	}
	return "public.ecr.aws/lambda/provided:al2"
}

// NewExecutor constructs the appropriate LambdaExecutor for the given mode.
func NewExecutor(cfg LambdaConfig) LambdaExecutor {
	switch cfg.Mode {
	case "docker":
		return NewDockerExecutor(cfg)
	case "k8s":
		return NewK8sExecutor(cfg)
	default:
		return &MockExecutor{}
	}
}
