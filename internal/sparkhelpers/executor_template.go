package sparkhelpers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

// BuildExecutorPodTemplate returns YAML bytes of a PodTemplate suitable for
// spark.kubernetes.executor.podTemplateFile. Applies platform overlay via
// k8shelpers.BuildPodSpec on an empty main container (no image, no command —
// Spark fills those in). If both overlay and callerTpl are nil/empty, returns
// minimal `spec: {}` YAML.
//
// ctx and k8s are forwarded to BuildPodSpec for the identityMutator call path;
// executor templates always pass nil for identityMutator so they are unused here.
func BuildExecutorPodTemplate(ctx context.Context, k8s kubernetes.Interface, overlay *platform.PlatformConfig, callerTpl []byte) ([]byte, error) {
	if overlay == nil && len(callerTpl) == 0 {
		return []byte("spec: {}\n"), nil
	}

	// Build with an empty main container — Spark fills image/command.
	base := k8shelpers.PodSpecInput{
		MainContainer: corev1.Container{
			Name: "spark-executor",
		},
	}

	tpl, err := k8shelpers.BuildPodSpec(ctx, k8s, base, overlay, callerTpl, nil)
	if err != nil {
		return nil, err
	}

	// Encode as a PodTemplateSpec YAML (Spark reads the spec field).
	out, err := yaml.Marshal(tpl)
	if err != nil {
		return nil, err
	}
	return out, nil
}
