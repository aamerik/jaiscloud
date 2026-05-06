package sparkhelpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"

	"jaiscloud/internal/platform"
)

func TestBuildExecutorPodTemplate_NilOverlay(t *testing.T) {
	got, err := BuildExecutorPodTemplate(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "spec: {}\n", string(got))
}

func TestBuildExecutorPodTemplate_EmptyOverlay(t *testing.T) {
	got, err := BuildExecutorPodTemplate(context.Background(), nil, nil, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "spec: {}\n", string(got))
}

func TestBuildExecutorPodTemplate_WithTLSOverlay(t *testing.T) {
	overlay := &platform.PlatformConfig{
		TLS: platform.TLSConfig{
			Enabled: true,
			CASources: []platform.CASource{
				{
					Name:   "testca",
					Source: platform.CASourceRef{Kind: "configMap", Name: "my-ca", Key: "ca.crt"},
				},
			},
			TruststorePassword: "changeit",
		},
	}

	got, err := BuildExecutorPodTemplate(context.Background(), nil, overlay, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, got)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	assert.NotEmpty(t, tpl.Spec.InitContainers, "expected TLS init containers from platform overlay")
}

func TestBuildExecutorPodTemplate_CallerTplOnly(t *testing.T) {
	callerTpl := []byte(`
spec:
  serviceAccountName: spark-sa
  containers:
  - name: spark-kubernetes-executor
    image: spark:3.5
`)
	got, err := BuildExecutorPodTemplate(context.Background(), nil, nil, callerTpl)
	require.NoError(t, err)
	assert.NotEmpty(t, got)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	assert.Equal(t, "spark-sa", tpl.Spec.ServiceAccountName)
}

// ─── container name ───────────────────────────────────────────────────────────

func TestBuildExecutorPodTemplate_ContainerNameIsSparkKubernetesExecutor_WithOverlay(t *testing.T) {
	// Real Spark injects the executor image/command into the container named
	// exactly "spark-kubernetes-executor". Any other name causes the pod to
	// spin up with no entrypoint and immediately fail.
	overlay := &platform.PlatformConfig{
		TLS: platform.TLSConfig{
			Enabled: true,
			CASources: []platform.CASource{{
				Name:   "ca",
				Source: platform.CASourceRef{Kind: "configMap", Name: "ca-cm", Key: "ca.crt"},
			}},
		},
	}
	got, err := BuildExecutorPodTemplate(context.Background(), nil, overlay, nil)
	require.NoError(t, err)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	require.NotEmpty(t, tpl.Spec.Containers, "expected main container in output")
	assert.Equal(t, "spark-kubernetes-executor", tpl.Spec.Containers[0].Name)
}

func TestBuildExecutorPodTemplate_ContainerNameNotOverriddenByCallerTpl(t *testing.T) {
	// BuildPodSpec merges callerTpl containers by position (index 0), not by
	// name. A callerTpl that still uses the old "spark-executor" name must not
	// replace the canonical "spark-kubernetes-executor" name in the output.
	callerTpl := []byte(`
spec:
  containers:
  - name: spark-executor
    image: spark:3.5
`)
	got, err := BuildExecutorPodTemplate(context.Background(), nil, nil, callerTpl)
	require.NoError(t, err)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	require.NotEmpty(t, tpl.Spec.Containers)
	assert.Equal(t, "spark-kubernetes-executor", tpl.Spec.Containers[0].Name,
		"container name must not be overwritten by callerTpl")
}

// ─── ImagePullPolicy ──────────────────────────────────────────────────────────

func TestBuildExecutorPodTemplate_ImagePullPolicyIfNotPresent_WithOverlay(t *testing.T) {
	overlay := &platform.PlatformConfig{
		TLS: platform.TLSConfig{
			Enabled: true,
			CASources: []platform.CASource{{
				Name:   "ca",
				Source: platform.CASourceRef{Kind: "configMap", Name: "ca-cm", Key: "ca.crt"},
			}},
		},
	}
	got, err := BuildExecutorPodTemplate(context.Background(), nil, overlay, nil)
	require.NoError(t, err)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	require.NotEmpty(t, tpl.Spec.Containers)
	assert.Equal(t, corev1.PullIfNotPresent, tpl.Spec.Containers[0].ImagePullPolicy)
}

func TestBuildExecutorPodTemplate_ImagePullPolicyIfNotPresent_WithCallerTpl(t *testing.T) {
	// ImagePullPolicy is not in the BuildPodSpec merge table, so the base value
	// (PullIfNotPresent) must survive regardless of what the callerTpl specifies.
	callerTpl := []byte(`
spec:
  containers:
  - name: spark-kubernetes-executor
    image: spark:3.5
    imagePullPolicy: Always
`)
	got, err := BuildExecutorPodTemplate(context.Background(), nil, nil, callerTpl)
	require.NoError(t, err)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	require.NotEmpty(t, tpl.Spec.Containers)
	assert.Equal(t, corev1.PullIfNotPresent, tpl.Spec.Containers[0].ImagePullPolicy,
		"ImagePullPolicy is not merged from callerTpl — base PullIfNotPresent must win")
}
