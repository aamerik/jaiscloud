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
  - name: spark-executor
    image: spark:3.5
`)
	got, err := BuildExecutorPodTemplate(context.Background(), nil, nil, callerTpl)
	require.NoError(t, err)
	assert.NotEmpty(t, got)

	var tpl corev1.PodTemplateSpec
	require.NoError(t, yaml.Unmarshal(got, &tpl))
	assert.Equal(t, "spark-sa", tpl.Spec.ServiceAccountName)
}
