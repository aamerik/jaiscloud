package platform

import (
	"testing"

	"jaiscloud/internal/k8stypes"
)

func minimalSpec() *k8stypes.PodSpec {
	return &k8stypes.PodSpec{
		Containers: []k8stypes.Container{{Name: "main", Image: "alpine"}},
	}
}

func TestApplyK8s_NilCfg(t *testing.T) {
	spec := minimalSpec()
	ctr := &spec.Containers[0]
	if err := ApplyK8s(spec, ctr, nil); err != nil {
		t.Errorf("nil cfg should be a no-op; got %v", err)
	}
	if len(spec.Volumes) != 0 {
		t.Errorf("no volumes should be added for nil cfg; got %v", spec.Volumes)
	}
}

func TestApplyK8s_TLSDisabled_ExtraVolsAndEnv(t *testing.T) {
	cfg := &PlatformConfig{
		TLS: TLSConfig{Enabled: false},
		Volumes: []VolumeSpec{{
			Name:   "extra-vol",
			Source: VolumeSource{Kind: "emptyDir", EmptyDir: &EmptyDirSource{}},
			Mounts: []MountSpec{{MountPath: "/tmp/extra"}},
		}},
		Env: map[string]string{"EXTRA_VAR": "value"},
	}
	spec := minimalSpec()
	ctr := &spec.Containers[0]
	if err := ApplyK8s(spec, ctr, cfg); err != nil {
		t.Fatalf("ApplyK8s: %v", err)
	}
	if len(spec.Volumes) != 1 || spec.Volumes[0].Name != "extra-vol" {
		t.Errorf("extra volume not added: %v", spec.Volumes)
	}
	found := false
	for _, e := range ctr.Env {
		if e.Name == "EXTRA_VAR" && e.Value == "value" {
			found = true
		}
	}
	if !found {
		t.Errorf("EXTRA_VAR not in container env: %v", ctr.Env)
	}
	if len(ctr.VolumeMounts) != 1 || ctr.VolumeMounts[0].MountPath != "/tmp/extra" {
		t.Errorf("volume mount not added: %v", ctr.VolumeMounts)
	}
}

func TestApplyK8s_VolumeConflict(t *testing.T) {
	spec := minimalSpec()
	spec.Volumes = []k8stypes.Volume{{Name: "conflict-vol"}}
	cfg := &PlatformConfig{
		TLS: TLSConfig{Enabled: false},
		Volumes: []VolumeSpec{{
			Name:   "conflict-vol",
			Source: VolumeSource{Kind: "emptyDir", EmptyDir: &EmptyDirSource{}},
			Mounts: []MountSpec{{MountPath: "/tmp"}},
		}},
		Env: map[string]string{},
	}
	ctr := &spec.Containers[0]
	if err := ApplyK8s(spec, ctr, cfg); err == nil {
		t.Error("expected conflict error, got nil")
	}
}

func TestApplyK8s_EnvMerge_ExistingEnvPreserved(t *testing.T) {
	spec := minimalSpec()
	spec.Containers[0].Env = []k8stypes.EnvVar{{Name: "EXISTING", Value: "keep"}}
	cfg := &PlatformConfig{
		TLS: TLSConfig{Enabled: false},
		Env: map[string]string{"NEW_VAR": "new"},
	}
	ctr := &spec.Containers[0]
	if err := ApplyK8s(spec, ctr, cfg); err != nil {
		t.Fatalf("ApplyK8s: %v", err)
	}
	names := make(map[string]string)
	for _, e := range ctr.Env {
		names[e.Name] = e.Value
	}
	if names["EXISTING"] != "keep" {
		t.Errorf("EXISTING env should be preserved; got %q", names["EXISTING"])
	}
	if names["NEW_VAR"] != "new" {
		t.Errorf("NEW_VAR should be added; got %q", names["NEW_VAR"])
	}
}

func TestApplyK8s_TLSEnabled_InjectsVolumesAndEnv(t *testing.T) {
	cfg := &PlatformConfig{
		TLS: TLSConfig{
			Enabled: true,
			CASources: []CASource{{
				Name:   "corp-ca",
				Source: CASourceRef{Kind: "configMap", Name: "corp-ca-cm"},
			}},
		},
	}
	spec := minimalSpec()
	ctr := &spec.Containers[0]
	if err := ApplyK8s(spec, ctr, cfg); err != nil {
		t.Fatalf("ApplyK8s with TLS: %v", err)
	}
	// At least one CA volume should be present.
	if len(spec.Volumes) == 0 {
		t.Error("TLS enabled: expected CA volumes on pod spec, got none")
	}
	// At least one init container should be present (JVM truststore or PEM bundle materializer).
	if len(spec.InitContainers) == 0 {
		t.Error("TLS enabled: expected init containers, got none")
	}
	// Container env should have SSL_CERT_FILE or JAVA_TOOL_OPTIONS (from materializers).
	envNames := make(map[string]bool)
	for _, e := range ctr.Env {
		envNames[e.Name] = true
	}
	if !envNames["SSL_CERT_FILE"] && !envNames["JAVA_TOOL_OPTIONS"] {
		t.Errorf("TLS enabled: expected SSL_CERT_FILE or JAVA_TOOL_OPTIONS in container env; got %v", ctr.Env)
	}
	// Container should have volume mounts from materializers.
	if len(ctr.VolumeMounts) == 0 {
		t.Error("TLS enabled: expected volume mounts on container, got none")
	}
}

func TestApplyK8s_MultipleVolumes(t *testing.T) {
	cfg := &PlatformConfig{
		TLS: TLSConfig{Enabled: false},
		Volumes: []VolumeSpec{
			{
				Name:   "vol-a",
				Source: VolumeSource{Kind: "emptyDir", EmptyDir: &EmptyDirSource{}},
				Mounts: []MountSpec{{MountPath: "/a"}},
			},
			{
				Name:   "vol-b",
				Source: VolumeSource{Kind: "configMap", ConfigMap: &ConfigMapSource{Name: "my-cm"}},
				Mounts: []MountSpec{{MountPath: "/b"}},
			},
		},
		Env: map[string]string{},
	}
	spec := minimalSpec()
	ctr := &spec.Containers[0]
	if err := ApplyK8s(spec, ctr, cfg); err != nil {
		t.Fatalf("ApplyK8s: %v", err)
	}
	if len(spec.Volumes) != 2 {
		t.Errorf("expected 2 volumes; got %d", len(spec.Volumes))
	}
	if len(ctr.VolumeMounts) != 2 {
		t.Errorf("expected 2 mounts; got %d", len(ctr.VolumeMounts))
	}
}
