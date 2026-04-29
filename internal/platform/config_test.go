package platform

import (
	"testing"
)

func TestLoadFromEnv_Defaults(t *testing.T) {
	// Clear relevant env vars so we get defaults.
	t.Setenv("JAISCLOUD_PLATFORM_TLS_ENABLED", "")
	t.Setenv("JAISCLOUD_PLATFORM_TLS_CA_SOURCES", "")
	t.Setenv("JAISCLOUD_PLATFORM_VOLUMES", "")
	t.Setenv("JAISCLOUD_PLATFORM_ENV", "")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if !cfg.TLS.Enabled {
		t.Error("TLS should be enabled by default")
	}
	if len(cfg.TLS.CASources) == 0 {
		t.Error("default CA sources should be non-empty")
	}
	if cfg.TLS.TruststorePassword != "changeit" {
		t.Errorf("default truststore password: got %q, want %q", cfg.TLS.TruststorePassword, "changeit")
	}
}

func TestLoadFromEnv_TLSDisabled(t *testing.T) {
	t.Setenv("JAISCLOUD_PLATFORM_TLS_ENABLED", "false")
	t.Setenv("JAISCLOUD_PLATFORM_TLS_CA_SOURCES", "")
	t.Setenv("JAISCLOUD_PLATFORM_VOLUMES", "")
	t.Setenv("JAISCLOUD_PLATFORM_ENV", "")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.TLS.Enabled {
		t.Error("TLS should be disabled")
	}
}

func TestLoadFromEnv_ExtraEnv(t *testing.T) {
	t.Setenv("JAISCLOUD_PLATFORM_TLS_ENABLED", "false")
	t.Setenv("JAISCLOUD_PLATFORM_TLS_CA_SOURCES", "")
	t.Setenv("JAISCLOUD_PLATFORM_VOLUMES", "")
	t.Setenv("JAISCLOUD_PLATFORM_ENV", `{"MY_VAR":"hello","OTHER":"world"}`)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.Env["MY_VAR"] != "hello" {
		t.Errorf("MY_VAR: got %q", cfg.Env["MY_VAR"])
	}
	if cfg.Env["OTHER"] != "world" {
		t.Errorf("OTHER: got %q", cfg.Env["OTHER"])
	}
}

func TestCanonicalise_ShortFormConfigMap(t *testing.T) {
	r := rawVolumeSpec{Name: "my-cm", ConfigMap: "my-configmap", MountPath: "/etc/conf"}
	vs, err := canonicalise(r)
	if err != nil {
		t.Fatalf("canonicalise: %v", err)
	}
	if vs.Source.Kind != "configMap" {
		t.Errorf("kind: got %q, want configMap", vs.Source.Kind)
	}
	if vs.Source.ConfigMap == nil || vs.Source.ConfigMap.Name != "my-configmap" {
		t.Errorf("ConfigMap.Name: got %+v", vs.Source.ConfigMap)
	}
	if len(vs.Mounts) != 1 || vs.Mounts[0].MountPath != "/etc/conf" {
		t.Errorf("mounts: got %+v", vs.Mounts)
	}
}

func TestCanonicalise_ShortFormSecret(t *testing.T) {
	r := rawVolumeSpec{Name: "my-secret", Secret: "my-secret-name", MountPath: "/etc/secret", ReadOnly: true}
	vs, err := canonicalise(r)
	if err != nil {
		t.Fatalf("canonicalise: %v", err)
	}
	if vs.Source.Kind != "secret" {
		t.Errorf("kind: got %q", vs.Source.Kind)
	}
	if vs.Mounts[0].ReadOnly != true {
		t.Error("mount should be read-only")
	}
}

func TestCanonicalise_ShortFormEmptyDir(t *testing.T) {
	r := rawVolumeSpec{Name: "scratch", EmptyDir: true, MountPath: "/tmp/scratch"}
	vs, err := canonicalise(r)
	if err != nil {
		t.Fatalf("canonicalise: %v", err)
	}
	if vs.Source.Kind != "emptyDir" {
		t.Errorf("kind: got %q", vs.Source.Kind)
	}
}

func TestCanonicalise_ShortFormPVC(t *testing.T) {
	r := rawVolumeSpec{Name: "data", PVC: "my-pvc", MountPath: "/data"}
	vs, err := canonicalise(r)
	if err != nil {
		t.Fatalf("canonicalise: %v", err)
	}
	if vs.Source.Kind != "pvc" {
		t.Errorf("kind: got %q", vs.Source.Kind)
	}
	if vs.Source.PVC == nil || vs.Source.PVC.ClaimName != "my-pvc" {
		t.Errorf("PVC.ClaimName: got %+v", vs.Source.PVC)
	}
}

func TestCanonicalise_NoName_Error(t *testing.T) {
	r := rawVolumeSpec{ConfigMap: "cm", MountPath: "/etc/cm"}
	_, err := canonicalise(r)
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestCanonicalise_NoMountPath_Error(t *testing.T) {
	r := rawVolumeSpec{Name: "cm-vol", ConfigMap: "cm-name"}
	_, err := canonicalise(r)
	if err == nil {
		t.Error("expected error for missing mountPath")
	}
}

func TestCanonicalise_NoSource_Error(t *testing.T) {
	r := rawVolumeSpec{Name: "bad-vol", MountPath: "/mnt"}
	_, err := canonicalise(r)
	if err == nil {
		t.Error("expected error when no source provided")
	}
}

func TestValidate_DuplicateCAAlias(t *testing.T) {
	cfg := &PlatformConfig{
		TLS: TLSConfig{
			Enabled: true,
			CASources: []CASource{
				{Name: "dup", Source: CASourceRef{Kind: "configMap", Name: "cm", Key: "ca.crt"}},
				{Name: "dup", Source: CASourceRef{Kind: "configMap", Name: "cm2", Key: "ca.crt"}},
			},
		},
		Env: map[string]string{},
	}
	if err := validate(cfg); err == nil {
		t.Error("expected error for duplicate CA alias")
	}
}

func TestValidate_DuplicateVolumeName(t *testing.T) {
	cfg := &PlatformConfig{
		TLS: TLSConfig{
			Enabled: false,
			CASources: []CASource{
				{Name: "ca", Source: CASourceRef{Kind: "configMap", Name: "cm", Key: "ca.crt"}},
			},
		},
		Volumes: []VolumeSpec{
			{Name: "dup-vol", Source: VolumeSource{Kind: "emptyDir", EmptyDir: &EmptyDirSource{}}},
			{Name: "dup-vol", Source: VolumeSource{Kind: "emptyDir", EmptyDir: &EmptyDirSource{}}},
		},
		Env: map[string]string{},
	}
	if err := validate(cfg); err == nil {
		t.Error("expected error for duplicate volume name")
	}
}

func TestValidate_HostPathNotAllowed(t *testing.T) {
	cfg := &PlatformConfig{
		TLS:               TLSConfig{Enabled: false},
		HostPathAllowlist: []string{"/allowed"},
		Volumes: []VolumeSpec{{
			Name: "hp",
			Source: VolumeSource{
				Kind:     "hostPath",
				HostPath: &HostPathSource{Path: "/forbidden/path"},
			},
		}},
		Env: map[string]string{},
	}
	if err := validate(cfg); err == nil {
		t.Error("expected error for hostPath not in allowlist")
	}
}

func TestValidate_HostPathAllowed(t *testing.T) {
	cfg := &PlatformConfig{
		TLS:               TLSConfig{Enabled: false},
		HostPathAllowlist: []string{"/allowed"},
		Volumes: []VolumeSpec{{
			Name: "hp",
			Source: VolumeSource{
				Kind:     "hostPath",
				HostPath: &HostPathSource{Path: "/allowed/subdir"},
			},
		}},
		Env: map[string]string{},
	}
	if err := validate(cfg); err != nil {
		t.Errorf("unexpected error for allowed hostPath: %v", err)
	}
}

func TestUnmarshalAuto_JSON(t *testing.T) {
	var m map[string]string
	if err := unmarshalAuto(`{"key":"val"}`, &m); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if m["key"] != "val" {
		t.Errorf("got %q", m["key"])
	}
}

func TestUnmarshalAuto_YAML(t *testing.T) {
	var m map[string]string
	if err := unmarshalAuto("key: val", &m); err != nil {
		t.Fatalf("YAML: %v", err)
	}
	if m["key"] != "val" {
		t.Errorf("got %q", m["key"])
	}
}
