package platform

import (
	"testing"
)

func TestApplyDocker_NilCfg(t *testing.T) {
	vols, envs, err := ApplyDocker(nil)
	if err != nil || len(vols) != 0 || len(envs) != 0 {
		t.Errorf("nil cfg should return empty args; got vols=%v envs=%v err=%v", vols, envs, err)
	}
}

func TestApplyDocker_TLSDisabled(t *testing.T) {
	cfg := &PlatformConfig{TLS: TLSConfig{Enabled: false}, Env: map[string]string{}}
	vols, envs, err := ApplyDocker(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vols) != 0 || len(envs) != 0 {
		t.Errorf("TLS disabled should return no args; got vols=%v envs=%v", vols, envs)
	}
}

func TestApplyDocker_TLSEnabled_ConfigMapSources_NoBundle(t *testing.T) {
	// ConfigMap-sourced CAs cannot be materialised on the Docker host — no bundle created.
	cfg := &PlatformConfig{
		TLS: TLSConfig{
			Enabled: true,
			CASources: []CASource{{
				Name:   "jaiscloud",
				Source: CASourceRef{Kind: "configMap", Name: "jaiscloud-ca-cert", Key: "ca.crt"},
			}},
		},
		Env: map[string]string{},
	}
	vols, envs, err := ApplyDocker(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No file-based CAs → no bundle → no volume/env added.
	if len(vols) != 0 {
		t.Errorf("expected no volume args for configMap CA source; got %v", vols)
	}
	if len(envs) != 0 {
		t.Errorf("expected no env args for configMap CA source; got %v", envs)
	}
}

// tlsEnabledCfg returns a PlatformConfig with TLS enabled using a ConfigMap CA source
// (which produces no host-side PEM bundle, so no TLS volume args are emitted).
// This lets tests for extra-env and extra-volumes reach the code past the TLS guard.
func tlsEnabledCfg(extra map[string]string, vols []VolumeSpec) *PlatformConfig {
	return &PlatformConfig{
		TLS: TLSConfig{
			Enabled: true,
			CASources: []CASource{{
				Name:   "jaiscloud",
				Source: CASourceRef{Kind: "configMap", Name: "jaiscloud-ca-cert", Key: "ca.crt"},
			}},
		},
		Volumes: vols,
		Env:     extra,
	}
}

func TestApplyDocker_ExtraEnv(t *testing.T) {
	cfg := tlsEnabledCfg(map[string]string{"MY_VAR": "hello"}, nil)
	_, envArgs, err := ApplyDocker(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for i := 0; i+1 < len(envArgs); i += 2 {
		if envArgs[i] == "-e" && envArgs[i+1] == "MY_VAR=hello" {
			found = true
		}
	}
	if !found {
		t.Errorf("MY_VAR=hello not found in envArgs: %v", envArgs)
	}
}

func TestApplyDocker_HostPathVolume(t *testing.T) {
	vols := []VolumeSpec{{
		Name: "my-host-vol",
		Source: VolumeSource{
			Kind:     "hostPath",
			HostPath: &HostPathSource{Path: "/tmp/certs"},
		},
		Mounts: []MountSpec{{MountPath: "/etc/certs"}},
	}}
	cfg := tlsEnabledCfg(map[string]string{}, vols)
	volArgs, _, err := ApplyDocker(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for i := 0; i+1 < len(volArgs); i += 2 {
		if volArgs[i] == "-v" && volArgs[i+1] == "/tmp/certs:/etc/certs:ro" {
			found = true
		}
	}
	if !found {
		t.Errorf("hostPath bind-mount not found in volArgs: %v", volArgs)
	}
}

func TestApplyDocker_ConfigMapVolume_NotBindMounted(t *testing.T) {
	vols := []VolumeSpec{{
		Name:   "my-cm",
		Source: VolumeSource{Kind: "configMap", ConfigMap: &ConfigMapSource{Name: "my-configmap"}},
		Mounts: []MountSpec{{MountPath: "/etc/conf"}},
	}}
	cfg := tlsEnabledCfg(map[string]string{}, vols)
	volArgs, _, err := ApplyDocker(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ConfigMap volumes cannot be bind-mounted in Docker mode; only hostPath can be.
	for i := 0; i+1 < len(volArgs); i += 2 {
		if volArgs[i] == "-v" {
			t.Errorf("configMap volume should not produce docker -v args; got %q", volArgs[i+1])
		}
	}
}
