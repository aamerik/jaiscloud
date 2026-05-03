package k8shelpers

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"jaiscloud/internal/platform"
)

// makePlatformWithTLS creates a PlatformConfig with TLS enabled and one CA source.
func makePlatformWithTLS() *platform.PlatformConfig {
	return &platform.PlatformConfig{
		TLS: platform.TLSConfig{
			Enabled:            true,
			TruststorePassword: "changeit",
			CASources: []platform.CASource{{
				Name:   "jaiscloud",
				Source: platform.CASourceRef{Kind: "configMap", Name: "jaiscloud-ca-cert", Key: "ca.crt"},
			}},
		},
	}
}

func makeBase(name string) PodSpecInput {
	return PodSpecInput{
		MainContainer: corev1.Container{
			Name:    name,
			Image:   "apache/spark:3.5",
			Command: []string{"spark-submit"},
		},
		Namespace: "test",
		Labels:    map[string]string{"app": "test"},
	}
}

func TestBuildPodSpec_NoCaller_TLSInitContainerPresent(t *testing.T) {
	tpl, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), makePlatformWithTLS(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have JVM + PEM init containers.
	if len(tpl.Spec.InitContainers) < 2 {
		t.Errorf("expected ≥2 init containers, got %d", len(tpl.Spec.InitContainers))
	}
	found := false
	for _, ic := range tpl.Spec.InitContainers {
		if ic.Name == "jvm-truststore-init" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected jvm-truststore-init init container")
	}
	// Should have truststore volume.
	foundVol := false
	for _, v := range tpl.Spec.Volumes {
		if v.Name == "jaiscloud-truststore" {
			foundVol = true
			break
		}
	}
	if !foundVol {
		t.Error("expected jaiscloud-truststore volume")
	}
}

func TestBuildPodSpec_CallerAddsVolume_BothPresent(t *testing.T) {
	callerTpl := []byte(`
spec:
  volumes:
    - name: "my-data"
      emptyDir: {}
  containers:
    - name: "spark-submit"
      volumeMounts:
        - name: "my-data"
          mountPath: "/data"
`)
	tpl, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), makePlatformWithTLS(), callerTpl, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasMyData := false
	hasTruststore := false
	for _, v := range tpl.Spec.Volumes {
		if v.Name == "my-data" {
			hasMyData = true
		}
		if v.Name == "jaiscloud-truststore" {
			hasTruststore = true
		}
	}
	if !hasMyData {
		t.Error("caller volume 'my-data' missing")
	}
	if !hasTruststore {
		t.Error("platform volume 'jaiscloud-truststore' missing")
	}
}

func TestBuildPodSpec_ConflictingVolume_CallerWins(t *testing.T) {
	callerTpl := []byte(`
spec:
  volumes:
    - name: "jaiscloud-truststore"
      configMap:
        name: "my-override"
`)
	tpl, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), makePlatformWithTLS(), callerTpl, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, v := range tpl.Spec.Volumes {
		if v.Name == "jaiscloud-truststore" {
			// Caller's configMap should win (not emptyDir).
			if v.VolumeSource.ConfigMap != nil && v.VolumeSource.ConfigMap.Name == "my-override" {
				return // correct
			}
			t.Errorf("expected caller's configMap to win, got: %+v", v.VolumeSource)
			return
		}
	}
	t.Error("volume 'jaiscloud-truststore' missing")
}

func TestBuildPodSpec_SecurityClassifiedEnv_Merged(t *testing.T) {
	callerTpl := []byte(`
spec:
  containers:
    - name: "spark-submit"
      env:
        - name: "JAVA_TOOL_OPTIONS"
          value: "-Xmx4g"
`)
	tpl, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), makePlatformWithTLS(), callerTpl, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tpl.Spec.Containers) == 0 {
		t.Fatal("no containers")
	}
	for _, e := range tpl.Spec.Containers[0].Env {
		if e.Name == "JAVA_TOOL_OPTIONS" {
			// Must contain BOTH caller value and platform directive.
			if !contains(e.Value, "-Xmx4g") {
				t.Errorf("expected caller -Xmx4g in JAVA_TOOL_OPTIONS, got: %s", e.Value)
			}
			if !contains(e.Value, "trustStore") {
				t.Errorf("expected platform trustStore in JAVA_TOOL_OPTIONS, got: %s", e.Value)
			}
			return
		}
	}
	t.Error("JAVA_TOOL_OPTIONS not found in container env")
}

func TestBuildPodSpec_OptOutTLS_InitContainerAbsent(t *testing.T) {
	RegisterOptOutToken("skip-tls") // idempotent
	callerTpl := []byte(`
metadata:
  annotations:
    jaiscloud.io/platform-overlay: "skip-tls"
`)
	tpl, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), makePlatformWithTLS(), callerTpl, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, ic := range tpl.Spec.InitContainers {
		if ic.Name == "jvm-truststore-init" || ic.Name == "pem-bundle-init" {
			t.Errorf("unexpected init container %q after skip-tls opt-out", ic.Name)
		}
	}
}

func TestBuildPodSpec_UnknownOptOutToken_Error(t *testing.T) {
	callerTpl := []byte(`
metadata:
  annotations:
    jaiscloud.io/platform-overlay: "skip-kafaka-certs"
`)
	_, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), makePlatformWithTLS(), callerTpl, nil)
	if !errors.Is(err, ErrUnknownOptOutToken) {
		t.Errorf("expected ErrUnknownOptOutToken, got: %v", err)
	}
}

func TestBuildPodSpec_NilOverlay_NoError(t *testing.T) {
	tpl, err := BuildPodSpec(context.Background(), nil, makeBase("spark-submit"), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tpl.Spec.InitContainers) != 0 {
		t.Errorf("expected no init containers with nil overlay, got %d", len(tpl.Spec.InitContainers))
	}
}

func TestSecurityClassifiedEnvMerge_AllKeys(t *testing.T) {
	keys := []string{
		"JAVA_OPTS", "SSL_CERT_FILE", "SSL_CERT_DIR",
		"AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS",
	}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			platform := []corev1.EnvVar{{Name: key, Value: "platform-value"}}
			caller := []corev1.EnvVar{{Name: key, Value: "caller-value"}}
			merged, _ := mergeEnv(platform, caller, map[string]bool{})
			for _, e := range merged {
				if e.Name == key {
					if !contains(e.Value, "caller-value") {
						t.Errorf("expected caller-value in %s, got %s", key, e.Value)
					}
					if !contains(e.Value, "platform-value") {
						t.Errorf("expected platform-value appended in %s, got %s", key, e.Value)
					}
					return
				}
			}
			t.Errorf("key %s not found in merged env", key)
		})
	}
}

func TestUnknownOptOutToken_ThreeTypos(t *testing.T) {
	typos := []string{"skip-kafaka-certs", "skp-tls", "skip-aws-credential"}
	for _, typo := range typos {
		_, err := parseOptOuts(typo)
		if !errors.Is(err, ErrUnknownOptOutToken) {
			t.Errorf("expected ErrUnknownOptOutToken for %q, got %v", typo, err)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
