package platform

import "jaiscloud/internal/k8stypes"

// nonJVMEnvVars lists the env vars that point every non-JVM runtime at the
// PEM bundle. Each runtime checks a different var and does not fall back.
var nonJVMEnvVars = []string{
	"SSL_CERT_FILE",       // OpenSSL, libcurl, Go net/http, Rust rustls-native-certs
	"REQUESTS_CA_BUNDLE",  // Python requests
	"NODE_EXTRA_CA_CERTS", // Node.js (does not read SSL_CERT_FILE)
	"AWS_CA_BUNDLE",       // AWS CLI + all AWS SDKs
	"GIT_SSL_CAINFO",      // git CLI (spark-submit --packages, pip install git+https)
	"CURL_CA_BUNDLE",      // curl on RHEL/UBI images
}

const (
	pemBundlePath     = "/etc/ssl/certs/jaiscloud-ca-bundle.pem"
	pemScratchVolName = "jaiscloud-pem-scratch"
	pemScratchMount   = "/tmp/jaiscloud-pem-build"
	pemInitImage      = "busybox:1.36"
)

type pemBundleMaterializer struct{}

func (pemBundleMaterializer) Name() string { return "pem-bundle" }

func (pemBundleMaterializer) InitContainer(cfg *TLSConfig, _ string) *k8stypes.Container {
	// Build a shell command that cat-concatenates each CA file into the bundle.
	cmd := "set -e; "
	for _, ca := range cfg.CASources {
		mountPath := pemCAMountPath(ca.Name)
		cmd += "cat " + mountPath + "/" + ca.Source.Key + " >> " + pemBundlePath + "; "
	}

	return &k8stypes.Container{
		Name:    "pem-bundle-init",
		Image:   pemInitImage,
		Command: []string{"sh", "-c", cmd},
		VolumeMounts: append(
			caMountSpecs(cfg.CASources),
			k8stypes.VolumeMount{
				Name:      pemScratchVolName,
				MountPath: "/etc/ssl/certs",
			},
		),
	}
}

func (pemBundleMaterializer) ContainerEnv(cfg *TLSConfig) []k8stypes.EnvVar {
	env := make([]k8stypes.EnvVar, 0, len(nonJVMEnvVars))
	for _, name := range nonJVMEnvVars {
		env = append(env, k8stypes.EnvVar{Name: name, Value: pemBundlePath})
	}
	return env
}

func (pemBundleMaterializer) Volumes() []k8stypes.Volume {
	return []k8stypes.Volume{{
		Name:     pemScratchVolName,
		EmptyDir: &k8stypes.EmptyDirVol{},
	}}
}

func (pemBundleMaterializer) VolumeMounts() []k8stypes.VolumeMount {
	return []k8stypes.VolumeMount{{
		Name:      pemScratchVolName,
		MountPath: "/etc/ssl/certs",
	}}
}

// pemCAMountPath returns the directory path inside the init container where a
// CA source is mounted.
func pemCAMountPath(alias string) string {
	return "/tmp/jaiscloud-ca/" + alias
}
