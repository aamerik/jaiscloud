package platform

import (
	"fmt"
	"strings"

	"jaiscloud/internal/k8stypes"
)

const (
	truststoreVolName = "jaiscloud-truststore"
	truststoreMount   = "/tmp/truststore"
	truststorePath    = "/tmp/truststore/cacerts"
	clientP12Path     = "/tmp/truststore/client.p12"
	jvmInitImage      = "eclipse-temurin:21-jre"
)

type jvmTruststoreMaterializer struct{}

func (jvmTruststoreMaterializer) Name() string { return "jvm-truststore" }

func (jvmTruststoreMaterializer) InitContainer(cfg *TLSConfig, _ string) *k8stypes.Container {
	password := cfg.TruststorePassword
	if password == "" {
		password = "changeit"
	}

	// Step 1: copy the default cacerts from the JRE.
	script := fmt.Sprintf("set -e; cp $JAVA_HOME/lib/security/cacerts %s; ", truststorePath)

	// Step 2: import each CA.
	for _, ca := range cfg.CASources {
		mountPath := pemCAMountPath(ca.Name)
		script += fmt.Sprintf(
			"keytool -importcert -noprompt -trustcacerts -alias %s -file %s/%s -keystore %s -storepass %s; ",
			ca.Name, mountPath, ca.Source.Key, truststorePath, password,
		)
	}

	// Step 3: convert client cert to PKCS12 if provided.
	if cfg.ClientCert != nil {
		mountPath := pemCAMountPath(cfg.ClientCert.Name)
		script += fmt.Sprintf(
			"keytool -importkeystore -noprompt -srckeystore %s/%s -srcstoretype PKCS12 -destkeystore %s -deststoretype PKCS12 -deststorepass %s; ",
			mountPath, cfg.ClientCert.Source.Key, clientP12Path, password,
		)
	}

	mounts := append(
		caMountSpecs(cfg.CASources),
		k8stypes.VolumeMount{Name: truststoreVolName, MountPath: truststoreMount},
	)
	if cfg.ClientCert != nil {
		mounts = append(mounts, k8stypes.VolumeMount{
			Name:      caVolumeName(cfg.ClientCert.Name),
			MountPath: pemCAMountPath(cfg.ClientCert.Name),
			ReadOnly:  true,
		})
	}

	return &k8stypes.Container{
		Name:         "jvm-truststore-init",
		Image:        jvmInitImage,
		Command:      []string{"sh", "-c", script},
		VolumeMounts: mounts,
	}
}

func (jvmTruststoreMaterializer) ContainerEnv(cfg *TLSConfig) []k8stypes.EnvVar {
	password := cfg.TruststorePassword
	if password == "" {
		password = "changeit"
	}

	jvmOpts := fmt.Sprintf(
		"-Djavax.net.ssl.trustStore=%s -Djavax.net.ssl.trustStorePassword=%s",
		truststorePath, password,
	)
	if cfg.ClientCert != nil {
		jvmOpts += fmt.Sprintf(
			" -Djavax.net.ssl.keyStore=%s -Djavax.net.ssl.keyStoreType=PKCS12 -Djavax.net.ssl.keyStorePassword=%s",
			clientP12Path, password,
		)
	}

	return []k8stypes.EnvVar{{
		Name:  "JAVA_TOOL_OPTIONS",
		Value: jvmOpts,
	}}
}

func (jvmTruststoreMaterializer) Volumes() []k8stypes.Volume {
	return []k8stypes.Volume{{
		Name:     truststoreVolName,
		EmptyDir: &k8stypes.EmptyDirVol{},
	}}
}

func (jvmTruststoreMaterializer) VolumeMounts() []k8stypes.VolumeMount {
	return []k8stypes.VolumeMount{{
		Name:      truststoreVolName,
		MountPath: truststoreMount,
	}}
}

// ── shared helpers ─────────────────────────────────────────────────────────

// caVolumeName returns a deterministic K8s volume name for a CA source alias.
func caVolumeName(alias string) string {
	return "jaiscloud-ca-" + strings.ToLower(strings.ReplaceAll(alias, "_", "-"))
}

// caMountSpecs returns volumeMount entries for each CASource, mounting them
// read-only into the init container.
func caMountSpecs(sources []CASource) []k8stypes.VolumeMount {
	mounts := make([]k8stypes.VolumeMount, len(sources))
	for i, ca := range sources {
		mounts[i] = k8stypes.VolumeMount{
			Name:      caVolumeName(ca.Name),
			MountPath: pemCAMountPath(ca.Name),
			ReadOnly:  true,
		}
	}
	return mounts
}

// caVolumes returns volume definitions for each CASource (ConfigMap or Secret).
func caVolumes(sources []CASource) []k8stypes.Volume {
	vols := make([]k8stypes.Volume, len(sources))
	for i, ca := range sources {
		vol := k8stypes.Volume{Name: caVolumeName(ca.Name)}
		switch ca.Source.Kind {
		case "configMap":
			vol.ConfigMap = &k8stypes.ConfigMapVol{Name: ca.Source.Name}
		case "secret":
			vol.Secret = &k8stypes.SecretVol{SecretName: ca.Source.Name}
		}
		vols[i] = vol
	}
	return vols
}
