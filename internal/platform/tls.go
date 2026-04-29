package platform

import "jaiscloud/internal/k8stypes"

// TLSConfig holds generic TLS trust configuration. When Enabled=true both
// jvm-truststore and pem-bundle materializers always run.
type TLSConfig struct {
	Enabled             bool
	CASources           []CASource
	ClientCert          *CASource
	TruststorePassword  string // default "changeit"; embedded in JAVA_TOOL_OPTIONS only
}

// CASource references a CA certificate stored in a K8s Secret or ConfigMap.
type CASource struct {
	Name   string    // unique alias used as keytool alias and label
	Source CASourceRef
}

// CASourceRef discriminates between a ConfigMap and a Secret source.
type CASourceRef struct {
	Kind string // "configMap" | "secret"
	Name string
	Key  string
}

// TLSMaterializer converts TLSConfig into pod fragments for a specific runtime format.
type TLSMaterializer interface {
	Name() string
	InitContainer(cfg *TLSConfig, image string) *k8stypes.Container
	ContainerEnv(cfg *TLSConfig) []k8stypes.EnvVar
	Volumes() []k8stypes.Volume
	VolumeMounts() []k8stypes.VolumeMount
}

// materializers lists the materializers applied when TLS.Enabled=true.
// Order: jvm-truststore first, pem-bundle second — both always run.
var materializers = []TLSMaterializer{
	jvmTruststoreMaterializer{},
	pemBundleMaterializer{},
}
