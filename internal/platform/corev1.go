package platform

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// PlatformFragmentsCorev1 returns the pod fragments that every JaisCloud-managed
// workload must carry, expressed as official k8s.io/api corev1 types.
//
// Callers (k8shelpers.BuildPodSpec) merge these fragments with the caller-
// supplied pod template per the documented merge rules. The fragments are NOT
// pre-merged — the caller owns merge ordering.
//
// Returns nil slices when cfg is nil or TLS is disabled (nothing to inject).
func PlatformFragmentsCorev1(cfg *PlatformConfig) (
	initContainers []corev1.Container,
	volumes []corev1.Volume,
	mainEnv []corev1.EnvVar,
	mainMounts []corev1.VolumeMount,
) {
	if cfg == nil {
		return
	}

	tls := &cfg.TLS

	if tls.Enabled && len(tls.CASources) > 0 {
		// CA source volumes (configMap or secret per source).
		for _, ca := range tls.CASources {
			volumes = append(volumes, caVolumeCorev1(ca))
		}
		if tls.ClientCert != nil {
			volumes = append(volumes, caVolumeCorev1(*tls.ClientCert))
		}

		// JVM truststore materializer.
		jvmInit, jvmVols, jvmEnv, jvmMounts := jvmTruststoreCorev1(tls)
		initContainers = append(initContainers, jvmInit)
		volumes = append(volumes, jvmVols...)
		mainEnv = append(mainEnv, jvmEnv...)
		mainMounts = append(mainMounts, jvmMounts...)

		// PEM bundle materializer.
		pemInit, pemVols, pemEnv, pemMounts := pemBundleCorev1(tls)
		initContainers = append(initContainers, pemInit)
		volumes = append(volumes, pemVols...)
		mainEnv = append(mainEnv, pemEnv...)
		mainMounts = append(mainMounts, pemMounts...)
	}

	// Extra platform volumes.
	for _, vs := range cfg.Volumes {
		volumes = append(volumes, platformVolumeCorev1(vs))
		for _, mnt := range vs.Mounts {
			mainMounts = append(mainMounts, corev1.VolumeMount{
				Name:      vs.Name,
				MountPath: mnt.MountPath,
				SubPath:   mnt.SubPath,
				ReadOnly:  mnt.ReadOnly,
			})
		}
	}

	// Extra platform env (sorted for deterministic output).
	keys := make([]string, 0, len(cfg.Env))
	for k := range cfg.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		mainEnv = append(mainEnv, corev1.EnvVar{Name: k, Value: cfg.Env[k]})
	}

	return
}

// caVolumeCorev1 creates a corev1.Volume for a CASource.
func caVolumeCorev1(ca CASource) corev1.Volume {
	name := "jaiscloud-ca-" + strings.ToLower(strings.ReplaceAll(ca.Name, "_", "-"))
	vol := corev1.Volume{Name: name}
	switch ca.Source.Kind {
	case "configMap":
		vol.VolumeSource = corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: ca.Source.Name},
			},
		}
	case "secret":
		vol.VolumeSource = corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: ca.Source.Name},
		}
	}
	return vol
}

// jvmTruststoreCorev1 returns the corev1 fragments for the JVM truststore materializer.
func jvmTruststoreCorev1(cfg *TLSConfig) (init corev1.Container, vols []corev1.Volume, env []corev1.EnvVar, mounts []corev1.VolumeMount) {
	const (
		volName        = "jaiscloud-truststore"
		mountPath      = "/tmp/truststore"
		truststorePath = "/tmp/truststore/cacerts"
		clientP12Path  = "/tmp/truststore/client.p12"
		initImage      = "eclipse-temurin:21-jre"
	)

	password := cfg.TruststorePassword
	if password == "" {
		password = "changeit"
	}

	script := fmt.Sprintf("set -e; cp $JAVA_HOME/lib/security/cacerts %s; ", truststorePath)
	for _, ca := range cfg.CASources {
		mp := "/tmp/jaiscloud-ca/" + ca.Name
		caVol := "jaiscloud-ca-" + strings.ToLower(strings.ReplaceAll(ca.Name, "_", "-"))
		script += fmt.Sprintf(
			"keytool -importcert -noprompt -trustcacerts -alias %s -file %s/%s -keystore %s -storepass %s; ",
			ca.Name, mp, ca.Source.Key, truststorePath, password,
		)
		_ = caVol
	}
	if cfg.ClientCert != nil {
		mp := "/tmp/jaiscloud-ca/" + cfg.ClientCert.Name
		script += fmt.Sprintf(
			"keytool -importkeystore -noprompt -srckeystore %s/%s -srcstoretype PKCS12 -destkeystore %s -deststoretype PKCS12 -deststorepass %s; ",
			mp, cfg.ClientCert.Source.Key, clientP12Path, password,
		)
	}

	// Mounts for the init container: CA sources + truststore scratch.
	var initMounts []corev1.VolumeMount
	for _, ca := range cfg.CASources {
		caVolName := "jaiscloud-ca-" + strings.ToLower(strings.ReplaceAll(ca.Name, "_", "-"))
		initMounts = append(initMounts, corev1.VolumeMount{
			Name:      caVolName,
			MountPath: "/tmp/jaiscloud-ca/" + ca.Name,
			ReadOnly:  true,
		})
	}
	initMounts = append(initMounts, corev1.VolumeMount{Name: volName, MountPath: mountPath})
	if cfg.ClientCert != nil {
		caVolName := "jaiscloud-ca-" + strings.ToLower(strings.ReplaceAll(cfg.ClientCert.Name, "_", "-"))
		initMounts = append(initMounts, corev1.VolumeMount{
			Name:      caVolName,
			MountPath: "/tmp/jaiscloud-ca/" + cfg.ClientCert.Name,
			ReadOnly:  true,
		})
	}

	init = corev1.Container{
		Name:         "jvm-truststore-init",
		Image:        initImage,
		Command:      []string{"sh", "-c", script},
		VolumeMounts: initMounts,
	}

	vols = []corev1.Volume{{
		Name:         volName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}

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
	env = []corev1.EnvVar{{Name: "JAVA_TOOL_OPTIONS", Value: jvmOpts}}

	mounts = []corev1.VolumeMount{{Name: volName, MountPath: mountPath}}
	return
}

// pemBundleCorev1 returns the corev1 fragments for the PEM bundle materializer.
func pemBundleCorev1(cfg *TLSConfig) (init corev1.Container, vols []corev1.Volume, env []corev1.EnvVar, mounts []corev1.VolumeMount) {
	const (
		pemBundlePath  = "/etc/ssl/certs/jaiscloud-ca-bundle.pem"
		scratchVolName = "jaiscloud-pem-scratch"
		initImage      = "busybox:1.36"
	)

	cmd := "set -e; "
	for _, ca := range cfg.CASources {
		mp := "/tmp/jaiscloud-ca/" + ca.Name
		cmd += "cat " + mp + "/" + ca.Source.Key + " >> " + pemBundlePath + "; "
	}

	var initMounts []corev1.VolumeMount
	for _, ca := range cfg.CASources {
		caVolName := "jaiscloud-ca-" + strings.ToLower(strings.ReplaceAll(ca.Name, "_", "-"))
		initMounts = append(initMounts, corev1.VolumeMount{
			Name:      caVolName,
			MountPath: "/tmp/jaiscloud-ca/" + ca.Name,
			ReadOnly:  true,
		})
	}
	initMounts = append(initMounts, corev1.VolumeMount{Name: scratchVolName, MountPath: "/etc/ssl/certs"})

	init = corev1.Container{
		Name:         "pem-bundle-init",
		Image:        initImage,
		Command:      []string{"sh", "-c", cmd},
		VolumeMounts: initMounts,
	}

	vols = []corev1.Volume{{
		Name:         scratchVolName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}

	nonJVMVars := []string{
		"SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "NODE_EXTRA_CA_CERTS",
		"AWS_CA_BUNDLE", "GIT_SSL_CAINFO", "CURL_CA_BUNDLE",
	}
	for _, name := range nonJVMVars {
		env = append(env, corev1.EnvVar{Name: name, Value: pemBundlePath})
	}

	mounts = []corev1.VolumeMount{{Name: scratchVolName, MountPath: "/etc/ssl/certs"}}
	return
}

// platformVolumeCorev1 converts a VolumeSpec to a corev1.Volume.
func platformVolumeCorev1(vs VolumeSpec) corev1.Volume {
	vol := corev1.Volume{Name: vs.Name}
	src := vs.Source
	switch src.Kind {
	case "configMap":
		if src.ConfigMap != nil {
			vol.VolumeSource = corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: src.ConfigMap.Name},
				},
			}
		}
	case "secret":
		if src.Secret != nil {
			vol.VolumeSource = corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: src.Secret.Name},
			}
		}
	case "emptyDir":
		vol.VolumeSource = corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	case "pvc":
		if src.PVC != nil {
			vol.VolumeSource = corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: src.PVC.ClaimName,
					ReadOnly:  src.PVC.ReadOnly,
				},
			}
		}
	case "hostPath":
		if src.HostPath != nil {
			ht := corev1.HostPathType(src.HostPath.Type)
			vol.VolumeSource = corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: src.HostPath.Path,
					Type: &ht,
				},
			}
		}
	case "csi":
		if src.CSI != nil {
			readOnly := src.CSI.ReadOnly
			vol.VolumeSource = corev1.VolumeSource{
				CSI: &corev1.CSIVolumeSource{
					Driver:           src.CSI.Driver,
					ReadOnly:         &readOnly,
					FSType:           &src.CSI.FSType,
					VolumeAttributes: src.CSI.VolumeAttributes,
				},
			}
		}
	}
	return vol
}
