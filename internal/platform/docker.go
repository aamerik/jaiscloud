package platform

import (
	"fmt"
	"log/slog"
)

// ApplyDocker returns additional -v and -e args for `docker run` based on
// platform config. TLS is handled by materialising the PEM bundle to a host
// temp file once at startup and bind-mounting it read-only into every container.
//
// Note: only CA sources with kind="file" can be materialised on the host.
// K8s ConfigMap/Secret sources are silently skipped — they are K8s-only.
// Validate that Docker-mode deployments use kind="file" CA sources.
func ApplyDocker(cfg *PlatformConfig) (volumeArgs, envArgs []string, err error) {
	if cfg == nil || !cfg.TLS.Enabled {
		return nil, nil, nil
	}

	bundlePath, err := cfg.materializePEMBundle()
	if err != nil {
		return nil, nil, fmt.Errorf("platform docker: materialize PEM bundle: %w", err)
	}

	if bundlePath != "" {
		volumeArgs = append(volumeArgs,
			"-v", bundlePath+":/etc/ssl/certs/jaiscloud-ca.pem:ro",
		)
		for _, name := range nonJVMEnvVars {
			envArgs = append(envArgs, "-e", name+"=/etc/ssl/certs/jaiscloud-ca.pem")
		}
	}

	// Extra volumes.
	for _, vs := range cfg.Volumes {
		for _, mnt := range vs.Mounts {
			// Only hostPath volumes can be bind-mounted in Docker mode.
			if vs.Source.Kind == "hostPath" && vs.Source.HostPath != nil {
				volumeArgs = append(volumeArgs, "-v",
					fmt.Sprintf("%s:%s:ro", vs.Source.HostPath.Path, mnt.MountPath),
				)
			} else {
				slog.Warn("platform docker: volume source cannot be bind-mounted, skipping",
					"kind", vs.Source.Kind, "mountPath", mnt.MountPath)
			}
		}
	}

	// Extra env.
	for k, v := range cfg.Env {
		envArgs = append(envArgs, "-e", k+"="+v)
	}

	return volumeArgs, envArgs, nil
}
