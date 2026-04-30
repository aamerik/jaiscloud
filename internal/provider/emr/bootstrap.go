package emr

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/executor/spark"
	"jaiscloud/internal/k8stypes"
)

// BootstrapConfig configures the bootstrap resolver.
type BootstrapConfig struct {
	// Image is the init container image used to run bootstrap scripts (e.g. amazon/aws-cli:2.18).
	Image string
	// MaxBytes is the maximum allowed size for a single bootstrap script.
	MaxBytes int64
	// Prefixes are the filesystem prefixes that bootstrap scripts may write to
	// (e.g. ["/etc/pki", "/home/hadoop"]). One emptyDir volume is created per prefix.
	Prefixes []string
}

// Resolve fetches each bootstrap script via fetcher and returns k8stypes fragments
// ready to inject into the spark-submit batch Job. Called by the EMR provider
// before calling SparkExecutor.Submit.
//
// Returns (nil, nil, nil, nil) when actions is empty.
func Resolve(
	ctx context.Context,
	fetcher blobfs.BlobFetcher,
	cfg BootstrapConfig,
	actions []BootstrapAction,
	sparkEnv []k8stypes.EnvVar,
) (initContainers []k8stypes.Container, volumes []k8stypes.Volume, mainMounts []k8stypes.VolumeMount, err error) {
	if len(actions) == 0 {
		return nil, nil, nil, nil
	}

	// One emptyDir volume per prefix, shared between init containers and the main container.
	for _, prefix := range cfg.Prefixes {
		volName := prefixVolumeName(prefix)
		volumes = append(volumes, k8stypes.Volume{
			Name:     volName,
			EmptyDir: &k8stypes.EmptyDirVol{},
		})
		mainMounts = append(mainMounts, k8stypes.VolumeMount{
			Name:      volName,
			MountPath: prefix,
		})
	}

	// One init container per bootstrap action.
	for _, action := range actions {
		data, fetchErr := fetcher.Fetch(ctx, action.S3Path)
		if fetchErr != nil {
			return nil, nil, nil, fmt.Errorf("bootstrap: fetch %q: %w", action.S3Path, fetchErr)
		}
		if cfg.MaxBytes > 0 && int64(len(data)) > cfg.MaxBytes {
			return nil, nil, nil, fmt.Errorf("bootstrap: script %q size %d exceeds limit %d",
				action.S3Path, len(data), cfg.MaxBytes)
		}

		patched := scrubHostCommands(data)
		encoded := base64.StdEncoding.EncodeToString(patched)

		// Build the shell invocation: decode base64 then pipe to sh with extra args.
		shellCmd := fmt.Sprintf("printf '%%s' %q | base64 -d | /bin/sh -s -- %s",
			encoded, shellQuoteArgs(action.Args))

		// Mount every prefix volume into this init container.
		mounts := make([]k8stypes.VolumeMount, 0, len(cfg.Prefixes))
		for _, prefix := range cfg.Prefixes {
			mounts = append(mounts, k8stypes.VolumeMount{
				Name:      prefixVolumeName(prefix),
				MountPath: prefix,
			})
		}

		zero := int64(0)
		ctr := k8stypes.Container{
			Name:    "bootstrap-" + sanitizeName(action.Name),
			Image:   cfg.Image,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{shellCmd},
			Env:     sparkEnv,
			SecurityContext: &k8stypes.SecurityContext{
				RunAsUser: &zero,
			},
			VolumeMounts: mounts,
		}
		initContainers = append(initContainers, ctr)
	}

	return initContainers, volumes, mainMounts, nil
}

// bootstrapSparkEnv builds the env vars injected into bootstrap init containers
// so scripts can call the JaisCloud S3 endpoint with the right credentials.
func bootstrapSparkEnv(cfg spark.SparkConfig) []k8stypes.EnvVar {
	var env []k8stypes.EnvVar
	if cfg.Region != "" {
		env = append(env,
			k8stypes.EnvVar{Name: "AWS_DEFAULT_REGION", Value: cfg.Region},
			k8stypes.EnvVar{Name: "AWS_REGION", Value: cfg.Region},
		)
	}
	if cfg.S3Endpoint != "" {
		env = append(env, k8stypes.EnvVar{Name: "AWS_ENDPOINT_URL", Value: cfg.S3Endpoint})
	}
	if cfg.AWSAccessKey != "" {
		env = append(env, k8stypes.EnvVar{Name: "AWS_ACCESS_KEY_ID", Value: cfg.AWSAccessKey})
	}
	if cfg.AWSSecretKey != "" {
		env = append(env, k8stypes.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", Value: cfg.AWSSecretKey})
	}
	return env
}

// hostCmdPrefixes lists shell command prefixes that are no-ops (or harmful) in
// a container context. Lines starting with any of these are commented out.
var hostCmdPrefixes = []string{
	"yum ", "yum\t",
	"apt-get ", "apt-get\t",
	"apt ", "apt\t",
	"dnf ", "dnf\t",
	"rpm ", "rpm\t",
	"systemctl ", "systemctl\t",
	"service ", "service\t",
	"chkconfig ", "chkconfig\t",
	"update-rc.d ", "update-rc.d\t",
}

// scrubHostCommands comments out lines that invoke host-only package managers
// or init-system commands. Returns the patched script bytes.
func scrubHostCommands(script []byte) []byte {
	var out strings.Builder
	scanner := bufio.NewScanner(strings.NewReader(string(script)))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimLeft(line, " \t")
		scrubbed := false
		for _, prefix := range hostCmdPrefixes {
			if strings.HasPrefix(trimmed, prefix) {
				out.WriteString("# [jaiscloud-skip] ")
				out.WriteString(line)
				out.WriteByte('\n')
				slog.Info("bootstrap: commented out host command", "line", line)
				scrubbed = true
				break
			}
		}
		if !scrubbed {
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}
	return []byte(out.String())
}

// prefixVolumeName converts a filesystem prefix to a valid K8s volume name.
// e.g. "/etc/pki" → "bootstrap-prefix-etc-pki"
func prefixVolumeName(prefix string) string {
	return "bootstrap-prefix-" + sanitizeName(prefix)
}

// sanitizeName converts an arbitrary string to a valid K8s DNS label segment.
func sanitizeName(s string) string {
	var b strings.Builder
	for _, ch := range strings.ToLower(s) {
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' {
			b.WriteRune(ch)
		} else {
			b.WriteByte('-')
		}
	}
	result := strings.Trim(b.String(), "-")
	// Collapse runs of dashes.
	for strings.Contains(result, "--") {
		result = strings.ReplaceAll(result, "--", "-")
	}
	if len(result) > 50 {
		result = strings.TrimRight(result[:50], "-")
	}
	return result
}

// shellQuoteArgs returns the args as a space-separated, single-quote-escaped string.
func shellQuoteArgs(args []string) string {
	if len(args) == 0 {
		return ""
	}
	quoted := make([]string, len(args))
	for i, a := range args {
		// Single-quote escaping: close quote, add escaped quote, reopen.
		escaped := strings.ReplaceAll(a, "'", "'\\''")
		quoted[i] = "'" + escaped + "'"
	}
	return strings.Join(quoted, " ")
}
