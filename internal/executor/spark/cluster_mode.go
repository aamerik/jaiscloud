package spark

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// ErrIncompatibleMasterInClusterMode is returned when a --master value is
// incompatible with Spark's K8s cluster-deploy mode. We whitelist compatible
// values rather than blacklisting known bad ones to prevent obscure driver-pod
// crashes at runtime.
var ErrIncompatibleMasterInClusterMode = errors.New(
	"--master value incompatible with Spark K8s cluster mode; allowed: k8s://..., local[*], local[N]",
)

// resolveMasterArgs preserves spark-submit master/deploy-mode args when cluster
// mode is active and the master is a compatible value. Otherwise rewrites to local[*].
//
// Cluster mode is preserved when:
//   - job.AllowClusterMode == true AND
//   - job.Config.Mode == "k8s" (cluster mode requires K8s executor) AND
//   - the --master arg is in the whitelist (k8s://..., local[*], local[N])
//
// Docker and mock executors always fall back to local[*] regardless of AllowClusterMode.
func resolveMasterArgs(job SparkJob, args []string) ([]string, error) {
	if !job.AllowClusterMode || job.Config.Mode != "k8s" {
		return rewriteSparkMaster(args), nil
	}
	master, ok := findMasterArg(args)
	if !ok {
		// No --master specified. Log a warning — in cluster deploy-mode Spark
		// needs an explicit k8s://... master or it may silently use the wrong one.
		slog.Warn("spark: cluster mode active but no --master arg found — " +
			"Spark will use its default master which may not be correct; " +
			"add --master k8s://<api-server> to sparkSubmitParameters")
		return args, nil
	}
	if !isClusterCompatibleMaster(master) {
		return nil, fmt.Errorf("%w: got %q", ErrIncompatibleMasterInClusterMode, master)
	}
	return args, nil
}

// findMasterArg returns the --master value, supporting both "--master X"
// (two-token) and "--master=X" (single-token) forms.
// Second return is false if --master is absent.
func findMasterArg(args []string) (string, bool) {
	for i, a := range args {
		if strings.HasPrefix(a, "--master=") {
			return strings.TrimPrefix(a, "--master="), true
		}
		if a == "--master" && i+1 < len(args) {
			return args[i+1], true
		}
	}
	return "", false
}

// isClusterCompatibleMaster returns true if the master value is compatible with
// Spark K8s cluster mode:
//   - k8s://... (native K8s cluster mode)
//   - local[*] / local[N] / local (same-JVM fallback)
func isClusterCompatibleMaster(master string) bool {
	if strings.HasPrefix(master, "k8s://") {
		return true
	}
	if master == "local" || master == "local[*]" {
		return true
	}
	if strings.HasPrefix(master, "local[") && strings.HasSuffix(master, "]") {
		return true
	}
	return false
}

// stripTemplateFileConfs removes any --conf entries for
// spark.kubernetes.{driver,executor}.podTemplateFile from args.
// Handles both "--conf k=v" (two-token) and "--conf=k=v" (single-token) forms.
//
// Called when cluster mode is not active so Spark in local mode does not attempt
// to download templates and fail.
func stripTemplateFileConfs(args []string) []string {
	const driverPrefix = "spark.kubernetes.driver.podTemplateFile="
	const execPrefix = "spark.kubernetes.executor.podTemplateFile="

	out := make([]string, 0, len(args))
	skipNext := false
	for i := 0; i < len(args); i++ {
		if skipNext {
			skipNext = false
			continue
		}
		a := args[i]
		// Single-token form: --conf=spark.kubernetes.*.podTemplateFile=...
		if strings.HasPrefix(a, "--conf=") {
			v := strings.TrimPrefix(a, "--conf=")
			if strings.HasPrefix(v, driverPrefix) || strings.HasPrefix(v, execPrefix) {
				continue
			}
		}
		// Two-token form: --conf spark.kubernetes.*.podTemplateFile=...
		if a == "--conf" && i+1 < len(args) {
			v := args[i+1]
			if strings.HasPrefix(v, driverPrefix) || strings.HasPrefix(v, execPrefix) {
				skipNext = true
				continue
			}
		}
		out = append(out, a)
	}
	return out
}
