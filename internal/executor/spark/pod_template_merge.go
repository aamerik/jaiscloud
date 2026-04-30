package spark

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/platform"
)

// MergeDriver applies a driver pod template onto the base spec JaisCloud built.
// Called after cloud transform contributions, before StripSchedulingFields and
// platform.ApplyK8s. Returns an error on hard conflicts.
//
// Soft conflicts (env name collision) are first-wins — JaisCloud's value kept.
// Zero-container templates are a no-op.
func MergeDriver(base *k8stypes.PodSpec, sparkCtr *k8stypes.Container, tmpl *k8stypes.PodSpec) error {
	if tmpl == nil || len(tmpl.Containers) == 0 {
		// Rule 1 — zero-container template: no-op
		return nil
	}
	tc := tmpl.Containers[0]

	// Rule 2a — image: JaisCloud wins (template value ignored)
	// Rule 2b — command: JaisCloud wins
	// Rule 2c — args: JaisCloud wins
	// (no action needed — sparkCtr already has the correct values)

	// Rule 2d — env: first-wins; append template entries not already present.
	// Preserve ValueFrom entries byte-identically.
	existing := make(map[string]struct{}, len(sparkCtr.Env))
	for _, e := range sparkCtr.Env {
		existing[e.Name] = struct{}{}
	}
	for _, e := range tc.Env {
		if _, ok := existing[e.Name]; !ok {
			sparkCtr.Env = append(sparkCtr.Env, e)
		}
	}

	// Rule 2e — volumeMounts: check for path collisions before appending.
	if err := platform.CheckMountPathConflicts(sparkCtr.VolumeMounts, tc.VolumeMounts); err != nil {
		return fmt.Errorf("driver template mount conflict: %w", err)
	}
	sparkCtr.VolumeMounts = append(sparkCtr.VolumeMounts, tc.VolumeMounts...)

	// Rule 2f — resources: template wins only if sparkCtr.Resources is nil.
	if sparkCtr.Resources == nil && tc.Resources != nil {
		sparkCtr.Resources = tc.Resources
	}

	// Rule 2g — container-level securityContext: template wins only if sparkCtr's is nil.
	if sparkCtr.SecurityContext == nil && tc.SecurityContext != nil {
		sparkCtr.SecurityContext = tc.SecurityContext
	}

	// Rule 2h — readinessProbe / livenessProbe: template wins unconditionally.
	if tc.ReadinessProbe != nil {
		sparkCtr.ReadinessProbe = tc.ReadinessProbe
	}
	if tc.LivenessProbe != nil {
		sparkCtr.LivenessProbe = tc.LivenessProbe
	}

	// Rule 3 — additional containers (index >= 1): append as sidecars.
	if len(tmpl.Containers) > 1 {
		base.Containers = append(base.Containers, tmpl.Containers[1:]...)
	}

	// Rule 4 — initContainers: prepend template's (run before cloud-transform inits).
	if len(tmpl.InitContainers) > 0 {
		base.InitContainers = append(tmpl.InitContainers, base.InitContainers...)
	}

	// Rule 5 — volumes: check name collisions before appending.
	if err := platform.CheckVolumeConflicts(base.Volumes, tmpl.Volumes); err != nil {
		return fmt.Errorf("driver template volume conflict: %w", err)
	}
	base.Volumes = append(base.Volumes, tmpl.Volumes...)

	// Rule 6 — serviceAccountName: template wins if non-empty.
	if tmpl.ServiceAccountName != "" {
		base.ServiceAccountName = tmpl.ServiceAccountName
	}

	// Rule 7 — NodeSelector/Tolerations/Affinity/TopologySpreadConstraints: template wins if set.
	// (StripSchedulingFields is called afterwards if cfg.StripScheduling is true.)
	if len(tmpl.NodeSelector) > 0 {
		base.NodeSelector = tmpl.NodeSelector
	}
	if len(tmpl.Tolerations) > 0 {
		base.Tolerations = tmpl.Tolerations
	}
	if tmpl.Affinity != nil {
		base.Affinity = tmpl.Affinity
	}
	if len(tmpl.TopologySpreadConstraints) > 0 {
		base.TopologySpreadConstraints = tmpl.TopologySpreadConstraints
	}

	// Rule 8 — pod-level SecurityContext: template wins if non-nil.
	if tmpl.SecurityContext != nil {
		base.SecurityContext = tmpl.SecurityContext
	}

	// Rule 9 — restartPolicy: force "Never".
	if tmpl.RestartPolicy != "" && tmpl.RestartPolicy != "Never" {
		slog.Info("spark: driver template restartPolicy overridden to Never",
			"template_value", tmpl.RestartPolicy)
	}
	base.RestartPolicy = "Never"

	return nil
}

// MergeExecutor applies an executor pod template. JaisCloud does not launch
// executor pods directly — Spark's K8s scheduler does — so no base container
// exists. Returns a copy of the template with RestartPolicy forced to "Never".
// tmpl nil → (nil, nil).
func MergeExecutor(tmpl *k8stypes.PodSpec) (*k8stypes.PodSpec, error) {
	if tmpl == nil {
		return nil, nil
	}
	cp := *tmpl
	if cp.RestartPolicy != "" && cp.RestartPolicy != "Never" {
		slog.Info("spark: executor template restartPolicy overridden to Never",
			"template_value", cp.RestartPolicy)
	}
	cp.RestartPolicy = "Never"
	return &cp, nil
}

// StripSchedulingFields zeroes NodeSelector, Tolerations, Affinity, and
// TopologySpreadConstraints. Called unconditionally when cfg.StripScheduling is true.
func StripSchedulingFields(spec *k8stypes.PodSpec) {
	spec.NodeSelector = nil
	spec.Tolerations = nil
	spec.Affinity = nil
	spec.TopologySpreadConstraints = nil
}

// ApplyResourceProfile enforces min(templateValue, profileValue) for CPU and memory
// on the spark container. When the template specifies no resources, the profile's
// values are applied as defaults. Writes back to both Requests and Limits.
//
// Appends --conf spark.{driver,executor}.{cores,memory} to args so Spark's
// internal accounting matches the pod resource requests. When args is nil
// (executor-side call), the conf append is skipped.
//
// profile is the ceiling; caller passes cfg.Resources.
func ApplyResourceProfile(
	spec *k8stypes.PodSpec,
	sparkCtr *k8stypes.Container,
	args []string,
	profile ResourceProfile,
) []string {
	if sparkCtr == nil {
		return args
	}

	// Driver-side when args != nil; executor-side when args == nil.
	cpuStr, memStr := profile.DriverCPU, profile.DriverMemory
	if args == nil {
		cpuStr, memStr = profile.ExecutorCPU, profile.ExecutorMemory
	}
	profileCPU, err := parseCPU(cpuStr)
	if err != nil || profileCPU <= 0 {
		profileCPU = 1000 // 1 core default
	}
	profileMem, err := parseMemory(memStr)
	if err != nil || profileMem <= 0 {
		profileMem = 1 << 30 // 1 GiB default
	}

	if sparkCtr.Resources == nil {
		sparkCtr.Resources = &k8stypes.Resources{}
	}
	if sparkCtr.Resources.Requests == nil {
		sparkCtr.Resources.Requests = map[string]string{}
	}
	if sparkCtr.Resources.Limits == nil {
		sparkCtr.Resources.Limits = map[string]string{}
	}

	// CPU
	appliedCPUm := profileCPU
	if raw, ok := sparkCtr.Resources.Requests["cpu"]; ok {
		if tmplCPU, err := parseCPU(raw); err == nil && tmplCPU < profileCPU {
			appliedCPUm = tmplCPU
		}
	}
	appliedCPUStr := millicoresToK8s(appliedCPUm)
	sparkCtr.Resources.Requests["cpu"] = appliedCPUStr
	if _, hasLimit := sparkCtr.Resources.Limits["cpu"]; !hasLimit {
		sparkCtr.Resources.Limits["cpu"] = appliedCPUStr
	}

	// Memory
	appliedMemB := profileMem
	if raw, ok := sparkCtr.Resources.Requests["memory"]; ok {
		if tmplMem, err := parseMemory(raw); err == nil && tmplMem < profileMem {
			appliedMemB = tmplMem
		}
	}
	appliedMemStr := bytesToK8sMemory(appliedMemB)
	sparkCtr.Resources.Requests["memory"] = appliedMemStr
	if _, hasLimit := sparkCtr.Resources.Limits["memory"]; !hasLimit {
		sparkCtr.Resources.Limits["memory"] = appliedMemStr
	}

	// Append Spark conf args so Spark's internal accounting matches pod resources.
	if args != nil {
		cores := strconv.FormatInt(appliedCPUm/1000, 10)
		if appliedCPUm%1000 != 0 {
			cores = fmt.Sprintf("%.1f", float64(appliedCPUm)/1000)
		}
		memMi := appliedMemB / (1024 * 1024)
		args = setOrAppendConf(args, "spark.driver.cores", cores)
		args = setOrAppendConf(args, "spark.driver.memory", fmt.Sprintf("%dm", memMi))
		args = setOrAppendConf(args, "spark.executor.cores", cores)
		args = setOrAppendConf(args, "spark.executor.memory", fmt.Sprintf("%dm", memMi))
	}

	return args
}

// parseCPU converts a K8s CPU quantity string to millicores.
// "1" → 1000, "500m" → 500, "1.5" → 1500.
func parseCPU(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty CPU value")
	}
	if strings.HasSuffix(s, "m") {
		v, err := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid millicore CPU %q: %w", s, err)
		}
		return v, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid CPU %q: %w", s, err)
	}
	return int64(f * 1000), nil
}

// parseMemory converts a K8s or Spark memory quantity to bytes.
//
//   - IEC suffix (Gi, Mi, Ki, Ti, Pi, Ei) → binary powers of 2 (K8s IEC)
//   - Lowercase suffix without 'i' (g, m, k, t) → binary (Spark convention; "1g"==1GiB)
//   - Uppercase suffix without 'i' (G, M, K, T) → REJECTED as ambiguous
//   - Raw integer, no suffix → bytes
func parseMemory(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty memory value")
	}

	// IEC suffixes: Ki Mi Gi Ti Pi Ei
	iecUnits := map[string]int64{
		"Ki": 1024,
		"Mi": 1024 * 1024,
		"Gi": 1024 * 1024 * 1024,
		"Ti": 1024 * 1024 * 1024 * 1024,
		"Pi": 1024 * 1024 * 1024 * 1024 * 1024,
		"Ei": 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
	}
	for suffix, mult := range iecUnits {
		if strings.HasSuffix(s, suffix) {
			v, err := strconv.ParseInt(strings.TrimSuffix(s, suffix), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid memory %q: %w", s, err)
			}
			return v * mult, nil
		}
	}

	// Spark-convention lowercase suffixes (binary, same as IEC)
	sparkUnits := map[string]int64{
		"g": 1024 * 1024 * 1024,
		"m": 1024 * 1024,
		"k": 1024,
		"t": 1024 * 1024 * 1024 * 1024,
	}
	lower := strings.ToLower(s)
	for suffix, mult := range sparkUnits {
		if strings.HasSuffix(lower, suffix) && lower == s { // only if already lowercase
			v, err := strconv.ParseInt(strings.TrimSuffix(s, suffix), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid memory %q: %w", s, err)
			}
			return v * mult, nil
		}
	}

	// Reject ambiguous uppercase SI suffixes (G, M, K, T but not Gi/Mi/Ki/Ti)
	for _, r := range []string{"G", "M", "K", "T", "P", "E"} {
		if strings.HasSuffix(s, r) {
			return 0, fmt.Errorf("ambiguous memory suffix %q in %q — use IEC notation (Gi, Mi) or Spark lowercase (g, m)", r, s)
		}
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory %q: %w", s, err)
	}
	return v, nil
}

// millicoresToK8s converts millicores to a K8s CPU string ("1000m" or "1" for full cores).
func millicoresToK8s(m int64) string {
	if m%1000 == 0 {
		return strconv.FormatInt(m/1000, 10)
	}
	return strconv.FormatInt(m, 10) + "m"
}

// bytesToK8sMemory converts bytes to a K8s memory string using Mi suffix.
func bytesToK8sMemory(b int64) string {
	mi := b / (1024 * 1024)
	return fmt.Sprintf("%dMi", mi)
}

// setOrAppendConf sets the value of a --conf key=value entry in args.
// If the key already exists (two-token or single-token form), it is updated in place.
// If not found, "--conf", "key=value" is appended.
func setOrAppendConf(args []string, key, value string) []string {
	entry := key + "=" + value
	for i, a := range args {
		if a == "--conf" && i+1 < len(args) {
			if strings.HasPrefix(args[i+1], key+"=") {
				args[i+1] = entry
				return args
			}
		}
		if strings.HasPrefix(a, "--conf="+key+"=") {
			args[i] = "--conf=" + entry
			return args
		}
	}
	return append(args, "--conf", entry)
}

// appendIfAbsent appends env entries from extra whose Name is not already
// present in existing. First-wins: the existing entry is kept on collision.
func appendIfAbsent(existing, extra []k8stypes.EnvVar) []k8stypes.EnvVar {
	seen := make(map[string]struct{}, len(existing))
	for _, e := range existing {
		seen[e.Name] = struct{}{}
	}
	for _, e := range extra {
		if _, ok := seen[e.Name]; !ok {
			existing = append(existing, e)
		}
	}
	return existing
}
