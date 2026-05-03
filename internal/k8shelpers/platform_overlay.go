package k8shelpers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"jaiscloud/internal/platform"
)

// PodSpecInput is the base spec a provider wants; BuildPodSpec layers
// platform overlay and (optionally) a caller-supplied pod template on top.
type PodSpecInput struct {
	MainContainer corev1.Container  // image, command, args, resources, env (caller-specific)
	Namespace     string
	Labels        map[string]string
	Annotations   map[string]string
}

// IdentityMutator wires cloud-specific identity into a pod template.
// Takes PodTemplateSpec so mutators can annotate metadata.annotations
// (IRSA, Azure MI, GCP WI) and set spec.ServiceAccountName.
type IdentityMutator func(ctx context.Context, k8s kubernetes.Interface, tpl *corev1.PodTemplateSpec) error

// BuildPodSpec assembles the final corev1.PodTemplateSpec by:
//  1. Starting from PodSpecInput (caller's main container + labels/annotations)
//  2. Merging platform overlay per the documented merge rules
//  3. If caller template bytes are supplied, parsing as YAML PodTemplate
//     and merging (caller fields win per the merge table)
//  4. Invoking identityMutator (if non-nil) for final cloud-specific identity wiring
//
// ctx and k8s are passed through to the identityMutator so it can make K8s API
// calls (e.g. IRSA SA lookup, Azure MI binding) and respect request deadlines.
//
// Opt-out annotations on the caller template (jaiscloud.io/platform-overlay=...)
// suppress specific overlay contributions. Unknown opt-out tokens return
// ErrUnknownOptOutToken so typos surface.
func BuildPodSpec(
	ctx context.Context,
	k8s kubernetes.Interface,
	base PodSpecInput,
	overlay *platform.PlatformConfig,
	callerTemplate []byte,
	identityMutator IdentityMutator,
) (corev1.PodTemplateSpec, error) {
	tpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta(toObjectMeta(base.Labels, base.Annotations)),
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{base.MainContainer},
		},
	}

	// Parse opt-outs from caller template first (so we can skip overlay steps).
	optOuts := map[string]bool{}
	if len(callerTemplate) > 0 {
		var callerTpl corev1.PodTemplateSpec
		if err := yaml.Unmarshal(callerTemplate, &callerTpl); err != nil {
			return corev1.PodTemplateSpec{}, fmt.Errorf("k8shelpers: parse caller pod template: %w", err)
		}
		// Validate + parse opt-out annotation.
		raw := callerTpl.Annotations["jaiscloud.io/platform-overlay"]
		if raw != "" {
			parsed, err := parseOptOuts(raw)
			if err != nil {
				return corev1.PodTemplateSpec{}, err
			}
			optOuts = parsed
		}
	}

	// Apply platform overlay.
	if overlay != nil {
		initContainers, vols, mainEnv, mainMounts := platform.PlatformFragmentsCorev1(overlay)

		if !optOuts["skip-tls"] {
			tpl.Spec.InitContainers = mergeInitContainers(tpl.Spec.InitContainers, initContainers)
			tpl.Spec.Volumes, _ = mergeVolumes(tpl.Spec.Volumes, vols)
			if len(tpl.Spec.Containers) > 0 {
				tpl.Spec.Containers[0].VolumeMounts = mergeVolumeMounts(
					tpl.Spec.Containers[0].VolumeMounts, mainMounts)
			}
		}
		if len(tpl.Spec.Containers) > 0 {
			merged, _ := mergeEnv(mainEnv, tpl.Spec.Containers[0].Env, optOuts)
			tpl.Spec.Containers[0].Env = merged
		}
	}

	// Merge caller template on top (caller wins per row rules).
	if len(callerTemplate) > 0 {
		var callerTpl corev1.PodTemplateSpec
		if err := yaml.Unmarshal(callerTemplate, &callerTpl); err != nil {
			return corev1.PodTemplateSpec{}, fmt.Errorf("k8shelpers: parse caller pod template: %w", err)
		}

		// Merge volumes (caller wins on conflict, warning emitted).
		merged, warnings := mergeVolumes(tpl.Spec.Volumes, callerTpl.Spec.Volumes)
		for _, w := range warnings {
			slog.Warn("k8shelpers: volume conflict", "detail", w)
		}
		tpl.Spec.Volumes = merged

		// Merge init containers (caller appended after platform).
		tpl.Spec.InitContainers = mergeInitContainers(tpl.Spec.InitContainers, callerTpl.Spec.InitContainers)

		// Merge main container fields if caller provides one.
		if len(callerTpl.Spec.Containers) > 0 && len(tpl.Spec.Containers) > 0 {
			caller := callerTpl.Spec.Containers[0]
			main := &tpl.Spec.Containers[0]

			if caller.Image != "" {
				main.Image = caller.Image
			}
			if len(caller.Command) > 0 {
				main.Command = caller.Command
			}
			if len(caller.Args) > 0 {
				main.Args = caller.Args
			}
			if caller.Resources.Requests != nil || caller.Resources.Limits != nil {
				main.Resources = caller.Resources
			}

			main.VolumeMounts = mergeVolumeMounts(main.VolumeMounts, caller.VolumeMounts)

			// Env: platform already merged above; now layer caller overrides.
			merged, warnings := mergeEnv(main.Env, caller.Env, optOuts)
			for _, w := range warnings {
				slog.Debug("k8shelpers: env merge", "detail", w)
			}
			main.Env = merged
		}

		// Merge serviceAccountName.
		if callerTpl.Spec.ServiceAccountName != "" {
			tpl.Spec.ServiceAccountName = callerTpl.Spec.ServiceAccountName
		}

		// Merge tolerations.
		tpl.Spec.Tolerations = append(tpl.Spec.Tolerations, callerTpl.Spec.Tolerations...)

		// Merge node selector (caller wins per key).
		if len(callerTpl.Spec.NodeSelector) > 0 {
			if tpl.Spec.NodeSelector == nil {
				tpl.Spec.NodeSelector = map[string]string{}
			}
			for k, v := range callerTpl.Spec.NodeSelector {
				tpl.Spec.NodeSelector[k] = v
			}
		}

		// Merge caller annotations/labels into metadata.
		for k, v := range callerTpl.Annotations {
			if tpl.Annotations == nil {
				tpl.Annotations = map[string]string{}
			}
			tpl.Annotations[k] = v
		}
		for k, v := range callerTpl.Labels {
			if tpl.Labels == nil {
				tpl.Labels = map[string]string{}
			}
			tpl.Labels[k] = v
		}
	}

	// Identity mutator runs last (authoritative).
	if identityMutator != nil {
		if err := identityMutator(ctx, k8s, &tpl); err != nil {
			return corev1.PodTemplateSpec{}, fmt.Errorf("k8shelpers: identity mutator: %w", err)
		}
	}

	return tpl, nil
}

// ── opt-out registry ────────────────────────────────────────────────────────

var (
	optOutMu     sync.RWMutex
	validOptOuts = map[string]bool{
		"skip-tls":         true,
		"skip-tls-verify":  true,
		"skip-kafka-certs": true,
		"skip-aws-creds":   true,
	}
)

// RegisterOptOutToken adds a cloud-specific opt-out token to the allowlist.
// Safe to call concurrently with BuildPodSpec.
func RegisterOptOutToken(token string) {
	optOutMu.Lock()
	defer optOutMu.Unlock()
	validOptOuts[token] = true
}

// parseOptOuts validates the annotation value and returns a set of tokens.
// Format: "skip-tls" or "skip-tls,skip-aws-creds" or "skip-env=JAVA_OPTS".
func parseOptOuts(raw string) (map[string]bool, error) {
	optOutMu.RLock()
	defer optOutMu.RUnlock()

	result := map[string]bool{}
	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		// strip "=VALUE" suffix for validation
		base := token
		if idx := strings.Index(token, "="); idx >= 0 {
			base = token[:idx]
		}
		// check exact match or prefix "skip-env"
		if !validOptOuts[base] && base != "skip-env" && base != "skip-init" &&
			base != "skip-volumes" && base != "skip-mounts" {
			return nil, fmt.Errorf("%w: %q", ErrUnknownOptOutToken, token)
		}
		result[token] = true
	}
	return result, nil
}

// ── merge helpers ───────────────────────────────────────────────────────────

// securityClassifiedEnvKeys get caller-wins-with-platform-appended behavior.
var securityClassifiedEnvKeys = map[string]bool{
	"JAVA_OPTS":           true,
	"JAVA_TOOL_OPTIONS":   true,
	"SSL_CERT_FILE":       true,
	"SSL_CERT_DIR":        true,
	"AWS_CA_BUNDLE":       true,
	"REQUESTS_CA_BUNDLE":  true,
	"CURL_CA_BUNDLE":      true,
	"NODE_EXTRA_CA_CERTS": true,
}

// mergeEnv merges caller env on top of platform env.
// For security-classified keys, platform value is appended to caller value.
// Returns merged slice (platform-order first, then caller-only keys) and warnings.
func mergeEnv(platform, caller []corev1.EnvVar, optOuts map[string]bool) ([]corev1.EnvVar, []string) {
	// Build map seeded with platform entries.
	m := make(map[string]corev1.EnvVar, len(platform)+len(caller))
	order := make([]string, 0, len(platform))
	for _, e := range platform {
		m[e.Name] = e
		order = append(order, e.Name)
	}

	var warnings []string
	callerOnlyOrder := make([]string, 0)
	for _, ce := range caller {
		pe, hasPlatform := m[ce.Name]
		skipEnvAll := optOuts["skip-env"]
		skipEnvKey := optOuts["skip-env="+ce.Name]

		if skipEnvAll || skipEnvKey {
			// Caller opt-out: use caller value, suppress platform.
			m[ce.Name] = ce
			if !hasPlatform {
				callerOnlyOrder = append(callerOnlyOrder, ce.Name)
			}
			continue
		}

		if hasPlatform && securityClassifiedEnvKeys[ce.Name] {
			// Both sides have this classified key: append platform to caller.
			// Skip ValueFrom — can't concat.
			if ce.ValueFrom == nil && pe.ValueFrom == nil {
				merged := collapseSpaces(ce.Value + " " + pe.Value)
				m[ce.Name] = corev1.EnvVar{Name: ce.Name, Value: merged}
				warnings = append(warnings, fmt.Sprintf("appended platform directive to caller's %s", ce.Name))
			} else {
				m[ce.Name] = ce // ValueFrom: caller wins
				warnings = append(warnings, fmt.Sprintf("caller overrode platform %s (ValueFrom)", ce.Name))
			}
		} else if hasPlatform {
			// Generic: caller wins.
			m[ce.Name] = ce
			warnings = append(warnings, fmt.Sprintf("caller overrode platform %s", ce.Name))
		} else {
			// Caller-only key.
			m[ce.Name] = ce
			callerOnlyOrder = append(callerOnlyOrder, ce.Name)
		}
	}

	// Emit in stable order: platform-first, then caller-only.
	out := make([]corev1.EnvVar, 0, len(m))
	seen := map[string]bool{}
	for _, name := range order {
		if !seen[name] {
			out = append(out, m[name])
			seen[name] = true
		}
	}
	for _, name := range callerOnlyOrder {
		if !seen[name] {
			out = append(out, m[name])
			seen[name] = true
		}
	}
	return out, warnings
}

// mergeVolumes merges caller volumes on top of platform volumes.
// Caller wins on name collision; returns the merged slice and warning strings.
func mergeVolumes(platform, caller []corev1.Volume) ([]corev1.Volume, []string) {
	m := make(map[string]corev1.Volume, len(platform)+len(caller))
	order := make([]string, 0, len(platform))
	for _, v := range platform {
		m[v.Name] = v
		order = append(order, v.Name)
	}

	var warnings []string
	callerOnly := make([]string, 0)
	for _, v := range caller {
		if _, exists := m[v.Name]; exists {
			m[v.Name] = v // caller wins
			warnings = append(warnings, fmt.Sprintf("volume %q present in both platform and caller; caller wins", v.Name))
		} else {
			m[v.Name] = v
			callerOnly = append(callerOnly, v.Name)
		}
	}

	out := make([]corev1.Volume, 0, len(m))
	seen := map[string]bool{}
	for _, name := range order {
		if !seen[name] {
			out = append(out, m[name])
			seen[name] = true
		}
	}
	for _, name := range callerOnly {
		if !seen[name] {
			out = append(out, m[name])
			seen[name] = true
		}
	}
	return out, warnings
}

// mergeVolumeMounts appends caller mounts to platform mounts; caller wins on mountPath collision.
func mergeVolumeMounts(platform, caller []corev1.VolumeMount) []corev1.VolumeMount {
	byPath := make(map[string]corev1.VolumeMount, len(platform))
	order := make([]string, 0, len(platform))
	for _, m := range platform {
		byPath[m.MountPath] = m
		order = append(order, m.MountPath)
	}
	callerOnly := make([]string, 0)
	for _, m := range caller {
		if _, exists := byPath[m.MountPath]; exists {
			byPath[m.MountPath] = m // caller wins
		} else {
			byPath[m.MountPath] = m
			callerOnly = append(callerOnly, m.MountPath)
		}
	}
	out := make([]corev1.VolumeMount, 0, len(byPath))
	seen := map[string]bool{}
	for _, path := range order {
		if !seen[path] {
			out = append(out, byPath[path])
			seen[path] = true
		}
	}
	for _, path := range callerOnly {
		if !seen[path] {
			out = append(out, byPath[path])
			seen[path] = true
		}
	}
	return out
}

// mergeInitContainers appends caller init containers after platform init containers.
// Caller wins on name collision.
func mergeInitContainers(platform, caller []corev1.Container) []corev1.Container {
	byName := make(map[string]int, len(platform))
	out := make([]corev1.Container, len(platform))
	copy(out, platform)
	for i, c := range out {
		byName[c.Name] = i
	}
	for _, c := range caller {
		if i, exists := byName[c.Name]; exists {
			out[i] = c // caller wins
		} else {
			out = append(out, c)
		}
	}
	return out
}

// collapseSpaces replaces multiple consecutive spaces with one.
func collapseSpaces(s string) string {
	parts := strings.Fields(s)
	return strings.Join(parts, " ")
}

// toObjectMeta builds a metav1.ObjectMeta from label and annotation maps.
func toObjectMeta(labels, annotations map[string]string) metav1.ObjectMeta {
	meta := metav1.ObjectMeta{}
	if len(labels) > 0 {
		meta.Labels = make(map[string]string, len(labels))
		for k, v := range labels {
			meta.Labels[k] = v
		}
	}
	if len(annotations) > 0 {
		meta.Annotations = make(map[string]string, len(annotations))
		for k, v := range annotations {
			meta.Annotations[k] = v
		}
	}
	return meta
}
