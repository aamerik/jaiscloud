package runexec

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
)

// defaultContainerPort is injected when a revision template omits a port.
const defaultContainerPort = 8080

// workloadName returns the deterministic, DNS-safe name shared by a revision's
// Pod and ClusterIP Service. The hash keeps it stable and collision-free while
// the sanitized service id keeps it debuggable.
func workloadName(svc runstore.Service, rev runstore.Revision) string {
	return workloadNameFor(svc.ProjectID, svc.Location, svc.ID, rev.ID)
}

func workloadNameFor(project, location, service, revision string) string {
	sum := sha256.Sum256([]byte(project + "/" + location + "/" + service + "/" + revision))
	hash := hex.EncodeToString(sum[:])[:10]
	base := sanitizeDNSLabel(service)
	if len(base) > 30 {
		base = base[:30]
	}
	if base == "" {
		base = "svc"
	}
	return "cloudrun-" + base + "-" + hash
}

func sanitizeDNSLabel(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		}
	}
	return strings.Trim(string(out), "-")
}

// buildPod renders a revision's Pod spec from its stored template. It returns
// the pod and the container port the ClusterIP Service must expose.
func buildPod(svc runstore.Service, rev runstore.Revision, name, namespace string) (*corev1.Pod, int32, error) {
	image, command, args, env, resources, port, err := containerSpec(svc, rev)
	if err != nil {
		return nil, 0, err
	}
	labels := map[string]string{
		labelApp:      labelAppValue,
		labelInstance: rev.ID,
		labelService:  svc.ID,
		labelRevision: rev.ID,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{{
				Name:      "app",
				Image:     image,
				Command:   command,
				Args:      args,
				Env:       env,
				Ports:     []corev1.ContainerPort{{Name: "http", ContainerPort: port}},
				Resources: resources,
			}},
		},
	}
	return pod, port, nil
}

// containerSpec extracts the fields the emulator models from the first
// container of a revision template. Cloud Run always injects PORT/K_SERVICE/
// K_REVISION/K_CONFIGURATION; those override any template-supplied value.
func containerSpec(svc runstore.Service, rev runstore.Revision) (image string, command, args []string, env []corev1.EnvVar, resources corev1.ResourceRequirements, port int32, err error) {
	containers, _ := rev.Data["containers"].([]any)
	if len(containers) == 0 {
		return "", nil, nil, nil, corev1.ResourceRequirements{}, 0, fmt.Errorf("cloudrun: revision %s has no container template", rev.ID)
	}
	c, _ := containers[0].(map[string]any)
	image, _ = c["image"].(string)
	if image == "" {
		return "", nil, nil, nil, corev1.ResourceRequirements{}, 0, fmt.Errorf("cloudrun: revision %s requires a container image", rev.ID)
	}
	command = stringSlice(c["command"])
	args = stringSlice(c["args"])
	port = containerPort(c)
	resources = resourceRequirements(c["resources"])
	env = mergeEnv(envVars(c["env"]), corev1.EnvVar{Name: envPort, Value: fmt.Sprint(port)},
		corev1.EnvVar{Name: envService, Value: svc.ID},
		corev1.EnvVar{Name: envRevision, Value: rev.ID},
		corev1.EnvVar{Name: envConfiguration, Value: svc.ID},
	)
	return image, command, args, env, resources, port, nil
}

// mergeEnv appends platform env after the template's, with the platform value
// winning a duplicate name so the injected Cloud Run variables are authoritative.
func mergeEnv(template []corev1.EnvVar, platform ...corev1.EnvVar) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(template)+len(platform))
	index := map[string]int{}
	for _, e := range template {
		index[e.Name] = len(out)
		out = append(out, e)
	}
	for _, e := range platform {
		if i, ok := index[e.Name]; ok {
			out[i] = e
			continue
		}
		index[e.Name] = len(out)
		out = append(out, e)
	}
	return out
}

func envVars(v any) []corev1.EnvVar {
	list, _ := v.([]any)
	out := make([]corev1.EnvVar, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		value, _ := m["value"].(string)
		out = append(out, corev1.EnvVar{Name: name, Value: value})
	}
	return out
}

func containerPort(c map[string]any) int32 {
	ports, _ := c["ports"].([]any)
	if len(ports) == 0 {
		return defaultContainerPort
	}
	m, ok := ports[0].(map[string]any)
	if !ok {
		return defaultContainerPort
	}
	if p := int32(intFrom(m["containerPort"])); p > 0 {
		return p
	}
	return defaultContainerPort
}

func resourceRequirements(v any) corev1.ResourceRequirements {
	m, _ := v.(map[string]any)
	return corev1.ResourceRequirements{
		Limits:   resourceList(m["limits"]),
		Requests: resourceList(m["requests"]),
	}
}

func resourceList(v any) corev1.ResourceList {
	m, _ := v.(map[string]any)
	if len(m) == 0 {
		return nil
	}
	out := corev1.ResourceList{}
	for name, raw := range m {
		s, ok := raw.(string)
		if !ok || s == "" {
			continue
		}
		q, err := resource.ParseQuantity(s)
		if err != nil {
			continue
		}
		out[corev1.ResourceName(name)] = q
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func stringSlice(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func intFrom(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return 0
	}
}

// serviceHost is the normalized invocation authority a service's data-plane
// requests carry. It must match runcore.InvocationAuthority (the core and the
// runtime share ConfigureInvocationHost at startup).
func serviceHost(project, location, id string) string {
	return runcore.InvocationAuthority(project, location, id)
}
