package platform

import (
	"sort"

	"jaiscloud/internal/k8stypes"
)

// ExtraEnv converts a string map to EnvVar entries sorted by key for
// deterministic manifest output.
func ExtraEnv(m map[string]string) []k8stypes.EnvVar {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	envs := make([]k8stypes.EnvVar, len(keys))
	for i, k := range keys {
		envs[i] = k8stypes.EnvVar{Name: k, Value: m[k]}
	}
	return envs
}

// MergeEnv appends src entries into dst with first-wins deduplication,
// matching K8s behaviour for duplicate names in a single container.
func MergeEnv(dst, src []k8stypes.EnvVar) []k8stypes.EnvVar {
	seen := make(map[string]struct{}, len(dst))
	for _, e := range dst {
		seen[e.Name] = struct{}{}
	}
	for _, e := range src {
		if _, ok := seen[e.Name]; !ok {
			dst = append(dst, e)
			seen[e.Name] = struct{}{}
		}
	}
	return dst
}
