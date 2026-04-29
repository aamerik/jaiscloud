package platform

import (
	"fmt"

	"jaiscloud/internal/k8stypes"
)

// checkVolumeConflicts returns an error if any volume in incoming has the same
// name as a volume already in existing. Called after cloud-transform volumes
// are on the spec and before platform volumes are appended.
func checkVolumeConflicts(existing, incoming []k8stypes.Volume) error {
	names := make(map[string]struct{}, len(existing))
	for _, v := range existing {
		names[v.Name] = struct{}{}
	}
	for _, v := range incoming {
		if _, ok := names[v.Name]; ok {
			return fmt.Errorf("platform: volume name conflict %q — already present on pod spec", v.Name)
		}
	}
	return nil
}
