package platform

import (
	"fmt"

	"jaiscloud/internal/k8stypes"
)

// CheckVolumeConflicts returns an error if any volume in incoming has the same
// name as a volume already in existing. Called after cloud-transform volumes
// are on the spec and before platform volumes are appended, and from
// spark.MergeDriver when merging pod template volumes.
func CheckVolumeConflicts(existing, incoming []k8stypes.Volume) error {
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

// CheckMountPathConflicts returns an error if any mount in extra uses a
// MountPath already claimed by a mount in existing. Exact string equality.
// Catches cases where bootstrap emptyDir mounts and platform extra-volume
// mounts overlap at a shared path such as /etc/pki.
func CheckMountPathConflicts(existing, extra []k8stypes.VolumeMount) error {
	seen := make(map[string]struct{}, len(existing))
	for _, m := range existing {
		seen[m.MountPath] = struct{}{}
	}
	for _, m := range extra {
		if _, ok := seen[m.MountPath]; ok {
			return fmt.Errorf("platform: mount path conflict at %q", m.MountPath)
		}
	}
	return nil
}
