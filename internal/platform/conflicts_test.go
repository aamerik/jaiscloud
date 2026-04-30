package platform

import (
	"strings"
	"testing"

	"jaiscloud/internal/k8stypes"
)

func TestCheckVolumeConflicts_NoConflict(t *testing.T) {
	existing := []k8stypes.Volume{{Name: "vol-a"}}
	incoming := []k8stypes.Volume{{Name: "vol-b"}}
	if err := CheckVolumeConflicts(existing, incoming); err != nil {
		t.Errorf("unexpected conflict error: %v", err)
	}
}

func TestCheckVolumeConflicts_Conflict(t *testing.T) {
	existing := []k8stypes.Volume{{Name: "shared-vol"}}
	incoming := []k8stypes.Volume{{Name: "shared-vol"}}
	if err := CheckVolumeConflicts(existing, incoming); err == nil {
		t.Error("expected conflict error, got nil")
	}
}

func TestCheckVolumeConflicts_EmptyIncoming(t *testing.T) {
	existing := []k8stypes.Volume{{Name: "vol-a"}}
	if err := CheckVolumeConflicts(existing, nil); err != nil {
		t.Errorf("empty incoming should not conflict: %v", err)
	}
}

func TestCheckVolumeConflicts_BothEmpty(t *testing.T) {
	if err := CheckVolumeConflicts(nil, nil); err != nil {
		t.Errorf("both empty should not conflict: %v", err)
	}
}

func TestCheckMountPathConflicts_NoConflict(t *testing.T) {
	existing := []k8stypes.VolumeMount{{MountPath: "/a"}, {MountPath: "/b"}}
	extra := []k8stypes.VolumeMount{{MountPath: "/c"}}
	if err := CheckMountPathConflicts(existing, extra); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckMountPathConflicts_Conflict(t *testing.T) {
	existing := []k8stypes.VolumeMount{{MountPath: "/etc/pki"}}
	extra := []k8stypes.VolumeMount{{MountPath: "/etc/pki"}}
	err := CheckMountPathConflicts(existing, extra)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "/etc/pki") {
		t.Errorf("error should cite conflicting path, got: %v", err)
	}
}

func TestCheckMountPathConflicts_FirstConflictReported(t *testing.T) {
	existing := []k8stypes.VolumeMount{{MountPath: "/a"}, {MountPath: "/b"}}
	extra := []k8stypes.VolumeMount{{MountPath: "/a"}, {MountPath: "/b"}}
	err := CheckMountPathConflicts(existing, extra)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "/a") {
		t.Errorf("first collision should be /a, got: %v", err)
	}
}

func TestCheckMountPathConflicts_EmptyExtra(t *testing.T) {
	existing := []k8stypes.VolumeMount{{MountPath: "/a"}}
	if err := CheckMountPathConflicts(existing, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckMountPathConflicts_EmptyExisting(t *testing.T) {
	extra := []k8stypes.VolumeMount{{MountPath: "/a"}}
	if err := CheckMountPathConflicts(nil, extra); err != nil {
		t.Errorf("unexpected error with empty existing: %v", err)
	}
}
