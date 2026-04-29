package platform

import (
	"testing"

	"jaiscloud/internal/k8stypes"
)

func TestCheckVolumeConflicts_NoConflict(t *testing.T) {
	existing := []k8stypes.Volume{{Name: "vol-a"}}
	incoming := []k8stypes.Volume{{Name: "vol-b"}}
	if err := checkVolumeConflicts(existing, incoming); err != nil {
		t.Errorf("unexpected conflict error: %v", err)
	}
}

func TestCheckVolumeConflicts_Conflict(t *testing.T) {
	existing := []k8stypes.Volume{{Name: "shared-vol"}}
	incoming := []k8stypes.Volume{{Name: "shared-vol"}}
	if err := checkVolumeConflicts(existing, incoming); err == nil {
		t.Error("expected conflict error, got nil")
	}
}

func TestCheckVolumeConflicts_EmptyIncoming(t *testing.T) {
	existing := []k8stypes.Volume{{Name: "vol-a"}}
	if err := checkVolumeConflicts(existing, nil); err != nil {
		t.Errorf("empty incoming should not conflict: %v", err)
	}
}

func TestCheckVolumeConflicts_BothEmpty(t *testing.T) {
	if err := checkVolumeConflicts(nil, nil); err != nil {
		t.Errorf("both empty should not conflict: %v", err)
	}
}
