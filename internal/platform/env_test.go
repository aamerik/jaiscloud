package platform

import (
	"testing"

	"jaiscloud/internal/k8stypes"
)

func TestExtraEnv_SortedOutput(t *testing.T) {
	m := map[string]string{"Z_VAR": "z", "A_VAR": "a", "M_VAR": "m"}
	envs := ExtraEnv(m)
	if len(envs) != 3 {
		t.Fatalf("expected 3 envs, got %d", len(envs))
	}
	if envs[0].Name != "A_VAR" || envs[1].Name != "M_VAR" || envs[2].Name != "Z_VAR" {
		t.Errorf("not sorted: %v", envs)
	}
}

func TestExtraEnv_Empty(t *testing.T) {
	envs := ExtraEnv(nil)
	if len(envs) != 0 {
		t.Errorf("expected empty, got %d", len(envs))
	}
}

func TestMergeEnv_FirstWins(t *testing.T) {
	dst := []k8stypes.EnvVar{{Name: "FOO", Value: "original"}}
	src := []k8stypes.EnvVar{{Name: "FOO", Value: "override"}, {Name: "BAR", Value: "bar"}}
	result := MergeEnv(dst, src)
	fooVal := ""
	for _, e := range result {
		if e.Name == "FOO" {
			fooVal = e.Value
		}
	}
	if fooVal != "original" {
		t.Errorf("FOO should keep dst value %q, got %q", "original", fooVal)
	}
	barFound := false
	for _, e := range result {
		if e.Name == "BAR" {
			barFound = true
		}
	}
	if !barFound {
		t.Error("BAR from src should be present after merge")
	}
}

func TestMergeEnv_EmptyDst(t *testing.T) {
	src := []k8stypes.EnvVar{{Name: "X", Value: "1"}}
	result := MergeEnv(nil, src)
	if len(result) != 1 || result[0].Name != "X" {
		t.Errorf("expected X=1, got %v", result)
	}
}

func TestMergeEnv_NoDuplicates(t *testing.T) {
	src := []k8stypes.EnvVar{{Name: "A", Value: "1"}, {Name: "A", Value: "2"}}
	result := MergeEnv(nil, src)
	count := 0
	for _, e := range result {
		if e.Name == "A" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 A entry, got %d", count)
	}
}
