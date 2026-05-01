package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveStateDir_ExplicitPath(t *testing.T) {
	dir := t.TempDir()
	got, err := ResolveStateDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != dir {
		t.Errorf("got %q, want %q", got, dir)
	}
}

func TestResolveStateDir_FallbackToTemp(t *testing.T) {
	// Pass an empty explicit path — should fall through to a valid temp dir.
	got, err := ResolveStateDir("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == "" {
		t.Error("state dir must not be empty")
	}
}

func TestLoadOrCreateInstanceID_EnvVar(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "test-fixed-id")
	id, source := LoadOrCreateInstanceID("")
	if id != "test-fixed-id" {
		t.Errorf("id: got %q, want test-fixed-id", id)
	}
	if source != "env" {
		t.Errorf("source: got %q, want env", source)
	}
}

func TestLoadOrCreateInstanceID_PersistsToFile(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "") // clear override
	dir := t.TempDir()

	id1, src1 := LoadOrCreateInstanceID(dir)
	if src1 != "file" {
		t.Errorf("first call source: got %q, want file", src1)
	}
	if id1 == "" {
		t.Fatal("id must not be empty")
	}

	// Second call reads from file.
	id2, src2 := LoadOrCreateInstanceID(dir)
	if src2 != "file" {
		t.Errorf("second call source: got %q, want file", src2)
	}
	if id1 != id2 {
		t.Errorf("id changed between calls: %q != %q", id1, id2)
	}

	// Verify UUID v4 format (8-4-4-4-12 hex digits).
	parts := strings.Split(id1, "-")
	if len(parts) != 5 {
		t.Errorf("expected UUID with 5 parts, got %v", parts)
	}
}

func TestLoadOrCreateInstanceID_ReadsExistingFile(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "")
	dir := t.TempDir()
	want := "existing-id-abc"
	if err := os.WriteFile(filepath.Join(dir, "instance-id"), []byte(want+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, src := LoadOrCreateInstanceID(dir)
	if id != want {
		t.Errorf("id: got %q, want %q", id, want)
	}
	if src != "file" {
		t.Errorf("source: got %q, want file", src)
	}
}

func TestNewUUIDv4_Format(t *testing.T) {
	id, err := newUUIDv4()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("expected 5 parts, got %d: %q", len(parts), id)
	}
	lengths := []int{8, 4, 4, 4, 12}
	for i, p := range parts {
		if len(p) != lengths[i] {
			t.Errorf("part %d: got len=%d want %d, value=%q", i, len(p), lengths[i], p)
		}
	}
}
