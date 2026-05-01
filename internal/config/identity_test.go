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

func TestWriteInstanceID_AtomicAndReadBack(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "")
	dir := t.TempDir()

	require := func(cond bool, msg string) {
		t.Helper()
		if !cond {
			t.Fatal(msg)
		}
	}

	err := WriteInstanceID(dir, "my-custom-id")
	require(err == nil, "WriteInstanceID should not error")

	// No .tmp file should persist.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("stale .tmp file found: %s", e.Name())
		}
	}

	// ReadBack: LoadOrCreateInstanceID should return the written value.
	id, src := LoadOrCreateInstanceID(dir)
	require(id == "my-custom-id", "id mismatch: "+id)
	require(src == "file", "source mismatch: "+src)
}

func TestWriteInstanceID_Overwrites(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "")
	dir := t.TempDir()

	_ = WriteInstanceID(dir, "first")
	_ = WriteInstanceID(dir, "second")

	id, _ := LoadOrCreateInstanceID(dir)
	if id != "second" {
		t.Fatalf("got %q, want second", id)
	}
}

func TestGenerateNewInstanceID_WritesValidUUID(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "")
	dir := t.TempDir()

	id, err := GenerateNewInstanceID(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == "" {
		t.Fatal("id must not be empty")
	}
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("expected UUID with 5 parts, got %v", parts)
	}

	// File should contain the same UUID.
	persisted, _ := LoadOrCreateInstanceID(dir)
	if persisted != id {
		t.Errorf("persisted id %q differs from returned id %q", persisted, id)
	}
}

func TestGenerateNewInstanceID_AlwaysGeneratesNewID(t *testing.T) {
	t.Setenv("JAISCLOUD_INSTANCE_ID", "")
	dir := t.TempDir()

	id1, _ := GenerateNewInstanceID(dir)
	id2, _ := GenerateNewInstanceID(dir)

	if id1 == id2 {
		t.Error("GenerateNewInstanceID should produce a different UUID each call")
	}
}

func TestExecutorMode_LambdaSpecificOverride(t *testing.T) {
	t.Setenv("JAISCLOUD_EXECUTOR_MODE", "docker")
	t.Setenv("JAISCLOUD_LAMBDA_EXECUTOR_MODE", "mock")

	mode, src := ExecutorMode("lambda", "mock")
	if mode != "mock" {
		t.Errorf("mode: got %q, want mock", mode)
	}
	if src != "JAISCLOUD_LAMBDA_EXECUTOR_MODE" {
		t.Errorf("source: got %q, want JAISCLOUD_LAMBDA_EXECUTOR_MODE", src)
	}
}

func TestExecutorMode_FallsBackToGeneric(t *testing.T) {
	t.Setenv("JAISCLOUD_EXECUTOR_MODE", "k8s")
	t.Setenv("JAISCLOUD_LAMBDA_EXECUTOR_MODE", "")

	mode, src := ExecutorMode("lambda", "mock")
	if mode != "k8s" {
		t.Errorf("mode: got %q, want k8s", mode)
	}
	if src != "JAISCLOUD_EXECUTOR_MODE" {
		t.Errorf("source: got %q, want JAISCLOUD_EXECUTOR_MODE", src)
	}
}

func TestExecutorMode_FallsBackToDefault(t *testing.T) {
	t.Setenv("JAISCLOUD_EXECUTOR_MODE", "")
	t.Setenv("JAISCLOUD_LAMBDA_EXECUTOR_MODE", "")

	mode, src := ExecutorMode("lambda", "mock")
	if mode != "mock" {
		t.Errorf("mode: got %q, want mock", mode)
	}
	if src != "default" {
		t.Errorf("source: got %q, want default", src)
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
