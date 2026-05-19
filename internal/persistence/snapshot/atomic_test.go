package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicWrite_NoOrphanTmp(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "state.json")
	data := []byte(`{"schema_version":3}`)

	if err := WriteAtomic(dst, data); err != nil {
		t.Fatalf("WriteAtomic: %v", err)
	}
	// Destination must exist.
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("dst not found: %v", err)
	}
	// No .tmp file should remain.
	if _, err := os.Stat(dst + ".tmp"); err == nil {
		t.Fatal("stale .tmp file remains after successful write")
	}
	// Content must be correct.
	got, _ := os.ReadFile(dst)
	if string(got) != string(data) {
		t.Errorf("content mismatch: got %q, want %q", got, data)
	}
}

func TestAtomicWrite_ErrorCleansTemp(t *testing.T) {
	dir := t.TempDir()
	// dst points inside a non-existent sub-directory so Rename will fail.
	dst := filepath.Join(dir, "nonexistent", "state.json")
	data := []byte(`{}`)

	err := WriteAtomic(dst, data)
	if err == nil {
		t.Fatal("expected error writing to non-existent directory")
	}
	// No .tmp anywhere.
	if _, err2 := os.Stat(dst + ".tmp"); err2 == nil {
		t.Fatal("stale .tmp file remains after failed write")
	}
	// Original dst unchanged (doesn't exist in this test).
	if _, err2 := os.Stat(dst); err2 == nil {
		t.Fatal("dst should not exist after failed atomic write")
	}
}

func TestAtomicWrite_OverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "state.json")

	if err := WriteAtomic(dst, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomic(dst, []byte("second")); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "second" {
		t.Errorf("expected 'second', got %q", got)
	}
}
