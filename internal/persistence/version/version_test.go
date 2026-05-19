package version

import (
	"errors"
	"testing"
)

func TestCheckSchemaVersion_Equal(t *testing.T) {
	if err := CheckSnapshotVersion(CodeSnapshotVersion); err != nil {
		t.Errorf("expected nil for equal version, got %v", err)
	}
}

func TestCheckSchemaVersion_TooNew(t *testing.T) {
	err := CheckSnapshotVersion(CodeSnapshotVersion + 1)
	if err == nil {
		t.Fatal("expected error for too-new version")
	}
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Errorf("expected ErrSchemaTooNew, got %v", err)
	}
}

func TestCheckSchemaVersion_TooOld(t *testing.T) {
	err := CheckSnapshotVersion(CodeSnapshotVersion - 1)
	if err == nil {
		t.Fatal("expected error for too-old version")
	}
	if !errors.Is(err, ErrSchemaTooOld) {
		t.Errorf("expected ErrSchemaTooOld, got %v", err)
	}
	// Must mention --fresh-start in the message.
	if !containsString(err.Error(), "--fresh-start") {
		t.Errorf("error message must contain '--fresh-start': %v", err)
	}
}

// --- KEK fingerprint ---

func TestFingerprintKEK_NoKEK(t *testing.T) {
	if got := FingerprintKEK(nil); got != "none" {
		t.Errorf("expected 'none' for nil key, got %q", got)
	}
}

func TestFingerprintKEK_EmptySlice(t *testing.T) {
	if got := FingerprintKEK([]byte{}); got != "none" {
		t.Errorf("expected 'none' for empty slice, got %q", got)
	}
}

func TestFingerprintKEK_Length(t *testing.T) {
	got := FingerprintKEK([]byte("some-key-material"))
	if len(got) != KEKFingerprintLen {
		t.Errorf("expected length %d, got %d (%q)", KEKFingerprintLen, len(got), got)
	}
}

func TestFingerprintKEK_Deterministic(t *testing.T) {
	key := []byte("deterministic-key")
	a := FingerprintKEK(key)
	b := FingerprintKEK(key)
	if a != b {
		t.Errorf("fingerprint not deterministic: %q vs %q", a, b)
	}
}

func TestFingerprintKEK_DifferentKeys(t *testing.T) {
	a := FingerprintKEK([]byte("key-one"))
	b := FingerprintKEK([]byte("key-two"))
	if a == b {
		t.Error("different keys should produce different fingerprints")
	}
}

// --- CheckKEKFingerprint ---

func TestCheckKEKFingerprint_BothNone(t *testing.T) {
	if err := CheckKEKFingerprint("none", "none"); err != nil {
		t.Errorf("expected nil for both 'none', got %v", err)
	}
}

func TestCheckKEKFingerprint_SameHex(t *testing.T) {
	fp := FingerprintKEK([]byte("same-key"))
	if err := CheckKEKFingerprint(fp, fp); err != nil {
		t.Errorf("expected nil for matching fingerprints, got %v", err)
	}
}

func TestCheckKEKFingerprint_MismatchHexHex(t *testing.T) {
	a := FingerprintKEK([]byte("key-a"))
	b := FingerprintKEK([]byte("key-b"))
	err := CheckKEKFingerprint(a, b)
	if !errors.Is(err, ErrKEKMismatch) {
		t.Errorf("expected ErrKEKMismatch, got %v", err)
	}
}

func TestCheckKEKFingerprint_EnvelopeHex_InstanceNone(t *testing.T) {
	fp := FingerprintKEK([]byte("some-key"))
	err := CheckKEKFingerprint(fp, "none")
	if !errors.Is(err, ErrKEKMismatch) {
		t.Errorf("expected ErrKEKMismatch, got %v", err)
	}
}

func TestCheckKEKFingerprint_EnvelopeNone_InstanceHex(t *testing.T) {
	fp := FingerprintKEK([]byte("some-key"))
	err := CheckKEKFingerprint("none", fp)
	if !errors.Is(err, ErrKEKMismatch) {
		t.Errorf("expected ErrKEKMismatch, got %v", err)
	}
}

func TestCheckKEKFingerprint_Stripped(t *testing.T) {
	// "stripped" envelope always passes regardless of instance KEK.
	fp := FingerprintKEK([]byte("some-key"))
	if err := CheckKEKFingerprint("stripped", fp); err != nil {
		t.Errorf("expected nil for 'stripped' envelope, got %v", err)
	}
	if err := CheckKEKFingerprint("stripped", "none"); err != nil {
		t.Errorf("expected nil for 'stripped' envelope with 'none' instance, got %v", err)
	}
}

func containsString(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
