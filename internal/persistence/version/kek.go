package version

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const (
	// KEKFingerprintLen is the hex-encoded length of the fingerprint (8 bytes = 16 hex chars).
	KEKFingerprintLen = 16

	// KEKFingerprintNone is the sentinel for "no KEK configured".
	KEKFingerprintNone = "none"

	// KEKFingerprintStripped is set on envelopes exported with --strip-kek.
	// On import it is treated identically to "none" — no KEK check is performed.
	KEKFingerprintStripped = "stripped"
)

// FingerprintKEK returns the first 8 bytes of SHA-256(kek) as 16 lowercase hex chars.
// Returns "none" when kek is nil or empty.
func FingerprintKEK(kek []byte) string {
	if len(kek) == 0 {
		return KEKFingerprintNone
	}
	sum := sha256.Sum256(kek)
	return hex.EncodeToString(sum[:8])
}

// CheckKEKFingerprint compares the envelope fingerprint against the running instance's fingerprint.
// Returns nil when they are compatible, ErrKEKMismatch otherwise.
//
// Compatibility table:
//
//	envelope="none",     instance="none"     → ok
//	envelope=<hex>,      instance=<same hex> → ok
//	envelope="stripped", instance=any        → ok (--strip-kek path)
//	all other combinations                   → ErrKEKMismatch
func CheckKEKFingerprint(envelopeFP, instanceFP string) error {
	// "stripped" envelope skips all KEK checks.
	if envelopeFP == KEKFingerprintStripped {
		return nil
	}
	if envelopeFP == instanceFP {
		return nil
	}
	// Mismatch — build a descriptive message.
	switch {
	case envelopeFP != KEKFingerprintNone && instanceFP == KEKFingerprintNone:
		return fmt.Errorf("%w: snapshot encrypted with KEK fingerprint %s; your instance has no KEK configured. "+
			"Set JAISCLOUD_KMS_MASTER_KEY to the original KEK, or re-export with --strip-kek", ErrKEKMismatch, envelopeFP)
	case envelopeFP == KEKFingerprintNone && instanceFP != KEKFingerprintNone:
		return fmt.Errorf("%w: snapshot has no wrapped key material; your instance uses KEK %s. "+
			"Re-export with --strip-kek, or clear JAISCLOUD_KMS_MASTER_KEY before importing", ErrKEKMismatch, instanceFP)
	default:
		return fmt.Errorf("%w: snapshot encrypted with KEK %s; your instance uses %s. "+
			"Set JAISCLOUD_KMS_MASTER_KEY to the original KEK, or re-export with --strip-kek", ErrKEKMismatch, envelopeFP, instanceFP)
	}
}
