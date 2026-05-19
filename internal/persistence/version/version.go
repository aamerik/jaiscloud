// Package version defines schema version constants and check functions for both
// snapshot envelopes and DB migrations.
package version

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	// CodeSnapshotVersion is the envelope schema version this binary writes and accepts.
	// No MinUpgradableFrom — pre-release, no backward compatibility needed.
	CodeSnapshotVersion = 3

	// CodeDBSchemaVersion is set in Phase 8 once the 014 migration gap is resolved.
	// PLACEHOLDER: do not use until Phase 8.
	CodeDBSchemaVersion = 0
)

var (
	// ErrSchemaTooOld indicates the stored schema is behind the binary.
	ErrSchemaTooOld = errors.New("schema version is outdated; wipe state with --fresh-start and restart")
	// ErrSchemaTooNew indicates the stored schema is ahead of the binary.
	ErrSchemaTooNew = errors.New("schema version is ahead of this binary; upgrade jaiscloud to the latest release")
	// ErrCloudMismatch indicates the snapshot was written by a different cloud binary.
	ErrCloudMismatch = errors.New("envelope cloud does not match running cloud")
	// ErrKEKMismatch indicates the KEK fingerprints do not match.
	ErrKEKMismatch = errors.New("envelope KEK fingerprint does not match running KEK")
)

// Envelope is the top-level container for exported state (schema v3).
type Envelope struct {
	SchemaVersion  int                        `json:"schema_version"`
	InstanceID     string                     `json:"instance_id,omitempty"`
	Cloud          string                     `json:"cloud,omitempty"`
	Region         string                     `json:"region,omitempty"`
	AccountID      string                     `json:"account_id,omitempty"`
	CreatedAt      time.Time                  `json:"created_at"`
	KEKFingerprint string                     `json:"kek_fingerprint,omitempty"`
	Stores         map[string]json.RawMessage `json:"stores"`
}

// CheckSnapshotVersion returns nil when stored matches the code version.
// Returns ErrSchemaTooNew or ErrSchemaTooOld otherwise.
func CheckSnapshotVersion(stored int) error {
	switch {
	case stored == CodeSnapshotVersion:
		return nil
	case stored > CodeSnapshotVersion:
		return fmt.Errorf("%w (stored=%d, binary supports up to %d)", ErrSchemaTooNew, stored, CodeSnapshotVersion)
	default:
		return fmt.Errorf("%w (stored=%d, binary requires %d): run with --fresh-start to wipe and restart", ErrSchemaTooOld, stored, CodeSnapshotVersion)
	}
}
