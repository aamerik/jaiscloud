// Package secret provides the Secret Manager store. Secrets and versions live
// in dedicated jc_sm_secrets / jc_sm_versions tables (mirroring AWS
// jc_sm_secrets / jc_sm_versions).
package secretmanager

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"
)

var (
	ErrNoSuchSecret  = errors.New("NoSuchSecret")
	ErrNoSuchVersion = errors.New("NoSuchVersion")
	ErrAlreadyExists = errors.New("AlreadyExists")
	// ErrManagedRotationEnabled is returned by a rotation helper when Cloud SQL
	// managed rotation is already ACTIVE for a secret: the API allows enabling
	// it only once (subsequent rotations go through RotateSecret).
	ErrManagedRotationEnabled = errors.New("managed rotation already enabled")
)

// Rotation is a secret's automatic-rotation schedule. NextRotationTime is an
// RFC3339 timestamp; RotationPeriod is a duration string (e.g. "3600s").
// ManagedRotation carries the Cloud SQL managed-rotation config/status, nil
// until EnableManagedRotation has been called.
type Rotation struct {
	NextRotationTime string           `json:"nextRotationTime"`
	RotationPeriod   string           `json:"rotationPeriod"`
	ManagedRotation  *ManagedRotation `json:"managedRotation,omitempty"`
}

// ManagedRotation is the Cloud SQL managed-rotation state for a secret (GCP's
// typed-secret managed rotation). State is "ACTIVE"/"INACTIVE"; InstanceID and
// Username are the linked Cloud SQL single-user credentials.
type ManagedRotation struct {
	State      string `json:"state"`
	InstanceID string `json:"instanceId,omitempty"`
	Username   string `json:"username,omitempty"`
}

// Replication is a secret's replication policy — the `replication` oneof from
// the API. Exactly one arm is set; a nil Replication on a stored record means
// the policy predates it being modelled, and reads back as the automatic arm
// (see Secret.ReplicationPolicy).
type Replication struct {
	Automatic   *AutomaticReplication   `json:"automatic,omitempty"`
	UserManaged *UserManagedReplication `json:"userManaged,omitempty"`
}

// AutomaticReplication replicates the payload without restrictions.
type AutomaticReplication struct {
	CustomerManagedEncryption *CustomerManagedEncryption `json:"customerManagedEncryption,omitempty"`
}

// UserManagedReplication replicates the payload into the listed locations.
type UserManagedReplication struct {
	Replicas []Replica `json:"replicas"`
}

// Replica is one user-managed replication location. CustomerManagedEncryption
// is per replica, and its key must reside in the replica's location.
type Replica struct {
	Location                  string                     `json:"location"`
	CustomerManagedEncryption *CustomerManagedEncryption `json:"customerManagedEncryption,omitempty"`
}

// CustomerManagedEncryption names the Cloud KMS CryptoKey that encrypts new
// versions. A nil pointer means Google-managed (default) encryption.
type CustomerManagedEncryption struct {
	KmsKeyName string `json:"kmsKeyName"`
}

// WrapKey returns the Cloud KMS key a version added now is wrapped with: the
// automatic arm's key, or the first user-managed replica's. The emulator keeps
// one wrapped DEK per version, so a user-managed policy whose replicas name
// different keys wraps with the first of them; the version's replicationStatus
// still reports the binding per replica (see service/secretmanager).
func (r *Replication) WrapKey() string {
	if r == nil {
		return ""
	}
	if r.Automatic != nil {
		if cme := r.Automatic.CustomerManagedEncryption; cme != nil {
			return cme.KmsKeyName
		}
		return ""
	}
	if r.UserManaged != nil {
		for _, rep := range r.UserManaged.Replicas {
			if cme := rep.CustomerManagedEncryption; cme != nil {
				return cme.KmsKeyName
			}
		}
	}
	return ""
}

// GeneratePassword returns a fresh random password for Cloud SQL managed
// rotation. The API only specifies "a random password will be generated", so
// the alphabet is an implementation detail: 24 CSPRNG bytes, base64-encoded
// (32 printable characters).
func GeneratePassword() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
}

// Secret is Secret Manager secret metadata.
type Secret struct {
	ID             string
	Labels         map[string]string
	Annotations    map[string]string
	CreateTime     time.Time
	NextVer        int
	Rotation       *Rotation      // nil when rotation is disabled
	VersionAliases map[string]int // alias name → version number, nil when none
	// Replication is the secret's replication policy. nil on records written
	// before it was modelled (they carry only KmsKeyName); use
	// ReplicationPolicy() to read it.
	Replication *Replication `json:"replication,omitempty"`
	// KmsKeyName is the effective wrapping key of new versions (the automatic
	// arm's key or the first user-managed replica's). Kept alongside the policy
	// because the version-wrap path and records written by older builds read
	// it; it is never the source of truth for the wire shape.
	KmsKeyName string
}

// ReplicationPolicy returns the secret's replication policy, reconstructing the
// automatic arm for records written before the policy was modelled. It never
// returns nil, so both transports render the same shape for every record.
func (s Secret) ReplicationPolicy() *Replication {
	if s.Replication != nil {
		return s.Replication
	}
	auto := &AutomaticReplication{}
	if s.KmsKeyName != "" {
		auto.CustomerManagedEncryption = &CustomerManagedEncryption{KmsKeyName: s.KmsKeyName}
	}
	return &Replication{Automatic: auto}
}

// SetReplication records r as the secret's replication policy and derives the
// effective wrapping key kept in KmsKeyName.
func (s *Secret) SetReplication(r *Replication) {
	s.Replication = r
	s.KmsKeyName = r.WrapKey()
}

// Version is a Secret Manager secret version.
type Version struct {
	SecretID   string
	VersionID  string
	State      string
	CreateTime time.Time
	// DestroyTime is set only when State is DESTROYED (output-only in the API).
	DestroyTime time.Time
	Data        string // base64 payload
	KmsKeyName  string
	WrappedDEK  []byte
}

// Store is the Secret Manager store.
type Store interface {
	CreateSecret(ctx context.Context, projectID, id string, s Secret) error
	GetSecret(ctx context.Context, projectID, id string) (Secret, error)
	UpdateSecret(ctx context.Context, projectID, id string, s Secret) error
	// UpdateSecretAtomic atomically reads the current secret, calls mutate to
	// compute the new value, and writes it back — no other GetSecret/
	// UpdateSecret/NextVersion for this secret can be observed or applied in
	// between. Callers doing a read-modify-write (a labels/rotation patch, an
	// automatic-rotation version bump) must use this instead of a separate
	// GetSecret+UpdateSecret pair: the latter has a lost-update window where a
	// concurrent NextVersion() call's counter advance gets silently rolled
	// back by the stale write. Returns ErrNoSuchSecret if the secret doesn't
	// exist (mutate is not called in that case).
	UpdateSecretAtomic(ctx context.Context, projectID, id string, mutate func(current Secret) (Secret, error)) (Secret, error)
	DeleteSecret(ctx context.Context, projectID, id string) error // cascades versions
	// DeleteSecretIf atomically deletes the secret (cascading its versions) only
	// when cond returns nil for the currently stored value. A non-nil cond error
	// aborts the delete and is returned unchanged; ErrNoSuchSecret when absent.
	// Callers use it for a precondition (the API etag) so that a concurrent
	// mutation cannot land between the check and the delete.
	DeleteSecretIf(ctx context.Context, projectID, id string, cond func(Secret) error) error
	ListSecrets(ctx context.Context, projectID string) ([]Secret, error)

	CreateVersion(ctx context.Context, projectID string, v Version) error
	GetVersion(ctx context.Context, projectID, secretID, versionID string) (Version, error)
	ListVersions(ctx context.Context, projectID, secretID string) ([]Version, error)
	UpdateVersion(ctx context.Context, projectID string, v Version) error
	// UpdateVersionIf atomically reads the current version, calls mutate, and
	// writes it back — no other transition can interleave. A non-nil mutate
	// error aborts the write and is returned unchanged; ErrNoSuchVersion when
	// the version is absent. Used for the version etag precondition.
	UpdateVersionIf(ctx context.Context, projectID, secretID, versionID string, mutate func(current Version) (Version, error)) (Version, error)

	// NextVersion atomically allocates the next version number for a secret and
	// advances its counter, so concurrent AddVersion calls never collide.
	NextVersion(ctx context.Context, projectID, secretID string) (int, error)

	Reset(ctx context.Context)
}
