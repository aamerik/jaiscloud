// Package secret provides the Secret Manager store. Secrets and versions live
// in dedicated jc_sm_secrets / jc_sm_versions tables (mirroring AWS
// jc_sm_secrets / jc_sm_versions).
package secretmanager

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchSecret  = errors.New("NoSuchSecret")
	ErrNoSuchVersion = errors.New("NoSuchVersion")
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Rotation is a secret's automatic-rotation schedule. NextRotationTime is an
// RFC3339 timestamp; RotationPeriod is a duration string (e.g. "3600s").
type Rotation struct {
	NextRotationTime string `json:"nextRotationTime"`
	RotationPeriod   string `json:"rotationPeriod"`
}

// Secret is Secret Manager secret metadata.
type Secret struct {
	ID             string
	Labels         map[string]string
	CreateTime     time.Time
	NextVer        int
	Rotation       *Rotation      // nil when rotation is disabled
	VersionAliases map[string]int // alias name → version number, nil when none
}

// Version is a Secret Manager secret version.
type Version struct {
	SecretID   string
	VersionID  string
	State      string
	CreateTime time.Time
	Data       string // base64 payload
}

// Store is the Secret Manager store.
type Store interface {
	CreateSecret(ctx context.Context, projectID, id string, s Secret) error
	GetSecret(ctx context.Context, projectID, id string) (Secret, error)
	UpdateSecret(ctx context.Context, projectID, id string, s Secret) error
	DeleteSecret(ctx context.Context, projectID, id string) error // cascades versions
	ListSecrets(ctx context.Context, projectID string) ([]Secret, error)

	CreateVersion(ctx context.Context, projectID string, v Version) error
	GetVersion(ctx context.Context, projectID, secretID, versionID string) (Version, error)
	ListVersions(ctx context.Context, projectID, secretID string) ([]Version, error)
	UpdateVersion(ctx context.Context, projectID string, v Version) error

	// NextVersion atomically allocates the next version number for a secret and
	// advances its counter, so concurrent AddVersion calls never collide.
	NextVersion(ctx context.Context, projectID, secretID string) (int, error)

	Reset(ctx context.Context)
}
