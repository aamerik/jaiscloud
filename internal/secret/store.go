// Package secret provides SecretsManager storage and business logic.
package secret

import (
	"context"
	"errors"
	"time"
)

var (
	ErrSecretNotFound  = errors.New("secret not found")
	ErrAlreadyExists   = errors.New("secret already exists")
	ErrSecretDeleted   = errors.New("secret is scheduled for deletion")
	ErrVersionNotFound = errors.New("secret version not found")
)

// SecretEntry holds metadata for a secret (not the value).
type SecretEntry struct {
	SecretID    string
	Name        string
	Description string
	KMSKeyID    string // empty = use account default key
	Tags        map[string]string
	DeletedAt   *time.Time // non-nil when scheduled for deletion
	CreatedAt   time.Time
	UpdatedAt   time.Time
	// Rotation fields.
	RotationEnabled      bool
	RotationLambdaARN    string
	AutoRotateAfterDays  int
	LastRotatedDate      *time.Time
	NextRotationDate     *time.Time
	LastAccessedDate     *time.Time
	// ResourcePolicy holds the resource-based policy JSON.
	ResourcePolicy string
}

// VersionEntry holds an encrypted version of a secret value.
type VersionEntry struct {
	SecretID     string
	VersionID    string
	SecretBinary []byte   // AES-GCM ciphertext (plaintext in lite/noop mode)
	IsBinary     bool     // true when caller stored SecretBinary (vs SecretString)
	Stages       []string // e.g. ["AWSCURRENT"], ["AWSPREVIOUS"]
	CreatedAt    time.Time
}

// SecretStore is the persistence interface for SecretsManager.
type SecretStore interface {
	// Secret CRUD
	CreateSecret(ctx context.Context, e SecretEntry) error
	GetSecret(ctx context.Context, secretID string) (SecretEntry, error)
	GetSecretByName(ctx context.Context, name string) (SecretEntry, error)
	UpdateSecret(ctx context.Context, e SecretEntry) error
	DeleteSecret(ctx context.Context, secretID string) error // hard delete
	ListSecrets(ctx context.Context) ([]SecretEntry, error)

	// Version operations
	PutVersion(ctx context.Context, v VersionEntry) error
	GetVersion(ctx context.Context, secretID, versionID string) (VersionEntry, error)
	GetVersionByStage(ctx context.Context, secretID, stage string) (VersionEntry, error)
	ListVersions(ctx context.Context, secretID string) ([]VersionEntry, error)
	UpdateVersionStages(ctx context.Context, secretID, versionID string, stages []string) error

	Reset()
}
