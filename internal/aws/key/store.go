package key

import (
	"context"
	"errors"
	"time"
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrAliasNotFound  = errors.New("alias not found")
	ErrGrantNotFound  = errors.New("grant not found")
	ErrKeyDisabled    = errors.New("key is disabled")
	ErrAlreadyExists  = errors.New("already exists")
)

// KeyEntry holds the persisted state of a KMS key.
type KeyEntry struct {
	KeyID           string
	Enabled         bool
	PendingDeletion bool      // true after ScheduleKeyDeletion until deletion date
	DeletionDate    time.Time // zero unless PendingDeletion is true
	Description     string
	KeyUsage        string // "ENCRYPT_DECRYPT" | "SIGN_VERIFY" | "GENERATE_VERIFY_MAC"
	KeySpec         string // "SYMMETRIC_DEFAULT" | "RSA_2048" | "ECC_NIST_P256" | "HMAC_256" | ...
	Origin          string // "AWS_KMS" | "EXTERNAL"
	Tags            map[string]string
	// KeyMaterial is the AES-GCM–encrypted symmetric key (32 bytes for SYMMETRIC_DEFAULT,
	// variable for HMAC). Nil for asymmetric keys that use PrivateKey instead.
	KeyMaterial []byte
	// PrivateKey holds the AES-GCM–encrypted DER-encoded PKCS8 private key for RSA/ECC keys.
	PrivateKey []byte
	// PublicKey holds the DER-encoded SubjectPublicKeyInfo public key for RSA/ECC keys (unencrypted).
	PublicKey []byte
	// MultiRegion indicates this is a multi-region key.
	MultiRegion bool
	// RotationEnabled tracks whether automatic key rotation is enabled.
	RotationEnabled bool
	// RotationPeriodInDays is the rotation period (0 = use default 365).
	RotationPeriodInDays int
	// PreviousKeyMaterials holds encrypted previous symmetric key materials for key rotation.
	// Decrypt tries each in order after the current material fails.
	PreviousKeyMaterials [][]byte
	// Policy holds the key policy JSON.
	Policy string
	// CreatedAt is the UTC creation timestamp.
	CreatedAt time.Time
}

// AliasEntry maps an alias name to a key ID.
type AliasEntry struct {
	AliasName   string
	TargetKeyID string
}

// GrantEntry records a KMS grant.
type GrantEntry struct {
	GrantID          string
	KeyID            string
	KeyArn           string
	GranteeARN       string
	RetiringPrincipal string
	Name             string
	Operations       []string
	Token            string
	IssuingAccount   string
	CreationDate     time.Time
}

// KeyStore is the persistence interface for KMS key metadata.
// MemoryKeyStore and PostgresKeyStore implement this.
type KeyStore interface {
	// Key CRUD
	CreateKey(ctx context.Context, e KeyEntry) error
	GetKey(ctx context.Context, keyID string) (KeyEntry, error)
	UpdateKey(ctx context.Context, e KeyEntry) error
	DeleteKey(ctx context.Context, keyID string) error
	ListKeys(ctx context.Context) ([]KeyEntry, error)

	// Alias operations
	CreateAlias(ctx context.Context, e AliasEntry) error
	GetAlias(ctx context.Context, aliasName string) (AliasEntry, error)
	DeleteAlias(ctx context.Context, aliasName string) error
	ListAliases(ctx context.Context, keyID string) ([]AliasEntry, error)

	// Grant operations
	CreateGrant(ctx context.Context, e GrantEntry) error
	GetGrant(ctx context.Context, grantID string) (GrantEntry, error)
	GetGrantByToken(ctx context.Context, token string) (GrantEntry, error)
	RevokeGrant(ctx context.Context, grantID string) error
	ListGrants(ctx context.Context, keyID string) ([]GrantEntry, error)

	// DEK bootstrap (one row per instance)
	LoadDEK(ctx context.Context) ([]byte, error)  // returns raw blob; ErrKeyNotFound if absent
	StoreDEK(ctx context.Context, blob []byte) error

	// Reset wipes all state (used by admin reset).
	Reset()
}
