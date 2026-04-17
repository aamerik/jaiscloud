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
	KeyID          string
	Enabled        bool
	PendingDeletion bool      // true after ScheduleKeyDeletion until deletion date
	DeletionDate   time.Time // zero unless PendingDeletion is true
	Description    string
	KeyUsage       string // "ENCRYPT_DECRYPT" | "SIGN_VERIFY"
	KeySpec        string // "SYMMETRIC_DEFAULT" | "RSA_2048" | ...
	Origin         string // "AWS_KMS" | "EXTERNAL"
	Tags           map[string]string
	// KeyMaterial is the AES-GCM–encrypted 32-byte data key used for
	// Encrypt/Decrypt operations on this logical KMS key.
	// Nil for metadata-only (pending import) keys.
	KeyMaterial []byte
}

// AliasEntry maps an alias name to a key ID.
type AliasEntry struct {
	AliasName   string
	TargetKeyID string
}

// GrantEntry records a KMS grant.
type GrantEntry struct {
	GrantID    string
	KeyID      string
	GranteeARN string
	Operations []string
	Token      string
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
	RevokeGrant(ctx context.Context, grantID string) error
	ListGrants(ctx context.Context, keyID string) ([]GrantEntry, error)

	// DEK bootstrap (one row per instance)
	LoadDEK(ctx context.Context) ([]byte, error)  // returns raw blob; ErrKeyNotFound if absent
	StoreDEK(ctx context.Context, blob []byte) error

	// Reset wipes all state (used by admin reset).
	Reset()
}
