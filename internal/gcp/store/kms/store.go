// Package kms provides the KMS store. Key rings, crypto keys, and crypto-key
// versions live in dedicated jc_kms_* tables (mirroring AWS jc_kms_*).
package kms

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchKeyRing   = errors.New("NoSuchKeyRing")
	ErrNoSuchCryptoKey = errors.New("NoSuchCryptoKey")
	ErrNoSuchVersion   = errors.New("NoSuchVersion")
	ErrAlreadyExists   = errors.New("AlreadyExists")
)

// KeyRing is a KMS key ring.
type KeyRing struct {
	Location   string
	ID         string
	CreateTime time.Time
}

// CryptoKey is a KMS crypto key.
type CryptoKey struct {
	Location       string
	KeyRingID      string
	ID             string
	Purpose        string
	CreateTime     time.Time
	PrimaryVersion string
	Algorithm      string // version template algorithm (e.g. GOOGLE_SYMMETRIC_ENCRYPTION, RSA_SIGN_PKCS1_2048_SHA256)
}

// Version is a KMS crypto-key version with its own key material.
type Version struct {
	KeyID       string
	Version     string
	State       string
	Algorithm   string
	CreateTime  time.Time
	KeyMaterial []byte // DEK-wrapped symmetric/HMAC key (at rest)
	PrivateKey  []byte // DEK-wrapped PKCS8 private DER (asymmetric)
	PublicKey   []byte // DEK-wrapped PKIX public DER (asymmetric)
}

// Store is the KMS store.
type Store interface {
	CreateKeyRing(ctx context.Context, projectID, location, id string, kr KeyRing) error
	GetKeyRing(ctx context.Context, projectID, location, id string) (KeyRing, error)
	ListKeyRings(ctx context.Context, projectID, location string) ([]KeyRing, error)

	CreateCryptoKey(ctx context.Context, projectID, location, keyringID, id string, ck CryptoKey) error
	GetCryptoKey(ctx context.Context, projectID, location, keyringID, id string) (CryptoKey, error)
	ListCryptoKeys(ctx context.Context, projectID, location, keyringID string) ([]CryptoKey, error)

	// CreateVersion allocates the next version number and stores the version
	// with DEK-wrapped key material. Returns the assigned version string.
	CreateVersion(ctx context.Context, projectID, location, keyringID, keyID string, v Version) (string, error)
	GetVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) (Version, error)
	ListVersions(ctx context.Context, projectID, location, keyringID, keyID string) ([]Version, error)
	UpdateVersionState(ctx context.Context, projectID, location, keyringID, keyID, version, state string) error
	UpdatePrimaryVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) error

	// KeyMaterial returns the raw AES-256 key material for a version,
	// unwrapping the DEK-encrypted blob stored at rest.
	KeyMaterial(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error)

	// PrivateKey returns the raw PKCS8 private DER for an asymmetric version.
	PrivateKey(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error)

	// PublicKey returns the raw PKIX public DER for an asymmetric version.
	PublicKey(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error)

	// ServerDEK returns the global server DEK.
	ServerDEK(ctx context.Context) ([]byte, error)

	Reset(ctx context.Context)
}
