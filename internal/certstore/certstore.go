// Package certstore persists the server's self-signed TLS certificate so that
// the same key pair is reused across restarts instead of being regenerated each
// time (which would break clients that pinned the certificate).
package certstore

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Load when no certificate has been stored yet.
var ErrNotFound = errors.New("certstore: certificate not found")

// renewThreshold is how far before expiry a stored cert is considered stale
// and should be regenerated.
const renewThreshold = 30 * 24 * time.Hour

// StoredCert holds a PEM-encoded certificate and its matching private key.
type StoredCert struct {
	CertPEM  []byte
	KeyPEM   []byte
	NotAfter time.Time
}

// NeedsRenewal reports whether the cert should be regenerated (missing or
// expiring within renewThreshold).
func (c *StoredCert) NeedsRenewal() bool {
	return time.Until(c.NotAfter) < renewThreshold
}

// CertStore is the persistence interface for the server TLS certificate.
type CertStore interface {
	// Load returns the stored certificate, or ErrNotFound if none exists yet.
	Load(ctx context.Context) (*StoredCert, error)
	// Save persists a certificate. Overwrites any existing entry.
	Save(ctx context.Context, c *StoredCert) error
}
