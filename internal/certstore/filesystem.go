package certstore

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	certFilename = "tls-cert.pem"
	keyFilename  = "tls-key.pem"
)

// FilesystemCertStore persists the TLS cert + key to two files in the state
// directory. Writes are atomic via .tmp → fsync → rename.
type FilesystemCertStore struct {
	dir string
}

func NewFilesystemCertStore(stateDir string) (*FilesystemCertStore, error) {
	if stateDir == "" {
		return nil, errors.New("certstore: state dir empty")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("certstore: mkdir %s: %w", stateDir, err)
	}
	return &FilesystemCertStore{dir: stateDir}, nil
}

func (s *FilesystemCertStore) Load(_ context.Context) (*StoredCert, error) {
	certPath := filepath.Join(s.dir, certFilename)
	keyPath := filepath.Join(s.dir, keyFilename)

	certPEM, err := os.ReadFile(certPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("certstore: read %s: %w", certPath, err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("certstore: read %s: %w", keyPath, err)
	}

	notAfter, err := parseNotAfter(certPEM)
	if err != nil {
		return nil, fmt.Errorf("certstore: parse cert expiry: %w", err)
	}
	return &StoredCert{
		CertPEM:  certPEM,
		KeyPEM:   keyPEM,
		NotAfter: notAfter,
	}, nil
}

func (s *FilesystemCertStore) Save(_ context.Context, c *StoredCert) error {
	if err := atomicWrite(filepath.Join(s.dir, certFilename), c.CertPEM, 0o600); err != nil {
		return err
	}
	if err := atomicWrite(filepath.Join(s.dir, keyFilename), c.KeyPEM, 0o600); err != nil {
		// Best-effort rollback so M2 migration retries cleanly on next boot.
		_ = os.Remove(filepath.Join(s.dir, certFilename))
		return err
	}
	return nil
}

// atomicWrite writes data to path via .tmp → fsync → rename.
func atomicWrite(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("certstore: open %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("certstore: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("certstore: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("certstore: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("certstore: rename %s→%s: %w", tmp, path, err)
	}
	return nil
}

// parseNotAfter extracts the NotAfter timestamp from a PEM-encoded certificate.
func parseNotAfter(certPEM []byte) (time.Time, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return time.Time{}, errors.New("no PEM block found")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, err
	}
	return cert.NotAfter, nil
}
