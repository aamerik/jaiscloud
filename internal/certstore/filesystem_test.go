package certstore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// selfSignedCert generates a minimal self-signed cert + key PEM pair for tests.
func selfSignedCert(t *testing.T, notAfter time.Time) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    clock.RealNow().Add(-time.Minute),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

func TestFilesystemCertStore_LoadEmpty(t *testing.T) {
	s, err := NewFilesystemCertStore(t.TempDir())
	require.NoError(t, err)

	_, err = s.Load(context.Background())
	require.ErrorIs(t, err, ErrNotFound)
}

func TestFilesystemCertStore_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemCertStore(dir)
	require.NoError(t, err)

	expiry := clock.RealNow().Add(10 * 365 * 24 * time.Hour).Truncate(time.Second)
	certPEM, keyPEM := selfSignedCert(t, expiry)

	ctx := context.Background()
	require.NoError(t, s.Save(ctx, &StoredCert{CertPEM: certPEM, KeyPEM: keyPEM, NotAfter: expiry}))

	got, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, certPEM, got.CertPEM)
	require.Equal(t, keyPEM, got.KeyPEM)
	require.WithinDuration(t, expiry, got.NotAfter, time.Second)
}

func TestFilesystemCertStore_AtomicWrite_NoTmpFileAfterSuccess(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemCertStore(dir)
	require.NoError(t, err)

	expiry := clock.RealNow().Add(time.Hour)
	certPEM, keyPEM := selfSignedCert(t, expiry)

	require.NoError(t, s.Save(context.Background(), &StoredCert{CertPEM: certPEM, KeyPEM: keyPEM, NotAfter: expiry}))

	// No .tmp files should remain after a successful write.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.NotContains(t, e.Name(), ".tmp", "stale .tmp file found: %s", e.Name())
	}
}

func TestFilesystemCertStore_EmptyStateDir_ReturnsError(t *testing.T) {
	_, err := NewFilesystemCertStore("")
	require.Error(t, err)
}

func TestFilesystemCertStore_DirPermissions(t *testing.T) {
	parent := t.TempDir()
	stateDir := filepath.Join(parent, "jcstate")

	s, err := NewFilesystemCertStore(stateDir)
	require.NoError(t, err)

	info, err := os.Stat(stateDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), "state dir must be 0700")

	// Save a cert and check file permissions.
	expiry := clock.RealNow().Add(time.Hour)
	certPEM, keyPEM := selfSignedCert(t, expiry)
	require.NoError(t, s.Save(context.Background(), &StoredCert{CertPEM: certPEM, KeyPEM: keyPEM, NotAfter: expiry}))

	for _, name := range []string{certFilename, keyFilename} {
		fi, err := os.Stat(filepath.Join(stateDir, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), "%s must be 0600", name)
	}
}

func TestFilesystemCertStore_MissingKeyReturnsNotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFilesystemCertStore(dir)
	require.NoError(t, err)

	// Write only the cert file, leave key missing.
	require.NoError(t, os.WriteFile(filepath.Join(dir, certFilename), []byte("not-pem"), 0o600))

	// Load should return ErrNotFound (key absent), not a parse error.
	_, err = s.Load(context.Background())
	require.ErrorIs(t, err, ErrNotFound)
}
