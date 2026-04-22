package certstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryCertStore(t *testing.T) {
	s := NewMemoryCertStore()
	ctx := context.Background()

	_, err := s.Load(ctx)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, s.Save(ctx, &StoredCert{
		CertPEM:  []byte("cert"),
		KeyPEM:   []byte("key"),
		NotAfter: time.Now().Add(time.Hour),
	}))

	// Memory store never persists — Load still returns ErrNotFound.
	_, err = s.Load(ctx)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStoredCertNeedsRenewal(t *testing.T) {
	c := &StoredCert{NotAfter: time.Now().Add(10 * 365 * 24 * time.Hour)}
	require.False(t, c.NeedsRenewal())

	expiring := &StoredCert{NotAfter: time.Now().Add(29 * 24 * time.Hour)}
	require.True(t, expiring.NeedsRenewal())

	past := &StoredCert{NotAfter: time.Now().Add(-time.Hour)}
	require.True(t, past.NeedsRenewal())
}
