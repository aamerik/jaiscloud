package certstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

func TestMemoryCertStore(t *testing.T) {
	s := NewMemoryCertStore()
	ctx := context.Background()

	_, err := s.Load(ctx)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, s.Save(ctx, &StoredCert{
		CertPEM:  []byte("cert"),
		KeyPEM:   []byte("key"),
		NotAfter: clock.RealNow().Add(time.Hour),
	}))

	// Memory store never persists — Load still returns ErrNotFound.
	_, err = s.Load(ctx)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStoredCertNeedsRenewal(t *testing.T) {
	c := &StoredCert{NotAfter: clock.RealNow().Add(10 * 365 * 24 * time.Hour)}
	require.False(t, c.NeedsRenewal())

	expiring := &StoredCert{NotAfter: clock.RealNow().Add(29 * 24 * time.Hour)}
	require.True(t, expiring.NeedsRenewal())

	past := &StoredCert{NotAfter: clock.RealNow().Add(-time.Hour)}
	require.True(t, past.NeedsRenewal())
}
