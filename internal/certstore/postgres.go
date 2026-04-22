package certstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCertStore persists a single TLS certificate row in the jc_tls_cert
// table. The table is created by migration 011_tls_cert.sql.
type PostgresCertStore struct {
	pool *pgxpool.Pool
}

func NewPostgresCertStore(pool *pgxpool.Pool) *PostgresCertStore {
	return &PostgresCertStore{pool: pool}
}

func (s *PostgresCertStore) Load(ctx context.Context) (*StoredCert, error) {
	var certPEM, keyPEM []byte
	var notAfter time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT cert_pem, key_pem, not_after FROM jc_tls_cert WHERE id = 'singleton'`,
	).Scan(&certPEM, &keyPEM, &notAfter)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &StoredCert{CertPEM: certPEM, KeyPEM: keyPEM, NotAfter: notAfter}, nil
}

func (s *PostgresCertStore) Save(ctx context.Context, c *StoredCert) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_tls_cert (id, cert_pem, key_pem, not_after)
		VALUES ('singleton', $1, $2, $3)
		ON CONFLICT (id) DO UPDATE
		  SET cert_pem  = EXCLUDED.cert_pem,
		      key_pem   = EXCLUDED.key_pem,
		      not_after = EXCLUDED.not_after,
		      created_at = now()
	`, c.CertPEM, c.KeyPEM, c.NotAfter)
	return err
}
