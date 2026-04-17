package key

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresKeyStore is a PostgreSQL-backed KeyStore.
// It shares the pool (and search_path) with PostgresResourceStore.
type PostgresKeyStore struct {
	pool *pgxpool.Pool
	dek  []byte // server DEK used to wrap/unwrap per-key material
}

// NewPostgresKeyStore creates a PostgresKeyStore that uses the given pool.
// dek is the server data-encryption key used to protect per-key material at rest.
func NewPostgresKeyStore(pool *pgxpool.Pool, dek []byte) *PostgresKeyStore {
	return &PostgresKeyStore{pool: pool, dek: dek}
}

// ─── Key CRUD ─────────────────────────────────────────────────────────────────

func (s *PostgresKeyStore) CreateKey(ctx context.Context, e KeyEntry) error {
	data, err := json.Marshal(map[string]any{
		"description": e.Description,
		"key_usage":   e.KeyUsage,
		"key_spec":    e.KeySpec,
		"origin":      e.Origin,
		"tags":        e.Tags,
	})
	if err != nil {
		return fmt.Errorf("kms postgres: marshal key data: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_kms_keys (key_id, key_data, key_material, enabled)
		VALUES ($1, $2, $3, $4)`,
		e.KeyID, data, e.KeyMaterial, e.Enabled,
	)
	if err != nil {
		if isPgUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("kms postgres: create key: %w", err)
	}
	return nil
}

func (s *PostgresKeyStore) GetKey(ctx context.Context, keyID string) (KeyEntry, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT key_id, key_data, key_material, enabled
		FROM jc_kms_keys WHERE key_id=$1`, keyID)
	return scanKey(row)
}

func (s *PostgresKeyStore) UpdateKey(ctx context.Context, e KeyEntry) error {
	data, err := json.Marshal(map[string]any{
		"description": e.Description,
		"key_usage":   e.KeyUsage,
		"key_spec":    e.KeySpec,
		"origin":      e.Origin,
		"tags":        e.Tags,
	})
	if err != nil {
		return fmt.Errorf("kms postgres: marshal key data: %w", err)
	}
	ct, err := s.pool.Exec(ctx, `
		UPDATE jc_kms_keys
		SET key_data=$2, key_material=$3, enabled=$4, updated_at=now()
		WHERE key_id=$1`,
		e.KeyID, data, e.KeyMaterial, e.Enabled,
	)
	if err != nil {
		return fmt.Errorf("kms postgres: update key: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrKeyNotFound
	}
	return nil
}

func (s *PostgresKeyStore) DeleteKey(ctx context.Context, keyID string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM jc_kms_keys WHERE key_id=$1`, keyID)
	if err != nil {
		return fmt.Errorf("kms postgres: delete key: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrKeyNotFound
	}
	return nil
}

func (s *PostgresKeyStore) ListKeys(ctx context.Context) ([]KeyEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT key_id, key_data, key_material, enabled FROM jc_kms_keys`)
	if err != nil {
		return nil, fmt.Errorf("kms postgres: list keys: %w", err)
	}
	defer rows.Close()
	var out []KeyEntry
	for rows.Next() {
		e, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─── Alias operations ─────────────────────────────────────────────────────────

func (s *PostgresKeyStore) CreateAlias(ctx context.Context, e AliasEntry) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_kms_aliases (alias_name, target_key_id) VALUES ($1, $2)`,
		e.AliasName, e.TargetKeyID,
	)
	if err != nil {
		if isPgUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("kms postgres: create alias: %w", err)
	}
	return nil
}

func (s *PostgresKeyStore) GetAlias(ctx context.Context, name string) (AliasEntry, error) {
	var e AliasEntry
	err := s.pool.QueryRow(ctx, `
		SELECT alias_name, target_key_id FROM jc_kms_aliases WHERE alias_name=$1`, name,
	).Scan(&e.AliasName, &e.TargetKeyID)
	if errors.Is(err, pgx.ErrNoRows) {
		return AliasEntry{}, ErrAliasNotFound
	}
	if err != nil {
		return AliasEntry{}, fmt.Errorf("kms postgres: get alias: %w", err)
	}
	return e, nil
}

func (s *PostgresKeyStore) DeleteAlias(ctx context.Context, name string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM jc_kms_aliases WHERE alias_name=$1`, name)
	if err != nil {
		return fmt.Errorf("kms postgres: delete alias: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrAliasNotFound
	}
	return nil
}

func (s *PostgresKeyStore) ListAliases(ctx context.Context, keyID string) ([]AliasEntry, error) {
	var rows pgx.Rows
	var err error
	if keyID == "" {
		rows, err = s.pool.Query(ctx, `SELECT alias_name, target_key_id FROM jc_kms_aliases`)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT alias_name, target_key_id FROM jc_kms_aliases WHERE target_key_id=$1`, keyID)
	}
	if err != nil {
		return nil, fmt.Errorf("kms postgres: list aliases: %w", err)
	}
	defer rows.Close()
	var out []AliasEntry
	for rows.Next() {
		var a AliasEntry
		if err := rows.Scan(&a.AliasName, &a.TargetKeyID); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ─── Grant operations ─────────────────────────────────────────────────────────

func (s *PostgresKeyStore) CreateGrant(ctx context.Context, e GrantEntry) error {
	data, _ := json.Marshal(map[string]any{
		"grantee_arn": e.GranteeARN,
		"operations":  e.Operations,
		"token":       e.Token,
	})
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_kms_grants (grant_id, key_id, grant_data) VALUES ($1, $2, $3)`,
		e.GrantID, e.KeyID, data,
	)
	if err != nil {
		if isPgUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("kms postgres: create grant: %w", err)
	}
	return nil
}

func (s *PostgresKeyStore) GetGrant(ctx context.Context, grantID string) (GrantEntry, error) {
	var e GrantEntry
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT grant_id, key_id, grant_data FROM jc_kms_grants WHERE grant_id=$1`, grantID,
	).Scan(&e.GrantID, &e.KeyID, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return GrantEntry{}, ErrGrantNotFound
	}
	if err != nil {
		return GrantEntry{}, fmt.Errorf("kms postgres: get grant: %w", err)
	}
	_ = json.Unmarshal(data, &e)
	return e, nil
}

func (s *PostgresKeyStore) RevokeGrant(ctx context.Context, grantID string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM jc_kms_grants WHERE grant_id=$1`, grantID)
	if err != nil {
		return fmt.Errorf("kms postgres: revoke grant: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrGrantNotFound
	}
	return nil
}

func (s *PostgresKeyStore) ListGrants(ctx context.Context, keyID string) ([]GrantEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT grant_id, key_id, grant_data FROM jc_kms_grants WHERE key_id=$1`, keyID)
	if err != nil {
		return nil, fmt.Errorf("kms postgres: list grants: %w", err)
	}
	defer rows.Close()
	var out []GrantEntry
	for rows.Next() {
		var e GrantEntry
		var data []byte
		if err := rows.Scan(&e.GrantID, &e.KeyID, &data); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(data, &e)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─── DEK bootstrap ────────────────────────────────────────────────────────────

func (s *PostgresKeyStore) LoadDEK(ctx context.Context) ([]byte, error) {
	var blob []byte
	err := s.pool.QueryRow(ctx, `SELECT dek_blob FROM jc_kms_dek WHERE id=1`).Scan(&blob)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("kms postgres: load dek: %w", err)
	}
	return blob, nil
}

func (s *PostgresKeyStore) StoreDEK(ctx context.Context, blob []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_kms_dek (id, dek_blob) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET dek_blob=$1`, blob)
	if err != nil {
		return fmt.Errorf("kms postgres: store dek: %w", err)
	}
	return nil
}

func (s *PostgresKeyStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_kms_grants`)
	s.pool.Exec(ctx, `DELETE FROM jc_kms_aliases`)
	s.pool.Exec(ctx, `DELETE FROM jc_kms_keys`)
	// Do NOT wipe jc_kms_dek — the DEK is a server-level secret, not user state.
}

// ─── helpers ──────────────────────────────────────────────────────────────────

type scannable interface {
	Scan(dest ...any) error
}

func scanKey(row scannable) (KeyEntry, error) {
	var e KeyEntry
	var data []byte
	if err := row.Scan(&e.KeyID, &data, &e.KeyMaterial, &e.Enabled); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return KeyEntry{}, ErrKeyNotFound
		}
		return KeyEntry{}, fmt.Errorf("kms postgres: scan key: %w", err)
	}
	var meta struct {
		Description string            `json:"description"`
		KeyUsage    string            `json:"key_usage"`
		KeySpec     string            `json:"key_spec"`
		Origin      string            `json:"origin"`
		Tags        map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(data, &meta)
	e.Description = meta.Description
	e.KeyUsage = meta.KeyUsage
	e.KeySpec = meta.KeySpec
	e.Origin = meta.Origin
	e.Tags = meta.Tags
	return e, nil
}

func isPgUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
