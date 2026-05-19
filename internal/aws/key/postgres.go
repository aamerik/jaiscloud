package key

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

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
	data, err := json.Marshal(keyDataMap(e))
	if err != nil {
		return fmt.Errorf("kms postgres: marshal key data: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_kms_keys (account_id, region, key_id, key_data, key_material, enabled)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		e.AccountID, e.Region, e.KeyID, data, e.KeyMaterial, e.Enabled,
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
		SELECT account_id, region, key_id, key_data, key_material, enabled
		FROM jc_kms_keys WHERE key_id=$1`, keyID)
	return scanKey(row)
}

func (s *PostgresKeyStore) UpdateKey(ctx context.Context, e KeyEntry) error {
	data, err := json.Marshal(keyDataMap(e))
	if err != nil {
		return fmt.Errorf("kms postgres: marshal key data: %w", err)
	}
	ct, err := s.pool.Exec(ctx, `
		UPDATE jc_kms_keys
		SET key_data=$2, key_material=$3, enabled=$4, updated_at=now()
		WHERE account_id=$5 AND region=$6 AND key_id=$1`,
		e.KeyID, data, e.KeyMaterial, e.Enabled, e.AccountID, e.Region,
	)
	if err != nil {
		return fmt.Errorf("kms postgres: update key: %w", err)
	}
	if ct.RowsAffected() == 0 {
		// Fallback: try update without account/region scope (e.g. entry loaded before fields were set)
		ct, err = s.pool.Exec(ctx, `
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

func (s *PostgresKeyStore) ListKeys(ctx context.Context, accountID string) ([]KeyEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT account_id, region, key_id, key_data, key_material, enabled FROM jc_kms_keys WHERE ($1='' OR account_id=$1)`, accountID)
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
		INSERT INTO jc_kms_aliases (account_id, region, alias_name, target_key_id) VALUES ($1, $2, $3, $4)`,
		e.AccountID, e.Region, e.AliasName, e.TargetKeyID,
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
		SELECT account_id, region, alias_name, target_key_id FROM jc_kms_aliases WHERE alias_name=$1`, name,
	).Scan(&e.AccountID, &e.Region, &e.AliasName, &e.TargetKeyID)
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
		rows, err = s.pool.Query(ctx, `SELECT account_id, region, alias_name, target_key_id FROM jc_kms_aliases`)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT account_id, region, alias_name, target_key_id FROM jc_kms_aliases WHERE target_key_id=$1`, keyID)
	}
	if err != nil {
		return nil, fmt.Errorf("kms postgres: list aliases: %w", err)
	}
	defer rows.Close()
	var out []AliasEntry
	for rows.Next() {
		var a AliasEntry
		if err := rows.Scan(&a.AccountID, &a.Region, &a.AliasName, &a.TargetKeyID); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ─── Grant operations ─────────────────────────────────────────────────────────

func (s *PostgresKeyStore) CreateGrant(ctx context.Context, e GrantEntry) error {
	data, _ := json.Marshal(e)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_kms_grants (account_id, region, grant_id, key_id, grant_data) VALUES ($1, $2, $3, $4, $5)`,
		e.AccountID, e.Region, e.GrantID, e.KeyID, data,
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

func (s *PostgresKeyStore) GetGrantByToken(ctx context.Context, token string) (GrantEntry, error) {
	// Scan all grants and match by token field stored in JSONB.
	rows, err := s.pool.Query(ctx, `SELECT grant_id, key_id, grant_data FROM jc_kms_grants`)
	if err != nil {
		return GrantEntry{}, fmt.Errorf("kms postgres: get grant by token: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e GrantEntry
		var data []byte
		if err := rows.Scan(&e.GrantID, &e.KeyID, &data); err != nil {
			return GrantEntry{}, err
		}
		_ = json.Unmarshal(data, &e)
		if e.Token == token {
			return e, nil
		}
	}
	if err := rows.Err(); err != nil {
		return GrantEntry{}, fmt.Errorf("kms postgres: get grant by token scan: %w", err)
	}
	return GrantEntry{}, ErrGrantNotFound
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

func (s *PostgresKeyStore) ResetScope(account, region string) {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_kms_grants  WHERE account_id=$1 AND region=$2`, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_kms_aliases WHERE account_id=$1 AND region=$2`, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_kms_keys    WHERE account_id=$1 AND region=$2`, account, region)
}

func (s *PostgresKeyStore) ResetAccount(account string) {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_kms_grants  WHERE account_id=$1`, account)
	s.pool.Exec(ctx, `DELETE FROM jc_kms_aliases WHERE account_id=$1`, account)
	s.pool.Exec(ctx, `DELETE FROM jc_kms_keys    WHERE account_id=$1`, account)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

type scannable interface {
	Scan(dest ...any) error
}

func keyDataMap(e KeyEntry) map[string]any {
	return map[string]any{
		"description":            e.Description,
		"key_usage":              e.KeyUsage,
		"key_spec":               e.KeySpec,
		"origin":                 e.Origin,
		"tags":                   e.Tags,
		"pending_deletion":       e.PendingDeletion,
		"deletion_date":          e.DeletionDate,
		"private_key":            e.PrivateKey,
		"public_key":             e.PublicKey,
		"multi_region":           e.MultiRegion,
		"rotation_enabled":       e.RotationEnabled,
		"rotation_period_days":   e.RotationPeriodInDays,
		"previous_key_materials": e.PreviousKeyMaterials,
		"policy":                 e.Policy,
		"created_at":             e.CreatedAt,
	}
}

func scanKey(row scannable) (KeyEntry, error) {
	var e KeyEntry
	var data []byte
	if err := row.Scan(&e.AccountID, &e.Region, &e.KeyID, &data, &e.KeyMaterial, &e.Enabled); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return KeyEntry{}, ErrKeyNotFound
		}
		return KeyEntry{}, fmt.Errorf("kms postgres: scan key: %w", err)
	}
	var meta struct {
		Description          string            `json:"description"`
		KeyUsage             string            `json:"key_usage"`
		KeySpec              string            `json:"key_spec"`
		Origin               string            `json:"origin"`
		Tags                 map[string]string `json:"tags"`
		PendingDeletion      bool              `json:"pending_deletion"`
		DeletionDate         time.Time         `json:"deletion_date"`
		PrivateKey           []byte            `json:"private_key"`
		PublicKey            []byte            `json:"public_key"`
		MultiRegion          bool              `json:"multi_region"`
		RotationEnabled      bool              `json:"rotation_enabled"`
		RotationPeriodDays   int               `json:"rotation_period_days"`
		PreviousKeyMaterials [][]byte          `json:"previous_key_materials"`
		Policy               string            `json:"policy"`
		CreatedAt            time.Time         `json:"created_at"`
	}
	_ = json.Unmarshal(data, &meta)
	e.Description = meta.Description
	e.KeyUsage = meta.KeyUsage
	e.KeySpec = meta.KeySpec
	e.Origin = meta.Origin
	e.Tags = meta.Tags
	e.PendingDeletion = meta.PendingDeletion
	e.DeletionDate = meta.DeletionDate
	e.PrivateKey = meta.PrivateKey
	e.PublicKey = meta.PublicKey
	e.MultiRegion = meta.MultiRegion
	e.RotationEnabled = meta.RotationEnabled
	e.RotationPeriodInDays = meta.RotationPeriodDays
	e.PreviousKeyMaterials = meta.PreviousKeyMaterials
	e.Policy = meta.Policy
	e.CreatedAt = meta.CreatedAt
	return e, nil
}

func isPgUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// ─── Snapshotter ──────────────────────────────────────────────────────────────

type pgKMSSnap struct {
	Keys    []pgKMSKeyRow   `json:"keys"`
	Aliases []pgKMSAliasRow `json:"aliases"`
	Grants  []pgKMSGrantRow `json:"grants"`
}

type pgKMSKeyRow struct {
	AccountID   string `json:"account_id"`
	Region      string `json:"region"`
	KeyID       string `json:"key_id"`
	KeyData     []byte `json:"key_data"`
	KeyMaterial []byte `json:"key_material"`
	Enabled     bool   `json:"enabled"`
}

type pgKMSAliasRow struct {
	AccountID   string `json:"account_id"`
	Region      string `json:"region"`
	AliasName   string `json:"alias_name"`
	TargetKeyID string `json:"target_key_id"`
}

type pgKMSGrantRow struct {
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
	GrantID   string `json:"grant_id"`
	KeyID     string `json:"key_id"`
	GrantData []byte `json:"grant_data"`
}

func (s *PostgresKeyStore) IsEmpty(ctx context.Context) (bool, error) {
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jc_kms_keys`).Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}

func (s *PostgresKeyStore) Snapshot(ctx context.Context, w io.Writer) error {
	rows, err := s.pool.Query(ctx, `SELECT account_id, region, key_id, key_data, key_material, enabled FROM jc_kms_keys ORDER BY account_id, region, key_id`)
	if err != nil {
		return fmt.Errorf("kms snapshot keys: %w", err)
	}
	defer rows.Close()
	var keys []pgKMSKeyRow
	for rows.Next() {
		var r pgKMSKeyRow
		if err := rows.Scan(&r.AccountID, &r.Region, &r.KeyID, &r.KeyData, &r.KeyMaterial, &r.Enabled); err != nil {
			return err
		}
		keys = append(keys, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	arows, err := s.pool.Query(ctx, `SELECT account_id, region, alias_name, target_key_id FROM jc_kms_aliases ORDER BY account_id, region, alias_name`)
	if err != nil {
		return fmt.Errorf("kms snapshot aliases: %w", err)
	}
	defer arows.Close()
	var aliases []pgKMSAliasRow
	for arows.Next() {
		var r pgKMSAliasRow
		if err := arows.Scan(&r.AccountID, &r.Region, &r.AliasName, &r.TargetKeyID); err != nil {
			return err
		}
		aliases = append(aliases, r)
	}
	if err := arows.Err(); err != nil {
		return err
	}

	grows, err := s.pool.Query(ctx, `SELECT account_id, region, grant_id, key_id, grant_data FROM jc_kms_grants ORDER BY account_id, region, grant_id`)
	if err != nil {
		return fmt.Errorf("kms snapshot grants: %w", err)
	}
	defer grows.Close()
	var grants []pgKMSGrantRow
	for grows.Next() {
		var r pgKMSGrantRow
		if err := grows.Scan(&r.AccountID, &r.Region, &r.GrantID, &r.KeyID, &r.GrantData); err != nil {
			return err
		}
		grants = append(grants, r)
	}
	if err := grows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(pgKMSSnap{Keys: keys, Aliases: aliases, Grants: grants})
}

func (s *PostgresKeyStore) Restore(ctx context.Context, r io.Reader) error {
	var snap pgKMSSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return fmt.Errorf("kms restore decode: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("kms restore begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_grants`); err != nil {
		return fmt.Errorf("kms restore delete grants: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_aliases`); err != nil {
		return fmt.Errorf("kms restore delete aliases: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_keys`); err != nil {
		return fmt.Errorf("kms restore delete keys: %w", err)
	}
	for _, k := range snap.Keys {
		if _, err := tx.Exec(ctx,
			`INSERT INTO jc_kms_keys (account_id, region, key_id, key_data, key_material, enabled) VALUES ($1,$2,$3,$4,$5,$6)`,
			k.AccountID, k.Region, k.KeyID, json.RawMessage(k.KeyData), k.KeyMaterial, k.Enabled,
		); err != nil {
			return fmt.Errorf("kms restore insert key %s: %w", k.KeyID, err)
		}
	}
	for _, a := range snap.Aliases {
		if _, err := tx.Exec(ctx,
			`INSERT INTO jc_kms_aliases (account_id, region, alias_name, target_key_id) VALUES ($1,$2,$3,$4)`,
			a.AccountID, a.Region, a.AliasName, a.TargetKeyID,
		); err != nil {
			return fmt.Errorf("kms restore insert alias %s: %w", a.AliasName, err)
		}
	}
	for _, g := range snap.Grants {
		if _, err := tx.Exec(ctx,
			`INSERT INTO jc_kms_grants (account_id, region, grant_id, key_id, grant_data) VALUES ($1,$2,$3,$4,$5)`,
			g.AccountID, g.Region, g.GrantID, g.KeyID, json.RawMessage(g.GrantData),
		); err != nil {
			return fmt.Errorf("kms restore insert grant %s: %w", g.GrantID, err)
		}
	}
	return tx.Commit(ctx)
}
