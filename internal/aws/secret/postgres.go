package secret

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSecretStore is a PostgreSQL-backed SecretStore.
type PostgresSecretStore struct {
	pool *pgxpool.Pool
}

func NewPostgresSecretStore(pool *pgxpool.Pool) *PostgresSecretStore {
	return &PostgresSecretStore{pool: pool}
}

// ─── Secret CRUD ──────────────────────────────────────────────────────────────

func (s *PostgresSecretStore) CreateSecret(ctx context.Context, e SecretEntry) error {
	data, _ := json.Marshal(secretMeta(e))
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_sm_secrets (account_id, region, secret_id, name, secret_data)
		VALUES ($1, $2, $3, $4, $5)`,
		e.AccountID, e.Region, e.SecretID, e.Name, data,
	)
	if err != nil {
		if isPgUnique(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("sm postgres: create secret: %w", err)
	}
	return nil
}

func (s *PostgresSecretStore) GetSecret(ctx context.Context, secretID string) (SecretEntry, error) {
	return s.scanSecret(ctx, `
		SELECT account_id, region, secret_id, name, secret_data, deleted_at, created_at, updated_at
		FROM jc_sm_secrets WHERE secret_id=$1`, secretID)
}

func (s *PostgresSecretStore) GetSecretByName(ctx context.Context, name string) (SecretEntry, error) {
	return s.scanSecret(ctx, `
		SELECT account_id, region, secret_id, name, secret_data, deleted_at, created_at, updated_at
		FROM jc_sm_secrets WHERE name=$1`, name)
}

func (s *PostgresSecretStore) UpdateSecret(ctx context.Context, e SecretEntry) error {
	data, _ := json.Marshal(secretMeta(e))
	ct, err := s.pool.Exec(ctx, `
		UPDATE jc_sm_secrets
		SET secret_data=$2, deleted_at=$3, updated_at=now()
		WHERE secret_id=$1`,
		e.SecretID, data, e.DeletedAt,
	)
	if err != nil {
		return fmt.Errorf("sm postgres: update secret: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrSecretNotFound
	}
	return nil
}

func (s *PostgresSecretStore) DeleteSecret(ctx context.Context, secretID string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM jc_sm_secrets WHERE secret_id=$1`, secretID)
	if err != nil {
		return fmt.Errorf("sm postgres: delete secret: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrSecretNotFound
	}
	return nil
}

func (s *PostgresSecretStore) ListSecrets(ctx context.Context) ([]SecretEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT account_id, region, secret_id, name, secret_data, deleted_at, created_at, updated_at
		FROM jc_sm_secrets`)
	if err != nil {
		return nil, fmt.Errorf("sm postgres: list secrets: %w", err)
	}
	defer rows.Close()
	var out []SecretEntry
	for rows.Next() {
		e, err := scanSecretRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─── Version operations ───────────────────────────────────────────────────────

func (s *PostgresSecretStore) PutVersion(ctx context.Context, v VersionEntry) error {
	// Derive account/region from the parent secret if not explicitly set.
	if v.AccountID == "" {
		s.pool.QueryRow(ctx, `SELECT account_id, region FROM jc_sm_secrets WHERE secret_id=$1`, v.SecretID).
			Scan(&v.AccountID, &v.Region)
	}

	// Demote current AWSCURRENT to AWSPREVIOUS in the same TX.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sm postgres: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if containsStage(v.Stages, "AWSCURRENT") {
		_, err = tx.Exec(ctx, `
			UPDATE jc_sm_versions
			SET stages = array_replace(stages, 'AWSCURRENT', 'AWSPREVIOUS')
			WHERE secret_id=$1 AND $2 != ALL(stages) AND 'AWSCURRENT' = ANY(stages)`,
			v.SecretID, v.VersionID,
		)
		if err != nil {
			return fmt.Errorf("sm postgres: demote current version: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO jc_sm_versions (account_id, region, secret_id, version_id, secret_binary, stages, is_binary)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (account_id, region, secret_id, version_id) DO UPDATE
		  SET secret_binary=$5, stages=$6, is_binary=$7`,
		v.AccountID, v.Region, v.SecretID, v.VersionID, v.SecretBinary, v.Stages, v.IsBinary,
	)
	if err != nil {
		return fmt.Errorf("sm postgres: put version: %w", err)
	}

	// 3.7.2 — prune unlabeled versions (empty Stages array) beyond 100 oldest.
	_, err = tx.Exec(ctx, `
		DELETE FROM jc_sm_versions
		WHERE secret_id=$1
		  AND cardinality(stages) = 0
		  AND version_id NOT IN (
		    SELECT version_id FROM jc_sm_versions
		    WHERE secret_id=$1 AND cardinality(stages) = 0
		    ORDER BY created_at DESC
		    LIMIT 100
		  )`,
		v.SecretID,
	)
	if err != nil {
		return fmt.Errorf("sm postgres: prune versions: %w", err)
	}

	return tx.Commit(ctx)
}

func (s *PostgresSecretStore) GetVersion(ctx context.Context, secretID, versionID string) (VersionEntry, error) {
	var v VersionEntry
	var stages []string
	err := s.pool.QueryRow(ctx, `
		SELECT account_id, region, secret_id, version_id, secret_binary, stages, is_binary, created_at
		FROM jc_sm_versions WHERE secret_id=$1 AND version_id=$2`,
		secretID, versionID,
	).Scan(&v.AccountID, &v.Region, &v.SecretID, &v.VersionID, &v.SecretBinary, &stages, &v.IsBinary, &v.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return VersionEntry{}, ErrVersionNotFound
	}
	if err != nil {
		return VersionEntry{}, fmt.Errorf("sm postgres: get version: %w", err)
	}
	v.Stages = stages
	return v, nil
}

func (s *PostgresSecretStore) GetVersionByStage(ctx context.Context, secretID, stage string) (VersionEntry, error) {
	var v VersionEntry
	var stages []string
	err := s.pool.QueryRow(ctx, `
		SELECT account_id, region, secret_id, version_id, secret_binary, stages, is_binary, created_at
		FROM jc_sm_versions WHERE secret_id=$1 AND $2=ANY(stages)
		ORDER BY created_at DESC LIMIT 1`,
		secretID, stage,
	).Scan(&v.AccountID, &v.Region, &v.SecretID, &v.VersionID, &v.SecretBinary, &stages, &v.IsBinary, &v.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return VersionEntry{}, ErrVersionNotFound
	}
	if err != nil {
		return VersionEntry{}, fmt.Errorf("sm postgres: get version by stage: %w", err)
	}
	v.Stages = stages
	return v, nil
}

func (s *PostgresSecretStore) ListVersions(ctx context.Context, secretID string) ([]VersionEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT account_id, region, secret_id, version_id, secret_binary, stages, is_binary, created_at
		FROM jc_sm_versions WHERE secret_id=$1 ORDER BY created_at DESC`,
		secretID,
	)
	if err != nil {
		return nil, fmt.Errorf("sm postgres: list versions: %w", err)
	}
	defer rows.Close()
	var out []VersionEntry
	for rows.Next() {
		var v VersionEntry
		var stages []string
		if err := rows.Scan(&v.AccountID, &v.Region, &v.SecretID, &v.VersionID, &v.SecretBinary, &stages, &v.IsBinary, &v.CreatedAt); err != nil {
			return nil, err
		}
		v.Stages = stages
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *PostgresSecretStore) UpdateVersionStages(ctx context.Context, secretID, versionID string, stages []string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE jc_sm_versions SET stages=$3 WHERE secret_id=$1 AND version_id=$2`,
		secretID, versionID, stages,
	)
	if err != nil {
		return fmt.Errorf("sm postgres: update version stages: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrVersionNotFound
	}
	return nil
}

func (s *PostgresSecretStore) DeleteVersionsByIDs(ctx context.Context, secretID string, versionIDs []string) error {
	if len(versionIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		DELETE FROM jc_sm_versions
		WHERE secret_id=$1 AND version_id = ANY($2)`,
		secretID, versionIDs,
	)
	if err != nil {
		return fmt.Errorf("sm postgres: delete versions: %w", err)
	}
	return nil
}

func (s *PostgresSecretStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_sm_versions`)
	s.pool.Exec(ctx, `DELETE FROM jc_sm_secrets`)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (s *PostgresSecretStore) scanSecret(ctx context.Context, query string, arg any) (SecretEntry, error) {
	row := s.pool.QueryRow(ctx, query, arg)
	e, err := scanSecretRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return SecretEntry{}, ErrSecretNotFound
	}
	return e, err
}

type pgScanner interface {
	Scan(dest ...any) error
}

func scanSecretRow(row pgScanner) (SecretEntry, error) {
	var e SecretEntry
	var data []byte
	var deletedAt *time.Time
	if err := row.Scan(&e.AccountID, &e.Region, &e.SecretID, &e.Name, &data, &deletedAt, &e.CreatedAt, &e.UpdatedAt); err != nil {
		return SecretEntry{}, err
	}
	e.DeletedAt = deletedAt
	var meta struct {
		Description         string            `json:"description"`
		KMSKeyID            string            `json:"kms_key_id"`
		Tags                map[string]string `json:"tags"`
		RotationEnabled     bool              `json:"rotation_enabled"`
		RotationLambdaARN   string            `json:"rotation_lambda_arn"`
		AutoRotateAfterDays int               `json:"auto_rotate_after_days"`
		LastRotatedDate     *time.Time        `json:"last_rotated_date"`
		NextRotationDate    *time.Time        `json:"next_rotation_date"`
		LastAccessedDate    *time.Time        `json:"last_accessed_date"`
		ResourcePolicy      string            `json:"resource_policy"`
	}
	_ = json.Unmarshal(data, &meta)
	e.Description = meta.Description
	e.KMSKeyID = meta.KMSKeyID
	e.Tags = meta.Tags
	e.RotationEnabled = meta.RotationEnabled
	e.RotationLambdaARN = meta.RotationLambdaARN
	e.AutoRotateAfterDays = meta.AutoRotateAfterDays
	e.LastRotatedDate = meta.LastRotatedDate
	e.NextRotationDate = meta.NextRotationDate
	e.LastAccessedDate = meta.LastAccessedDate
	e.ResourcePolicy = meta.ResourcePolicy
	return e, nil
}

func secretMeta(e SecretEntry) map[string]any {
	return map[string]any{
		"description":            e.Description,
		"kms_key_id":             e.KMSKeyID,
		"tags":                   e.Tags,
		"rotation_enabled":       e.RotationEnabled,
		"rotation_lambda_arn":    e.RotationLambdaARN,
		"auto_rotate_after_days": e.AutoRotateAfterDays,
		"last_rotated_date":      e.LastRotatedDate,
		"next_rotation_date":     e.NextRotationDate,
		"last_accessed_date":     e.LastAccessedDate,
		"resource_policy":        e.ResourcePolicy,
	}
}

func isPgUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
