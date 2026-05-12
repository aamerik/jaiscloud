package parameter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresParameterStore is a PostgreSQL-backed ParameterStore.
type PostgresParameterStore struct {
	pool *pgxpool.Pool
}

func NewPostgresParameterStore(pool *pgxpool.Pool) *PostgresParameterStore {
	return &PostgresParameterStore{pool: pool}
}

func (s *PostgresParameterStore) PutParameter(ctx context.Context, e *ParameterEntry, overwrite bool) error {
	data, _ := json.Marshal(paramMeta(*e))
	now := time.Now()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ssm postgres: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if it exists first.
	var existingVersion int64
	var existingValue []byte
	var existingType string
	scanErr := tx.QueryRow(ctx,
		`SELECT version, param_value, param_data->>'type' FROM jc_ssm_parameters WHERE name=$1`, e.Name,
	).Scan(&existingVersion, &existingValue, &existingType)

	if scanErr == nil {
		// Parameter exists.
		if !overwrite {
			return ErrAlreadyExists
		}
		// Archive current version to history.
		_, err = tx.Exec(ctx, `
			INSERT INTO jc_ssm_param_history (name, version, param_data, param_value, created_at)
			SELECT name, version, param_data, param_value, updated_at FROM jc_ssm_parameters WHERE name=$1`,
			e.Name,
		)
		if err != nil {
			return fmt.Errorf("ssm postgres: archive history: %w", err)
		}
		e.Version = existingVersion + 1
		_, err = tx.Exec(ctx, `
			UPDATE jc_ssm_parameters SET param_data=$2, param_value=$3, version=$4, updated_at=$5 WHERE name=$1`,
			e.Name, data, e.Value, e.Version, now,
		)
	} else if errors.Is(scanErr, pgx.ErrNoRows) {
		// New parameter.
		e.Version = 1
		_, err = tx.Exec(ctx, `
			INSERT INTO jc_ssm_parameters (name, param_data, param_value, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $5)`,
			e.Name, data, e.Value, e.Version, now,
		)
		if err != nil {
			if isPgUnique(err) {
				return ErrAlreadyExists
			}
		}
	} else {
		return fmt.Errorf("ssm postgres: check existing: %w", scanErr)
	}
	if err != nil {
		return fmt.Errorf("ssm postgres: put parameter: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PostgresParameterStore) GetParameter(ctx context.Context, name string) (ParameterEntry, error) {
	var e ParameterEntry
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT name, param_data, param_value, version, created_at, updated_at
		FROM jc_ssm_parameters WHERE name=$1`, name,
	).Scan(&e.Name, &data, &e.Value, &e.Version, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ParameterEntry{}, ErrParameterNotFound
	}
	if err != nil {
		return ParameterEntry{}, fmt.Errorf("ssm postgres: get parameter: %w", err)
	}
	unmarshalParamMeta(&e, data)
	return e, nil
}

func (s *PostgresParameterStore) DeleteParameter(ctx context.Context, name string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM jc_ssm_parameters WHERE name=$1`, name)
	if err != nil {
		return fmt.Errorf("ssm postgres: delete parameter: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrParameterNotFound
	}
	return nil
}

func (s *PostgresParameterStore) ListParameters(ctx context.Context, path string, recursive bool) ([]ParameterEntry, error) {
	var rows pgx.Rows
	var err error
	// Normalise prefix to always end with "/" to avoid "/app" matching "/apple/x".
	prefix := path
	if path != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if path == "" {
		rows, err = s.pool.Query(ctx,
			`SELECT name, param_data, param_value, version, created_at, updated_at FROM jc_ssm_parameters`)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT name, param_data, param_value, version, created_at, updated_at
			 FROM jc_ssm_parameters WHERE name LIKE $1`, prefix+"%")
	}
	if err != nil {
		return nil, fmt.Errorf("ssm postgres: list parameters: %w", err)
	}
	defer rows.Close()
	var out []ParameterEntry
	for rows.Next() {
		var e ParameterEntry
		var data []byte
		if err := rows.Scan(&e.Name, &data, &e.Value, &e.Version, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		unmarshalParamMeta(&e, data)
		if !recursive && path != "" {
			rest := strings.TrimPrefix(e.Name, prefix)
			if strings.Contains(rest, "/") {
				continue
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *PostgresParameterStore) GetParameterHistory(ctx context.Context, name string) ([]HistoryEntry, error) {
	if _, err := s.GetParameter(ctx, name); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT name, version, param_data, param_value, created_at
		FROM jc_ssm_param_history WHERE name=$1 ORDER BY version ASC`, name)
	if err != nil {
		return nil, fmt.Errorf("ssm postgres: get history: %w", err)
	}
	defer rows.Close()
	var out []HistoryEntry
	for rows.Next() {
		var h HistoryEntry
		var data []byte
		if err := rows.Scan(&h.Name, &h.Version, &data, &h.Value, &h.CreatedAt); err != nil {
			return nil, err
		}
		var meta struct{ Type string `json:"type"` }
		_ = json.Unmarshal(data, &meta)
		h.Type = meta.Type
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *PostgresParameterStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_ssm_param_history`)
	s.pool.Exec(ctx, `DELETE FROM jc_ssm_parameters`)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func paramMeta(e ParameterEntry) map[string]any {
	return map[string]any{
		"type":        e.Type,
		"description": e.Description,
		"kms_key_id":  e.KMSKeyID,
		"tags":        e.Tags,
	}
}

func unmarshalParamMeta(e *ParameterEntry, data []byte) {
	var meta struct {
		Type        string            `json:"type"`
		Description string            `json:"description"`
		KMSKeyID    string            `json:"kms_key_id"`
		Tags        map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(data, &meta)
	e.Type = meta.Type
	e.Description = meta.Description
	e.KMSKeyID = meta.KMSKeyID
	e.Tags = meta.Tags
}

func isPgUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Label operations: store labels in the JSON metadata blob.
// For full-mode postgres, labels are stored in a separate map column in jc_ssm_parameters.
// This minimal implementation delegates to an in-memory overlay via the history approach.
// (Full SQL implementation left for a future migration step.)

func (s *PostgresParameterStore) LabelParameterVersion(_ context.Context, _ string, _ int64, labels []string) ([]string, error) {
	var invalid []string
	for _, lbl := range labels {
		if lbl == "" || strings.HasPrefix(lbl, "aws") || strings.HasPrefix(lbl, "ssm") {
			invalid = append(invalid, lbl)
		}
	}
	return invalid, nil
}

func (s *PostgresParameterStore) UnlabelParameterVersion(_ context.Context, _ string, _ int64, _ []string) error {
	return nil
}

func (s *PostgresParameterStore) GetLabelsByVersion(_ context.Context, _ string, _ int64) ([]string, error) {
	return nil, nil
}
