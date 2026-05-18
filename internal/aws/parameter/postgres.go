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

	// Check if it exists first (scoped by account+region).
	var existingVersion int64
	var existingValue []byte
	var existingType string
	scanErr := tx.QueryRow(ctx,
		`SELECT version, param_value, param_data->>'type' FROM jc_ssm_parameters
		 WHERE account_id=$1 AND region=$2 AND name=$3`,
		e.AccountID, e.Region, e.Name,
	).Scan(&existingVersion, &existingValue, &existingType)

	if scanErr == nil {
		// Parameter exists.
		if !overwrite {
			return ErrAlreadyExists
		}
		// Archive current version to history.
		_, err = tx.Exec(ctx, `
			INSERT INTO jc_ssm_param_history (account_id, region, name, version, param_data, param_value, created_at)
			SELECT account_id, region, name, version, param_data, param_value, updated_at
			FROM jc_ssm_parameters
			WHERE account_id=$1 AND region=$2 AND name=$3`,
			e.AccountID, e.Region, e.Name,
		)
		if err != nil {
			return fmt.Errorf("ssm postgres: archive history: %w", err)
		}
		e.Version = existingVersion + 1
		_, err = tx.Exec(ctx, `
			UPDATE jc_ssm_parameters
			SET param_data=$4, param_value=$5, version=$6, updated_at=$7
			WHERE account_id=$1 AND region=$2 AND name=$3`,
			e.AccountID, e.Region, e.Name, data, e.Value, e.Version, now,
		)
	} else if errors.Is(scanErr, pgx.ErrNoRows) {
		// New parameter.
		e.Version = 1
		_, err = tx.Exec(ctx, `
			INSERT INTO jc_ssm_parameters (account_id, region, name, param_data, param_value, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			e.AccountID, e.Region, e.Name, data, e.Value, e.Version, now,
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
		SELECT account_id, region, name, param_data, param_value, version, created_at, updated_at
		FROM jc_ssm_parameters WHERE name=$1`, name,
	).Scan(&e.AccountID, &e.Region, &e.Name, &data, &e.Value, &e.Version, &e.CreatedAt, &e.UpdatedAt)
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
			`SELECT account_id, region, name, param_data, param_value, version, created_at, updated_at FROM jc_ssm_parameters`)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT account_id, region, name, param_data, param_value, version, created_at, updated_at
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
		if err := rows.Scan(&e.AccountID, &e.Region, &e.Name, &data, &e.Value, &e.Version, &e.CreatedAt, &e.UpdatedAt); err != nil {
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
		"tier":        e.Tier,
		"tags":        e.Tags,
	}
}

func unmarshalParamMeta(e *ParameterEntry, data []byte) {
	var meta struct {
		Type        string            `json:"type"`
		Description string            `json:"description"`
		KMSKeyID    string            `json:"kms_key_id"`
		Tier        string            `json:"tier"`
		Tags        map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(data, &meta)
	e.Type = meta.Type
	e.Description = meta.Description
	e.KMSKeyID = meta.KMSKeyID
	e.Tier = meta.Tier
	e.Tags = meta.Tags
}

func isPgUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *PostgresParameterStore) LabelParameterVersion(ctx context.Context, name string, version int64, labels []string) ([]string, error) {
	// Resolve account/region from the parameter record.
	e, err := s.GetParameter(ctx, name)
	if err != nil {
		return nil, err
	}
	var invalid []string
	var valid []string
	for _, lbl := range labels {
		if lbl == "" || strings.HasPrefix(lbl, "aws") || strings.HasPrefix(lbl, "ssm") {
			invalid = append(invalid, lbl)
		} else {
			valid = append(valid, lbl)
		}
	}
	for _, lbl := range valid {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO jc_ssm_parameter_labels (account_id, region, parameter_name, version, label)
			 VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
			e.AccountID, e.Region, name, version, lbl,
		)
		if err != nil {
			return invalid, fmt.Errorf("ssm postgres: label parameter: %w", err)
		}
	}
	return invalid, nil
}

func (s *PostgresParameterStore) UnlabelParameterVersion(ctx context.Context, name string, version int64, labels []string) error {
	if len(labels) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM jc_ssm_parameter_labels
		 WHERE parameter_name=$1 AND version=$2 AND label=ANY($3)`,
		name, version, labels,
	)
	return err
}

func (s *PostgresParameterStore) GetLabelsByVersion(ctx context.Context, name string, version int64) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT label FROM jc_ssm_parameter_labels
		 WHERE parameter_name=$1 AND version=$2
		 ORDER BY label`,
		name, version,
	)
	if err != nil {
		return nil, fmt.Errorf("ssm postgres: get labels: %w", err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var lbl string
		if err := rows.Scan(&lbl); err != nil {
			return nil, err
		}
		result = append(result, lbl)
	}
	return result, rows.Err()
}
