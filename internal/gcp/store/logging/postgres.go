package logging

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_log_entries.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func nullableJSON(v any) any {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func (s *PostgresStore) Write(ctx context.Context, projectID string, e LogEntry) error {
	if e.Timestamp.IsZero() {
		e.Timestamp = clock.Now()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_log_entries
			(project_id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, insert_id, labels)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, projectID, e.LogName, e.ResourceType, nullableJSON(e.ResourceLabels), e.Severity, e.PayloadType, e.TextPayload,
		nullableJSON(e.JsonPayload), e.Timestamp, e.InsertID, nullableJSON(e.Labels))
	return err
}

func (s *PostgresStore) List(ctx context.Context, projectID string) ([]LogEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, insert_id, labels
		FROM jc_log_entries WHERE project_id=$1 ORDER BY timestamp, id
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LogEntry
	for rows.Next() {
		var e LogEntry
		var jsonPayload, resourceLabels, labels []byte
		if err := rows.Scan(&e.ID, &e.LogName, &e.ResourceType, &resourceLabels, &e.Severity, &e.PayloadType, &e.TextPayload, &jsonPayload, &e.Timestamp, &e.InsertID, &labels); err != nil {
			return nil, err
		}
		if len(jsonPayload) > 0 {
			json.Unmarshal(jsonPayload, &e.JsonPayload)
		}
		if len(resourceLabels) > 0 {
			json.Unmarshal(resourceLabels, &e.ResourceLabels)
		}
		if len(labels) > 0 {
			json.Unmarshal(labels, &e.Labels)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *PostgresStore) ListLogs(ctx context.Context, projectID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT log_name FROM jc_log_entries WHERE project_id=$1 ORDER BY log_name
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteLog(ctx context.Context, projectID, logName string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_log_entries WHERE project_id=$1 AND log_name=$2`, projectID, logName)
	return err
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_log_entries`)
}
