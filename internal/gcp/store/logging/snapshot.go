package logging

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(s.entries)
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap map[string][]LogEntry
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap == nil {
		snap = make(map[string][]LogEntry)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = snap
	var maxID int64
	for _, entries := range snap {
		for _, e := range entries {
			if e.ID > maxID {
				maxID = e.ID
			}
		}
	}
	s.nextID = maxID + 1
	return nil
}

// --- Postgres store ---

// reseatSequenceSQL re-seats the jc_log_entries id sequence to the current max
// id after a restore inserts explicit id values, so the next BIGSERIAL Write
// does not collide with a restored id. COALESCE(MAX(id),1) keeps the sequence
// valid on an empty table (MAX would otherwise be NULL and setval would fail).
const reseatSequenceSQL = `SELECT setval(pg_get_serial_sequence('jc_log_entries','id'), COALESCE(MAX(id),1)) FROM jc_log_entries`

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_log_entries`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type row struct {
		ProjectID string   `json:"projectId"`
		Entry     LogEntry `json:"entry"`
	}
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, insert_id, labels
		FROM jc_log_entries ORDER BY project_id, id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()
	entries := make([]row, 0)
	for rows.Next() {
		var r row
		var jsonPayload, resourceLabels, labels []byte
		if err := rows.Scan(&r.ProjectID, &r.Entry.ID, &r.Entry.LogName, &r.Entry.ResourceType, &resourceLabels, &r.Entry.Severity, &r.Entry.PayloadType, &r.Entry.TextPayload, &jsonPayload, &r.Entry.Timestamp, &r.Entry.InsertID, &labels); err != nil {
			return err
		}
		if len(jsonPayload) > 0 {
			json.Unmarshal(jsonPayload, &r.Entry.JsonPayload)
		}
		if len(resourceLabels) > 0 {
			json.Unmarshal(resourceLabels, &r.Entry.ResourceLabels)
		}
		if len(labels) > 0 {
			json.Unmarshal(labels, &r.Entry.Labels)
		}
		entries = append(entries, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(map[string]any{"entries": entries})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Entries []struct {
			ProjectID string   `json:"projectId"`
			Entry     LogEntry `json:"entry"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_log_entries`); err != nil {
		return err
	}
	for _, r := range snap.Entries {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_log_entries
				(project_id, id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, insert_id, labels)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`, r.ProjectID, r.Entry.ID, r.Entry.LogName, r.Entry.ResourceType, nullableJSON(r.Entry.ResourceLabels), r.Entry.Severity, r.Entry.PayloadType,
			r.Entry.TextPayload, nullableJSON(r.Entry.JsonPayload), r.Entry.Timestamp, r.Entry.InsertID, nullableJSON(r.Entry.Labels)); err != nil {
			return err
		}
	}
	// Re-seat the id sequence so the next BIGSERIAL Write does not collide with
	// a restored explicit id value.
	if _, err := tx.Exec(ctx, reseatSequenceSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
