package functions

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.functions) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{"functions": s.functions})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Functions map[string]map[string]Function `json:"functions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Functions == nil {
		snap.Functions = map[string]map[string]Function{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.functions = snap.Functions
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_functions`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type functionRow struct {
		ProjectID string   `json:"projectId"`
		Function  Function `json:"function"`
	}
	functions := make([]functionRow, 0)
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, function_id, location, runtime, entry_point, source_upload_url, source_archive_url,
		       https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
		       available_memory_mb, timeout, description
		FROM jc_functions ORDER BY project_id, location, function_id
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r functionRow
		var eventTrigger, env, labels []byte
		if err := rows.Scan(&r.ProjectID, &r.Function.ID, &r.Function.Location, &r.Function.Runtime, &r.Function.EntryPoint,
			&r.Function.SourceUploadURL, &r.Function.SourceArchiveURL, &r.Function.HttpsTriggerURL, &eventTrigger, &env,
			&r.Function.Status, &r.Function.CreateTime, &r.Function.UpdateTime, &labels, &r.Function.AvailableMemoryMB,
			&r.Function.Timeout, &r.Function.Description); err != nil {
			rows.Close()
			return err
		}
		if len(eventTrigger) > 0 {
			json.Unmarshal(eventTrigger, &r.Function.EventTrigger)
		}
		json.Unmarshal(env, &r.Function.EnvironmentVariables)
		json.Unmarshal(labels, &r.Function.Labels)
		functions = append(functions, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(map[string]any{"functions": functions})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Functions []struct {
			ProjectID string   `json:"projectId"`
			Function  Function `json:"function"`
		} `json:"functions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_functions`); err != nil {
		return err
	}
	for _, r := range snap.Functions {
		env, _ := json.Marshal(r.Function.EnvironmentVariables)
		labels, _ := json.Marshal(r.Function.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_functions
				(project_id, location, function_id, runtime, entry_point, source_upload_url, source_archive_url,
				 https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
				 available_memory_mb, timeout, description)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		`, r.ProjectID, r.Function.Location, r.Function.ID, r.Function.Runtime, r.Function.EntryPoint,
			r.Function.SourceUploadURL, r.Function.SourceArchiveURL, r.Function.HttpsTriggerURL, nullableJSON(r.Function.EventTrigger),
			json.RawMessage(env), r.Function.Status, r.Function.CreateTime, r.Function.UpdateTime, json.RawMessage(labels),
			r.Function.AvailableMemoryMB, r.Function.Timeout, r.Function.Description); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
