package monitoring

import (
	"context"
	"encoding/json"
	"io"
	"sort"
)

// snapRow pairs a project with a single stored value.
type descriptorRow struct {
	Project    string           `json:"project"`
	Descriptor MetricDescriptor `json:"descriptor"`
}

type seriesRow struct {
	Project string     `json:"project"`
	Series  TimeSeries `json:"series"`
}

type policyRow struct {
	Project string      `json:"project"`
	Policy  AlertPolicy `json:"policy"`
}

// monitoringSnap is the JSON snapshot shape shared by both backends.
type monitoringSnap struct {
	Descriptors []descriptorRow `json:"descriptors"`
	Series      []seriesRow     `json:"series"`
	Policies    []policyRow     `json:"policies"`
}

// MemoryStore Snapshot/Restore/IsEmpty.

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.descriptors) == 0 && len(s.series) == 0 && len(s.policies) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := monitoringSnap{}

	projects := make([]string, 0, len(s.descriptors))
	for p := range s.descriptors {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		types := make([]string, 0, len(s.descriptors[p]))
		for t := range s.descriptors[p] {
			types = append(types, t)
		}
		sort.Strings(types)
		for _, t := range types {
			snap.Descriptors = append(snap.Descriptors, descriptorRow{Project: p, Descriptor: s.descriptors[p][t]})
		}
	}

	projects = projects[:0]
	for p := range s.series {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		keys := make([]string, 0, len(s.series[p]))
		for k := range s.series[p] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			snap.Series = append(snap.Series, seriesRow{Project: p, Series: *s.series[p][k]})
		}
	}

	projects = projects[:0]
	for p := range s.policies {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		ids := make([]string, 0, len(s.policies[p]))
		for id := range s.policies[p] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			snap.Policies = append(snap.Policies, policyRow{Project: p, Policy: s.policies[p][id]})
		}
	}

	return json.NewEncoder(w).Encode(snap)
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap monitoringSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	descriptors := make(map[string]map[string]MetricDescriptor)
	for _, row := range snap.Descriptors {
		if descriptors[row.Project] == nil {
			descriptors[row.Project] = make(map[string]MetricDescriptor)
		}
		descriptors[row.Project][row.Descriptor.Type] = row.Descriptor
	}
	series := make(map[string]map[string]*TimeSeries)
	for _, row := range snap.Series {
		if series[row.Project] == nil {
			series[row.Project] = make(map[string]*TimeSeries)
		}
		ts := row.Series
		series[row.Project][seriesKey(ts)] = &ts
	}
	policies := make(map[string]map[string]AlertPolicy)
	for _, row := range snap.Policies {
		if policies[row.Project] == nil {
			policies[row.Project] = make(map[string]AlertPolicy)
		}
		policies[row.Project][row.Policy.ID] = row.Policy
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.descriptors = descriptors
	s.series = series
	s.policies = policies
	return nil
}

// PostgresStore Snapshot/Restore/IsEmpty.

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM jc_monitoring_metric_descriptors)
		     + (SELECT count(*) FROM jc_monitoring_time_series)
		     + (SELECT count(*) FROM jc_monitoring_alert_policies)
	`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	snap := monitoringSnap{}

	rows, err := s.pool.Query(ctx, `
		SELECT project_id, `+descriptorCols+` FROM jc_monitoring_metric_descriptors ORDER BY project_id, type
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p string
		var d MetricDescriptor
		var labels, mrt []byte
		if err := rows.Scan(&p, &d.Type, &d.MetricKind, &d.ValueType, &d.Unit, &d.Description, &d.DisplayName, &labels, &mrt); err != nil {
			rows.Close()
			return err
		}
		if len(labels) > 0 {
			_ = json.Unmarshal(labels, &d.Labels)
		}
		if len(mrt) > 0 {
			_ = json.Unmarshal(mrt, &d.MonitoredResourceTypes)
		}
		snap.Descriptors = append(snap.Descriptors, descriptorRow{Project: p, Descriptor: d})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.pool.Query(ctx, `
		SELECT project_id, `+seriesCols+` FROM jc_monitoring_time_series ORDER BY project_id, metric_type, resource_type, series_key
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p string
		ts, err := scanTimeSeries(rows.Scan)
		if err != nil {
			rows.Close()
			return err
		}
		snap.Series = append(snap.Series, seriesRow{Project: p, Series: ts})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.pool.Query(ctx, `
		SELECT project_id, `+policyCols+` FROM jc_monitoring_alert_policies ORDER BY project_id, id
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p string
		pol, err := scanAlertPolicy(rows.Scan)
		if err != nil {
			rows.Close()
			return err
		}
		snap.Policies = append(snap.Policies, policyRow{Project: p, Policy: pol})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap monitoringSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_monitoring_metric_descriptors`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_monitoring_time_series`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_monitoring_alert_policies`); err != nil {
		return err
	}
	for _, row := range snap.Descriptors {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_monitoring_metric_descriptors
				(project_id, type, metric_kind, value_type, unit, description, display_name, labels, monitored_resource_types)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, row.Project, row.Descriptor.Type, row.Descriptor.MetricKind, row.Descriptor.ValueType, row.Descriptor.Unit,
			row.Descriptor.Description, row.Descriptor.DisplayName, jsonb(row.Descriptor.Labels), jsonb(row.Descriptor.MonitoredResourceTypes)); err != nil {
			return err
		}
	}
	for _, row := range snap.Series {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_monitoring_time_series
				(project_id, series_key, metric_type, metric_labels, resource_type, resource_labels, metric_kind, value_type, unit, points)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, row.Project, seriesKey(row.Series), row.Series.MetricType, jsonb(row.Series.MetricLabels),
			row.Series.ResourceType, jsonb(row.Series.ResourceLabels), row.Series.MetricKind, row.Series.ValueType,
			row.Series.Unit, jsonb(row.Series.Points)); err != nil {
			return err
		}
	}
	for _, row := range snap.Policies {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_monitoring_alert_policies
				(project_id, id, display_name, combiner, enabled, documentation, conditions, notification_channels, user_labels)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, row.Project, row.Policy.ID, row.Policy.DisplayName, row.Policy.Combiner, row.Policy.Enabled,
			jsonb(row.Policy.Documentation), jsonb(row.Policy.Conditions), jsonb(row.Policy.NotificationChannels),
			jsonb(row.Policy.UserLabels)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
