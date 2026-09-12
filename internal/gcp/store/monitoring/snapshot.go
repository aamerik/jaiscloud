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

type channelRow struct {
	Project string              `json:"project"`
	Channel NotificationChannel `json:"channel"`
}

type incidentRow struct {
	Project  string   `json:"project"`
	Incident Incident `json:"incident"`
}

// monitoringSnap is the JSON snapshot shape shared by both backends.
type monitoringSnap struct {
	Descriptors []descriptorRow `json:"descriptors"`
	Series      []seriesRow     `json:"series"`
	Policies    []policyRow     `json:"policies"`
	Channels    []channelRow    `json:"channels,omitempty"`
	Incidents   []incidentRow   `json:"incidents,omitempty"`
}

// MemoryStore Snapshot/Restore/IsEmpty.

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.descriptors) == 0 && len(s.series) == 0 && len(s.policies) == 0 &&
		len(s.channels) == 0 && len(s.incidents) == 0, nil
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

	projects = projects[:0]
	for p := range s.channels {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		ids := make([]string, 0, len(s.channels[p]))
		for id := range s.channels[p] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			snap.Channels = append(snap.Channels, channelRow{Project: p, Channel: s.channels[p][id]})
		}
	}

	projects = projects[:0]
	for p := range s.incidents {
		projects = append(projects, p)
	}
	sort.Strings(projects)
	for _, p := range projects {
		ids := make([]string, 0, len(s.incidents[p]))
		for id := range s.incidents[p] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			snap.Incidents = append(snap.Incidents, incidentRow{Project: p, Incident: s.incidents[p][id]})
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
	channels := make(map[string]map[string]NotificationChannel)
	for _, row := range snap.Channels {
		if channels[row.Project] == nil {
			channels[row.Project] = make(map[string]NotificationChannel)
		}
		channels[row.Project][row.Channel.ID] = row.Channel
	}
	incidents := make(map[string]map[string]Incident)
	for _, row := range snap.Incidents {
		if incidents[row.Project] == nil {
			incidents[row.Project] = make(map[string]Incident)
		}
		incidents[row.Project][row.Incident.ID] = row.Incident
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.descriptors = descriptors
	s.series = series
	s.policies = policies
	s.channels = channels
	s.incidents = incidents
	return nil
}

// PostgresStore Snapshot/Restore/IsEmpty.

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM jc_monitoring_metric_descriptors)
		     + (SELECT count(*) FROM jc_monitoring_time_series)
		     + (SELECT count(*) FROM jc_monitoring_alert_policies)
		     + (SELECT count(*) FROM jc_monitoring_notification_channels)
		     + (SELECT count(*) FROM jc_monitoring_incidents)
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

	rows, err = s.pool.Query(ctx, `
		SELECT project_id, `+channelCols+` FROM jc_monitoring_notification_channels ORDER BY project_id, id
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p string
		var c NotificationChannel
		var labels, userLabels []byte
		if err := rows.Scan(&p, &c.ID, &c.Type, &c.DisplayName, &c.Description, &labels, &userLabels, &c.Enabled, &c.VerificationStatus, &c.CreateTime, &c.UpdateTime); err != nil {
			rows.Close()
			return err
		}
		if len(labels) > 0 {
			_ = json.Unmarshal(labels, &c.Labels)
		}
		if len(userLabels) > 0 {
			_ = json.Unmarshal(userLabels, &c.UserLabels)
		}
		snap.Channels = append(snap.Channels, channelRow{Project: p, Channel: c})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = s.pool.Query(ctx, `
		SELECT project_id, `+incidentCols+` FROM jc_monitoring_incidents ORDER BY project_id, started_at, id
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p string
		var inc Incident
		var notifications []byte
		if err := rows.Scan(&p, &inc.ID, &inc.ProjectID, &inc.PolicyID, &inc.ConditionName, &inc.State, &inc.StartedAt, &inc.EndedAt, &inc.Reason, &notifications); err != nil {
			rows.Close()
			return err
		}
		if len(notifications) > 0 {
			_ = json.Unmarshal(notifications, &inc.Notifications)
		}
		snap.Incidents = append(snap.Incidents, incidentRow{Project: p, Incident: inc})
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
	if _, err := tx.Exec(ctx, `DELETE FROM jc_monitoring_notification_channels`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_monitoring_incidents`); err != nil {
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
	for _, row := range snap.Channels {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_monitoring_notification_channels
				(project_id, id, type, display_name, description, labels, user_labels, enabled, verification_status, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		`, row.Project, row.Channel.ID, row.Channel.Type, row.Channel.DisplayName, row.Channel.Description,
			jsonb(row.Channel.Labels), jsonb(row.Channel.UserLabels), row.Channel.Enabled, row.Channel.VerificationStatus,
			row.Channel.CreateTime, row.Channel.UpdateTime); err != nil {
			return err
		}
	}
	for _, row := range snap.Incidents {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_monitoring_incidents
				(project_id, id, policy_id, condition_name, state, started_at, ended_at, reason, notifications)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, row.Project, row.Incident.ID, row.Incident.PolicyID, row.Incident.ConditionName, row.Incident.State,
			row.Incident.StartedAt, row.Incident.EndedAt, row.Incident.Reason, jsonb(row.Incident.Notifications)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
