package managedkafka

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clusters) == 0 && len(s.topics) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"clusters": s.clusters,
		"topics":   s.topics,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Clusters map[string]map[string]Cluster `json:"clusters"`
		Topics   map[string]map[string]Topic   `json:"topics"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Clusters == nil {
		snap.Clusters = map[string]map[string]Cluster{}
	}
	if snap.Topics == nil {
		snap.Topics = map[string]map[string]Topic{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = snap.Clusters
	s.topics = snap.Topics
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_mk_clusters`).Scan(&n); err != nil {
		return false, err
	}
	if n > 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_mk_topics`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type clusterRow struct {
		ProjectID string  `json:"projectId"`
		Cluster   Cluster `json:"cluster"`
	}
	type topicRow struct {
		ProjectID string `json:"projectId"`
		Topic     Topic  `json:"topic"`
	}

	clusters := make([]clusterRow, 0)
	topics := make([]topicRow, 0)

	crows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, config, labels, create_time, update_time
		FROM jc_mk_clusters ORDER BY project_id, location, cluster_name
	`)
	if err != nil {
		return err
	}
	for crows.Next() {
		var r clusterRow
		var config, labels []byte
		if err := crows.Scan(&r.ProjectID, &r.Cluster.Location, &r.Cluster.Name, &config, &labels, &r.Cluster.CreateTime, &r.Cluster.UpdateTime); err != nil {
			crows.Close()
			return err
		}
		r.Cluster.Config = json.RawMessage(config)
		json.Unmarshal(labels, &r.Cluster.Labels)
		clusters = append(clusters, r)
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return err
	}

	trows, err := s.pool.Query(ctx, `
		SELECT project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time
		FROM jc_mk_topics ORDER BY project_id, location, cluster_name, topic_name
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r topicRow
		var config []byte
		if err := trows.Scan(&r.ProjectID, &r.Topic.Location, &r.Topic.ClusterName, &r.Topic.Name, &r.Topic.PartitionCount, &r.Topic.ReplicationFactor, &config, &r.Topic.CreateTime, &r.Topic.UpdateTime); err != nil {
			trows.Close()
			return err
		}
		r.Topic.Config = json.RawMessage(config)
		topics = append(topics, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"clusters": clusters,
		"topics":   topics,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Clusters []struct {
			ProjectID string  `json:"projectId"`
			Cluster   Cluster `json:"cluster"`
		} `json:"clusters"`
		Topics []struct {
			ProjectID string `json:"projectId"`
			Topic     Topic  `json:"topic"`
		} `json:"topics"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_mk_clusters", "jc_mk_topics"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Clusters {
		labels, _ := json.Marshal(r.Cluster.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_mk_clusters
				(project_id, location, cluster_name, config, labels, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, r.ProjectID, r.Cluster.Location, r.Cluster.Name, nullableJSONRaw(r.Cluster.Config, "{}"), nullableJSONRaw(labels, "{}"), r.Cluster.CreateTime, r.Cluster.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Topics {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_mk_topics
				(project_id, location, cluster_name, topic_name, partition_count, replication_factor, config, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Topic.Location, r.Topic.ClusterName, r.Topic.Name, r.Topic.PartitionCount, r.Topic.ReplicationFactor, nullableJSONRaw(r.Topic.Config, "{}"), r.Topic.CreateTime, r.Topic.UpdateTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
