package dataproc

import (
	"context"
	"encoding/json"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// ClusterInput carries the caller-supplied fields of a cluster create/update.
// Config is the raw ClusterConfig JSON stored verbatim.
type ClusterInput struct {
	Labels map[string]string
	Config json.RawMessage
}

// ClusterInputFromMap builds a ClusterInput from a Discovery/proto Cluster map.
func ClusterInputFromMap(body map[string]any) ClusterInput {
	in := ClusterInput{Labels: bodyStringMap(body, "labels")}
	if cfg, ok := body["config"].(map[string]any); ok {
		if data, err := json.Marshal(cfg); err == nil {
			in.Config = data
		}
	}
	return in
}

// ParseMask splits a comma-separated update mask (REST query form) into paths.
func ParseMask(mask string) []string { return splitMask(mask) }

// CreateCluster creates a cluster and returns it with the done create
// operation.
func (s *Service) CreateCluster(ctx context.Context, project, region, name string, in ClusterInput) (dpstore.Cluster, dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	now := clock.Now().UTC()
	c := dpstore.Cluster{
		ProjectID:     project,
		Region:        region,
		Name:          name,
		Status:        dpstore.ClusterStatus{State: "RUNNING", StateStartTime: now},
		StatusHistory: []dpstore.ClusterStatus{{State: "CREATING", StateStartTime: now}},
		ClusterUUID:   randomHex(32),
		CreateTime:    now,
		UpdateTime:    now,
		Labels:        in.Labels,
		Config:        in.Config,
	}
	if err := s.store.CreateCluster(ctx, project, region, c); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	target := ClusterName(project, region, name)
	op, err := s.storeOperation(ctx, project, region, "create", target,
		clusterOperationMetadata(name, c.ClusterUUID, "CREATE"),
		ClusterJSON(c))
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	return c, op, nil
}

// GetCluster returns one cluster.
func (s *Service) GetCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, invalidArgument("missing region or clusterName")
	}
	c, err := s.store.GetCluster(ctx, project, region, name)
	if err != nil {
		return dpstore.Cluster{}, mapErr(err)
	}
	return c, nil
}

// ListClusters returns a cursor page of the clusters in a region.
func (s *Service) ListClusters(ctx context.Context, project, region string, pageSize int, pageToken string) ([]dpstore.Cluster, string, error) {
	if region == "" {
		return nil, "", invalidArgument("missing region")
	}
	clusters, err := s.store.ListClusters(ctx, project, region)
	if err != nil {
		return nil, "", err
	}
	// filter is accepted but ignored (documented limitation, matches workflows).
	page, next := pageClusters(clusters, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateCluster merges the caller's config/labels into the stored cluster and
// returns it with the done update operation. The mask selects which top-level
// fields apply; an empty mask applies everything supplied.
func (s *Service) UpdateCluster(ctx context.Context, project, region, name string, in ClusterInput, mask []string) (dpstore.Cluster, dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	apply := func(field string) bool {
		return len(mask) == 0 || containsMaskField(mask, field)
	}
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		if apply("labels") && in.Labels != nil {
			c.Labels = in.Labels
		}
		if in.Config != nil {
			stored := map[string]any{}
			if len(c.Config) > 0 {
				_ = json.Unmarshal(c.Config, &stored)
			}
			applyConfigMask(stored, mustJSONMap(in.Config), mask)
			if data, err := json.Marshal(stored); err == nil {
				c.Config = data
			}
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	target := ClusterName(project, region, name)
	op, err := s.storeOperation(ctx, project, region, "update", target,
		clusterOperationMetadata(name, c.ClusterUUID, "UPDATE"),
		ClusterJSON(c))
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	return c, op, nil
}

// DeleteCluster deletes a cluster and returns the done delete operation whose
// response is google.protobuf.Empty.
func (s *Service) DeleteCluster(ctx context.Context, project, region, name string) (dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	c, err := s.store.GetCluster(ctx, project, region, name)
	if err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	if err := s.store.DeleteCluster(ctx, project, region, name); err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	target := ClusterName(project, region, name)
	op, err := s.storeOperation(ctx, project, region, "delete", target,
		clusterOperationMetadata(name, c.ClusterUUID, "DELETE"),
		map[string]any{})
	if err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	return op, nil
}

func (s *Service) startStopCluster(ctx context.Context, project, region, name, toState, verb, operationType string) (dpstore.Cluster, dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		c.StatusHistory = append(c.StatusHistory, c.Status)
		c.Status = dpstore.ClusterStatus{State: toState, StateStartTime: clock.Now().UTC()}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	target := ClusterName(project, region, name)
	op, err := s.storeOperation(ctx, project, region, verb, target,
		clusterOperationMetadata(name, c.ClusterUUID, operationType),
		ClusterJSON(c))
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	return c, op, nil
}

// StartCluster transitions a cluster to RUNNING and returns the done operation.
func (s *Service) StartCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, dpstore.Operation, error) {
	return s.startStopCluster(ctx, project, region, name, "RUNNING", "start", "START")
}

// StopCluster transitions a cluster to STOPPED and returns the done operation.
func (s *Service) StopCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, dpstore.Operation, error) {
	return s.startStopCluster(ctx, project, region, name, "STOPPED", "stop", "STOP")
}

// DiagnoseCluster is an unimplemented stub: it fails loud rather than silently
// succeeding, so SDK callers observe a real UNIMPLEMENTED error.
func (s *Service) DiagnoseCluster() error {
	return model.NewProviderError("Unimplemented", "DiagnoseCluster is not supported by the emulator", 501)
}

// pageClusters paginates cluster records using the shared GCP paging helper.
func pageClusters(clusters []dpstore.Cluster, params map[string]any) ([]dpstore.Cluster, string) {
	return paging.Page(clusters, func(c dpstore.Cluster) string { return c.Name }, params)
}

// --- mask helpers ---

// containsMaskField reports whether the mask contains the given top-level field
// (or a sub-path of it).
func containsMaskField(mask []string, field string) bool {
	for _, part := range mask {
		if part == field || strings.HasPrefix(part, field+".") {
			return true
		}
	}
	return false
}

func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// applyConfigMask merges fields from the request config onto the stored config,
// honoring the updateMask's config.* paths. Paths may be nested (e.g.
// config.worker_config.num_instances, the documented updatable field) and may
// use either the proto's snake_case or the JSON shape's camelCase segments; both
// are normalized to the camelCase keys the stored config uses. Only sub-fields
// present in the request config (under the masked path) are applied. An empty
// mask merges every supplied top-level key.
func applyConfigMask(stored, incoming map[string]any, mask []string) {
	if incoming == nil {
		return
	}
	if len(mask) == 0 {
		// No mask: merge all incoming top-level keys verbatim.
		for k, v := range incoming {
			stored[k] = v
		}
		return
	}
	for _, field := range mask {
		if !strings.HasPrefix(field, "config.") {
			continue
		}
		path := strings.Split(field[len("config."):], ".")
		if len(path) == 0 || path[0] == "" {
			continue
		}
		cur := incoming
		ok := true
		for _, seg := range path[:len(path)-1] {
			next, isMap := cur[camelKey(seg)].(map[string]any)
			if !isMap {
				ok = false
				break
			}
			cur = next
		}
		if !ok {
			continue
		}
		leaf := camelKey(path[len(path)-1])
		v, exists := cur[leaf]
		if !exists {
			continue
		}
		setNestedCamel(stored, path[:len(path)-1], leaf, v)
	}
}

// camelKey converts a snake_case path segment to the camelCase JSON key the
// Discovery/proto shape uses. An already-camelCase segment passes through.
func camelKey(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	parts := strings.Split(s, "_")
	out := parts[0]
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		out += strings.ToUpper(p[:1]) + p[1:]
	}
	return out
}

// setNestedCamel stores value at the camelCase-normalized path under stored,
// creating intermediate maps as needed.
func setNestedCamel(stored map[string]any, parents []string, leaf string, value any) {
	cur := stored
	for _, seg := range parents {
		key := camelKey(seg)
		next, ok := cur[key].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[key] = next
		}
		cur = next
	}
	cur[leaf] = value
}
