package dataproc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// substateRunning is the dataproc.v1.JobStatus.Substate rendered while a job is
// non-terminal. The emulator hands a submitted job straight to the executor and
// its only live state is RUNNING, so it reports the real QUEUED substate
// ("the Job has been received and is awaiting execution"; dataproc.v1 documents
// QUEUED as applying to RUNNING). The other defined substates (SUBMITTED,
// STALE_STATUS) describe agent hand-off/staleness the emulator does not model,
// and terminal jobs omit substate entirely because every defined substate
// applies only to RUNNING.
const substateRunning = "QUEUED"

// formatTimestamp renders a business timestamp as the RFC3339Nano string the
// Discovery JSON shape uses.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// --- Cluster rendering ---

func clusterStatusMap(s dpstore.ClusterStatus) map[string]any {
	out := map[string]any{
		"state":          s.State,
		"stateStartTime": formatTimestamp(s.StateStartTime),
	}
	if s.Detail != "" {
		out["detail"] = s.Detail
	}
	return out
}

// ClusterJSON renders a stored Cluster as a dataproc.v1.Cluster wire map.
func ClusterJSON(c dpstore.Cluster) map[string]any {
	out := map[string]any{
		"projectId":   c.ProjectID,
		"clusterName": c.Name,
		"status":      clusterStatusMap(c.Status),
	}
	if c.ClusterUUID != "" {
		out["clusterUuid"] = c.ClusterUUID
	}
	if len(c.Config) > 0 {
		var config map[string]any
		if json.Unmarshal(c.Config, &config) == nil && config != nil {
			out["config"] = config
		}
	}
	if c.Labels != nil {
		out["labels"] = c.Labels
	}
	if len(c.StatusHistory) > 0 {
		history := make([]any, 0, len(c.StatusHistory))
		for _, s := range c.StatusHistory {
			history = append(history, clusterStatusMap(s))
		}
		out["statusHistory"] = history
	}
	return out
}

// --- Job rendering ---

func jobStatusMap(s dpstore.JobStatus) map[string]any {
	out := map[string]any{
		"state":          s.State,
		"stateStartTime": formatTimestamp(s.StateStartTime),
	}
	if s.Details != "" {
		out["details"] = s.Details
	}
	// substate is an enum-valued field in dataproc.v1.JobStatus; proto3 JSON
	// omits it at its default (UNSPECIFIED), so only render a set value.
	if s.Substate != "" {
		out["substate"] = s.Substate
	}
	return out
}

// JobJSON renders a stored Job as a dataproc.v1.Job wire map.
func JobJSON(j dpstore.Job) map[string]any {
	out := map[string]any{
		"reference": map[string]any{"projectId": j.ProjectID, "jobId": j.JobID},
		"placement": map[string]any{"clusterName": j.PlacementClusterName},
		"status":    jobStatusMap(j.Status),
		"done":      jobTerminal(j.Status.State),
	}
	if j.JobUUID != "" {
		out["jobUuid"] = j.JobUUID
	}
	if j.Type != "" && len(j.TypeJob) > 0 {
		var typeJob map[string]any
		if json.Unmarshal(j.TypeJob, &typeJob) == nil && typeJob != nil {
			out[j.Type] = typeJob
		}
	}
	if j.DriverOutputResourceURI != "" {
		out["driverOutputResourceUri"] = j.DriverOutputResourceURI
	}
	if j.DriverControlFilesURI != "" {
		out["driverControlFilesUri"] = j.DriverControlFilesURI
	}
	if j.Labels != nil {
		out["labels"] = j.Labels
	}
	if len(j.StatusHistory) > 0 {
		history := make([]any, 0, len(j.StatusHistory))
		for _, s := range j.StatusHistory {
			history = append(history, jobStatusMap(s))
		}
		out["statusHistory"] = history
	}
	return out
}

// jobTerminal reports whether a job state is terminal (done).
func jobTerminal(state string) bool {
	switch state {
	case "DONE", "ERROR", "CANCELLED":
		return true
	}
	return false
}

// --- Operations ---

// clusterOperationMetadata renders ClusterOperationMetadata for an operation.
func clusterOperationMetadata(clusterName, clusterUUID, operationType string) map[string]any {
	return map[string]any{
		"@type":         "type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata",
		"clusterName":   clusterName,
		"clusterUuid":   clusterUUID,
		"operationType": operationType,
		"status":        map[string]any{"state": "DONE"},
		"statusHistory": []any{map[string]any{"state": "DONE"}},
	}
}

// jobOperationMetadata renders JobMetadata for a submit-as-operation.
func jobOperationMetadata(jobID, state, operationType string, start time.Time) map[string]any {
	return map[string]any{
		"@type":         "type.googleapis.com/google.cloud.dataproc.v1.JobMetadata",
		"jobId":         jobID,
		"operationType": operationType,
		"startTime":     formatTimestamp(start),
		"status":        map[string]any{"state": state},
	}
}

// OperationJSON renders a stored Operation as a google.longrunning.Operation.
func OperationJSON(op dpstore.Operation) map[string]any {
	name := OperationName(op.ProjectID, op.Region, op.ID)
	var metadata any = map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done && op.Response != "" {
		var response any = map[string]any{}
		if json.Unmarshal([]byte(op.Response), &response) == nil {
			out["response"] = response
		}
	}
	return out
}

// storeOperation persists a done operation and returns it.
func (s *Service) storeOperation(ctx context.Context, project, region, verb, target string, metadata, response map[string]any) (dpstore.Operation, error) {
	now := clock.Now().UTC()
	op := dpstore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Region:     region,
		Done:       true,
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if metadata != nil {
		metaJSON, _ := json.Marshal(metadata)
		op.Metadata = string(metaJSON)
	}
	if response != nil {
		respJSON, _ := json.Marshal(response)
		op.Response = string(respJSON)
	}
	s.sweepOperations(ctx)
	if err := s.store.CreateOperation(ctx, project, region, op); err != nil {
		return dpstore.Operation{}, err
	}
	return op, nil
}

// sweepOperations lazily deletes completed operations older than the retention
// window. It runs on each operation creation so jc_dataproc_operations stays
// bounded without a background goroutine.
func (s *Service) sweepOperations(ctx context.Context) {
	if s.operationTTL <= 0 {
		return
	}
	if _, err := s.store.DeleteStaleOperations(ctx, clock.Now().UTC().Add(-s.operationTTL)); err != nil {
		slog.Warn("dataproc: operation sweep failed", "err", err)
	}
}
