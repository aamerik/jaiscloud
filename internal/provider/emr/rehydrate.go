package emr

import (
	"context"
	"encoding/json"
	"log/slog"

	"jaiscloud/internal/executor/spark"
)

// clusterRehydrate is the minimal subset of an emr_cluster row needed for rehydration.
type clusterRehydrate struct {
	Id     string           `json:"Id"`
	Status struct {
		State string `json:"State"`
	} `json:"Status"`
	Steps []map[string]any `json:"Steps"`
}

// rehydratePoller re-tracks non-terminal steps into the StatusPoller on startup.
// Called at the end of New(...) when a poller is present.
func (p *EMRProvider) rehydratePoller(ctx context.Context) {
	if p.poller == nil {
		return
	}
	entries, err := p.resources.List(ctx, rtCluster, "")
	if err != nil {
		slog.Warn("emr: rehydratePoller: list clusters failed", "err", err)
		return
	}
	var tracked int
	for _, e := range entries {
		var c clusterRehydrate
		if err := json.Unmarshal(e.Data, &c); err != nil {
			slog.Warn("emr: rehydratePoller: unmarshal failed", "id", e.ID, "err", err)
			continue
		}
		if isTerminalClusterState(c.Status.State) {
			continue
		}
		for _, step := range c.Steps {
			sid, _ := step["Id"].(string)
			// HEDGED ASSERTIONS — step["Status"] may be nil or a non-map in
			// partial/malformed rows; never use single-return form on nested maps.
			statusMap, _ := step["Status"].(map[string]any)
			sstate, _ := statusMap["State"].(string)
			if sid == "" || isTerminalStepState(sstate) {
				continue
			}
			p.poller.Track(sid, sparkStateFromStepState(sstate))
			tracked++
		}
	}
	slog.Info("emr: rehydrated non-terminal steps into poller", "count", tracked)
}

// isTerminalClusterState reports whether an EMR cluster state is terminal.
func isTerminalClusterState(s string) bool {
	switch s {
	case "TERMINATED", "TERMINATED_WITH_ERRORS":
		return true
	}
	return false
}

// isTerminalStepState reports whether an EMR step state is terminal.
func isTerminalStepState(s string) bool {
	switch s {
	case "COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED":
		return true
	}
	return false
}

// sparkStateFromStepState maps an EMR step state string to a SparkState.
func sparkStateFromStepState(s string) spark.SparkState {
	switch s {
	case "RUNNING":
		return spark.StateRunning
	case "COMPLETED":
		return spark.StateCompleted
	case "FAILED", "CANCELLED", "INTERRUPTED":
		return spark.StateFailed
	default:
		return spark.StatePending
	}
}
