package emroneks

import (
	"context"
	"encoding/json"
	"log/slog"

	"jaiscloud/internal/executor/spark"
)

// jobRunRehydrate is the subset of a persisted jobRun row needed for rehydration.
type jobRunRehydrate struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// rehydratePoller re-tracks non-terminal job runs into the StatusPoller on startup.
// Called at the end of New(...) when a poller is present.
func (p *EMRContainersProvider) rehydratePoller(ctx context.Context) {
	if p.poller == nil {
		return
	}
	entries, err := p.resources.List(ctx, rtJobRun, "")
	if err != nil {
		slog.Warn("emroneks: rehydratePoller: list jobRuns failed", "err", err)
		return
	}
	var tracked int
	for _, e := range entries {
		var jr jobRunRehydrate
		if err := json.Unmarshal(e.Data, &jr); err != nil {
			slog.Warn("emroneks: rehydratePoller: unmarshal failed", "id", e.ID, "err", err)
			continue
		}
		if isTerminalJobRunState(jr.State) {
			continue
		}
		// Poller tracks by the same key Submit uses: "vcID/jobID".
		p.poller.Track(e.ID, sparkStateFromJobRunState(jr.State))
		tracked++
	}
	slog.Info("emroneks: rehydrated non-terminal jobRuns into poller", "count", tracked)
}

// isTerminalJobRunState reports whether a job-run state string is terminal.
func isTerminalJobRunState(s string) bool {
	switch s {
	case "COMPLETED", "FAILED", "CANCELLED":
		return true
	}
	return false
}

// sparkStateFromJobRunState maps an EMR-on-EKS job-run state to a SparkState.
func sparkStateFromJobRunState(s string) spark.SparkState {
	switch s {
	case "RUNNING":
		return spark.StateRunning
	case "COMPLETED":
		return spark.StateCompleted
	case "FAILED":
		return spark.StateFailed
	case "CANCELLED":
		return spark.StateFailed
	default:
		return spark.StatePending
	}
}
