package emr

import (
	"context"
	"log/slog"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

// handlerCtx captures the cloud/region/account triplet from a NormalizedRequest
// at handler entry so downstream goroutines can publish state-change events
// without losing provenance (nr is not safe to read from goroutines after the
// handler returns).
type handlerCtx struct {
	cloud     model.Cloud
	region    string
	accountID string
}

func newHandlerCtx(nr *model.NormalizedRequest) handlerCtx {
	return handlerCtx{cloud: nr.Cloud, region: nr.Region, accountID: nr.AccountID}
}

// deriveStepReason returns a state-change code + human-readable message for a
// step transition.
func deriveStepReason(state, failureReason string) (code, reason string) {
	switch state {
	case "PENDING":
		return "PENDING", "Step submitted"
	case "RUNNING":
		return "RUNNING", "Step running"
	case "COMPLETED":
		return "STEP_COMPLETED", "Step completed successfully"
	case "CANCELLED":
		return "USER_REQUEST", "User cancelled step"
	case "FAILED":
		if failureReason != "" {
			return "STEP_FAILURE", failureReason
		}
		return "STEP_FAILURE", "Step failed"
	case "INTERRUPTED":
		return "CLUSTER_TERMINATED", "Cluster terminated mid-step"
	}
	return "", state
}

// deriveClusterReason returns a state-change code + reason for a cluster transition.
func deriveClusterReason(state, message string) (code, reason string) {
	switch state {
	case "STARTING":
		return "CLUSTER_STARTING", "Cluster is starting"
	case "BOOTSTRAPPING":
		return "BOOTSTRAPPING", "Cluster is running bootstrap actions"
	case "RUNNING":
		return "STEP_RUNNING", "Cluster is running steps"
	case "WAITING":
		return "READY", "Cluster is waiting for steps"
	case "TERMINATING":
		return "USER_REQUEST", "User requested termination"
	case "TERMINATED":
		return "ALL_STEPS_COMPLETED", "All steps completed"
	case "TERMINATED_WITH_ERRORS":
		if message != "" {
			return "STEP_FAILURE", message
		}
		return "STEP_FAILURE", "Cluster terminated due to step failure"
	}
	return "", state
}

// emitStepStateChange updates the step record in the cluster and publishes an
// EMRStepState event on the bus with full cloud/region/account fields.
func (p *EMRProvider) emitStepStateChange(h handlerCtx, clusterID, stepID, state, reason string) {
	stepName, ok := p.updateStepRecord(context.Background(), h.accountID, h.region, clusterID, stepID, state, reason)
	if !ok {
		slog.Error("emr: emitStepStateChange — failed to load cluster for step record update",
			"clusterId", clusterID, "stepId", stepID, "state", state)
	}
	code, detailReason := deriveStepReason(state, reason)
	p.bus.Publish(events.Event{
		Type: events.EventEMRStepState,
		Payload: events.EMRStepStateEvent{
			JobFlowID:         clusterID,
			StepID:            stepID,
			Name:              stepName,
			State:             state,
			FailureReason:     reason,
			Message:           detailReason,
			StateChangeCode:   code,
			StateChangeReason: detailReason,
			Region:            h.region,
			AccountID:         h.accountID,
			Cloud:             h.cloud,
			OccurredAt:        clock.Now(),
		},
	})
}

// cancelStepIfPending applies CANCELLED to a step only if its current stored
// state is PENDING. The check and update happen inside a single loadCluster
// call, closing the CANCEL_AND_WAIT TOCTOU window vs concurrent runStep
// promotion (PENDING→RUNNING between snapshot and emit).
func (p *EMRProvider) cancelStepIfPending(ctx context.Context, h handlerCtx, clusterID, stepID, reason string) {
	c, err := p.loadCluster(ctx, h.accountID, h.region, clusterID)
	if err != nil {
		slog.Error("emr: cancelStepIfPending — failed to load cluster",
			"clusterId", clusterID, "stepId", stepID, "err", err)
		return
	}
	now := nowUnix()
	stepName := ""
	applied := false
	for i, raw := range c.Steps {
		sid, _ := raw["Id"].(string)
		if sid != stepID {
			continue
		}
		stepName, _ = raw["Name"].(string)
		status, _ := raw["Status"].(map[string]any)
		if st, _ := status["State"].(string); st != "PENDING" {
			return // concurrent runStep already advanced — skip
		}
		status["State"] = "CANCELLED"
		timeline, _ := status["Timeline"].(map[string]any)
		if timeline == nil {
			timeline = map[string]any{}
			status["Timeline"] = timeline
		}
		timeline["EndDateTime"] = now
		c.Steps[i] = raw
		applied = true
		break
	}
	if !applied {
		return
	}
	p.saveCluster(ctx, h.accountID, h.region, c)
	code, detailReason := deriveStepReason("CANCELLED", reason)
	p.bus.Publish(events.Event{
		Type: events.EventEMRStepState,
		Payload: events.EMRStepStateEvent{
			JobFlowID:         clusterID,
			StepID:            stepID,
			Name:              stepName,
			State:             "CANCELLED",
			FailureReason:     reason,
			Message:           detailReason,
			StateChangeCode:   code,
			StateChangeReason: detailReason,
			Region:            h.region,
			AccountID:         h.accountID,
			Cloud:             h.cloud,
			OccurredAt:        clock.Now(),
		},
	})
}

// emitClusterStateChange publishes an EMRClusterState event on the bus.
func (p *EMRProvider) emitClusterStateChange(h handlerCtx, clusterID, name, state, message string) {
	code, detailReason := deriveClusterReason(state, message)
	p.bus.Publish(events.Event{
		Type: events.EventEMRClusterState,
		Payload: events.EMRClusterStateEvent{
			ClusterID:         clusterID,
			Name:              name,
			State:             state,
			Message:           detailReason,
			StateChangeCode:   code,
			StateChangeReason: detailReason,
			Region:            h.region,
			AccountID:         h.accountID,
			Cloud:             h.cloud,
			OccurredAt:        clock.Now(),
		},
	})
}
