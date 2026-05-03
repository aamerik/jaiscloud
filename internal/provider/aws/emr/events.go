package emr

import (
	"context"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

// emitStepStateChange updates the step record in the cluster and publishes
// an EMRStepState event on the bus.
func (p *EMRProvider) emitStepStateChange(clusterID, stepID, state, reason string) {
	stepName := p.updateStepRecord(context.Background(), clusterID, stepID, state, reason)
	p.bus.Publish(events.Event{
		Type: events.EventEMRStepState,
		Payload: events.EMRStepStateEvent{
			JobFlowID:     clusterID,
			StepID:        stepID,
			Name:          stepName,
			State:         state,
			FailureReason: reason,
		},
	})
}

// emitClusterStateChange publishes an EMRClusterState event on the bus.
func (p *EMRProvider) emitClusterStateChange(clusterID, state, reason string) {
	p.bus.Publish(events.Event{
		Type: events.EventEMRClusterState,
		Payload: events.EMRClusterStateEvent{
			ClusterID: clusterID,
			State:     state,
			Message:   reason,
			Cloud:     model.CloudAWS,
		},
	})
}
