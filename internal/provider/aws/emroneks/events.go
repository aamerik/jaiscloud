package emroneks

import (
	"context"

	"jaiscloud/internal/events"
)

// emitJobRunStateChange updates the job-run record in the store and publishes
// an EMRJobRunState event on the bus.
func (p *EMRContainersProvider) emitJobRunStateChange(vcID, jrID, state, reason string) {
	ctx := context.Background()
	jr, err := p.loadJobRun(ctx, vcID, jrID)
	if err != nil {
		return
	}
	jr.State = state
	if state == "FAILED" && reason != "" {
		jr.FailureReason = reason
	}
	p.saveJobRun(ctx, jr)
	p.bus.Publish(events.Event{
		Type: events.EventEMRJobRunState,
		Payload: events.EMRJobRunStateEvent{
			VirtualClusterID: vcID,
			JobRunID:         jrID,
			Name:             jr.Name,
			State:            state,
			FailureReason:    reason,
		},
	})
}
