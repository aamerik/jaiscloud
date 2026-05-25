package emroneks

import (
	"context"
	"log/slog"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

// handlerCtx captures the cloud/region/account triplet from a NormalizedRequest
// at handler entry so downstream goroutines can publish state-change events
// without losing provenance.
type handlerCtx struct {
	cloud     model.Cloud
	region    string
	accountID string
}

func newHandlerCtx(nr *model.NormalizedRequest) handlerCtx {
	return handlerCtx{cloud: nr.Cloud, region: nr.Region, accountID: nr.AccountID}
}

// emitJobRunStateChange updates the job-run record in the store and publishes
// an EMRJobRunState event on the bus with full cloud/region/account provenance.
func (p *EMRContainersProvider) emitJobRunStateChange(h handlerCtx, vcID, jrID, state, reason string) {
	ctx := context.Background()
	jr, err := p.loadJobRun(ctx, h.accountID, h.region, vcID, jrID)
	if err != nil {
		slog.Warn("emroneks: emitJobRunStateChange — failed to load job run",
			"vcID", vcID, "jobRunID", jrID, "state", state, "err", err)
		return
	}
	jr.State = state
	if state == "FAILED" && reason != "" {
		jr.FailureReason = reason
	}
	p.saveJobRun(ctx, h.accountID, h.region, jr)
	p.bus.Publish(events.Event{
		Type: events.EventEMRJobRunState,
		Payload: events.EMRJobRunStateEvent{
			VirtualClusterID: vcID,
			JobRunID:         jrID,
			Name:             jr.Name,
			State:            state,
			ReleaseLabel:     jr.ReleaseLabel,
			ExecutionRoleArn: jr.ExecutionRole,
			FailureReason:    reason,
			CreatedAt:        jr.CreatedAt,
			UpdatedAt:        clock.Now(),
			Region:           h.region,
			AccountID:        h.accountID,
			Cloud:            h.cloud,
		},
	})
}
