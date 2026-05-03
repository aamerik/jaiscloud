package emroneks

import (
	"context"
	"log/slog"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

// runJobRun executes a StartJobRun via sparkhelpers.SubmitClientMode.
// Runs in a goroutine; publishes state transitions via emitJobRunStateChange.
func (p *EMRContainersProvider) runJobRun(ctx context.Context, vcID, jrID string, params map[string]any) {
	p.emitJobRunStateChange(vcID, jrID, "RUNNING", "")

	ep, sparkArgs, jarArgs := extractJobRunEntryPoint(params)
	if ep == nil {
		p.emitJobRunStateChange(vcID, jrID, "FAILED", "no sparkSubmitJobDriver in jobDriver")
		return
	}

	ns := p.namespace
	if ns == "" {
		ns = "jaiscloud"
	}

	job := sparkhelpers.ClientModeJob{
		JobID:           jrID,
		Namespace:       ns,
		EntryPoint:      ep,
		SparkSubmitArgs: sparkArgs,
		JarArgs:         jarArgs,
		PlatformOverlay: p.platformCfg,
		Labels: map[string]string{
			"jaiscloud.io/provider":        "emroneks",
			"jaiscloud.io/vc-id":           vcID,
			"jaiscloud.io/job-run-id":      jrID,
			"app.kubernetes.io/managed-by": "jaiscloud",
		},
	}

	handle, err := sparkhelpers.SubmitClientMode(ctx, p.k8sClient, job)
	if err != nil {
		slog.Warn("emroneks: SubmitClientMode failed", "jobRun", jrID, "err", err)
		p.emitJobRunStateChange(vcID, jrID, "FAILED", err.Error())
		_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emroneks/jobruns", jrID,
			k8shelpers.BuildSnapshotFromError(err))
		return
	}

	final, err := sparkhelpers.WaitTerminal(ctx, p.k8sClient, handle)
	if err != nil {
		slog.Warn("emroneks: WaitTerminal failed", "jobRun", jrID, "err", err)
		p.emitJobRunStateChange(vcID, jrID, "FAILED", err.Error())
		_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emroneks/jobruns", jrID,
			k8shelpers.BuildSnapshotFromError(err))
		return
	}

	state := finalToJobRunState(final)
	reason := ""
	if state == "FAILED" {
		reason = final.SparkReason
	}
	p.emitJobRunStateChange(vcID, jrID, state, reason)
	_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emroneks/jobruns", jrID,
		k8shelpers.BuildSnapshot(final.Final, state))
}

// finalToJobRunState maps a sparkhelpers.Final to an EMR job-run state string.
func finalToJobRunState(f sparkhelpers.Final) string {
	if f.SparkSucceeded {
		return "COMPLETED"
	}
	if f.Cancelled {
		return "CANCELLED"
	}
	return "FAILED"
}
