package emr

import (
	"context"
	"log/slog"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

// runSparkSubmitStep executes a spark-submit step via sparkhelpers.SubmitClientMode.
// Runs in a goroutine; publishes state transitions via emitStepStateChange.
func (p *EMRProvider) runSparkSubmitStep(ctx context.Context, clusterID, stepID string, stepCfg map[string]any) {
	p.emitStepStateChange(clusterID, stepID, "RUNNING", "")

	argv := extractStepArgv(stepCfg)
	if len(argv) == 0 {
		p.emitStepStateChange(clusterID, stepID, "FAILED", "empty HadoopJarStep.Args")
		return
	}

	// argv[0] is "spark-submit"; translate YARN master → k8s client mode.
	translated := sparkhelpers.TranslateEMREC2YarnArgs(argv[1:])

	ep, sparkArgs, userArgs, err := parseSparkSubmitArgv(translated)
	if err != nil {
		slog.Warn("emr: failed to parse spark-submit args", "step", stepID, "err", err)
		p.emitStepStateChange(clusterID, stepID, "FAILED", err.Error())
		_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err))
		return
	}

	ns := p.namespace
	if ns == "" {
		ns = "jaiscloud"
	}

	job := sparkhelpers.ClientModeJob{
		JobID:           stepID,
		Namespace:       ns,
		EntryPoint:      ep,
		SparkSubmitArgs: sparkArgs,
		JarArgs:         userArgs,
		PlatformOverlay: p.platformCfg,
		Labels: map[string]string{
			"jaiscloud.io/provider":          "emr",
			"jaiscloud.io/cluster-id":        clusterID,
			"jaiscloud.io/step-id":           stepID,
			"app.kubernetes.io/managed-by":   "jaiscloud",
		},
	}

	handle, err := sparkhelpers.SubmitClientMode(ctx, p.k8sClient, job)
	if err != nil {
		slog.Warn("emr: SubmitClientMode failed", "step", stepID, "err", err)
		p.emitStepStateChange(clusterID, stepID, "FAILED", err.Error())
		_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err))
		return
	}

	final, err := sparkhelpers.WaitTerminal(ctx, p.k8sClient, handle)
	if err != nil {
		slog.Warn("emr: WaitTerminal failed", "step", stepID, "err", err)
		p.emitStepStateChange(clusterID, stepID, "FAILED", err.Error())
		_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err))
		return
	}

	state := finalToStepState(final)
	reason := ""
	if state == "FAILED" {
		reason = final.SparkReason
	}
	p.emitStepStateChange(clusterID, stepID, state, reason)
	_ = k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID,
		k8shelpers.BuildSnapshot(final.Final, state))
}

// finalToStepState maps a sparkhelpers.Final to an EMR step state string.
func finalToStepState(f sparkhelpers.Final) string {
	if f.SparkSucceeded {
		return "COMPLETED"
	}
	if f.Cancelled {
		return "CANCELLED"
	}
	return "FAILED"
}
