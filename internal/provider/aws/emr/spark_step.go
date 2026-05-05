package emr

import (
	"context"
	"log/slog"

	"jaiscloud/internal/k8shelpers"
	sparkaws "jaiscloud/internal/provider/aws/sparkaws"
	"jaiscloud/internal/sparkhelpers"
)

// runSparkSubmitStep executes a spark-submit step via sparkhelpers.SubmitClientMode.
// Runs in a goroutine; publishes state transitions via emitStepStateChange.
func (p *EMRProvider) runSparkSubmitStep(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	actionOnFailure, _ := stepCfg["ActionOnFailure"].(string)
	if actionOnFailure == "" {
		actionOnFailure = "CONTINUE"
	}

	p.emitStepStateChange(h, clusterID, stepID, "RUNNING", "")

	// failStep emits FAILED and applies ActionOnFailure cascade. Every error path
	// uses this helper so the cascade is never forgotten.
	failStep := func(reason string) {
		p.emitStepStateChange(h, clusterID, stepID, "FAILED", reason)
		p.cascadeOnStepFailure(ctx, h, clusterID, stepID, actionOnFailure)
	}

	argv := extractStepArgv(stepCfg)
	if len(argv) == 0 {
		failStep("empty HadoopJarStep.Args")
		return
	}

	// argv[0] is "spark-submit"; translate YARN master → k8s client mode.
	translated := sparkhelpers.TranslateEMREC2YarnArgs(argv[1:])

	ep, sparkArgs, userArgs, err := parseSparkSubmitArgv(translated)
	if err != nil {
		slog.Warn("emr: failed to parse spark-submit args", "step", stepID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(err.Error())
		return
	}

	ns := p.namespace
	if ns == "" {
		ns = "jaiscloud"
	}

	labels := map[string]string{
		"jaiscloud.io/provider":        "emr",
		"jaiscloud.io/cluster-id":      clusterID,
		"jaiscloud.io/step-id":         stepID,
		"jaiscloud.io/spark-id":        stepID,
		"app.kubernetes.io/managed-by": "jaiscloud",
	}
	if p.instanceID != "" {
		labels["jaiscloud.io/instance-id"] = p.instanceID
	}

	job := sparkhelpers.ClientModeJob{
		JobID:           stepID,
		Namespace:       ns,
		Image:           p.sparkImage,
		EntryPoint:      ep,
		SparkSubmitArgs: sparkArgs,
		JarArgs:         userArgs,
		PlatformOverlay: p.platformCfg,
		ExtraDriverEnv:  sparkaws.DriverEnv(p.awsEmulator),
		ExtraSparkConfs: sparkaws.DriverSparkConfs(p.awsEmulator),
		Labels:          labels,
	}

	handle, err := sparkhelpers.SubmitClientMode(ctx, p.k8sClient, job)
	if err != nil {
		slog.Warn("emr: SubmitClientMode failed", "step", stepID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(err.Error())
		return
	}

	final, err := sparkhelpers.WaitTerminal(ctx, p.k8sClient, handle)
	if err != nil {
		slog.Warn("emr: WaitTerminal failed", "step", stepID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(err.Error())
		return
	}

	state := finalToStepState(final)
	reason := ""
	if state == "FAILED" {
		reason = final.SparkReason
	}
	if state == "FAILED" {
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID,
			k8shelpers.BuildSnapshot(final.Final, state)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(reason)
	} else {
		p.emitStepStateChange(h, clusterID, stepID, state, reason)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, "emr/steps", stepID,
			k8shelpers.BuildSnapshot(final.Final, state)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
	}
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
