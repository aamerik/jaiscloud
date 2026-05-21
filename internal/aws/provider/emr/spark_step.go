package emr

import (
	"context"
	"log/slog"

	sparkaws "jaiscloud/internal/aws/provider/sparkaws"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

// clusterConfToSparkConfs converts cluster Configurations[] spark-defaults entries
// into "--conf k=v" flags suitable for appending to SparkSubmitArgs.
//
// For core-site/hdfs-site/yarn-site classifications, we note them in a comment
// but skip actual implementation (would require ConfigMap-mounted XML in k8s mode).
func clusterConfToSparkConfs(confs []emrConfiguration) []string {
	var out []string
	for _, c := range confs {
		switch c.Classification {
		case "spark-defaults":
			for k, v := range c.Properties {
				out = append(out, "--conf", k+"="+v)
			}
		// TODO(future): core-site / hdfs-site / yarn-site would be mounted as
		// ConfigMap XML files at /etc/hadoop/conf/<site>.xml in k8s mode.
		default:
			// other classifications (e.g. hadoop) — ignored for now
		}
	}
	return out
}

// runSparkSubmitStep executes a spark-submit step via sparkhelpers.SubmitClientMode.
// Runs in a goroutine; publishes state transitions via emitStepStateChange.
func (p *EMRProvider) runSparkSubmitStep(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	actionOnFailure, _ := stepCfg["ActionOnFailure"].(string)
	if actionOnFailure == "" {
		actionOnFailure = "CONTINUE"
	}

	sink := p.LogSinkForStep(clusterID, stepID, "")
	p.emitStepStateChange(h, clusterID, stepID, "RUNNING", "")

	// failStep emits FAILED and applies ActionOnFailure cascade. Every error path
	// uses this helper so the cascade is never forgotten.
	failStep := func(reason string) {
		p.flushStepLogs(ctx, clusterID, stepID, sink)
		p.emitStepStateChange(h, clusterID, stepID, "FAILED", reason)
		p.cascadeOnStepFailure(ctx, h, clusterID, stepID, actionOnFailure)
	}

	argv := extractStepArgv(stepCfg)
	if len(argv) == 0 {
		failStep("empty HadoopJarStep.Args")
		return
	}

	// Load cluster Configurations to build spark-defaults --conf flags.
	var confArgs []string
	if c, loadErr := p.loadCluster(ctx, h.accountID, h.region, clusterID); loadErr == nil {
		confArgs = clusterConfToSparkConfs(c.Configurations)
	}

	// argv[0] is "spark-submit"; translate YARN master → k8s client mode.
	translated := sparkhelpers.TranslateEMREC2YarnArgs(argv[1:])

	ep, sparkArgs, userArgs, err := parseSparkSubmitArgv(translated)
	if err != nil {
		slog.Warn("emr: failed to parse spark-submit args", "step", stepID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, h.accountID, h.region, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(err.Error())
		return
	}

	// Prepend cluster Configurations confs before step-level spark args.
	// Spark last-value-wins, so step args override cluster defaults.
	sparkArgs = append(confArgs, sparkArgs...)

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

	// Build a per-step emulator config that carries the caller's account and
	// region so Spark driver pods SigV4-sign as the submitting account, not
	// the server-global default (§18.1).
	stepEmulator := p.awsEmulator
	if stepEmulator != nil {
		copy := *stepEmulator
		copy.AccountID = h.accountID
		copy.Region = h.region
		stepEmulator = &copy
	}
	driverEnv := sparkaws.DriverEnv(stepEmulator)
	job := sparkhelpers.ClientModeJob{
		JobID:              stepID,
		Namespace:          ns,
		Image:              p.sparkImage,
		EntryPoint:         ep,
		SparkSubmitArgs:    sparkArgs,
		JarArgs:            userArgs,
		PlatformOverlay:    p.platformCfg,
		ServiceAccountName: p.serviceAccountName,
		ExtraDriverEnv:     driverEnv,
		ExtraSparkConfs:    sparkaws.DriverSparkConfsFromEnv(stepEmulator, driverEnv),
		Labels:             labels,
	}

	handle, err := sparkhelpers.SubmitClientMode(ctx, p.k8sClient, job)
	if err != nil {
		slog.Warn("emr: SubmitClientMode failed", "step", stepID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, h.accountID, h.region, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(err.Error())
		return
	}

	final, err := sparkhelpers.WaitTerminal(ctx, p.k8sClient, handle)
	if err != nil {
		slog.Warn("emr: WaitTerminal failed", "step", stepID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, h.accountID, h.region, "emr/steps", stepID, k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(err.Error())
		return
	}

	// Collect driver pod logs into sink for S3 upload.
	if err := k8shelpers.TailLogs(ctx, p.k8sClient, handle, k8shelpers.LogKindMain, sink); err != nil {
		slog.Warn("emr: TailLogs failed", "step", stepID, "err", err)
		// proceed anyway; logs best-effort
	}

	state := finalToStepState(final)
	reason := ""
	if state == "FAILED" {
		reason = final.SparkReason
	}
	if state == "FAILED" {
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, h.accountID, h.region, "emr/steps", stepID,
			k8shelpers.BuildSnapshot(final.Final, state)); snapErr != nil {
			slog.Error("emr: PersistTerminalSnapshot failed", "prefix", "emr/steps", "id", stepID, "err", snapErr)
		}
		failStep(reason)
	} else {
		p.flushStepLogs(ctx, clusterID, stepID, sink)
		p.emitStepStateChange(h, clusterID, stepID, state, reason)
		if snapErr := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, h.accountID, h.region, "emr/steps", stepID,
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
