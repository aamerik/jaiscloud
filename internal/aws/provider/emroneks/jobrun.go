package emroneks

import (
	"context"
	"log/slog"

	sparkaws "jaiscloud/internal/aws/provider/sparkaws"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

// runJobRun executes a StartJobRun via sparkhelpers.SubmitClientMode.
// Runs in a goroutine; publishes state transitions via emitJobRunStateChange.
func (p *EMRContainersProvider) runJobRun(ctx context.Context, h handlerCtx,
	vc virtualCluster, jrID string, executionRoleArn string, params map[string]any) {

	vcID := vc.ID

	// Set up the log sink and extract monitoringConfiguration early so it is
	// available at every terminal exit point below.
	monitoringConfig, _ := params["monitoringConfiguration"].(map[string]any)
	logSink := p.LogSinkForJobRun(vcID, jrID, "")

	// flushLogs is a helper called at every terminal exit to ship buffered logs.
	flushLogs := func() {
		p.flushJobRunLogs(context.Background(), vcID, jrID, monitoringConfig, logSink)
	}

	// Per-jobrun cancellable context — CancelJobRun signals this to interrupt.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	p.cancelsMu.Lock()
	p.cancels[jrID] = runCancel
	p.cancelsMu.Unlock()

	defer func() {
		p.cancelsMu.Lock()
		delete(p.cancels, jrID)
		p.cancelsMu.Unlock()
	}()

	// Bail early if CancelJobRun already signalled this context.
	if runCtx.Err() != nil {
		return
	}
	p.emitJobRunStateChange(h, vcID, jrID, "RUNNING", "")

	ep, sparkArgs, jarArgs := extractJobRunEntryPoint(params)
	if ep == nil {
		p.emitJobRunStateChange(h, vcID, jrID, "FAILED", "no sparkSubmitJobDriver in jobDriver")
		return
	}

	ns := vc.Namespace
	if ns == "" {
		ns = p.namespace
	}
	if ns == "" {
		ns = "jaiscloud"
	}

	identityMutator := buildIRSAMutator(ns, vc.ServiceAccountName, executionRoleArn)

	labels := map[string]string{
		"jaiscloud.io/provider":        "emroneks",
		"jaiscloud.io/vc-id":           vcID,
		"jaiscloud.io/vc-name":         vc.Name,
		"jaiscloud.io/job-run-id":      jrID,
		"jaiscloud.io/spark-id":        jrID,
		"app.kubernetes.io/managed-by": "jaiscloud",
	}
	if p.instanceID != "" {
		labels["jaiscloud.io/instance-id"] = p.instanceID
	}

	// Per-job emulator config: use caller's account+region so driver pods
	// SigV4-sign as the submitting account (§18.1).
	jobEmulator := p.awsEmulator
	if jobEmulator != nil {
		copy := *jobEmulator
		copy.AccountID = h.accountID
		copy.Region = h.region
		jobEmulator = &copy
	}
	driverEnv := sparkaws.DriverEnv(jobEmulator)
	job := sparkhelpers.ClientModeJob{
		JobID:              jrID,
		Namespace:          ns,
		Image:              p.sparkImage,
		EntryPoint:         ep,
		SparkSubmitArgs:    sparkArgs,
		JarArgs:            jarArgs,
		PlatformOverlay:    p.platformCfg,
		IdentityMutator:    identityMutator,
		ServiceAccountName: p.serviceAccountName,
		ExtraDriverEnv:     driverEnv,
		ExtraSparkConfs:    sparkaws.DriverSparkConfsFromEnv(jobEmulator, driverEnv),
		Labels:             labels,
	}

	handle, err := sparkhelpers.SubmitClientMode(runCtx, p.k8sClient, job)
	if err != nil {
		if runCtx.Err() != nil {
			return // cancelled by CancelJobRun — let it emit CANCELLED
		}
		slog.Warn("emroneks: SubmitClientMode failed", "jobRun", jrID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(runCtx, p.resources, h.accountID, h.region, "emroneks/jobruns", jrID,
			k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emroneks: PersistTerminalSnapshot failed", "prefix", "emroneks/jobruns", "id", jrID, "err", snapErr)
		}
		flushLogs()
		p.emitJobRunStateChange(h, vcID, jrID, "FAILED", err.Error())
		return
	}

	// Persist the handle so CancelJobRun can delete the k8s Job.
	// Use p.ctx (not runCtx): runCtx may already be cancelled by CancelJobRun,
	// but the handle must survive to allow k8s Job deletion. p.ctx is only
	// cancelled on Shutdown(), so the save succeeds for per-job cancellations.
	if jr, loadErr := p.loadJobRun(p.ctx, h.accountID, h.region, vcID, jrID); loadErr != nil {
		slog.Error("emroneks: failed to store JobHandle on jobRun", "jobRun", jrID, "err", loadErr)
	} else {
		jr.JobHandle = &handle
		p.saveJobRun(p.ctx, h.accountID, h.region, jr)
	}

	final, err := sparkhelpers.WaitTerminal(runCtx, p.k8sClient, handle)
	if err != nil {
		if runCtx.Err() != nil {
			return // cancelled by CancelJobRun — let it emit CANCELLED
		}
		slog.Warn("emroneks: WaitTerminal failed", "jobRun", jrID, "err", err)
		if snapErr := k8shelpers.PersistTerminalSnapshot(runCtx, p.resources, h.accountID, h.region, "emroneks/jobruns", jrID,
			k8shelpers.BuildSnapshotFromError(err)); snapErr != nil {
			slog.Error("emroneks: PersistTerminalSnapshot failed", "prefix", "emroneks/jobruns", "id", jrID, "err", snapErr)
		}
		flushLogs()
		p.emitJobRunStateChange(h, vcID, jrID, "FAILED", err.Error())
		return
	}

	state := finalToJobRunState(final)
	reason := ""
	if state == "FAILED" {
		reason = final.SparkReason
	}
	flushLogs()
	p.emitJobRunStateChange(h, vcID, jrID, state, reason)
	if snapErr := k8shelpers.PersistTerminalSnapshot(runCtx, p.resources, h.accountID, h.region, "emroneks/jobruns", jrID,
		k8shelpers.BuildSnapshot(final.Final, state)); snapErr != nil {
		slog.Error("emroneks: PersistTerminalSnapshot failed", "prefix", "emroneks/jobruns", "id", jrID, "err", snapErr)
	}
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
