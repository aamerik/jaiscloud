package dataproc

import (
	"context"
	"log/slog"
	"time"

	"jaiscloud/internal/gcp/sparkgcp"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

const tailLogsTimeout = 60 * time.Second

// cancelKey scopes a job cancellation to its project+region so a CancelJob in
// one region cannot cancel a same-named job in another.
func cancelKey(project, region, jobID string) string {
	return project + "/" + region + "/" + jobID
}

// runJob executes a stored job via sparkhelpers.SubmitClientMode. Runs in a
// goroutine; the terminal state is written back to the jobs store (first-write
// wins via the store UpdateJob) and a terminal snapshot is persisted for
// post-GC rehydration parity.
func (s *Service) runJob(ctx context.Context, project, region string, j dpstore.Job) {
	jobID := j.JobID

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	key := cancelKey(project, region, jobID)
	s.cancelsMu.Lock()
	s.cancels[key] = runCancel
	s.cancelsMu.Unlock()
	defer func() {
		s.cancelsMu.Lock()
		delete(s.cancels, key)
		s.cancelsMu.Unlock()
	}()

	ep, sparkArgs, jarArgs, err := entryPointForJob(j)
	if err != nil {
		s.finishJob(project, region, j, "ERROR", err.Error())
		return
	}

	ns := s.namespace
	if ns == "" {
		ns = "jaiscloud"
	}

	var identityMutator k8shelpers.IdentityMutator
	if s.serviceAccountName != "" {
		identityMutator = sparkgcp.BuildWorkloadIdentityMutator(ns, s.serviceAccountName, project)
	}

	labels := map[string]string{
		"jaiscloud.io/provider":        "dataproc",
		"jaiscloud.io/project":         project,
		"jaiscloud.io/region":          region,
		"jaiscloud.io/cluster-name":    j.PlacementClusterName,
		"jaiscloud.io/job-id":          jobID,
		"jaiscloud.io/spark-id":        jobID,
		"app.kubernetes.io/managed-by": "jaiscloud",
	}
	if s.instanceID != "" {
		labels["jaiscloud.io/instance-id"] = s.instanceID
	}

	// Per-job emulator config so pods talk to the local emulator as the
	// submitting project.
	jobEmulator := s.gcpEmulator
	if jobEmulator != nil {
		copy := *jobEmulator
		copy.ProjectID = project
		jobEmulator = &copy
	}
	driverEnv := sparkgcp.DriverEnv(jobEmulator)

	clientJob := sparkhelpers.ClientModeJob{
		JobID:              jobID,
		Namespace:          ns,
		Image:              s.sparkImage,
		EntryPoint:         ep,
		SparkSubmitPath:    s.sparkSubmitPath,
		SparkSubmitArgs:    sparkArgs,
		JarArgs:            jarArgs,
		PlatformOverlay:    s.platformCfg,
		IdentityMutator:    identityMutator,
		ServiceAccountName: s.serviceAccountName,
		ExtraDriverEnv:     driverEnv,
		ExtraSparkConfs:    sparkgcp.DriverSparkConfsFromEnv(jobEmulator, driverEnv),
		Labels:             labels,
	}

	handle, err := sparkhelpers.SubmitClientMode(runCtx, s.k8sClient, clientJob)
	if err != nil {
		if runCtx.Err() != nil {
			return // cancelled by CancelJob
		}
		slog.Warn("dataproc: SubmitClientMode failed", "job", jobID, "err", err)
		s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshotFromError(err))
		s.finishJob(project, region, j, "ERROR", err.Error())
		return
	}

	final, err := sparkhelpers.WaitTerminal(runCtx, s.k8sClient, handle)
	if err != nil {
		if runCtx.Err() != nil {
			return // cancelled by CancelJob
		}
		slog.Warn("dataproc: WaitTerminal failed", "job", jobID, "err", err)
		s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshotFromError(err))
		s.finishJob(project, region, j, "ERROR", err.Error())
		return
	}

	tailCtx, tailCancel := context.WithTimeout(ctx, tailLogsTimeout)
	defer tailCancel()
	if tailErr := k8shelpers.TailLogs(tailCtx, s.k8sClient, handle, k8shelpers.LogKindMain, &discardWriter{}); tailErr != nil {
		slog.Warn("dataproc: TailLogs failed", "job", jobID, "err", tailErr)
	}

	state, reason := finalToJobState(final)
	s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshot(final.Final, state))
	s.finishJob(project, region, j, state, reason)
}

// entryPointForJob derives the sparkhelpers.EntryPoint + args for a stored job.
func entryPointForJob(j dpstore.Job) (sparkhelpers.EntryPoint, []string, []string, error) {
	ep, jarArgs, err := jobToEntryPoint(j.Type, mustJSONMap(j.TypeJob))
	if err != nil {
		return nil, nil, nil, err
	}
	return ep, propertiesToConfArgs(mustJSONMap(j.TypeJob)), jarArgs, nil
}

func (s *Service) persistSnapshot(ctx context.Context, project, region, jobID string, snap k8shelpers.Snapshot) {
	if err := k8shelpers.PersistTerminalSnapshot(ctx, s.resources, project, region, "dataproc/jobs", jobID, snap); err != nil {
		slog.Error("dataproc: PersistTerminalSnapshot failed", "prefix", "dataproc/jobs", "id", jobID, "err", err)
	}
}

// finalToJobState maps a sparkhelpers.Final to a Dataproc job state string.
func finalToJobState(f sparkhelpers.Final) (state, reason string) {
	if f.SparkSucceeded {
		return "DONE", ""
	}
	if f.Cancelled {
		return "CANCELLED", ""
	}
	return "ERROR", f.SparkReason
}

// discardWriter drops driver logs (Dataproc has no per-job log sink in the
// emulator; logs are best-effort tailed for Spark exit classification only).
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
