package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// SubmitJob submits a job to a cluster. In mock mode the job completes
// synchronously; in k8s mode a background goroutine runs it and the terminal
// state is written back to the store.
func (s *Service) SubmitJob(ctx context.Context, project, region string, in JobInput) (dpstore.Job, error) {
	return s.submitJob(ctx, project, region, in)
}

func (s *Service) submitJob(ctx context.Context, project, region string, in JobInput) (dpstore.Job, error) {
	if region == "" {
		return dpstore.Job{}, invalidArgument("missing region")
	}
	// placement.clusterName is REQUIRED (dataproc.v1.JobPlacement). Validate it
	// before the cluster existence check.
	if in.PlacementClusterName == "" {
		return dpstore.Job{}, invalidArgument("missing placement.clusterName")
	}

	j := jobToStore(project, region, in)
	if _, err := s.store.GetCluster(ctx, project, region, j.PlacementClusterName); err != nil {
		return dpstore.Job{}, model.NewProviderError("NotFound", "cluster not found: "+j.PlacementClusterName, 404)
	}

	// Fail-loud on unsupported job types — never silently succeed.
	if unsupportedJobTypes[j.Type] {
		now := clock.Now().UTC()
		j.Status = dpstore.JobStatus{
			State:          "ERROR",
			Details:        "job type " + j.Type + " is not supported by the emulator",
			StateStartTime: now,
		}
		if err := s.store.CreateJob(ctx, project, region, j); err != nil {
			return dpstore.Job{}, mapCreateJobErr(err)
		}
		return j, nil
	}

	// Validate the entry point before storing (malformed Spark job → ERROR).
	if _, _, err := jobToEntryPoint(j.Type, mustJSONMap(j.TypeJob)); err != nil {
		now := clock.Now().UTC()
		j.Status = dpstore.JobStatus{State: "ERROR", Details: err.Error(), StateStartTime: now}
		if createErr := s.store.CreateJob(ctx, project, region, j); createErr != nil {
			return dpstore.Job{}, mapCreateJobErr(createErr)
		}
		return j, nil
	}

	if err := s.store.CreateJob(ctx, project, region, j); err != nil {
		return dpstore.Job{}, mapCreateJobErr(err)
	}

	// Mock mode: complete synchronously (no goroutine). K8s mode: run for real.
	if s.k8sClient == nil {
		now := clock.Now().UTC()
		j.StatusHistory = append(j.StatusHistory, j.Status)
		j.Status = dpstore.JobStatus{State: "DONE", StateStartTime: now}
		j.DriverOutputResourceURI = "gs://jaiscloud-dataproc/" + j.JobUUID + "/driveroutput"
		if err := s.store.UpdateJob(ctx, project, region, j); err != nil {
			slog.Warn("dataproc: mock SubmitJob update failed", "job", j.JobID, "err", err)
		}
		return j, nil
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runJob(s.ctx, project, region, j)
	}()
	return j, nil
}

// mapCreateJobErr translates store.CreateJob's sentinel errors into the
// matching wire error. Used by every CreateJob call site in submitJob so a
// duplicate client-specified jobId always surfaces as 409 AlreadyExists,
// regardless of which job-type branch triggered the create.
func mapCreateJobErr(err error) error {
	if errors.Is(err, dpstore.ErrAlreadyExists) {
		return model.NewProviderError("AlreadyExists", "job already exists", 409)
	}
	return err
}

// SubmitJobAsOperation wraps SubmitJob in a long-running operation.
func (s *Service) SubmitJobAsOperation(ctx context.Context, project, region string, in JobInput) (dpstore.Operation, error) {
	if region == "" {
		return dpstore.Operation{}, invalidArgument("missing region")
	}
	j, err := s.submitJob(ctx, project, region, in)
	if err != nil {
		return dpstore.Operation{}, err
	}
	target := JobName(project, region, j.JobID)
	now := clock.Now().UTC()
	op := dpstore.Operation{
		ID:         j.JobID,
		ProjectID:  project,
		Region:     region,
		Done:       jobTerminal(j.Status.State),
		Verb:       "submit",
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if meta, err := json.Marshal(jobOperationMetadata(j.JobID, j.Status.State, "SUBMIT", now)); err == nil {
		op.Metadata = string(meta)
	}
	if op.Done {
		if resp, err := json.Marshal(JobJSON(j)); err == nil {
			op.Response = string(resp)
		}
	}
	if err := s.store.CreateOperation(ctx, project, region, op); err != nil {
		return dpstore.Operation{}, err
	}
	return op, nil
}

// GetJob returns one job.
func (s *Service) GetJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	if region == "" || jobID == "" {
		return dpstore.Job{}, invalidArgument("missing region or jobId")
	}
	j, err := s.store.GetJob(ctx, project, region, jobID)
	if err != nil {
		return dpstore.Job{}, mapErr(err)
	}
	return j, nil
}

// ListJobs returns a cursor page of the jobs in a region.
func (s *Service) ListJobs(ctx context.Context, project, region string, pageSize int, pageToken string) ([]dpstore.Job, string, error) {
	if region == "" {
		return nil, "", invalidArgument("missing region")
	}
	jobs, err := s.store.ListJobs(ctx, project, region)
	if err != nil {
		return nil, "", err
	}
	// clusterName and filter are accepted but ignored (documented limitation).
	page, next := paging.Page(jobs, func(j dpstore.Job) string { return j.JobID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateJob applies the masked fields of a job update. Dataproc jobs are
// immutable except for labels.
func (s *Service) UpdateJob(ctx context.Context, project, region, jobID string, in JobInput, mask []string) (dpstore.Job, error) {
	if region == "" || jobID == "" {
		return dpstore.Job{}, invalidArgument("missing region or jobId")
	}
	applyLabels := len(mask) == 0 || containsMaskField(mask, "labels")
	j, err := s.store.UpdateJobAtomic(ctx, project, region, jobID, func(j dpstore.Job) (dpstore.Job, error) {
		if applyLabels && in.Labels != nil {
			j.Labels = in.Labels
		}
		return j, nil
	})
	if err != nil {
		return dpstore.Job{}, mapErr(err)
	}
	return j, nil
}

// DeleteJob removes a terminal job. An active job cannot be deleted
// (FAILED_PRECONDITION).
func (s *Service) DeleteJob(ctx context.Context, project, region, jobID string) error {
	if region == "" || jobID == "" {
		return invalidArgument("missing region or jobId")
	}
	j, err := s.store.GetJob(ctx, project, region, jobID)
	if err != nil {
		return mapErr(err)
	}
	if !jobTerminal(j.Status.State) {
		return &model.ProviderError{
			Code:       "FailedPrecondition",
			Message:    "job is active and cannot be deleted",
			HTTPStatus: 400,
			Status:     "FAILED_PRECONDITION",
		}
	}
	if err := s.store.DeleteJob(ctx, project, region, jobID); err != nil {
		return mapErr(err)
	}
	return nil
}

// CancelJob transitions a non-terminal job to CANCELLED and closes its
// SubmitJobAsOperation LRO. A terminal job is a no-op.
func (s *Service) CancelJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	if region == "" || jobID == "" {
		return dpstore.Job{}, invalidArgument("missing region or jobId")
	}
	now := clock.Now().UTC()
	var transitioned bool
	j, err := s.store.UpdateJobAtomic(ctx, project, region, jobID, func(j dpstore.Job) (dpstore.Job, error) {
		if jobTerminal(j.Status.State) {
			return j, nil // already terminal — nothing to cancel
		}
		transitioned = true
		j.StatusHistory = append(j.StatusHistory, j.Status)
		j.Status = dpstore.JobStatus{State: "CANCELLED", StateStartTime: now}
		return j, nil
	})
	if err != nil {
		return dpstore.Job{}, mapErr(err)
	}
	if !transitioned {
		return j, nil
	}

	s.cancelsMu.Lock()
	cancel, ok := s.cancels[cancelKey(project, region, jobID)]
	s.cancelsMu.Unlock()
	if ok {
		cancel()
	}
	// Close out the SubmitJobAsOperation LRO for this transition — finishJob
	// won't run it (or will see the job already terminal and no-op) once
	// CancelJob has won the race for the terminal-state transition.
	s.completeSubmitOperation(project, region, j)
	return j, nil
}

// GetOperation returns a persisted long-running operation.
func (s *Service) GetOperation(ctx context.Context, project, region, opID string) (dpstore.Operation, error) {
	if region == "" || opID == "" {
		return dpstore.Operation{}, invalidArgument("missing region or operationId")
	}
	op, err := s.store.GetOperation(ctx, project, region, opID)
	if err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	return op, nil
}

// finishJob writes the terminal state back to the jobs store. The get-check-
// set cycle is atomic (UpdateJobAtomic) so it can't race with a concurrent
// CancelJob: whichever of the two acquires the lock first wins the terminal-
// state transition, and the other sees the already-terminal state inside its
// own mutate and no-ops instead of overwriting it.
func (s *Service) finishJob(project, region string, j dpstore.Job, state, details string) {
	now := clock.Now().UTC()
	var transitioned bool
	fresh, err := s.store.UpdateJobAtomic(context.Background(), project, region, j.JobID, func(fresh dpstore.Job) (dpstore.Job, error) {
		if jobTerminal(fresh.Status.State) {
			return fresh, nil // already terminal (e.g. cancelled) — first write wins
		}
		transitioned = true
		fresh.StatusHistory = append(fresh.StatusHistory, fresh.Status)
		fresh.Status = dpstore.JobStatus{State: state, Details: details, StateStartTime: now}
		if state == "DONE" && fresh.DriverOutputResourceURI == "" {
			fresh.DriverOutputResourceURI = "gs://jaiscloud-dataproc/" + fresh.JobUUID + "/driveroutput"
		}
		return fresh, nil
	})
	if err != nil {
		slog.Warn("dataproc: finishJob update failed", "job", j.JobID, "err", err)
		return
	}
	if !transitioned {
		return
	}
	s.completeSubmitOperation(project, region, fresh)
}

// completeSubmitOperation flips the SubmitJobAsOperation long-running operation
// (id == job id) to done=true with the terminal job as its response. No-op when
// the job was submitted without an operation (plain SubmitJob) or the operation
// is already terminal.
func (s *Service) completeSubmitOperation(project, region string, j dpstore.Job) {
	op, err := s.store.GetOperation(context.Background(), project, region, j.JobID)
	if err != nil {
		return
	}
	if op.Done {
		return
	}
	resp, _ := json.Marshal(JobJSON(j))
	op.Done = true
	op.Response = string(resp)
	op.EndTime = clock.Now().UTC()
	if err := s.store.UpdateOperation(context.Background(), project, region, op); err != nil {
		slog.Warn("dataproc: completeSubmitOperation failed", "job", j.JobID, "err", err)
	}
}
