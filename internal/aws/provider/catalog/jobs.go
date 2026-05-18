// Package catalog — Glue Jobs implementation.
package catalog

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%016x", b)
}

// ─── Resource types ───────────────────────────────────────────────────────────

const (
	rtJob    = "glue_job"
	rtJobRun = "glue_job_run"
)

// ─── Types ────────────────────────────────────────────────────────────────────

type jobEntry struct {
	Name             string            `json:"Name"`
	Role             string            `json:"Role"`
	Command          map[string]any    `json:"Command"` // {Name, ScriptLocation, PythonVersion}
	DefaultArguments map[string]string `json:"DefaultArguments,omitempty"`
	MaxCapacity      float64           `json:"MaxCapacity,omitempty"`
	Timeout          int               `json:"Timeout,omitempty"`
	CreatedOn        time.Time         `json:"CreatedOn"`
	LastModifiedOn   time.Time         `json:"LastModifiedOn"`
}

type jobRunEntry struct {
	Id           string     `json:"Id"`
	JobName      string     `json:"JobName"`
	JobRunState  string     `json:"JobRunState"` // STARTING | RUNNING | STOPPING | STOPPED | SUCCEEDED | FAILED
	StartedOn    time.Time  `json:"StartedOn"`
	CompletedOn  *time.Time `json:"CompletedOn,omitempty"`
	ErrorMessage string     `json:"ErrorMessage,omitempty"`
}

// ─── ID helpers ───────────────────────────────────────────────────────────────

func jobID(name string) string             { return "job/" + name }
func jobRunID(jobName, runID string) string { return "run/" + jobName + "/" + runID }

// ─── Persistence helpers ──────────────────────────────────────────────────────

func (p *GlueProvider) saveJob(ctx context.Context, account, region string, j jobEntry) error {
	data, _ := json.Marshal(j)
	entry := store.ResourceEntry{Type: rtJob, ID: jobID(j.Name), Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *GlueProvider) loadJob(ctx context.Context, account, region, name string) (jobEntry, error) {
	e, err := p.resources.Get(ctx, account, region, rtJob, jobID(name))
	if err == store.ErrNotFound {
		return jobEntry{}, &model.ProviderError{
			Code:       "NotFound",
			Message:    fmt.Sprintf("Job %s not found", name),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err != nil {
		return jobEntry{}, err
	}
	var j jobEntry
	json.Unmarshal(e.Data, &j)
	return j, nil
}

func (p *GlueProvider) saveJobRun(ctx context.Context, account, region string, run jobRunEntry) error {
	data, _ := json.Marshal(run)
	entry := store.ResourceEntry{Type: rtJobRun, ID: jobRunID(run.JobName, run.Id), Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *GlueProvider) loadJobRun(ctx context.Context, account, region, jobName, runID string) (jobRunEntry, error) {
	e, err := p.resources.Get(ctx, account, region, rtJobRun, jobRunID(jobName, runID))
	if err == store.ErrNotFound {
		return jobRunEntry{}, &model.ProviderError{
			Code:       "NotFound",
			Message:    fmt.Sprintf("JobRun %s not found for job %s", runID, jobName),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err != nil {
		return jobRunEntry{}, err
	}
	var run jobRunEntry
	json.Unmarshal(e.Data, &run)
	return run, nil
}

// ─── Job operations ───────────────────────────────────────────────────────────

func (p *GlueProvider) CreateJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtJob, jobID(name)); err == nil {
		return nil, &model.ProviderError{Code: "AlreadyExists", Message: fmt.Sprintf("Job %s already exists", name), HTTPStatus: http.StatusBadRequest}
	}

	cmd := anyMapParam(nr.Params, "Command")
	defArgs := strMapParam(nr.Params, "DefaultArguments")
	maxCap := 0.0
	if v, ok := nr.Params["MaxCapacity"].(float64); ok {
		maxCap = v
	}
	timeout := 0
	if v, ok := nr.Params["Timeout"].(float64); ok {
		timeout = int(v)
	}

	now := time.Now()
	j := jobEntry{
		Name:             name,
		Role:             strParam(nr.Params, "Role"),
		Command:          cmd,
		DefaultArguments: defArgs,
		MaxCapacity:      maxCap,
		Timeout:          timeout,
		CreatedOn:        now,
		LastModifiedOn:   now,
	}
	if err := p.saveJob(ctx, nr.AccountID, nr.Region, j); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Name": name}), nil
}

func (p *GlueProvider) UpdateJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "JobName")
	j, err := p.loadJob(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	update, _ := nr.Params["JobUpdate"].(map[string]any)
	if update == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "JobUpdate is required", HTTPStatus: http.StatusBadRequest}
	}
	if v, ok := update["Role"].(string); ok {
		j.Role = v
	}
	if v, ok := update["Command"].(map[string]any); ok {
		j.Command = v
	}
	if v, ok := update["DefaultArguments"].(map[string]any); ok {
		j.DefaultArguments = toStringMap(v)
	}
	if v, ok := update["MaxCapacity"].(float64); ok {
		j.MaxCapacity = v
	}
	if v, ok := update["Timeout"].(float64); ok {
		j.Timeout = int(v)
	}
	j.LastModifiedOn = time.Now()
	if err := p.saveJob(ctx, nr.AccountID, nr.Region, j); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"JobName": name}), nil
}

func (p *GlueProvider) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "JobName")
	if _, err := p.loadJob(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtJob, jobID(name)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"JobName": name}), nil
}

func (p *GlueProvider) GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "JobName")
	j, err := p.loadJob(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Job": jobToWire(j)}), nil
}

func (p *GlueProvider) GetJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtJob, "job/")
	if err != nil {
		return nil, err
	}
	jobs := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var j jobEntry
		if json.Unmarshal(e.Data, &j) == nil {
			jobs = append(jobs, jobToWire(j))
		}
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(jobs, maxResults, token, "GetJobs")
	if pgErr != nil {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: pgErr.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data := map[string]any{"Jobs": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *GlueProvider) StartJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobName := strParam(nr.Params, "JobName")
	if _, err := p.loadJob(ctx, nr.AccountID, nr.Region, jobName); err != nil {
		return nil, err
	}
	runID := newID()
	now := time.Now()
	run := jobRunEntry{
		Id:          runID,
		JobName:     jobName,
		JobRunState: "STARTING",
		StartedOn:   now,
	}

	// Mock mode: immediately mark as SUCCEEDED
	completedNow := time.Now()
	run.JobRunState = "SUCCEEDED"
	run.CompletedOn = &completedNow

	if err := p.saveJobRun(ctx, nr.AccountID, nr.Region, run); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"JobRunId": runID}), nil
}

func (p *GlueProvider) GetJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobName := strParam(nr.Params, "JobName")
	runID := strParam(nr.Params, "RunId")
	run, err := p.loadJobRun(ctx, nr.AccountID, nr.Region, jobName, runID)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"JobRun": jobRunToWire(run)}), nil
}

func (p *GlueProvider) GetJobRuns(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobName := strParam(nr.Params, "JobName")
	prefix := "run/" + jobName + "/"
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtJobRun, prefix)
	if err != nil {
		return nil, err
	}
	runs := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var run jobRunEntry
		if json.Unmarshal(e.Data, &run) == nil && run.JobName == jobName {
			runs = append(runs, jobRunToWire(run))
		}
	}
	maxResults := 200
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(runs, maxResults, token, "GetJobRuns")
	if pgErr != nil {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: pgErr.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data := map[string]any{"JobRuns": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *GlueProvider) BatchStopJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobName := strParam(nr.Params, "JobName")
	runIDs := strSliceParam(nr.Params, "JobRunIds")

	errors := []map[string]any{}
	for _, runID := range runIDs {
		run, err := p.loadJobRun(ctx, nr.AccountID, nr.Region, jobName, runID)
		if err != nil {
			errors = append(errors, map[string]any{
				"JobRunId":    runID,
				"ErrorDetail": map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": "JobRun not found"},
			})
			continue
		}
		// Only RUNNING jobs can be stopped; others are a no-op
		if run.JobRunState == "RUNNING" || run.JobRunState == "STARTING" {
			run.JobRunState = "STOPPED"
			now := time.Now()
			run.CompletedOn = &now
			p.saveJobRun(ctx, nr.AccountID, nr.Region, run) //nolint:errcheck
		}
	}
	return provider.OK(map[string]any{
		"SuccessfulSubmissions": []map[string]any{},
		"Errors":               errors,
	}), nil
}

// ─── Wire serialisation ───────────────────────────────────────────────────────

func jobToWire(j jobEntry) map[string]any {
	return map[string]any{
		"Name":             j.Name,
		"Role":             j.Role,
		"Command":          j.Command,
		"DefaultArguments": j.DefaultArguments,
		"MaxCapacity":      j.MaxCapacity,
		"Timeout":          j.Timeout,
		"CreatedOn":        float64(j.CreatedOn.UnixMilli()) / 1000.0,
		"LastModifiedOn":   float64(j.LastModifiedOn.UnixMilli()) / 1000.0,
	}
}

func jobRunToWire(run jobRunEntry) map[string]any {
	m := map[string]any{
		"Id":          run.Id,
		"JobName":     run.JobName,
		"JobRunState": run.JobRunState,
		"StartedOn":   float64(run.StartedOn.UnixMilli()) / 1000.0,
	}
	if run.CompletedOn != nil {
		m["CompletedOn"] = float64(run.CompletedOn.UnixMilli()) / 1000.0
	}
	if run.ErrorMessage != "" {
		m["ErrorMessage"] = run.ErrorMessage
	}
	return m
}

// ─── Utility helpers ──────────────────────────────────────────────────────────

func toStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}
