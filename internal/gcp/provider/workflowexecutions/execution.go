// Package workflowexecutions implements the Cloud Workflows Executions provider
// (workflowexecutions.googleapis.com): Create/Get/List/CancelExecution. The
// emulator runs the workflow to completion synchronously on CreateExecution and
// returns the terminal execution (a documented simplification of GCP's async
// model).
package workflowexecutions

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/google/uuid"
)

// Provider handles workflow execution resources.
type Provider struct {
	workflows workflowsstore.Store
	engine    *workflowengine.Engine
}

// New returns a Provider backed by the given store and engine.
func New(workflows workflowsstore.Store, eng *workflowengine.Engine) *Provider {
	return &Provider{workflows: workflows, engine: eng}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"WorkflowExecution.CreateExecution": p.CreateExecution,
		"WorkflowExecution.GetExecution":    p.GetExecution,
		"WorkflowExecution.ListExecutions":  p.ListExecutions,
		"WorkflowExecution.CancelExecution": p.CancelExecution,
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	if body == nil {
		return nil
	}
	m, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// parseExecName extracts location, workflow ID, and execution ID from a
// relative name (locations/{l}/workflows/{w}[/executions[/{e}]]).
func parseExecName(name string) (location, workflowID, executionID string) {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		switch p {
		case "locations":
			if i+1 < len(parts) {
				location = parts[i+1]
			}
		case "workflows":
			if i+1 < len(parts) {
				workflowID = parts[i+1]
			}
		case "executions":
			if i+1 < len(parts) {
				executionID = parts[i+1]
			}
		}
	}
	return location, workflowID, executionID
}

func mapErr(err error) error {
	if errors.Is(err, workflowsstore.ErrNoSuchExecution) {
		return model.NewProviderError("NotFound", "execution not found", 404)
	}
	if errors.Is(err, workflowsstore.ErrNoSuchWorkflow) {
		return model.NewProviderError("NotFound", "workflow not found", 404)
	}
	if errors.Is(err, workflowsstore.ErrAlreadyExists) {
		return model.NewProviderError("AlreadyExists", "execution already exists", 409)
	}
	return err
}

func (p *Provider) executionToMap(nr *model.NormalizedRequest, e workflowsstore.Execution) map[string]any {
	out := map[string]any{
		"name":               nr.ResourceID("workflow-execution", e.Location+"/"+e.WorkflowID+"/"+e.ID),
		"state":              e.State,
		"workflowRevisionId": e.WorkflowRevisionID,
	}
	if !e.StartTime.IsZero() {
		out["startTime"] = e.StartTime.Format(time.RFC3339Nano)
	}
	if !e.EndTime.IsZero() {
		out["endTime"] = e.EndTime.Format(time.RFC3339Nano)
	}
	if e.Duration != "" {
		out["duration"] = e.Duration
	}
	if e.Argument != "" {
		out["argument"] = e.Argument
	}
	if e.Result != "" {
		out["result"] = e.Result
	}
	if e.Error != nil {
		out["error"] = executionErrorMap(e.Error)
	}
	if e.CallLogLevel != "" {
		out["callLogLevel"] = e.CallLogLevel
	}
	if e.Labels != nil {
		out["labels"] = e.Labels
	}
	if len(e.CurrentSteps) > 0 {
		steps := make([]any, 0, len(e.CurrentSteps))
		for _, s := range e.CurrentSteps {
			steps = append(steps, map[string]any{"routine": s.Routine, "step": s.Step})
		}
		out["status"] = map[string]any{"currentSteps": steps}
	}
	return out
}

func executionErrorMap(err *workflowsstore.ExecutionError) map[string]any {
	out := map[string]any{"payload": err.Payload}
	if err.Context != "" {
		out["context"] = err.Context
	}
	if err.StackTrace != nil {
		elements := make([]any, 0, len(err.StackTrace.Elements))
		for _, el := range err.StackTrace.Elements {
			em := map[string]any{"step": el.Step, "routine": el.Routine}
			if el.Position != nil {
				em["position"] = map[string]any{
					"line":   el.Position.Line,
					"column": el.Position.Column,
					"length": el.Position.Length,
				}
			}
			elements = append(elements, em)
		}
		out["stackTrace"] = map[string]any{"elements": elements}
	}
	return out
}

// errorPayload renders an engine error as the execution error payload JSON
// ({"message":..., "tags":[step]}).
func errorPayload(err error, step string) string {
	tags := []any{}
	if step != "" {
		tags = append(tags, step)
	}
	msg := err.Error()
	payload, _ := json.Marshal(map[string]any{"message": msg, "tags": tags})
	return string(payload)
}

func (p *Provider) CreateExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, workflowID, _ := parseExecName(strParam(nr, "name"))
	if location == "" || workflowID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing workflow in parent", 400)
	}
	wf, err := p.workflows.GetWorkflow(ctx, nr.AccountID, location, workflowID)
	if err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)

	argStr := bodyString(body, "argument")
	var argument any
	if argStr != "" {
		if err := json.Unmarshal([]byte(argStr), &argument); err != nil {
			return nil, model.NewProviderError("InvalidArgument", "argument must be valid JSON", 400)
		}
	}

	executionID := uuid.New().String()
	now := clock.Now().UTC()

	// Run the workflow to completion synchronously (documented simplification).
	// Inject the emulator's project/location/workflow context so GCP built-in
	// environment variables (GOOGLE_CLOUD_*) resolve via sys.get_env.
	res := p.engine.ExecuteWithContext(ctx, wf.SourceContents, argument, workflowengine.WorkflowContext{
		ProjectID:      nr.AccountID,
		Location:       location,
		WorkflowID:     workflowID,
		RevisionID:     wf.RevisionID,
		ServiceAccount: wf.ServiceAccount,
		ExecutionID:    executionID,
	})

	e := workflowsstore.Execution{
		ID:                 executionID,
		WorkflowID:         workflowID,
		Location:           location,
		Argument:           argStr,
		StartTime:          now,
		WorkflowRevisionID: wf.RevisionID,
		CallLogLevel:       bodyString(body, "callLogLevel"),
		Labels:             bodyStringMap(body, "labels"),
	}

	if res.Step != "" {
		e.CurrentSteps = []workflowsstore.Step{{Routine: "main", Step: res.Step}}
	}

	if res.Err != nil {
		var ve *workflowengine.ValidationError
		if errors.As(res.Err, &ve) {
			return nil, model.NewProviderError("InvalidArgument", ve.Error(), 400)
		}
		e.State = "FAILED"
		e.Error = &workflowsstore.ExecutionError{
			Payload: errorPayload(res.Err, res.Step),
			Context: res.Err.Error(),
		}
	} else {
		e.State = "SUCCEEDED"
		resultJSON, _ := json.Marshal(res.Value)
		e.Result = string(resultJSON)
	}
	e.EndTime = clock.Now().UTC()
	e.Duration = durationString(e.EndTime.Sub(e.StartTime))

	if err := p.workflows.CreateExecution(ctx, nr.AccountID, location, workflowID, executionID, e); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.executionToMap(nr, e)), nil
}

func (p *Provider) GetExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, workflowID, executionID := parseExecName(strParam(nr, "name"))
	if location == "" || workflowID == "" || executionID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing execution name", 400)
	}
	e, err := p.workflows.GetExecution(ctx, nr.AccountID, location, workflowID, executionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.executionToMap(nr, e)), nil
}

func (p *Provider) ListExecutions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, workflowID, _ := parseExecName(strParam(nr, "name"))
	if location == "" || workflowID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing workflow in parent", 400)
	}
	execs, err := p.workflows.ListExecutions(ctx, nr.AccountID, location, workflowID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(execs, func(e workflowsstore.Execution) string { return e.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, e := range page {
		items = append(items, p.executionToMap(nr, e))
	}
	resp := map[string]any{"executions": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CancelExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, workflowID, executionID := parseExecName(strParam(nr, "name"))
	if location == "" || workflowID == "" || executionID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing execution name", 400)
	}
	e, err := p.workflows.GetExecution(ctx, nr.AccountID, location, workflowID, executionID)
	if err != nil {
		return nil, mapErr(err)
	}
	// Executions complete synchronously in the emulator, so cancel is a no-op
	// except for the (never-observed) ACTIVE transition.
	if e.State == "ACTIVE" {
		e.State = "CANCELLED"
		e.EndTime = clock.Now().UTC()
		e.Error = &workflowsstore.ExecutionError{Payload: errorPayload(errors.New("execution cancelled"), "")}
		if err := p.workflows.UpdateExecution(ctx, nr.AccountID, location, workflowID, executionID, e); err != nil {
			return nil, mapErr(err)
		}
	}
	return provider.OK(p.executionToMap(nr, e)), nil
}

// durationString renders a proto3 Duration JSON string (e.g. "0.5s").
func durationString(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}
