// Package stepfunctions implements the AWS Step Functions provider in lite mode.
// All executions complete instantly with SUCCEEDED status — no real ASL engine.
package stepfunctions

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
	"jaiscloud/internal/aws/provider/stepfunctions/engine"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	sfnstore "jaiscloud/internal/aws/store/stepfunctions"
)

var nameRe = regexp.MustCompile(`^[0-9A-Za-z_-]+$`)

type Provider struct {
	store  *sfnstore.MemoryStepFunctionsStore
	engine *engine.ExecutionEngine // nil in lite mode
}

// Option is a functional option for the Provider.
type Option func(*Provider)

// WithEngine attaches an ExecutionEngine for real ASL execution.
func WithEngine(eng *engine.ExecutionEngine) Option {
	return func(p *Provider) { p.engine = eng }
}

func New(store *sfnstore.MemoryStepFunctionsStore, opts ...Option) *Provider {
	p := &Provider{store: store}
	for _, o := range opts {
		o(p)
	}
	return p
}

// SetEngine attaches the execution engine after construction (used in main.go
// to break the registry→engine→registry circular dependency).
func (p *Provider) SetEngine(eng *engine.ExecutionEngine) {
	p.engine = eng
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// State machine CRUD
		"StepFunctions.CreateStateMachine":              p.CreateStateMachine,
		"StepFunctions.DescribeStateMachine":            p.DescribeStateMachine,
		"StepFunctions.UpdateStateMachine":              p.UpdateStateMachine,
		"StepFunctions.DeleteStateMachine":              p.DeleteStateMachine,
		"StepFunctions.ListStateMachines":               p.ListStateMachines,
		"StepFunctions.ValidateStateMachineDefinition":  p.ValidateStateMachineDefinition,

		// Executions
		"StepFunctions.StartExecution":        p.StartExecution,
		"StepFunctions.StartSyncExecution":    p.StartSyncExecution,
		"StepFunctions.StopExecution":         p.StopExecution,
		"StepFunctions.DescribeExecution":     p.DescribeExecution,
		"StepFunctions.ListExecutions":        p.ListExecutions,
		"StepFunctions.GetExecutionHistory":   p.GetExecutionHistory,
		"StepFunctions.RedriveExecution":      p.RedriveExecution,

		// Versions
		"StepFunctions.PublishStateMachineVersion":   p.PublishStateMachineVersion,
		"StepFunctions.DeleteStateMachineVersion":    p.DeleteStateMachineVersion,
		"StepFunctions.ListStateMachineVersions":     p.ListStateMachineVersions,
		"StepFunctions.DescribeStateMachineForExecution": p.DescribeStateMachineForExecution,

		// Aliases
		"StepFunctions.CreateStateMachineAlias":   p.CreateStateMachineAlias,
		"StepFunctions.UpdateStateMachineAlias":   p.UpdateStateMachineAlias,
		"StepFunctions.DeleteStateMachineAlias":   p.DeleteStateMachineAlias,
		"StepFunctions.DescribeStateMachineAlias": p.DescribeStateMachineAlias,
		"StepFunctions.ListStateMachineAliases":   p.ListStateMachineAliases,

		// Activities
		"StepFunctions.CreateActivity":     p.CreateActivity,
		"StepFunctions.DeleteActivity":     p.DeleteActivity,
		"StepFunctions.DescribeActivity":   p.DescribeActivity,
		"StepFunctions.ListActivities":     p.ListActivities,
		"StepFunctions.GetActivityTask":    p.GetActivityTask,
		"StepFunctions.SendTaskSuccess":    p.SendTaskSuccess,
		"StepFunctions.SendTaskFailure":    p.SendTaskFailure,
		"StepFunctions.SendTaskHeartbeat": p.SendTaskHeartbeat,

		// Tags
		"StepFunctions.TagResource":        p.TagResource,
		"StepFunctions.UntagResource":      p.UntagResource,
		"StepFunctions.ListTagsForResource": p.ListTagsForResource,
	}
}

func (p *Provider) Reset() { p.store.Reset() }

// ─── State Machine CRUD ───────────────────────────────────────────────────────

func (p *Provider) CreateStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	if !validateName(name) {
		return nil, sfnErr("InvalidName", fmt.Sprintf("Invalid name: '%s'", name), 400)
	}

	definition, _ := nr.Params["definition"].(string)
	if _, err := asl.Parse(definition); err != nil {
		return nil, sfnErr("InvalidDefinition", fmt.Sprintf("Invalid definition: %s", err.Error()), 400)
	}
	if diags := asl.Validate(mustParseSM(definition)); hasErrors(diags) {
		return nil, sfnErr("InvalidDefinition", diags[0].Message, 400)
	}

	roleARN, _ := nr.Params["roleArn"].(string)
	smType := sfnstore.StateMachineTypeStandard
	if t, ok := nr.Params["type"].(string); ok && t == "EXPRESS" {
		smType = sfnstore.StateMachineTypeExpress
	}

	arn := nr.ResourceID("sfn-state-machine", name)
	revisionID := newUUID()

	sm := &sfnstore.StateMachine{
		Name:        name,
		ARN:         arn,
		RevisionID:  revisionID,
		Definition:  definition,
		RoleARN:     roleARN,
		Type:        smType,
		Status:      sfnstore.StateMachineStatusActive,
		CreateDate:  time.Now().UTC(),
		Tags:        parseTags(nr.Params["tags"], "key", "value"),
		Versions:    make(map[int64]*sfnstore.StateMachineVersion),
		Aliases:     make(map[string]*sfnstore.StateMachineAlias),
		Description: strParam(nr.Params["description"]),
	}

	if lc, ok := nr.Params["loggingConfiguration"].(map[string]any); ok {
		sm.LoggingConfiguration = parseLoggingConfig(lc)
	}
	if tc, ok := nr.Params["tracingConfiguration"].(map[string]any); ok {
		sm.TracingConfiguration = &sfnstore.TracingConfiguration{
			Enabled: boolParam(tc["enabled"]),
		}
	}

	if err := p.store.CreateStateMachine(sm); err != nil {
		return nil, storeErr(err)
	}

	return provider.OK(map[string]any{
		"stateMachineArn": arn,
		"creationDate":    sm.CreateDate.Unix(),
	}), nil
}

func (p *Provider) DescribeStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["stateMachineArn"].(string)
	sm, err := p.store.GetStateMachine(arn)
	if err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(smToMap(sm)), nil
}

func (p *Provider) UpdateStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["stateMachineArn"].(string)
	def, _ := nr.Params["definition"].(string)
	roleARN, _ := nr.Params["roleArn"].(string)
	revisionID, _ := nr.Params["revisionId"].(string)
	description, _ := nr.Params["description"].(string)

	if def != "" && !isValidJSON(def) {
		return nil, sfnErr("InvalidDefinition", "Definition is not valid JSON", 400)
	}

	var logging *sfnstore.LoggingConfiguration
	if lc, ok := nr.Params["loggingConfiguration"].(map[string]any); ok {
		logging = parseLoggingConfig(lc)
	}
	var tracing *sfnstore.TracingConfiguration
	if tc, ok := nr.Params["tracingConfiguration"].(map[string]any); ok {
		tracing = &sfnstore.TracingConfiguration{Enabled: boolParam(tc["enabled"])}
	}

	newRevision, err := p.store.UpdateStateMachine(arn, def, roleARN, logging, tracing, description, revisionID)
	if err != nil {
		return nil, storeErr(err)
	}

	return provider.OK(map[string]any{
		"updateDate": time.Now().UTC().Unix(),
		"revisionId": newRevision,
	}), nil
}

func (p *Provider) DeleteStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["stateMachineArn"].(string)
	if err := p.store.DeleteStateMachine(arn); err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListStateMachines(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	maxResults := 100
	if mr, ok := nr.Params["maxResults"].(float64); ok && mr > 0 {
		maxResults = int(mr)
	}
	nextToken, _ := nr.Params["nextToken"].(string)

	all := p.store.ListStateMachines(nr.AccountID)

	start := 0
	if nextToken != "" {
		for i, sm := range all {
			if sm.ARN == nextToken {
				start = i
				break
			}
		}
	}
	end := start + maxResults
	var outToken *string
	if end < len(all) {
		t := all[end].ARN
		outToken = &t
	} else {
		end = len(all)
	}
	page := all[start:end]

	items := make([]any, len(page))
	for i, sm := range page {
		items[i] = map[string]any{
			"stateMachineArn": sm.ARN,
			"name":            sm.Name,
			"type":            string(sm.Type),
			"creationDate":    sm.CreateDate.Unix(),
		}
	}

	resp := map[string]any{"stateMachines": items}
	if outToken != nil {
		resp["nextToken"] = *outToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) ValidateStateMachineDefinition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	def, _ := nr.Params["definition"].(string)
	sm, err := asl.Parse(def)
	if err != nil {
		return provider.OK(map[string]any{
			"result": "FAIL",
			"diagnostics": []asl.ValidationDiagnostic{{
				Severity: "ERROR",
				Code:     "INVALID_JSON",
				Message:  err.Error(),
				Location: "",
			}},
		}), nil
	}
	diags := asl.Validate(sm)
	result := "OK"
	if hasErrors(diags) {
		result = "FAIL"
	}
	return provider.OK(map[string]any{
		"result":      result,
		"diagnostics": diags,
	}), nil
}

// ─── Executions ───────────────────────────────────────────────────────────────

func (p *Provider) StartExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	smARN, _ := nr.Params["stateMachineArn"].(string)
	sm, err := p.store.GetStateMachine(smARN)
	if err != nil {
		return nil, storeErr(err)
	}

	execName, _ := nr.Params["name"].(string)
	if execName == "" {
		execName = newUUID()
	} else if !validateName(execName) {
		return nil, sfnErr("InvalidName", fmt.Sprintf("Invalid execution name: '%s'", execName), 400)
	}

	input, _ := nr.Params["input"].(string)
	if input == "" {
		input = "{}"
	} else if !isValidJSON(input) {
		return nil, sfnErr("InvalidExecutionInput", "Input is not valid JSON", 400)
	}

	traceHeader, _ := nr.Params["traceHeader"].(string)

	// Extract state machine name from ARN
	smName := smNameFromARN(sm.ARN)
	execARN := nr.ResourceID("sfn-execution", smName+"/"+execName)

	t := time.Now().UTC()

	if p.engine != nil {
		// Engine mode: start RUNNING, engine will finalize
		exec := &sfnstore.Execution{
			Name:            execName,
			ARN:             execARN,
			StateMachineARN: smARN,
			Status:          sfnstore.ExecutionStatusRunning,
			StartDate:       t,
			Input:           input,
			InputDetails:    map[string]any{"included": true},
			TraceHeader:     traceHeader,
			History:         []sfnstore.HistoryEvent{},
		}
		if err := p.store.StartExecution(exec); err != nil {
			return nil, storeErr(err)
		}

		def, parseErr := asl.Parse(sm.Definition)
		if parseErr != nil {
			return nil, sfnErr("InvalidDefinition", parseErr.Error(), 400)
		}
		p.engine.Start(execARN, def, input)
	} else {
		// Lite mode: instant SUCCEEDED
		stopTime := t
		exec := &sfnstore.Execution{
			Name:            execName,
			ARN:             execARN,
			StateMachineARN: smARN,
			Status:          sfnstore.ExecutionStatusSucceeded,
			StartDate:       t,
			StopDate:        &stopTime,
			Input:           input,
			InputDetails:    map[string]any{"included": true},
			Output:          input,
			OutputDetails:   map[string]any{"included": true},
			TraceHeader:     traceHeader,
			History:         []sfnstore.HistoryEvent{},
		}
		if err := p.store.StartExecution(exec); err != nil {
			return nil, storeErr(err)
		}
		_ = p.store.AppendHistory(execARN, sfnstore.HistoryEvent{
			Timestamp: t, Type: "ExecutionStarted",
			ExecutionStartedEventDetails: &sfnstore.ExecutionStartedEventDetails{Input: input, RoleArn: sm.RoleARN},
		})
		_ = p.store.AppendHistory(execARN, sfnstore.HistoryEvent{
			Timestamp: t, Type: "ExecutionSucceeded",
			ExecutionSucceededEventDetails: &sfnstore.ExecutionSucceededEventDetails{Output: input},
		})
	}

	return provider.OK(map[string]any{
		"executionArn": execARN,
		"startDate":    t.Unix(),
	}), nil
}

func (p *Provider) StartSyncExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	smARN, _ := nr.Params["stateMachineArn"].(string)
	sm, err := p.store.GetStateMachine(smARN)
	if err != nil {
		return nil, storeErr(err)
	}
	if sm.Type != sfnstore.StateMachineTypeExpress {
		return nil, sfnErr("StateMachineTypeNotSupported", "StartSyncExecution is only supported for EXPRESS state machines", 400)
	}

	input, _ := nr.Params["input"].(string)
	if input == "" {
		input = "{}"
	}

	execName, _ := nr.Params["name"].(string)
	if execName == "" {
		execName = newUUID()
	}

	smName := smNameFromARN(sm.ARN)
	execARN := nr.ResourceID("sfn-express-execution", smName+"/"+execName+":"+newUUID())
	t := time.Now().UTC()

	return provider.OK(map[string]any{
		"executionArn":          execARN,
		"stateMachineArn":       smARN,
		"name":                  execName,
		"startDate":             t.Unix(),
		"stopDate":              t.Unix(),
		"status":                "SUCCEEDED",
		"input":                 input,
		"output":                input,
		"billedDuration":        0,
		"billedMemoryUsedInMB": 64,
	}), nil
}

func (p *Provider) StopExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	execARN, _ := nr.Params["executionArn"].(string)
	errMsg, _ := nr.Params["error"].(string)
	cause, _ := nr.Params["cause"].(string)

	if p.engine != nil {
		p.engine.Stop(execARN, errMsg, cause)
	} else {
		if err := p.store.StopExecution(execARN, errMsg, cause); err != nil {
			return nil, storeErr(err)
		}
	}

	return provider.OK(map[string]any{
		"stopDate": time.Now().UTC().Unix(),
	}), nil
}

func (p *Provider) DescribeExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	execARN, _ := nr.Params["executionArn"].(string)
	exec, err := p.store.GetExecution(execARN)
	if err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(execToMap(exec)), nil
}

func (p *Provider) ListExecutions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	smARN, _ := nr.Params["stateMachineArn"].(string)
	statusFilter := sfnstore.ExecutionStatus(strParam(nr.Params["statusFilter"]))

	maxResults := 100
	if mr, ok := nr.Params["maxResults"].(float64); ok && mr > 0 {
		maxResults = int(mr)
	}
	nextToken, _ := nr.Params["nextToken"].(string)

	all := p.store.ListExecutions(smARN, statusFilter)

	start := 0
	if nextToken != "" {
		for i, e := range all {
			if e.ARN == nextToken {
				start = i
				break
			}
		}
	}
	end := start + maxResults
	var outToken *string
	if end < len(all) {
		t := all[end].ARN
		outToken = &t
	} else {
		end = len(all)
	}
	page := all[start:end]

	items := make([]any, len(page))
	for i, e := range page {
		item := map[string]any{
			"executionArn":    e.ARN,
			"stateMachineArn": e.StateMachineARN,
			"name":            e.Name,
			"status":          string(e.Status),
			"startDate":       e.StartDate.Unix(),
		}
		if e.StopDate != nil {
			item["stopDate"] = e.StopDate.Unix()
		}
		items[i] = item
	}

	resp := map[string]any{"executions": items}
	if outToken != nil {
		resp["nextToken"] = *outToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetExecutionHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	execARN, _ := nr.Params["executionArn"].(string)
	reverseOrder, _ := nr.Params["reverseOrder"].(bool)

	events, err := p.store.GetExecutionHistory(execARN, reverseOrder)
	if err != nil {
		return nil, storeErr(err)
	}

	maxResults := 1000
	if mr, ok := nr.Params["maxResults"].(float64); ok && mr > 0 {
		maxResults = int(mr)
	}
	if len(events) > maxResults {
		events = events[:maxResults]
	}

	items := make([]any, len(events))
	for i, ev := range events {
		items[i] = historyEventToMap(ev)
	}

	return provider.OK(map[string]any{"events": items}), nil
}

func (p *Provider) RedriveExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Lite mode stub — return the current execution state
	execARN, _ := nr.Params["executionArn"].(string)
	_, err := p.store.GetExecution(execARN)
	if err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{
		"redriveDate": time.Now().UTC().Unix(),
	}), nil
}

// ─── Versions ─────────────────────────────────────────────────────────────────

func (p *Provider) PublishStateMachineVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	smARN, _ := nr.Params["stateMachineArn"].(string)
	description, _ := nr.Params["description"].(string)
	revisionID, _ := nr.Params["revisionId"].(string)

	ver, err := p.store.PublishVersion(smARN, description, revisionID)
	if err != nil {
		return nil, storeErr(err)
	}

	return provider.OK(map[string]any{
		"stateMachineVersionArn": ver.ARN,
		"creationDate":           ver.CreationDate.Unix(),
	}), nil
}

func (p *Provider) DeleteStateMachineVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	versionARN, _ := nr.Params["stateMachineVersionArn"].(string)
	if err := p.store.DeleteVersion(versionARN); err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListStateMachineVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	smARN, _ := nr.Params["stateMachineArn"].(string)
	versions := p.store.ListVersions(smARN)

	items := make([]any, len(versions))
	for i, v := range versions {
		items[i] = map[string]any{
			"stateMachineVersionArn": v.ARN,
			"creationDate":           v.CreationDate.Unix(),
		}
	}
	return provider.OK(map[string]any{"stateMachineVersions": items}), nil
}

func (p *Provider) DescribeStateMachineForExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	execARN, _ := nr.Params["executionArn"].(string)
	exec, err := p.store.GetExecution(execARN)
	if err != nil {
		return nil, storeErr(err)
	}
	sm, err := p.store.GetStateMachine(exec.StateMachineARN)
	if err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(smToMap(sm)), nil
}

// ─── Aliases ─────────────────────────────────────────────────────────────────

func (p *Provider) CreateStateMachineAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	description, _ := nr.Params["description"].(string)
	routing := parseRoutingConfig(nr.Params["routingConfiguration"])

	// We need the SM ARN from the routing config
	if len(routing) == 0 {
		return nil, sfnErr("ValidationException", "routingConfiguration is required", 400)
	}
	smARN, _, err := parseVersionARN(routing[0].StateMachineVersionARN)
	if err != nil {
		return nil, sfnErr("InvalidArn", "Invalid version ARN in routing configuration", 400)
	}

	alias, storeErr2 := p.store.CreateAlias(smARN, name, routing, description)
	if storeErr2 != nil {
		return nil, storeErr(storeErr2)
	}

	return provider.OK(map[string]any{
		"stateMachineAliasArn": alias.ARN,
		"creationDate":         alias.CreationDate.Unix(),
	}), nil
}

func (p *Provider) UpdateStateMachineAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasARN, _ := nr.Params["stateMachineAliasArn"].(string)
	description, _ := nr.Params["description"].(string)
	routing := parseRoutingConfig(nr.Params["routingConfiguration"])

	if err := p.store.UpdateAlias(aliasARN, routing, description); err != nil {
		return nil, storeErr(err)
	}

	return provider.OK(map[string]any{
		"updateDate": time.Now().UTC().Unix(),
	}), nil
}

func (p *Provider) DeleteStateMachineAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasARN, _ := nr.Params["stateMachineAliasArn"].(string)
	if err := p.store.DeleteAlias(aliasARN); err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeStateMachineAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasARN, _ := nr.Params["stateMachineAliasArn"].(string)
	alias, err := p.store.DescribeAlias(aliasARN)
	if err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(aliasToMap(alias)), nil
}

func (p *Provider) ListStateMachineAliases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	smARN, _ := nr.Params["stateMachineArn"].(string)
	aliases := p.store.ListAliases(smARN)

	items := make([]any, len(aliases))
	for i, a := range aliases {
		items[i] = aliasToMap(a)
	}
	return provider.OK(map[string]any{"stateMachineAliases": items}), nil
}

// ─── Activities ───────────────────────────────────────────────────────────────

func (p *Provider) CreateActivity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	if !validateName(name) {
		return nil, sfnErr("InvalidName", fmt.Sprintf("Invalid activity name: '%s'", name), 400)
	}

	arn := nr.ResourceID("sfn-activity", name)
	act := &sfnstore.Activity{
		Name:         name,
		ARN:          arn,
		CreationDate: time.Now().UTC(),
		Tags:         parseTags(nr.Params["tags"], "key", "value"),
	}

	if err := p.store.CreateActivity(act); err != nil {
		return nil, storeErr(err)
	}

	return provider.OK(map[string]any{
		"activityArn":  arn,
		"creationDate": act.CreationDate.Unix(),
	}), nil
}

func (p *Provider) DeleteActivity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["activityArn"].(string)
	if err := p.store.DeleteActivity(arn); err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeActivity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["activityArn"].(string)
	act, err := p.store.GetActivity(arn)
	if err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{
		"activityArn":  act.ARN,
		"name":         act.Name,
		"creationDate": act.CreationDate.Unix(),
	}), nil
}

func (p *Provider) ListActivities(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	activities := p.store.ListActivities()
	maxResults := 1000
	if mr, ok := nr.Params["maxResults"].(float64); ok && mr > 0 {
		maxResults = int(mr)
	}
	nextToken, _ := nr.Params["nextToken"].(string)
	start := 0
	if nextToken != "" {
		for i, a := range activities {
			if a.ARN == nextToken {
				start = i + 1
				break
			}
		}
	}
	end := start + maxResults
	if end > len(activities) {
		end = len(activities)
	}
	page := activities[start:end]
	items := make([]any, len(page))
	for i, a := range page {
		items[i] = map[string]any{
			"activityArn":  a.ARN,
			"name":         a.Name,
			"creationDate": a.CreationDate.Unix(),
		}
	}
	resp := map[string]any{"activities": items}
	if end < len(activities) {
		resp["nextToken"] = activities[end-1].ARN
	}
	return provider.OK(resp), nil
}

// GetActivityTask, SendTask* are stubs (no real task workers in lite mode).

func (p *Provider) GetActivityTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"taskToken": "", "input": ""}), nil
}

func (p *Provider) SendTaskSuccess(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	token, _ := nr.Params["taskToken"].(string)
	output, _ := nr.Params["output"].(string)
	if p.engine != nil {
		if err := p.engine.SendTaskSuccess(token, output); err != nil {
			return nil, sfnErr("TaskDoesNotExist", err.Error(), 400)
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) SendTaskFailure(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	token, _ := nr.Params["taskToken"].(string)
	errCode, _ := nr.Params["error"].(string)
	cause, _ := nr.Params["cause"].(string)
	if p.engine != nil {
		if err := p.engine.SendTaskFailure(token, errCode, cause); err != nil {
			return nil, sfnErr("TaskDoesNotExist", err.Error(), 400)
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) SendTaskHeartbeat(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *Provider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	tags := parseTags(nr.Params["tags"], "key", "value")
	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	var keys []string
	if ks, ok := nr.Params["tagKeys"].([]any); ok {
		for _, k := range ks {
			if s, ok := k.(string); ok {
				keys = append(keys, s)
			}
		}
	}
	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, storeErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	tags := p.store.ListTags(arn)
	tagList := make([]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"key": k, "value": v})
	}
	return provider.OK(map[string]any{"tags": tagList}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func validateName(name string) bool {
	return len(name) >= 1 && len(name) <= 80 && nameRe.MatchString(name)
}

func isValidJSON(s string) bool {
	if s == "" {
		return false
	}
	var v any
	return json.Unmarshal([]byte(s), &v) == nil
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func storeErr(err error) *model.ProviderError {
	if e := sfnstore.AsSFNError(err); e != nil {
		return &model.ProviderError{Code: e.Code, Message: e.Message, HTTPStatus: e.Status}
	}
	return &model.ProviderError{Code: "InternalFailure", Message: err.Error(), HTTPStatus: 500}
}

func sfnErr(code, message string, status int) *model.ProviderError {
	return &model.ProviderError{Code: code, Message: message, HTTPStatus: status}
}

func strParam(v any) string {
	s, _ := v.(string)
	return s
}

func boolParam(v any) bool {
	b, _ := v.(bool)
	return b
}

func parseTags(raw any, keyField, valueField string) map[string]string {
	out := make(map[string]string)
	items, ok := raw.([]any)
	if !ok {
		return out
	}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		k, _ := m[keyField].(string)
		v, _ := m[valueField].(string)
		if k != "" {
			out[k] = v
		}
	}
	return out
}

func parseLoggingConfig(m map[string]any) *sfnstore.LoggingConfiguration {
	lc := &sfnstore.LoggingConfiguration{}
	lc.Level, _ = m["level"].(string)
	lc.IncludeExecutionData, _ = m["includeExecutionData"].(bool)
	return lc
}

func parseRoutingConfig(raw any) []sfnstore.RoutingConfig {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]sfnstore.RoutingConfig, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		versionARN, _ := m["stateMachineVersionArn"].(string)
		weight := 100
		if w, ok := m["weight"].(float64); ok {
			weight = int(w)
		}
		out = append(out, sfnstore.RoutingConfig{
			StateMachineVersionARN: versionARN,
			Weight:                 weight,
		})
	}
	return out
}

func parseVersionARN(versionARN string) (string, int64, error) {
	return sfnstore.ParseVersionARN(versionARN)
}

func smToMap(sm *sfnstore.StateMachine) map[string]any {
	m := map[string]any{
		"stateMachineArn": sm.ARN,
		"name":            sm.Name,
		"status":          string(sm.Status),
		"definition":      sm.Definition,
		"roleArn":         sm.RoleARN,
		"type":            string(sm.Type),
		"creationDate":    sm.CreateDate.Unix(),
		"revisionId":      sm.RevisionID,
		"description":     sm.Description,
	}
	if sm.LoggingConfiguration != nil {
		m["loggingConfiguration"] = map[string]any{
			"level":                sm.LoggingConfiguration.Level,
			"includeExecutionData": sm.LoggingConfiguration.IncludeExecutionData,
			"destinations":         sm.LoggingConfiguration.Destinations,
		}
	}
	if sm.TracingConfiguration != nil {
		m["tracingConfiguration"] = map[string]any{
			"enabled": sm.TracingConfiguration.Enabled,
		}
	}
	return m
}

func execToMap(e *sfnstore.Execution) map[string]any {
	m := map[string]any{
		"executionArn":    e.ARN,
		"stateMachineArn": e.StateMachineARN,
		"name":            e.Name,
		"status":          string(e.Status),
		"startDate":       e.StartDate.Unix(),
		"input":           e.Input,
		"inputDetails":    e.InputDetails,
		"redriveCount":    e.RedriveCount,
	}
	if e.StopDate != nil {
		m["stopDate"] = e.StopDate.Unix()
	}
	if e.Status == sfnstore.ExecutionStatusSucceeded {
		m["output"] = e.Output
		m["outputDetails"] = e.OutputDetails
	}
	if e.Error != "" {
		m["error"] = e.Error
	}
	if e.Cause != "" {
		m["cause"] = e.Cause
	}
	if e.TraceHeader != "" {
		m["traceHeader"] = e.TraceHeader
	}
	return m
}

func aliasToMap(a *sfnstore.StateMachineAlias) map[string]any {
	routing := make([]any, len(a.RoutingConfiguration))
	for i, rc := range a.RoutingConfiguration {
		routing[i] = map[string]any{
			"stateMachineVersionArn": rc.StateMachineVersionARN,
			"weight":                 rc.Weight,
		}
	}
	return map[string]any{
		"stateMachineAliasArn":  a.ARN,
		"name":                  a.Name,
		"description":           a.Description,
		"routingConfiguration":  routing,
		"creationDate":          a.CreationDate.Unix(),
		"updateDate":            a.UpdateDate.Unix(),
	}
}

func historyEventToMap(ev sfnstore.HistoryEvent) map[string]any {
	m := map[string]any{
		"id":              ev.ID,
		"previousEventId": ev.PreviousEventID,
		"timestamp":       ev.Timestamp.Unix(),
		"type":            ev.Type,
	}
	if ev.ExecutionStartedEventDetails != nil {
		m["executionStartedEventDetails"] = map[string]any{
			"input":   ev.ExecutionStartedEventDetails.Input,
			"roleArn": ev.ExecutionStartedEventDetails.RoleArn,
		}
	}
	if ev.ExecutionSucceededEventDetails != nil {
		m["executionSucceededEventDetails"] = map[string]any{
			"output": ev.ExecutionSucceededEventDetails.Output,
		}
	}
	if ev.ExecutionFailedEventDetails != nil {
		m["executionFailedEventDetails"] = map[string]any{
			"error": ev.ExecutionFailedEventDetails.Error,
			"cause": ev.ExecutionFailedEventDetails.Cause,
		}
	}
	if ev.ExecutionAbortedEventDetails != nil {
		m["executionAbortedEventDetails"] = map[string]any{
			"error": ev.ExecutionAbortedEventDetails.Error,
			"cause": ev.ExecutionAbortedEventDetails.Cause,
		}
	}
	return m
}

func smNameFromARN(arn string) string {
	// arn:aws:states:region:account:stateMachine:name
	parts := splitARN(arn)
	if len(parts) == 0 {
		return arn
	}
	return parts[len(parts)-1]
}

func splitARN(arn string) []string {
	parts := make([]string, 0)
	segment := ""
	for _, c := range arn {
		if c == ':' {
			parts = append(parts, segment)
			segment = ""
		} else {
			segment += string(c)
		}
	}
	if segment != "" {
		parts = append(parts, segment)
	}
	return parts
}

// mustParseSM parses a definition that was already validated as valid JSON.
// Panics are impossible here since we only call it after asl.Parse succeeded.
func mustParseSM(definition string) *asl.StateMachineDefinition {
	sm, _ := asl.Parse(definition)
	return sm
}

func hasErrors(diags []asl.ValidationDiagnostic) bool {
	for _, d := range diags {
		if d.Severity == "ERROR" {
			return true
		}
	}
	return false
}
