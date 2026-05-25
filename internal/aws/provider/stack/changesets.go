package stack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtChangeSet  = "cfn_changeset"
	rtStackEvent = "cfn_stack_event"
)

// ─── ChangeSet types ──────────────────────────────────────────────────────────

type cfChange struct {
	Action            string   `json:"Action"`
	LogicalResourceId string   `json:"LogicalResourceId"`
	ResourceType      string   `json:"ResourceType"`
	Replacement       string   `json:"Replacement"`
	Scope             []string `json:"Scope,omitempty"`
}

type cfChangeSet struct {
	ChangeSetId   string     `json:"ChangeSetId"`
	ChangeSetName string     `json:"ChangeSetName"`
	StackName     string     `json:"StackName"`
	StackId       string     `json:"StackId"`
	Status        string     `json:"Status"`
	StatusReason  string     `json:"StatusReason"`
	Changes       []cfChange `json:"Changes"`
	Parameters    []cfParam  `json:"Parameters"`
	TemplateBody  string     `json:"TemplateBody"`
	Description   string     `json:"Description"`
	CreationTime  time.Time  `json:"CreationTime"`
}

func (cs cfChangeSet) toWire() map[string]any {
	changes := make([]map[string]any, 0, len(cs.Changes))
	for _, c := range cs.Changes {
		rc := map[string]any{
			"Action":            c.Action,
			"LogicalResourceId": c.LogicalResourceId,
			"ResourceType":      c.ResourceType,
			"Replacement":       c.Replacement,
		}
		if len(c.Scope) > 0 {
			rc["Scope"] = c.Scope
		}
		changes = append(changes, map[string]any{
			"Type":           "Resource",
			"ResourceChange": rc,
		})
	}
	params := make([]map[string]any, 0, len(cs.Parameters))
	for _, p := range cs.Parameters {
		params = append(params, map[string]any{"ParameterKey": p.ParameterKey, "ParameterValue": p.ParameterValue})
	}
	return map[string]any{
		"ChangeSetId":   cs.ChangeSetId,
		"ChangeSetName": cs.ChangeSetName,
		"StackName":     cs.StackName,
		"StackId":       cs.StackId,
		"Status":        cs.Status,
		"StatusReason":  cs.StatusReason,
		"Changes":       changes,
		"Parameters":    params,
		"Description":   cs.Description,
		"CreationTime":  cs.CreationTime.UTC().Format(time.RFC3339),
	}
}

func changeSetKey(stackName, changeSetName string) string {
	return stackName + "/" + changeSetName
}

func (p *StackProvider) CreateChangeSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	stackName := strParam(nr.Params, "StackName")
	csName := strParam(nr.Params, "ChangeSetName")
	if stackName == "" || csName == "" {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "StackName and ChangeSetName are required", HTTPStatus: http.StatusBadRequest}
	}

	// Stack must exist
	se, serr := p.resources.Get(ctx, nr.AccountID, nr.Region, rtStack, stackName)
	var stackID string
	if serr == nil {
		var s cfStack
		_ = json.Unmarshal(se.Data, &s)
		stackID = s.StackId
	}

	callerParams := parseCallerParams(nr.Params)
	params := paramsToSlice(callerParams)

	// Compute real diff between old and new templates.
	newTemplateBody := strParam(nr.Params, "TemplateBody")
	computedChanges := []cfChange{}
	if newTemplateBody != "" {
		newDoc, parseErr := parseTemplate(newTemplateBody)
		if parseErr == nil {
			var oldDoc map[string]any
			if serr == nil {
				var existingStack cfStack
				_ = json.Unmarshal(se.Data, &existingStack)
				oldDoc, _ = parseTemplate(existingStack.Template)
			}
			if oldDoc == nil {
				oldDoc = map[string]any{"Resources": map[string]any{}}
			}
			rawChanges := BuildChangeSet(oldDoc, newDoc, p.handlers)
			for _, rc := range rawChanges {
				computedChanges = append(computedChanges, cfChange{
					Action:            rc.Action,
					LogicalResourceId: rc.LogicalResourceID,
					ResourceType:      rc.ResourceType,
					Replacement:       rc.Replacement,
					Scope:             rc.Scope,
				})
			}
		}
	}

	cs := cfChangeSet{
		ChangeSetId:   nr.ResourceID("cfn-changeset", changeSetKey(stackName, csName)),
		ChangeSetName: csName,
		StackName:     stackName,
		StackId:       stackID,
		Status:        "CREATE_COMPLETE",
		StatusReason:  "Complete",
		Changes:       computedChanges,
		Parameters:    params,
		TemplateBody:  newTemplateBody,
		Description:   strParam(nr.Params, "Description"),
		CreationTime:  clock.Now(),
	}

	data, _ := json.Marshal(cs)
	key := changeSetKey(stackName, csName)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtChangeSet, ID: key, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "AlreadyExistsException", Message: "change set " + csName + " already exists for stack " + stackName, HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Id": cs.ChangeSetId, "StackId": stackID}), nil
}

func (p *StackProvider) DescribeChangeSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	stackName := strParam(nr.Params, "StackName")
	csName := strParam(nr.Params, "ChangeSetName")
	key := changeSetKey(stackName, csName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtChangeSet, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ChangeSetNotFoundException", Message: fmt.Sprintf("change set %s not found for stack %s", csName, stackName), HTTPStatus: http.StatusNotFound}
	}
	var cs cfChangeSet
	_ = json.Unmarshal(e.Data, &cs)
	return provider.OK(cs.toWire()), nil
}

func (p *StackProvider) ListChangeSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	stackName := strParam(nr.Params, "StackName")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtChangeSet, "")
	prefix := stackName + "/"
	var summaries []map[string]any
	for _, e := range entries {
		if !strings.HasPrefix(e.ID, prefix) {
			continue
		}
		var cs cfChangeSet
		if json.Unmarshal(e.Data, &cs) != nil {
			continue
		}
		summaries = append(summaries, map[string]any{
			"ChangeSetId":   cs.ChangeSetId,
			"ChangeSetName": cs.ChangeSetName,
			"StackName":     cs.StackName,
			"Status":        cs.Status,
			"Description":   cs.Description,
			"CreationTime":  cs.CreationTime.UTC().Format(time.RFC3339),
		})
	}
	if summaries == nil {
		summaries = []map[string]any{}
	}
	return provider.OK(map[string]any{"Summaries": summaries}), nil
}

func (p *StackProvider) ExecuteChangeSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	stackName := strParam(nr.Params, "StackName")
	csName := strParam(nr.Params, "ChangeSetName")
	key := changeSetKey(stackName, csName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtChangeSet, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ChangeSetNotFoundException", Message: fmt.Sprintf("change set %s not found", csName), HTTPStatus: http.StatusNotFound}
	}
	var cs cfChangeSet
	_ = json.Unmarshal(e.Data, &cs)

	if cs.TemplateBody != "" {
		// Synthesize an UpdateStack-like request and execute it
		fakeParams := map[string]any{
			"StackName":    stackName,
			"TemplateBody": cs.TemplateBody,
		}
		for i, param := range cs.Parameters {
			fakeParams[fmt.Sprintf("Parameters.member.%d.ParameterKey", i+1)] = param.ParameterKey
			fakeParams[fmt.Sprintf("Parameters.member.%d.ParameterValue", i+1)] = param.ParameterValue
		}
		fakeNR := &model.NormalizedRequest{
			Service:    nr.Service,
			Action:     "UpdateStack",
			Params:     fakeParams,
			Region:     nr.Region,
			AccountID:  nr.AccountID,
			Clock:      nr.Clock,
			ResourceID: nr.ResourceID,
		}
		_, _ = p.UpdateStack(ctx, fakeNR)
	}

	cs.Status = "EXECUTE_COMPLETE"
	data, _ := json.Marshal(cs)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtChangeSet, ID: key, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *StackProvider) DeleteChangeSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	stackName := strParam(nr.Params, "StackName")
	csName := strParam(nr.Params, "ChangeSetName")
	key := changeSetKey(stackName, csName)
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtChangeSet, key); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "ChangeSetNotFoundException", Message: fmt.Sprintf("change set %s not found", csName), HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Stack Events ─────────────────────────────────────────────────────────────

type cfStackEvent struct {
	EventId              string    `json:"EventId"`
	StackId              string    `json:"StackId"`
	StackName            string    `json:"StackName"`
	LogicalResourceId    string    `json:"LogicalResourceId"`
	PhysicalResourceId   string    `json:"PhysicalResourceId"`
	ResourceType         string    `json:"ResourceType"`
	ResourceStatus       string    `json:"ResourceStatus"`
	ResourceStatusReason string    `json:"ResourceStatusReason"`
	Timestamp            time.Time `json:"Timestamp"`
}

func (p *StackProvider) DescribeStackEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	stackName := strParam(nr.Params, "StackName")

	// Derive synthetic events from the stack's current resources
	se, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtStack, stackName)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "stack " + stackName + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var s cfStack
	_ = json.Unmarshal(se.Data, &s)

	events := []map[string]any{}
	// Stack-level event
	events = append(events, map[string]any{
		"EventId":            shortID(),
		"StackId":            s.StackId,
		"StackName":          s.StackName,
		"LogicalResourceId":  s.StackName,
		"PhysicalResourceId": s.StackId,
		"ResourceType":       "AWS::CloudFormation::Stack",
		"ResourceStatus":     s.StackStatus,
		"Timestamp":          s.CreationTime.UTC().Format(time.RFC3339),
	})
	// Per-resource events
	for _, r := range s.Resources {
		events = append(events, map[string]any{
			"EventId":            shortID(),
			"StackId":            s.StackId,
			"StackName":          s.StackName,
			"LogicalResourceId":  r.LogicalResourceId,
			"PhysicalResourceId": r.PhysicalResourceId,
			"ResourceType":       r.ResourceType,
			"ResourceStatus":     r.ResourceStatus,
			"Timestamp":          s.CreationTime.UTC().Format(time.RFC3339),
		})
	}
	return provider.OK(map[string]any{"StackEvents": events}), nil
}
