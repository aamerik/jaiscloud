// Package stack implements the CloudFormation provider (StackProvider).
//
// Phase 7 additions:
//   - Intrinsics engine (Ref, Fn::Sub, Fn::GetAtt, Fn::Join, Fn::If, …)
//   - Topological sort — respects DependsOn and implicit Ref/GetAtt deps
//   - Resource dispatcher — actually provisions underlying resources via registered handlers
//   - Outputs section — resolved after all resources are created
//   - Rollback — on CREATE failure, already-created resources are deleted in reverse order
package stack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// StackProvider handles CloudFormation stacks and resources.
type StackProvider struct {
	resources store.ResourceStore
	// handlers maps AWS resource type (e.g. "AWS::SQS::Queue") to a handler.
	// If no handler is registered for a type, the resource is metadata-only.
	handlers map[string]ResourceHandler
}

// New constructs a StackProvider.
func New(resources store.ResourceStore) *StackProvider {
	return &StackProvider{
		resources: resources,
		handlers:  make(map[string]ResourceHandler),
	}
}

// RegisterHandler registers a provisioning handler for an AWS resource type.
// Call this in main.go before the first stack operation.
func (p *StackProvider) RegisterHandler(resourceType string, h ResourceHandler) {
	p.handlers[resourceType] = h
}

// Routes returns all CloudFormation handler registrations.
func (p *StackProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudFormation.CreateStack":            p.CreateStack,
		"CloudFormation.UpdateStack":            p.UpdateStack,
		"CloudFormation.DeleteStack":            p.DeleteStack,
		"CloudFormation.DescribeStacks":         p.DescribeStacks,
		"CloudFormation.ListStacks":             p.ListStacks,
		"CloudFormation.DescribeStackResources": p.DescribeStackResources,
		"CloudFormation.ValidateTemplate":       p.ValidateTemplate,
		"CloudFormation.GetTemplate":            p.GetTemplate,
		// ChangeSets (14.8)
		"CloudFormation.CreateChangeSet":   p.CreateChangeSet,
		"CloudFormation.DescribeChangeSet": p.DescribeChangeSet,
		"CloudFormation.ListChangeSets":    p.ListChangeSets,
		"CloudFormation.ExecuteChangeSet":  p.ExecuteChangeSet,
		"CloudFormation.DeleteChangeSet":   p.DeleteChangeSet,
		// Stack events (14.8)
		"CloudFormation.DescribeStackEvents": p.DescribeStackEvents,
	}
}

const rtStack = "cfn_stack"

// cfResource is the stored representation of a CloudFormation resource.
type cfResource struct {
	LogicalResourceId  string         `json:"LogicalResourceId"`
	PhysicalResourceId string         `json:"PhysicalResourceId"`
	ResourceType       string         `json:"ResourceType"`
	ResourceStatus     string         `json:"ResourceStatus"`
	Attributes         map[string]any `json:"Attributes,omitempty"`
}

type cfParam struct {
	ParameterKey   string `json:"ParameterKey"`
	ParameterValue string `json:"ParameterValue"`
}

type cfOutput struct {
	OutputKey   string `json:"OutputKey"`
	OutputValue string `json:"OutputValue"`
	Description string `json:"Description"`
	ExportName  string `json:"ExportName,omitempty"`
}

type cfStack struct {
	StackId      string            `json:"StackId"`
	StackName    string            `json:"StackName"`
	StackStatus  string            `json:"StackStatus"`
	Description  string            `json:"Description"`
	CreationTime time.Time         `json:"CreationTime"`
	Parameters   []cfParam         `json:"Parameters"`
	Outputs      []cfOutput        `json:"Outputs"`
	Resources    []cfResource      `json:"Resources"`
	Tags         map[string]string `json:"Tags"`
	Template     string            `json:"Template"`
}

func (s cfStack) toWire() map[string]any {
	params := make([]map[string]any, len(s.Parameters))
	for i, p := range s.Parameters {
		params[i] = map[string]any{"ParameterKey": p.ParameterKey, "ParameterValue": p.ParameterValue}
	}
	outputs := make([]map[string]any, len(s.Outputs))
	for i, o := range s.Outputs {
		m := map[string]any{
			"OutputKey":   o.OutputKey,
			"OutputValue": o.OutputValue,
			"Description": o.Description,
		}
		if o.ExportName != "" {
			m["ExportName"] = o.ExportName
		}
		outputs[i] = m
	}
	return map[string]any{
		"StackId":      s.StackId,
		"StackName":    s.StackName,
		"StackStatus":  s.StackStatus,
		"Description":  s.Description,
		"CreationTime": s.CreationTime.UTC().Format(time.RFC3339),
		"Parameters":   params,
		"Outputs":      outputs,
	}
}

func (s cfStack) toSummary() map[string]any {
	return map[string]any{
		"StackId":      s.StackId,
		"StackName":    s.StackName,
		"StackStatus":  s.StackStatus,
		"CreationTime": s.CreationTime.UTC().Format(time.RFC3339),
	}
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (p *StackProvider) CreateStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "StackName is required", HTTPStatus: http.StatusBadRequest}
	}

	template := strParam(nr.Params, "TemplateBody")
	if template == "" {
		template = strParam(nr.Params, "TemplateURL")
	}

	doc, err := parseTemplate(template)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "invalid template: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	stackID := nr.ResourceID(model.RTCFNStack, name+"/"+shortID())

	rc := newResolveCtx(nr.Region, nr.AccountID, nr.Port)
	rc.pseudoParams["AWS::StackName"] = name
	rc.pseudoParams["AWS::StackId"] = stackID

	// Resolve Parameters.
	callerParams := parseCallerParams(nr.Params)
	if tplParams, ok := doc["Parameters"].(map[string]any); ok {
		rc.resolveParameters(tplParams, callerParams)
	} else {
		for k, v := range callerParams {
			rc.params[k] = v
		}
	}

	// Load Mappings.
	if m, ok := doc["Mappings"].(map[string]any); ok {
		rc.mappings = m
	}

	// Evaluate Conditions.
	if conds, ok := doc["Conditions"].(map[string]any); ok {
		rc.evaluateConditions(conds)
	}

	// Provision resources.
	resourcesDef, _ := doc["Resources"].(map[string]any)
	if len(resourcesDef) == 0 {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "template must have at least one resource", HTTPStatus: http.StatusBadRequest}
	}

	order, err := topoSort(resourcesDef)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	created := make([]cfResource, 0, len(order))
	var provisionErr error
	for _, logicalID := range order {
		resDef, _ := resourcesDef[logicalID].(map[string]any)
		resType, _ := resDef["Type"].(string)

		// Skip resources whose Condition evaluates to false.
		if cond, ok := resDef["Condition"].(string); ok {
			if !rc.conditions[cond] {
				continue
			}
		}

		props, _ := resDef["Properties"].(map[string]any)
		resolvedProps := rc.resolvePropsMap(props)

		physicalID, attrs, err := p.provisionResource(ctx, logicalID, resType, resolvedProps, stackID, nr)
		if err != nil {
			slog.Warn("cloudformation: resource creation failed", "logical", logicalID, "type", resType, "err", err)
			provisionErr = fmt.Errorf("resource %s (%s) failed: %w", logicalID, resType, err)
			break
		}

		r := cfResource{
			LogicalResourceId:  logicalID,
			PhysicalResourceId: physicalID,
			ResourceType:       resType,
			ResourceStatus:     "CREATE_COMPLETE",
			Attributes:         attrs,
		}
		created = append(created, r)
		rc.resources[logicalID] = &created[len(created)-1]
	}

	// Rollback on failure.
	if provisionErr != nil {
		p.rollback(ctx, created)
		return nil, &model.ProviderError{Code: "CreateStack_ROLLBACK_COMPLETE", Message: provisionErr.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Resolve Outputs.
	outputs := p.resolveOutputs(doc, rc, name)

	s := cfStack{
		StackId:      stackID,
		StackName:    name,
		StackStatus:  "CREATE_COMPLETE",
		Description:  strParam(nr.Params, "Description"),
		CreationTime: time.Now().UTC(),
		Parameters:   paramsToSlice(rc.params),
		Outputs:      outputs,
		Resources:    created,
		Tags:         map[string]string{},
		Template:     template,
	}

	data, _ := json.Marshal(s)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtStack, ID: name, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, &model.ProviderError{Code: "AlreadyExistsException", Message: "Stack already exists: " + name, HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"StackId": stackID}), nil
}

func (p *StackProvider) UpdateStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	s, err := p.loadStack(ctx, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found: " + name, HTTPStatus: http.StatusBadRequest}
	}

	newTemplate := strParam(nr.Params, "TemplateBody")
	if newTemplate == "" {
		newTemplate = s.Template // UsePreviousTemplate
	}
	s.Template = newTemplate

	doc, err := parseTemplate(newTemplate)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "invalid template: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	rc := newResolveCtx(nr.Region, nr.AccountID, nr.Port)
	rc.pseudoParams["AWS::StackName"] = name
	rc.pseudoParams["AWS::StackId"] = s.StackId

	callerParams := parseCallerParams(nr.Params)
	if tplParams, ok := doc["Parameters"].(map[string]any); ok {
		rc.resolveParameters(tplParams, callerParams)
	}
	if m, ok := doc["Mappings"].(map[string]any); ok {
		rc.mappings = m
	}
	if conds, ok := doc["Conditions"].(map[string]any); ok {
		rc.evaluateConditions(conds)
	}

	// Re-populate existing resources in the resolve context (needed for Ref resolution).
	for i := range s.Resources {
		rc.resources[s.Resources[i].LogicalResourceId] = &s.Resources[i]
	}

	resourcesDef, _ := doc["Resources"].(map[string]any)
	if len(resourcesDef) == 0 {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "template must have at least one resource", HTTPStatus: http.StatusBadRequest}
	}

	order, err := topoSort(resourcesDef)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Build new resource list, keeping or recreating each resource.
	oldByLogical := make(map[string]cfResource, len(s.Resources))
	for _, r := range s.Resources {
		oldByLogical[r.LogicalResourceId] = r
	}

	newResources := make([]cfResource, 0, len(order))
	for _, logicalID := range order {
		resDef, _ := resourcesDef[logicalID].(map[string]any)
		resType, _ := resDef["Type"].(string)

		if cond, ok := resDef["Condition"].(string); ok {
			if !rc.conditions[cond] {
				continue
			}
		}

		props, _ := resDef["Properties"].(map[string]any)
		resolvedProps := rc.resolvePropsMap(props)

		if old, exists := oldByLogical[logicalID]; exists && old.ResourceType == resType {
			// Existing resource: update in-place (metadata).
			old.Attributes = updateAttributes(old.Attributes, resolvedProps)
			newResources = append(newResources, old)
			rc.resources[logicalID] = &newResources[len(newResources)-1]
		} else {
			// New resource or type changed: provision it.
			physicalID, attrs, err := p.provisionResource(ctx, logicalID, resType, resolvedProps, s.StackId, nr)
			if err != nil {
				slog.Warn("cloudformation: update resource creation failed", "logical", logicalID, "err", err)
				physicalID = name + "-" + logicalID
				attrs = nil
			}
			r := cfResource{
				LogicalResourceId:  logicalID,
				PhysicalResourceId: physicalID,
				ResourceType:       resType,
				ResourceStatus:     "UPDATE_COMPLETE",
				Attributes:         attrs,
			}
			newResources = append(newResources, r)
			rc.resources[logicalID] = &newResources[len(newResources)-1]
		}
	}

	// Delete resources removed in the new template.
	newByLogical := make(map[string]bool, len(newResources))
	for _, r := range newResources {
		newByLogical[r.LogicalResourceId] = true
	}
	for _, old := range s.Resources {
		if !newByLogical[old.LogicalResourceId] {
			p.deprovisionResource(ctx, old)
		}
	}

	outputs := p.resolveOutputs(doc, rc, name)

	s.Resources = newResources
	s.Outputs = outputs
	s.StackStatus = "UPDATE_COMPLETE"
	s.Parameters = paramsToSlice(rc.params)
	p.saveStack(ctx, s)
	return provider.OK(map[string]any{"StackId": s.StackId}), nil
}

func (p *StackProvider) DeleteStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	if strings.HasPrefix(name, "arn:") {
		parts := strings.Split(name, "/")
		if len(parts) >= 2 {
			name = parts[1]
		}
	}

	s, err := p.loadStack(ctx, name)
	if err == nil {
		// Delete resources in reverse creation order.
		p.rollback(ctx, s.Resources)
	}

	if err := p.resources.Delete(ctx, rtStack, name); err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *StackProvider) DescribeStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	if name != "" {
		if strings.HasPrefix(name, "arn:") {
			parts := strings.Split(name, "/")
			if len(parts) >= 2 {
				name = parts[1]
			}
		}
		s, err := p.loadStack(ctx, name)
		if err != nil {
			return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found: " + name, HTTPStatus: http.StatusBadRequest}
		}
		return provider.OK(map[string]any{"Stacks": []map[string]any{s.toWire()}}), nil
	}
	entries, _ := p.resources.List(ctx, rtStack, "")
	stacks := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var s cfStack
		json.Unmarshal(e.Data, &s)
		stacks = append(stacks, s.toWire())
	}
	return provider.OK(map[string]any{"Stacks": stacks}), nil
}

func (p *StackProvider) ListStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtStack, "")
	summaries := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var s cfStack
		json.Unmarshal(e.Data, &s)
		summaries = append(summaries, s.toSummary())
	}
	return provider.OK(map[string]any{"StackSummaries": summaries}), nil
}

func (p *StackProvider) DescribeStackResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	s, err := p.loadStack(ctx, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	resources := make([]map[string]any, len(s.Resources))
	for i, r := range s.Resources {
		resources[i] = map[string]any{
			"StackName":          s.StackName,
			"LogicalResourceId":  r.LogicalResourceId,
			"PhysicalResourceId": r.PhysicalResourceId,
			"ResourceType":       r.ResourceType,
			"ResourceStatus":     r.ResourceStatus,
		}
	}
	return provider.OK(map[string]any{"StackResources": resources}), nil
}

func (p *StackProvider) ValidateTemplate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	template := strParam(nr.Params, "TemplateBody")
	if template != "" {
		if _, err := parseTemplate(template); err != nil {
			return nil, &model.ProviderError{Code: "ValidationError", Message: "invalid template JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	return provider.OK(map[string]any{"Parameters": []any{}}), nil
}

func (p *StackProvider) GetTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	s, err := p.loadStack(ctx, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(map[string]any{"TemplateBody": s.Template}), nil
}

// ─── Resource provisioning ────────────────────────────────────────────────────

func (p *StackProvider) provisionResource(ctx context.Context, logicalID, resourceType string, props map[string]any, stackID string, nr *model.NormalizedRequest) (physicalID string, attrs map[string]any, err error) {
	if h, ok := p.handlers[resourceType]; ok && h.Create != nil {
		return h.Create(ctx, logicalID, props, nr)
	}
	// Metadata-only fallback: generate a deterministic physical ID.
	physicalID = stackPhysicalID(nr.Params, logicalID)
	return physicalID, nil, nil
}

func (p *StackProvider) deprovisionResource(ctx context.Context, r cfResource) {
	if h, ok := p.handlers[r.ResourceType]; ok && h.Delete != nil {
		if err := h.Delete(ctx, r.PhysicalResourceId, nil); err != nil {
			slog.Warn("cloudformation: deprovision failed", "logical", r.LogicalResourceId, "physical", r.PhysicalResourceId, "err", err)
		}
	}
}

// rollback deletes resources in reverse order (last created → first).
func (p *StackProvider) rollback(ctx context.Context, created []cfResource) {
	for i := len(created) - 1; i >= 0; i-- {
		p.deprovisionResource(ctx, created[i])
	}
}

// ─── Outputs resolution ───────────────────────────────────────────────────────

func (p *StackProvider) resolveOutputs(doc map[string]any, rc *resolveCtx, stackName string) []cfOutput {
	outSection, ok := doc["Outputs"].(map[string]any)
	if !ok {
		return nil
	}
	outputs := make([]cfOutput, 0, len(outSection))
	for key, def := range outSection {
		m, ok := def.(map[string]any)
		if !ok {
			continue
		}
		value := fmt.Sprintf("%v", rc.Resolve(m["Value"]))
		desc, _ := m["Description"].(string)
		o := cfOutput{
			OutputKey:   key,
			OutputValue: value,
			Description: desc,
		}
		if export, ok := m["Export"].(map[string]any); ok {
			o.ExportName = fmt.Sprintf("%v", rc.Resolve(export["Name"]))
		}
		outputs = append(outputs, o)
	}
	return outputs
}

// ─── Template parsing ─────────────────────────────────────────────────────────

// parseTemplate unmarshals a CloudFormation template from JSON.
func parseTemplate(body string) (map[string]any, error) {
	if body == "" {
		return nil, fmt.Errorf("empty template body")
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (p *StackProvider) loadStack(ctx context.Context, name string) (cfStack, error) {
	e, err := p.resources.Get(ctx, rtStack, name)
	if err != nil {
		return cfStack{}, err
	}
	var s cfStack
	return s, json.Unmarshal(e.Data, &s)
}

func (p *StackProvider) saveStack(ctx context.Context, s cfStack) {
	data, _ := json.Marshal(s)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtStack, ID: s.StackName, Data: data})
}

func (rc *resolveCtx) resolvePropsMap(props map[string]any) map[string]any {
	if props == nil {
		return map[string]any{}
	}
	v := rc.Resolve(props)
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return props
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// parseCallerParams extracts Parameters.member.N.ParameterKey / ParameterValue from the request.
func parseCallerParams(params map[string]any) map[string]string {
	out := make(map[string]string)
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("Parameters.member.%d.ParameterKey", i)]
		val := params[fmt.Sprintf("Parameters.member.%d.ParameterValue", i)]
		if key == nil {
			break
		}
		out[fmt.Sprintf("%v", key)] = fmt.Sprintf("%v", val)
	}
	return out
}

func paramsToSlice(m map[string]string) []cfParam {
	out := make([]cfParam, 0, len(m))
	for k, v := range m {
		out = append(out, cfParam{ParameterKey: k, ParameterValue: v})
	}
	return out
}

func stackPhysicalID(params map[string]any, logicalID string) string {
	stackName := strParam(params, "StackName")
	if stackName != "" {
		return stackName + "-" + logicalID
	}
	return logicalID
}

func updateAttributes(old map[string]any, props map[string]any) map[string]any {
	if old == nil {
		return nil
	}
	return old
}

func shortID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

