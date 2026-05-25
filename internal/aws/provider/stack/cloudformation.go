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

	"jaiscloud/internal/clock"
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
	// exports holds cross-stack export values for Fn::ImportValue resolution.
	exports *ExportTable
}

// New constructs a StackProvider.
func New(resources store.ResourceStore) *StackProvider {
	return &StackProvider{
		resources: resources,
		handlers:  make(map[string]ResourceHandler),
		exports:   NewExportTable(),
	}
}

// Reset wipes all in-memory CloudFormation state (stacks, changesets, exports).
func (p *StackProvider) Reset(ctx context.Context) {
	p.exports.Reset(ctx)
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
	Condition   string `json:"Condition,omitempty"`
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
	return s.toWireWithDoc(nil, nil)
}

// toWireWithDoc produces the DescribeStacks wire response, applying NoEcho masking
// and filtering conditional outputs. doc and conditions may be nil (no masking/filtering).
func (s cfStack) toWireWithDoc(doc map[string]any, conditions map[string]bool) map[string]any {
	// Build NoEcho set from template Parameters section.
	noEcho := map[string]bool{}
	if doc != nil {
		if tplParams, ok := doc["Parameters"].(map[string]any); ok {
			for paramName, def := range tplParams {
				if defMap, ok := def.(map[string]any); ok {
					if ne, ok := defMap["NoEcho"]; ok {
						switch v := ne.(type) {
						case bool:
							if v {
								noEcho[paramName] = true
							}
						case string:
							if strings.EqualFold(v, "true") {
								noEcho[paramName] = true
							}
						}
					}
				}
			}
		}
	}

	params := make([]map[string]any, len(s.Parameters))
	for i, p := range s.Parameters {
		val := p.ParameterValue
		if noEcho[p.ParameterKey] {
			val = "****"
		}
		params[i] = map[string]any{"ParameterKey": p.ParameterKey, "ParameterValue": val}
	}

	// Build outputs, skipping those whose Condition evaluates to false.
	outputs := make([]map[string]any, 0, len(s.Outputs))
	for _, o := range s.Outputs {
		if o.Condition != "" && conditions != nil {
			if !conditions[o.Condition] {
				continue
			}
		}
		m := map[string]any{
			"OutputKey":   o.OutputKey,
			"OutputValue": o.OutputValue,
			"Description": o.Description,
		}
		if o.ExportName != "" {
			m["ExportName"] = o.ExportName
		}
		outputs = append(outputs, m)
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

	if isSAMTemplate(doc) {
		doc, err = TransformSAM(doc)
		if err != nil {
			return nil, model.NewProviderError("ValidationError", "SAM transform failed: "+err.Error(), 400)
		}
	}

	stackID := nr.ResourceID(model.RTCFNStack, name+"/"+shortID())

	rc := newResolveCtx(nr.Region, nr.AccountID, nr.Port)
	rc.pseudoParams["AWS::StackName"] = name
	rc.pseudoParams["AWS::StackId"] = stackID
	rc.exports = p.exports

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
		CreationTime: clock.Now(),
		Parameters:   paramsToSlice(rc.params),
		Outputs:      outputs,
		Resources:    created,
		Tags:         map[string]string{},
		Template:     template,
	}

	data, _ := json.Marshal(s)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtStack, ID: name, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, &model.ProviderError{Code: "AlreadyExistsException", Message: "Stack already exists: " + name, HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}

	// Register cross-stack exports.
	for _, o := range outputs {
		if o.ExportName != "" {
			p.exports.Register(o.ExportName, o.OutputValue, stackID)
		}
	}

	return provider.OK(map[string]any{"StackId": stackID}), nil
}

func (p *StackProvider) UpdateStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	s, err := p.loadStack(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found: " + name, HTTPStatus: http.StatusBadRequest}
	}

	newTemplateBody := strParam(nr.Params, "TemplateBody")
	if newTemplateBody == "" {
		newTemplateBody = s.Template // UsePreviousTemplate
	}

	newDoc, err := parseTemplate(newTemplateBody)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "invalid template: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// SAM transform on new template.
	if isSAMTemplate(newDoc) {
		newDoc, err = TransformSAM(newDoc)
		if err != nil {
			return nil, model.NewProviderError("ValidationError", "SAM transform failed: "+err.Error(), 400)
		}
	}

	// Parse old template for diff.
	oldDoc, _ := parseTemplate(s.Template)
	if oldDoc == nil {
		oldDoc = map[string]any{"Resources": map[string]any{}}
	}

	rc := newResolveCtx(nr.Region, nr.AccountID, nr.Port)
	rc.pseudoParams["AWS::StackName"] = name
	rc.pseudoParams["AWS::StackId"] = s.StackId
	rc.exports = p.exports

	callerParams := parseCallerParams(nr.Params)
	if tplParams, ok := newDoc["Parameters"].(map[string]any); ok {
		rc.resolveParameters(tplParams, callerParams)
	}
	if m, ok := newDoc["Mappings"].(map[string]any); ok {
		rc.mappings = m
	}
	if conds, ok := newDoc["Conditions"].(map[string]any); ok {
		rc.evaluateConditions(conds)
	}

	// Re-populate existing resources in the resolve context (needed for Ref resolution).
	for i := range s.Resources {
		rc.resources[s.Resources[i].LogicalResourceId] = &s.Resources[i]
	}

	resourcesDef, _ := newDoc["Resources"].(map[string]any)
	if len(resourcesDef) == 0 {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "template must have at least one resource", HTTPStatus: http.StatusBadRequest}
	}

	order, err := topoSort(resourcesDef)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Build old resolve context to compute resolved old properties.
	oldRc := newResolveCtx(nr.Region, nr.AccountID, nr.Port)
	oldRc.pseudoParams["AWS::StackName"] = name
	oldRc.pseudoParams["AWS::StackId"] = s.StackId
	oldParamMap := make(map[string]string, len(s.Parameters))
	for _, param := range s.Parameters {
		oldParamMap[param.ParameterKey] = param.ParameterValue
	}
	if tplParams, ok := oldDoc["Parameters"].(map[string]any); ok {
		oldRc.resolveParameters(tplParams, oldParamMap)
	}
	for i := range s.Resources {
		oldRc.resources[s.Resources[i].LogicalResourceId] = &s.Resources[i]
	}

	// Compute diff between resolved old and resolved new properties so that
	// parameter-driven changes (e.g. QueueName: {Ref: Param}) are detected.
	resolvedOldDoc := buildResolvedDoc(oldDoc, oldRc)
	resolvedNewDoc := buildResolvedDoc(newDoc, rc)
	changes := BuildChangeSet(resolvedOldDoc, resolvedNewDoc, p.handlers)

	// Build lookup maps for the diff.
	oldByLogical := make(map[string]cfResource, len(s.Resources))
	for _, r := range s.Resources {
		oldByLogical[r.LogicalResourceId] = r
	}

	// changeMap maps logicalID → ResourceChange for quick lookup.
	changeMap := make(map[string]ResourceChange, len(changes))
	for _, c := range changes {
		changeMap[c.LogicalResourceID] = c
	}

	// Process removes first (resources no longer in new template).
	for _, c := range changes {
		if c.Action != "Remove" {
			continue
		}
		if old, ok := oldByLogical[c.LogicalResourceID]; ok {
			p.deprovisionResource(ctx, old)
		}
	}

	// Build new resource list in topo order.
	newResources := make([]cfResource, 0, len(order))
	var updateErr error
	var updatedSoFar []cfResource

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

		change, hasChange := changeMap[logicalID]

		switch {
		case !hasChange:
			// Unchanged resource: keep as-is.
			if old, exists := oldByLogical[logicalID]; exists {
				newResources = append(newResources, old)
				rc.resources[logicalID] = &newResources[len(newResources)-1]
				continue
			}
			// Not in old template (shouldn't happen if diff is correct) — provision.
			fallthrough

		case hasChange && change.Action == "Add":
			// New resource.
			physicalID, attrs, provErr := p.provisionResource(ctx, logicalID, resType, resolvedProps, s.StackId, nr)
			if provErr != nil {
				slog.Warn("cloudformation: update add failed", "logical", logicalID, "err", provErr)
				updateErr = fmt.Errorf("resource %s (%s) failed: %w", logicalID, resType, provErr)
				break
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
			updatedSoFar = append(updatedSoFar, r)

		case hasChange && change.Action == "Modify":
			old, _ := oldByLogical[logicalID]
			oldProps := oldPropsFor(oldDoc, logicalID)

			if change.Replacement == "True" {
				// Replace: delete old, create new.
				p.deprovisionResource(ctx, old)
				physicalID, attrs, provErr := p.provisionResource(ctx, logicalID, resType, resolvedProps, s.StackId, nr)
				if provErr != nil {
					slog.Warn("cloudformation: update replace failed", "logical", logicalID, "err", provErr)
					updateErr = fmt.Errorf("resource %s (%s) failed: %w", logicalID, resType, provErr)
					break
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
				updatedSoFar = append(updatedSoFar, r)
			} else {
				// In-place update.
				h, hasHandler := p.handlers[resType]
				if hasHandler && h.Update != nil {
					newPhysID, attrs, replacement, updErr := h.Update(ctx, logicalID, old.PhysicalResourceId, oldProps, resolvedProps, nr)
					if updErr != nil {
						slog.Warn("cloudformation: update modify failed", "logical", logicalID, "err", updErr)
						updateErr = fmt.Errorf("resource %s (%s) failed: %w", logicalID, resType, updErr)
						break
					}
					if replacement {
						// Handler decided replacement is needed.
						p.deprovisionResource(ctx, old)
					}
					r := cfResource{
						LogicalResourceId:  logicalID,
						PhysicalResourceId: newPhysID,
						ResourceType:       resType,
						ResourceStatus:     "UPDATE_COMPLETE",
						Attributes:         attrs,
					}
					newResources = append(newResources, r)
					rc.resources[logicalID] = &newResources[len(newResources)-1]
					updatedSoFar = append(updatedSoFar, r)
				} else if hasHandler && h.Update == nil {
					// No Update func: do Delete+Create replacement.
					p.deprovisionResource(ctx, old)
					physicalID, attrs, provErr := p.provisionResource(ctx, logicalID, resType, resolvedProps, s.StackId, nr)
					if provErr != nil {
						slog.Warn("cloudformation: update recreate failed", "logical", logicalID, "err", provErr)
						updateErr = fmt.Errorf("resource %s (%s) failed: %w", logicalID, resType, provErr)
						break
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
					updatedSoFar = append(updatedSoFar, r)
				} else {
					// Metadata-only: update attributes.
					old.Attributes = updateAttributes(old.Attributes, resolvedProps)
					old.ResourceStatus = "UPDATE_COMPLETE"
					newResources = append(newResources, old)
					rc.resources[logicalID] = &newResources[len(newResources)-1]
				}
			}
		}

		if updateErr != nil {
			break
		}
	}

	// On error: attempt rollback of newly-created/modified resources, restore old state.
	if updateErr != nil {
		for i := len(updatedSoFar) - 1; i >= 0; i-- {
			p.deprovisionResource(ctx, updatedSoFar[i])
		}
		s.StackStatus = "UPDATE_ROLLBACK_COMPLETE"
		p.saveStack(ctx, nr.AccountID, nr.Region, s)
		return nil, &model.ProviderError{Code: "UpdateStack_ROLLBACK_COMPLETE", Message: updateErr.Error(), HTTPStatus: http.StatusBadRequest}
	}

	outputs := p.resolveOutputs(newDoc, rc, name)

	s.Resources = newResources
	s.Outputs = outputs
	s.StackStatus = "UPDATE_COMPLETE"
	s.Parameters = paramsToSlice(rc.params)
	s.Template = newTemplateBody
	p.saveStack(ctx, nr.AccountID, nr.Region, s)
	return provider.OK(map[string]any{"StackId": s.StackId}), nil
}

// oldPropsFor extracts the Properties map for a logical resource from an old template doc.
func oldPropsFor(oldDoc map[string]any, logicalID string) map[string]any {
	resources, _ := oldDoc["Resources"].(map[string]any)
	if resources == nil {
		return nil
	}
	res, _ := resources[logicalID].(map[string]any)
	if res == nil {
		return nil
	}
	props, _ := res["Properties"].(map[string]any)
	return props
}

func (p *StackProvider) DeleteStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	if strings.HasPrefix(name, "arn:") {
		parts := strings.Split(name, "/")
		if len(parts) >= 2 {
			name = parts[1]
		}
	}

	s, err := p.loadStack(ctx, nr.AccountID, nr.Region, name)
	if err == nil {
		// Guard: reject if any exported value is imported by another stack.
		if exportErr := p.exports.DeleteStack(s.StackId); exportErr != nil {
			return nil, &model.ProviderError{Code: "ExportInUseException", Message: exportErr.Error(), HTTPStatus: http.StatusBadRequest}
		}
		// Delete resources in reverse creation order.
		p.rollback(ctx, s.Resources)
	}

	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtStack, name); err != nil {
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
		s, err := p.loadStack(ctx, nr.AccountID, nr.Region, name)
		if err != nil {
			return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found: " + name, HTTPStatus: http.StatusBadRequest}
		}
		doc, _ := parseTemplate(s.Template)
		conds := p.stackConditions(doc, s)
		return provider.OK(map[string]any{"Stacks": []map[string]any{s.toWireWithDoc(doc, conds)}}), nil
	}
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtStack, "")
	stacks := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var s cfStack
		json.Unmarshal(e.Data, &s)
		doc, _ := parseTemplate(s.Template)
		conds := p.stackConditions(doc, s)
		stacks = append(stacks, s.toWireWithDoc(doc, conds))
	}
	return provider.OK(map[string]any{"Stacks": stacks}), nil
}

func (p *StackProvider) ListStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtStack, "")
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
	s, err := p.loadStack(ctx, nr.AccountID, nr.Region, name)
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
	s, err := p.loadStack(ctx, nr.AccountID, nr.Region, name)
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
		// Skip outputs whose Condition evaluates to false.
		condName, _ := m["Condition"].(string)
		if condName != "" && !rc.conditions[condName] {
			continue
		}
		value := fmt.Sprintf("%v", rc.Resolve(m["Value"]))
		desc, _ := m["Description"].(string)
		o := cfOutput{
			OutputKey:   key,
			OutputValue: value,
			Description: desc,
			Condition:   condName,
		}
		if export, ok := m["Export"].(map[string]any); ok {
			o.ExportName = fmt.Sprintf("%v", rc.Resolve(export["Name"]))
		}
		outputs = append(outputs, o)
	}
	return outputs
}

// ─── Template parsing ─────────────────────────────────────────────────────────

// parseTemplate unmarshals a CloudFormation template from JSON or YAML.
func parseTemplate(body string) (map[string]any, error) {
	if body == "" {
		return nil, fmt.Errorf("empty template body")
	}
	doc, err := ParseTemplate([]byte(body))
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (p *StackProvider) loadStack(ctx context.Context, account, region, name string) (cfStack, error) {
	e, err := p.resources.Get(ctx, account, region, rtStack, name)
	if err != nil {
		return cfStack{}, err
	}
	var s cfStack
	return s, json.Unmarshal(e.Data, &s)
}

func (p *StackProvider) saveStack(ctx context.Context, account, region string, s cfStack) {
	data, _ := json.Marshal(s)
	p.resources.Update(ctx, account, region, store.ResourceEntry{Type: rtStack, ID: s.StackName, Data: data})
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
	return fmt.Sprintf("%016x", clock.Now().UnixNano())
}

// stackConditions rebuilds the conditions map for a stored stack by re-evaluating
// the template's Conditions section with the stack's saved parameter values.
// Returns nil if doc is nil or has no Conditions section.
func (p *StackProvider) stackConditions(doc map[string]any, s cfStack) map[string]bool {
	if doc == nil {
		return nil
	}
	condsDef, ok := doc["Conditions"].(map[string]any)
	if !ok || len(condsDef) == 0 {
		return nil
	}
	rc := newResolveCtx("", "", 0)
	for _, param := range s.Parameters {
		rc.params[param.ParameterKey] = param.ParameterValue
	}
	rc.evaluateConditions(condsDef)
	return rc.conditions
}
