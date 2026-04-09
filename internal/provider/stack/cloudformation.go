// Package stack implements the CloudFormation provider (StackProvider).
package stack

import (
	"context"
	"encoding/json"
	"fmt"
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
}

func New(resources store.ResourceStore) *StackProvider {
	return &StackProvider{resources: resources}
}

func (p *StackProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudFormation.CreateStack":            p.CreateStack,
		"CloudFormation.UpdateStack":            p.UpdateStack,
		"CloudFormation.DeleteStack":            p.DeleteStack,
		"CloudFormation.DescribeStacks":         p.DescribeStacks,
		"CloudFormation.ListStacks":             p.ListStacks,
		"CloudFormation.DescribeStackResources": p.DescribeStackResources,
		"CloudFormation.ValidateTemplate":       p.ValidateTemplate,
	}
}

const rtStack = "cfn_stack"

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

type cfParam struct {
	ParameterKey   string `json:"ParameterKey"`
	ParameterValue string `json:"ParameterValue"`
}

type cfOutput struct {
	OutputKey   string `json:"OutputKey"`
	OutputValue string `json:"OutputValue"`
	Description string `json:"Description"`
}

type cfResource struct {
	LogicalResourceId  string `json:"LogicalResourceId"`
	PhysicalResourceId string `json:"PhysicalResourceId"`
	ResourceType       string `json:"ResourceType"`
	ResourceStatus     string `json:"ResourceStatus"`
}

func (s cfStack) toWire() map[string]any {
	params := make([]map[string]any, len(s.Parameters))
	for i, p := range s.Parameters {
		params[i] = map[string]any{"ParameterKey": p.ParameterKey, "ParameterValue": p.ParameterValue}
	}
	outputs := make([]map[string]any, len(s.Outputs))
	for i, o := range s.Outputs {
		outputs[i] = map[string]any{"OutputKey": o.OutputKey, "OutputValue": o.OutputValue, "Description": o.Description}
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

	stackId := fmt.Sprintf("arn:aws:cloudformation:us-east-1:000000000000:stack/%s/%s", name, shortID())
	s := cfStack{
		StackId:      stackId,
		StackName:    name,
		StackStatus:  "CREATE_COMPLETE",
		Description:  strParam(nr.Params, "Description"),
		CreationTime: time.Now().UTC(),
		Parameters:   parseParams(nr.Params),
		Template:     template,
		Tags:         map[string]string{},
		Resources:    parseTemplateResources(template, name),
	}

	data, _ := json.Marshal(s)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtStack, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "AlreadyExistsException", Message: "Stack already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"StackId": stackId, "CreateStack": true}), nil
}

func (p *StackProvider) UpdateStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	s, err := p.loadStack(ctx, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found", HTTPStatus: http.StatusBadRequest}
	}
	if template := strParam(nr.Params, "TemplateBody"); template != "" {
		s.Template = template
		s.Resources = parseTemplateResources(template, name)
	}
	s.StackStatus = "UPDATE_COMPLETE"
	if params := parseParams(nr.Params); len(params) > 0 {
		s.Parameters = params
	}
	p.saveStack(ctx, s)
	return provider.OK(map[string]any{"StackId": s.StackId, "UpdateStack": true}), nil
}

func (p *StackProvider) DeleteStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	// strip ARN if given
	if strings.HasPrefix(name, "arn:") {
		parts := strings.Split(name, "/")
		if len(parts) >= 2 {
			name = parts[1]
		}
	}
	if err := p.resources.Delete(ctx, rtStack, name); err == store.ErrNotFound {
		// CloudFormation silently succeeds on delete of non-existent stack
	}
	return provider.OK(map[string]any{"DeleteStack": true}), nil
}

func (p *StackProvider) DescribeStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StackName")
	if name != "" {
		// strip ARN
		if strings.HasPrefix(name, "arn:") {
			parts := strings.Split(name, "/")
			if len(parts) >= 2 {
				name = parts[1]
			}
		}
		s, err := p.loadStack(ctx, name)
		if err != nil {
			return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found", HTTPStatus: http.StatusBadRequest}
		}
		return provider.OK(map[string]any{"Stacks": []map[string]any{s.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtStack, "")
	if err != nil {
		return nil, err
	}
	stacks := []map[string]any{}
	for _, e := range entries {
		var s cfStack
		json.Unmarshal(e.Data, &s)
		stacks = append(stacks, s.toWire())
	}
	return provider.OK(map[string]any{"Stacks": stacks}), nil
}

func (p *StackProvider) ListStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtStack, "")
	if err != nil {
		return nil, err
	}
	summaries := []map[string]any{}
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
		return nil, &model.ProviderError{Code: "ValidationError", Message: "Stack not found", HTTPStatus: http.StatusBadRequest}
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

func (p *StackProvider) ValidateTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Always valid in the emulator
	return provider.OK(map[string]any{"StackStatus": "VALID", "ValidateTemplate": true}), nil
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

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func parseParams(params map[string]any) []cfParam {
	var out []cfParam
	// AWS Query protocol sends Parameters.member.1.ParameterKey / ParameterValue
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("Parameters.member.%d.ParameterKey", i)]
		val := params[fmt.Sprintf("Parameters.member.%d.ParameterValue", i)]
		if key == nil {
			break
		}
		out = append(out, cfParam{
			ParameterKey:   fmt.Sprintf("%v", key),
			ParameterValue: fmt.Sprintf("%v", val),
		})
	}
	return out
}

// parseTemplateResources parses a CloudFormation JSON template body and
// extracts resource logical IDs and types for DescribeStackResources.
func parseTemplateResources(template, stackName string) []cfResource {
	if template == "" {
		return nil
	}
	// Try JSON parse first.
	var doc map[string]any
	if err := json.Unmarshal([]byte(template), &doc); err == nil {
		if resources, ok := doc["Resources"].(map[string]any); ok {
			var out []cfResource
			for logicalID, v := range resources {
				if m, ok := v.(map[string]any); ok {
					rtype, _ := m["Type"].(string)
					out = append(out, cfResource{
						LogicalResourceId:  logicalID,
						PhysicalResourceId: fmt.Sprintf("%s-%s", stackName, logicalID),
						ResourceType:       rtype,
						ResourceStatus:     "CREATE_COMPLETE",
					})
				}
			}
			return out
		}
	}
	// Fallback: scan for "Type": "AWS::..." lines (YAML/malformed JSON).
	var resources []cfResource
	lines := strings.Split(template, "\n")
	var currentLogical string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Detect logical resource ID lines: `"LogicalId": {` or `LogicalId:` (YAML)
		if strings.HasSuffix(line, `": {`) {
			name := strings.TrimSuffix(line, `": {`)
			name = strings.Trim(name, `"`)
			if name != "" && name != "Resources" && name != "Properties" && !strings.Contains(name, "AWS::") {
				currentLogical = name
			}
		}
		if strings.Contains(line, `"Type": "AWS::`) && currentLogical != "" {
			start := strings.Index(line, `"AWS::`)
			end := strings.LastIndex(line, `"`)
			if start >= 0 && end > start {
				resources = append(resources, cfResource{
					LogicalResourceId:  currentLogical,
					PhysicalResourceId: fmt.Sprintf("%s-%s", stackName, currentLogical),
					ResourceType:       line[start+1 : end],
					ResourceStatus:     "CREATE_COMPLETE",
				})
				currentLogical = ""
			}
		}
	}
	return resources
}

func shortID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}
