// Package events implements the EventBridge provider.
// Rules and targets are stored in ResourceStore. On state-change events from
// EMR/EMR-Containers the provider matches rules and delivers to SQS targets.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	sqsstore "jaiscloud/internal/store/aws/sqs"
)

const (
	resTypeRule   = "eb_rule"
	resTypeTarget = "eb_target"
)

// EventBridgeProvider handles EventBridge (CloudWatch Events) operations.
type EventBridgeProvider struct {
	resources store.ResourceStore
	messages  sqsstore.SQSMessageStore
	bus       *events.EventBus
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, bus *events.EventBus) *EventBridgeProvider {
	p := &EventBridgeProvider{resources: resources, messages: messages, bus: bus}
	p.subscribeToEventBus()
	return p
}

func (p *EventBridgeProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"EventBridge.PutRule":           p.PutRule,
		"EventBridge.DeleteRule":        p.DeleteRule,
		"EventBridge.DescribeRule":      p.DescribeRule,
		"EventBridge.ListRules":         p.ListRules,
		"EventBridge.EnableRule":        p.EnableRule,
		"EventBridge.DisableRule":       p.DisableRule,
		"EventBridge.PutTargets":        p.PutTargets,
		"EventBridge.RemoveTargets":     p.RemoveTargets,
		"EventBridge.ListTargetsByRule": p.ListTargetsByRule,
		"EventBridge.PutEvents":         p.PutEvents,
	}
}

// ─── rule CRUD ────────────────────────────────────────────────────────────────

type ruleData struct {
	Name         string `json:"Name"`
	Arn          string `json:"Arn"`
	EventPattern string `json:"EventPattern,omitempty"`
	ScheduleExpr string `json:"ScheduleExpression,omitempty"`
	State        string `json:"State"`
	Description  string `json:"Description,omitempty"`
}

func (p *EventBridgeProvider) PutRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	state := strParam(nr.Params, "State")
	if state == "" {
		state = "ENABLED"
	}

	// nr.ResourceID is always set by the gateway for every cloud.
	// AWSResourceID returns arn:aws:events:..., AzureResourceID/GCPResourceID return stubs.
	arn := nr.ResourceID("events-rule", name)

	rule := ruleData{
		Name:         name,
		Arn:          arn,
		EventPattern: strParam(nr.Params, "EventPattern"),
		ScheduleExpr: strParam(nr.Params, "ScheduleExpression"),
		State:        state,
		Description:  strParam(nr.Params, "Description"),
	}
	raw, _ := json.Marshal(rule)
	entry := store.ResourceEntry{Type: resTypeRule, ID: name, Data: raw}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		} else {
			return nil, err
		}
	}
	return provider.OK(map[string]any{"RuleArn": arn}), nil
}

func (p *EventBridgeProvider) DeleteRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	_ = p.resources.Delete(ctx, resTypeRule, name)
	// Cascade: remove all targets for this rule.
	entries, _ := p.resources.List(ctx, resTypeTarget, name+"/")
	for _, e := range entries {
		_ = p.resources.Delete(ctx, resTypeTarget, e.ID)
	}
	return provider.OK(nil), nil
}

func (p *EventBridgeProvider) DescribeRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeRule, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Rule " + name + " does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var rule ruleData
	_ = json.Unmarshal(e.Data, &rule)
	return provider.OK(map[string]any{
		"Name":               rule.Name,
		"Arn":                rule.Arn,
		"EventPattern":       rule.EventPattern,
		"ScheduleExpression": rule.ScheduleExpr,
		"State":              rule.State,
		"Description":        rule.Description,
	}), nil
}

func (p *EventBridgeProvider) ListRules(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix := strParam(nr.Params, "NamePrefix")
	entries, _ := p.resources.List(ctx, resTypeRule, prefix)
	rules := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var rule ruleData
		if err := json.Unmarshal(e.Data, &rule); err != nil {
			continue
		}
		rules = append(rules, map[string]any{
			"Name":               rule.Name,
			"Arn":                rule.Arn,
			"EventPattern":       rule.EventPattern,
			"ScheduleExpression": rule.ScheduleExpr,
			"State":              rule.State,
		})
	}
	return provider.OK(map[string]any{"Rules": rules}), nil
}

func (p *EventBridgeProvider) EnableRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setRuleState(ctx, strParam(nr.Params, "Name"), "ENABLED")
}

func (p *EventBridgeProvider) DisableRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setRuleState(ctx, strParam(nr.Params, "Name"), "DISABLED")
}

func (p *EventBridgeProvider) setRuleState(ctx context.Context, name, state string) (*model.ProviderResponse, error) {
	e, err := p.resources.Get(ctx, resTypeRule, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Rule " + name + " does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var rule ruleData
	_ = json.Unmarshal(e.Data, &rule)
	rule.State = state
	raw, _ := json.Marshal(rule)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeRule, ID: name, Data: raw})
	return provider.OK(nil), nil
}

// ─── target CRUD ──────────────────────────────────────────────────────────────

// targetData stores a rule target. TargetType and QueueName are resolved
// at PutTargets time from the cloud-specific ARN so delivery is cloud-agnostic.
type targetData struct {
	ID         string `json:"Id"`
	Arn        string `json:"Arn"`
	TargetType string `json:"TargetType,omitempty"` // "sqs" | "" (unsupported type)
	QueueName  string `json:"QueueName,omitempty"`  // pre-resolved queue name for sqs targets
}

func (p *EventBridgeProvider) PutTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ruleName := strParam(nr.Params, "Rule")
	if ruleName == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Rule is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, resTypeRule, ruleName); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Rule " + ruleName + " does not exist", HTTPStatus: http.StatusBadRequest}
	}

	targets, _ := nr.Params["Targets"].([]any)
	var failed []map[string]any
	for _, t := range targets {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		id, _ := tm["Id"].(string)
		arn, _ := tm["Arn"].(string)
		if id == "" || arn == "" {
			failed = append(failed, map[string]any{"TargetId": id, "ErrorCode": "ValidationException", "ErrorMessage": "Id and Arn are required"})
			continue
		}
		// Resolve target type and queue name at write time using the known cloud.
		// Delivery path uses only the pre-resolved fields — no ARN parsing at delivery.
		td := resolveTargetMeta(nr.Cloud, id, arn)
		raw, _ := json.Marshal(td)
		storeID := ruleName + "/" + id
		entry := store.ResourceEntry{Type: resTypeTarget, ID: storeID, Data: raw}
		if err := p.resources.Create(ctx, entry); err != nil {
			if err == store.ErrAlreadyExists {
				_ = p.resources.Update(ctx, entry)
			}
		}
	}
	return provider.OK(map[string]any{
		"FailedEntryCount": len(failed),
		"FailedEntries":    failed,
	}), nil
}

// resolveTargetMeta extracts TargetType and QueueName from the cloud-specific ARN.
// This is the only place that knows about cloud-specific ARN formats for targets.
func resolveTargetMeta(cloud model.Cloud, id, arn string) targetData {
	td := targetData{ID: id, Arn: arn}
	switch cloud {
	case model.CloudAWS:
		// AWS SQS ARN: arn:aws:sqs:{region}:{account}:{queueName}
		if strings.Contains(arn, ":sqs:") {
			parts := strings.Split(arn, ":")
			if len(parts) >= 6 {
				td.TargetType = "sqs"
				td.QueueName = parts[5]
			}
		}
	case model.CloudAzure:
		// Azure Service Bus format (future): /subscriptions/.../queues/{name}
		// Stub: no supported target types yet
	case model.CloudGCP:
		// GCP Pub/Sub format (future): projects/{project}/topics/{topic}
		// Stub: no supported target types yet
	}
	return td
}

func (p *EventBridgeProvider) RemoveTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ruleName := strParam(nr.Params, "Rule")
	ids, _ := nr.Params["Ids"].([]any)
	var failed []map[string]any
	for _, idAny := range ids {
		id, _ := idAny.(string)
		if id == "" {
			continue
		}
		if err := p.resources.Delete(ctx, resTypeTarget, ruleName+"/"+id); err != nil {
			failed = append(failed, map[string]any{"TargetId": id, "ErrorCode": "TargetNotFound", "ErrorMessage": err.Error()})
		}
	}
	return provider.OK(map[string]any{
		"FailedEntryCount": len(failed),
		"FailedEntries":    failed,
	}), nil
}

func (p *EventBridgeProvider) ListTargetsByRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ruleName := strParam(nr.Params, "Rule")
	entries, _ := p.resources.List(ctx, resTypeTarget, ruleName+"/")
	targets := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var td targetData
		if err := json.Unmarshal(e.Data, &td); err != nil {
			continue
		}
		targets = append(targets, map[string]any{"Id": td.ID, "Arn": td.Arn})
	}
	return provider.OK(map[string]any{"Targets": targets}), nil
}

// ─── PutEvents ────────────────────────────────────────────────────────────────

// PutEvents pushes custom events directly into the rule-matching pipeline.
// Each entry is matched against all ENABLED rules; matching targets receive the event.
func (p *EventBridgeProvider) PutEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := nr.Params["Entries"].([]any)
	results := make([]map[string]any, 0, len(entries))
	for _, raw := range entries {
		em, ok := raw.(map[string]any)
		if !ok {
			results = append(results, map[string]any{"ErrorCode": "MalformedEntry", "ErrorMessage": "entry must be a JSON object"})
			continue
		}
		envelope := map[string]any{
			"version":     "0",
			"id":          newEventID(),
			"source":      em["Source"],
			"detail-type": em["DetailType"],
			"detail":      em["Detail"],
			"account":     nr.AccountID,
			"region":      nr.Region,
			"time":        time.Now().UTC().Format(time.RFC3339),
		}
		p.deliverEvent(ctx, envelope)
		results = append(results, map[string]any{"EventId": envelope["id"]})
	}
	return provider.OK(map[string]any{
		"FailedEntryCount": 0,
		"Entries":          results,
	}), nil
}

// ─── event delivery ───────────────────────────────────────────────────────────

// subscribeToEventBus registers handlers for EMR state-change events published
// by the EMR and EMR Containers providers.
func (p *EventBridgeProvider) subscribeToEventBus() {
	p.bus.Subscribe(events.EventEMRStepState, func(e events.Event) {
		ev, ok := e.Payload.(events.EMRStepStateEvent)
		if !ok {
			return
		}
		p.deliverEvent(context.Background(), buildEMRStepEnvelope(ev))
	})

	p.bus.Subscribe(events.EventEMRJobRunState, func(e events.Event) {
		ev, ok := e.Payload.(events.EMRJobRunStateEvent)
		if !ok {
			return
		}
		p.deliverEvent(context.Background(), buildEMRJobRunEnvelope(ev))
	})
}

// deliverEvent matches envelope against all ENABLED rules and sends to matching SQS targets.
func (p *EventBridgeProvider) deliverEvent(ctx context.Context, envelope map[string]any) {
	entries, _ := p.resources.List(ctx, resTypeRule, "")
	for _, e := range entries {
		var rule ruleData
		if err := json.Unmarshal(e.Data, &rule); err != nil {
			continue
		}
		if rule.State != "ENABLED" {
			continue
		}
		if !matchesPattern(rule.EventPattern, envelope) {
			continue
		}
		targets, _ := p.resources.List(ctx, resTypeTarget, rule.Name+"/")
		for _, te := range targets {
			var td targetData
			if err := json.Unmarshal(te.Data, &td); err != nil {
				continue
			}
			p.deliverToTarget(ctx, td, envelope)
		}
	}
}

// deliverToTarget sends the event to the target.
// Uses pre-resolved TargetType/QueueName — no cloud-specific ARN parsing here.
func (p *EventBridgeProvider) deliverToTarget(ctx context.Context, td targetData, envelope map[string]any) {
	if td.TargetType != "sqs" {
		// Unsupported target type: silently skip (consistent with AWS behaviour for
		// target types the emulator does not support).
		return
	}
	entries, _ := p.resources.List(ctx, "queue", td.QueueName)
	if len(entries) == 0 {
		return
	}
	queueURL := entries[0].ID
	body, _ := json.Marshal(envelope)
	_, _ = p.messages.Send(ctx, sqsstore.SQSMessage{
		QueueURL: queueURL,
		Body:     string(body),
	})
}

// ─── event envelope builders ──────────────────────────────────────────────────

// buildEMRStepEnvelope builds an EventBridge envelope for an EMR step state change.
// The source is derived from ev.Cloud so the envelope is correct for any cloud.
func buildEMRStepEnvelope(ev events.EMRStepStateEvent) map[string]any {
	detail := map[string]any{
		"jobFlowId": ev.JobFlowID,
		"stepId":    ev.StepID,
		"state":     ev.State,
	}
	if ev.FailureReason != "" {
		detail["failureReason"] = ev.FailureReason
	}
	return map[string]any{
		"version":     "0",
		"id":          newEventID(),
		"source":      string(ev.Cloud) + ".emr",
		"account":     ev.AccountID,
		"time":        time.Now().UTC().Format(time.RFC3339),
		"region":      ev.Region,
		"detail-type": "EMR Step Status Change",
		"detail":      detail,
	}
}

// buildEMRJobRunEnvelope builds an EventBridge envelope for an EMR Containers job run state change.
func buildEMRJobRunEnvelope(ev events.EMRJobRunStateEvent) map[string]any {
	detail := map[string]any{
		"virtualClusterId": ev.VirtualClusterID,
		"id":               ev.JobRunID,
		"state":            ev.State,
	}
	if ev.FailureReason != "" {
		detail["failureReason"] = ev.FailureReason
	}
	return map[string]any{
		"version":     "0",
		"id":          newEventID(),
		"source":      string(ev.Cloud) + ".emr-containers",
		"account":     ev.AccountID,
		"time":        time.Now().UTC().Format(time.RFC3339),
		"region":      ev.Region,
		"detail-type": "EMR Containers Job Run State Change",
		"detail":      detail,
	}
}

// ─── pattern matching ─────────────────────────────────────────────────────────

// matchesPattern checks whether an event envelope matches an EventBridge event pattern.
// Supports top-level field matching and nested detail field matching.
// An empty pattern matches all events.
func matchesPattern(pattern string, envelope map[string]any) bool {
	if pattern == "" {
		return true
	}
	var p map[string]any
	if err := json.Unmarshal([]byte(pattern), &p); err != nil {
		return false
	}
	return matchObject(p, envelope)
}

func matchObject(pattern, event map[string]any) bool {
	for key, patternVal := range pattern {
		eventVal, exists := event[key]
		if !exists {
			return false
		}
		switch pv := patternVal.(type) {
		case []any:
			// Array in pattern means "value must be one of these"
			if !matchOneOf(pv, eventVal) {
				return false
			}
		case map[string]any:
			// Nested object: recurse into detail sub-fields
			evMap, ok := eventVal.(map[string]any)
			if !ok {
				return false
			}
			if !matchObject(pv, evMap) {
				return false
			}
		default:
			if fmt.Sprintf("%v", patternVal) != fmt.Sprintf("%v", eventVal) {
				return false
			}
		}
	}
	return true
}

func matchOneOf(options []any, val any) bool {
	for _, opt := range options {
		if fmt.Sprintf("%v", opt) == fmt.Sprintf("%v", val) {
			return true
		}
	}
	return false
}

// ─── helpers ──────────────────────────────────────────────────────────────────

var eventCounter atomic.Uint64

func newEventID() string {
	n := eventCounter.Add(1)
	return fmt.Sprintf("eb-%016x", n)
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
