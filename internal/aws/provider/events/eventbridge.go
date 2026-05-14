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
	resTypeRule              = "eb_rule"
	resTypeTarget            = "eb_target"
	resTypeBus               = "eb_bus"
	resTypeEBTags            = "eb_tags"
	resTypeArchive           = "eb_archive"
	resTypeReplay            = "eb_replay"
	resTypeConnection        = "eb_connection"
	resTypeApiDestination    = "eb_api_destination"
	jaiscloudHostPlaceholder = "jaiscloud-host"
)

// EventBridgeProvider handles EventBridge (CloudWatch Events) operations.
type EventBridgeProvider struct {
	resources store.ResourceStore
	messages  sqsstore.SQSMessageStore
	bus       *events.EventBus
	port      int
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, bus *events.EventBus) *EventBridgeProvider {
	p := &EventBridgeProvider{resources: resources, messages: messages, bus: bus}
	p.subscribeToEventBus()
	return p
}

// WithPort sets the port used when resolving the placeholder queue URL in deliverToTarget.
func (p *EventBridgeProvider) WithPort(port int) *EventBridgeProvider {
	p.port = port
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
		// Event Bus CRUD
		"EventBridge.CreateEventBus":   p.CreateEventBus,
		"EventBridge.DeleteEventBus":   p.DeleteEventBus,
		"EventBridge.DescribeEventBus": p.DescribeEventBus,
		"EventBridge.ListEventBuses":   p.ListEventBuses,
		// Tags
		"EventBridge.TagResource":              p.TagResource,
		"EventBridge.UntagResource":            p.UntagResource,
		"EventBridge.ListTagsForResource":      p.ListTagsForResource,
		// Archive + Replay (13.6)
		"EventBridge.CreateArchive":     p.CreateArchive,
		"EventBridge.DescribeArchive":   p.DescribeArchive,
		"EventBridge.ListArchives":      p.ListArchives,
		"EventBridge.UpdateArchive":     p.UpdateArchive,
		"EventBridge.DeleteArchive":     p.DeleteArchive,
		"EventBridge.StartReplay":       p.StartReplay,
		"EventBridge.DescribeReplay":    p.DescribeReplay,
		"EventBridge.ListReplays":       p.ListReplays,
		"EventBridge.CancelReplay":      p.CancelReplay,
		// Connection + ApiDestination (13.7)
		"EventBridge.CreateConnection":          p.CreateConnection,
		"EventBridge.DescribeConnection":        p.DescribeConnection,
		"EventBridge.UpdateConnection":          p.UpdateConnection,
		"EventBridge.DeleteConnection":          p.DeleteConnection,
		"EventBridge.ListConnections":           p.ListConnections,
		"EventBridge.DeauthorizeConnection":     p.DeauthorizeConnection,
		"EventBridge.CreateApiDestination":      p.CreateApiDestination,
		"EventBridge.DescribeApiDestination":    p.DescribeApiDestination,
		"EventBridge.UpdateApiDestination":      p.UpdateApiDestination,
		"EventBridge.DeleteApiDestination":      p.DeleteApiDestination,
		"EventBridge.ListApiDestinations":       p.ListApiDestinations,
	}
}

// ─── rule CRUD ────────────────────────────────────────────────────────────────

type ruleData struct {
	Name         string `json:"Name"`
	Arn          string `json:"Arn"`
	EventBusName string `json:"EventBusName,omitempty"`
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

	busName := strParam(nr.Params, "EventBusName")
	if busName == "" {
		busName = "default"
	}
	rule := ruleData{
		Name:         name,
		Arn:          arn,
		EventBusName: busName,
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
	busFilter := strParam(nr.Params, "EventBusName")
	if busFilter == "" {
		busFilter = "default"
	}
	entries, _ := p.resources.List(ctx, resTypeRule, prefix)
	rules := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var rule ruleData
		if err := json.Unmarshal(e.Data, &rule); err != nil {
			continue
		}
		ruleBus := rule.EventBusName
		if ruleBus == "" {
			ruleBus = "default"
		}
		if ruleBus != busFilter {
			continue
		}
		rules = append(rules, map[string]any{
			"Name":               rule.Name,
			"Arn":                rule.Arn,
			"EventBusName":       rule.EventBusName,
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

// targetData stores a rule target. TargetType and QueueURL are resolved
// at PutTargets time from the cloud-specific ARN so delivery is cloud-agnostic.
// Storing QueueURL directly avoids a runtime resource-store lookup and removes
// any coupling to the cloud's queue resource type name ("sqs_queues", etc.).
type targetData struct {
	ID         string `json:"Id"`
	Arn        string `json:"Arn"`
	TargetType string `json:"TargetType,omitempty"` // "sqs" | "" (unsupported type)
	QueueURL   string `json:"QueueURL,omitempty"`   // pre-resolved queue URL for sqs targets
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
		// Resolve target type and queue URL at write time.
		// Storing the full queue URL means delivery never touches the resource store.
		td := resolveTargetMeta(nr.Cloud, id, arn, nr.AccountID)
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

// resolveTargetMeta extracts TargetType and QueueURL from the cloud-specific ARN.
// The queue URL is stored with a placeholder host that is resolved at delivery time
// so the URL remains correct after a server restart on a different port.
// This is the only place that knows about cloud-specific ARN formats for targets.
func resolveTargetMeta(cloud model.Cloud, id, arn, accountID string) targetData {
	td := targetData{ID: id, Arn: arn}
	switch cloud {
	case model.CloudAWS:
		// AWS SQS ARN: arn:aws:sqs:{region}:{account}:{queueName}
		if strings.Contains(arn, ":sqs:") {
			parts := strings.Split(arn, ":")
			if len(parts) >= 6 {
				td.TargetType = "sqs"
				td.QueueURL = fmt.Sprintf("http://%s/%s/%s", jaiscloudHostPlaceholder, accountID, parts[5])
			}
		}
	case model.CloudAzure:
		// Azure Service Bus format (future): /subscriptions/.../queues/{name}
	case model.CloudGCP:
		// GCP Pub/Sub format (future): projects/{project}/topics/{topic}
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
	failed := 0
	for _, raw := range entries {
		em, ok := raw.(map[string]any)
		if !ok {
			results = append(results, map[string]any{"ErrorCode": "MalformedEntry", "ErrorMessage": "entry must be a JSON object"})
			failed++
			continue
		}
		// Validate event bus exists (skip for default bus which always exists).
		busName, _ := em["EventBusName"].(string)
		if busName != "" && busName != "default" {
			if _, err := p.resources.Get(ctx, resTypeBus, busName); err != nil {
				results = append(results, map[string]any{
					"ErrorCode":    "InvalidParameterException",
					"ErrorMessage": "Event bus " + busName + " does not exist",
				})
				failed++
				continue
			}
		}
		// Detail is a JSON string in the wire protocol; parse it so pattern
		// matching on nested fields (e.g. {"detail":{"state":["X"]}}) works.
		var detail any = em["Detail"]
		if s, ok := em["Detail"].(string); ok {
			var parsed any
			if json.Unmarshal([]byte(s), &parsed) == nil {
				detail = parsed
			}
		}
		envelope := map[string]any{
			"version":     "0",
			"id":          newEventID(),
			"source":      em["Source"],
			"detail-type": em["DetailType"],
			"detail":      detail,
			"account":     nr.AccountID,
			"region":      nr.Region,
			"time":        time.Now().UTC().Format(time.RFC3339),
		}
		p.deliverEvent(ctx, envelope)
		results = append(results, map[string]any{"EventId": envelope["id"]})
	}
	return provider.OK(map[string]any{
		"FailedEntryCount": failed,
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

	p.bus.Subscribe(events.EventEMRClusterState, func(e events.Event) {
		ev, ok := e.Payload.(events.EMRClusterStateEvent)
		if !ok {
			return
		}
		p.deliverEvent(context.Background(), buildEMRClusterEnvelope(ev))
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
// Uses pre-resolved TargetType/QueueURL — no resource-store lookup, no cloud coupling.
// The placeholder host in QueueURL is replaced with the actual localhost:port at delivery time.
func (p *EventBridgeProvider) deliverToTarget(ctx context.Context, td targetData, envelope map[string]any) {
	if td.TargetType != "sqs" || td.QueueURL == "" {
		return
	}
	host := fmt.Sprintf("localhost:%d", p.port)
	queueURL := strings.Replace(td.QueueURL, jaiscloudHostPlaceholder, host, 1)
	body, _ := json.Marshal(envelope)
	_, _, _ = p.messages.Send(ctx, sqsstore.SQSMessage{
		QueueURL:  queueURL,
		MessageID: newEventID(),
		Body:      string(body),
	})
}

// ─── event envelope builders ──────────────────────────────────────────────────

// deriveSeverity returns the EventBridge severity label for a terminal state.
func deriveSeverity(state string) string {
	switch state {
	case "FAILED", "TERMINATED_WITH_ERRORS":
		return "ERROR"
	case "CANCELLED", "INTERRUPTED":
		return "WARN"
	default:
		return "INFO"
	}
}

// stateChangeReasonObj builds the nested {code, message} object that real AWS
// puts in detail.stateChangeReason for both step and cluster events.
func stateChangeReasonObj(code, message string) map[string]any {
	return map[string]any{"code": code, "message": message}
}

// buildEMRStepEnvelope builds an EventBridge envelope for an EMR step state change.
// Matches real AWS "EMR Step Status Change" schema.
func buildEMRStepEnvelope(ev events.EMRStepStateEvent) map[string]any {
	name := ev.Name
	if name == "" {
		name = "emr-step-" + ev.StepID
	}
	severity := ev.Severity
	if severity == "" {
		severity = deriveSeverity(ev.State)
	}
	msg := ev.Message
	if msg == "" {
		msg = ev.FailureReason
	}
	detail := map[string]any{
		"clusterId":         ev.JobFlowID,
		"stepId":            ev.StepID,
		"name":              name,
		"state":             ev.State,
		"severity":          severity,
		"message":           msg,
		"stateChangeReason": stateChangeReasonObj(ev.StateChangeCode, ev.StateChangeReason),
	}
	if ev.ActionOnFailure != "" {
		detail["actionOnFailure"] = ev.ActionOnFailure
	}
	eventTime := ev.OccurredAt
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}
	return map[string]any{
		"version":     "0",
		"id":          newEventID(),
		"source":      awsSource(ev.Cloud, "emr"),
		"account":     ev.AccountID,
		"time":        eventTime.UTC().Format(time.RFC3339),
		"region":      ev.Region,
		"resources":   []any{},
		"detail-type": "EMR Step Status Change",
		"detail":      detail,
	}
}

// buildEMRJobRunEnvelope builds an EventBridge envelope for an EMR Containers job run state change.
// Matches real AWS "EMR Job Run State Change" schema.
func buildEMRJobRunEnvelope(ev events.EMRJobRunStateEvent) map[string]any {
	name := ev.Name
	if name == "" {
		name = "emr-jr-" + ev.JobRunID
	}
	detail := map[string]any{
		"virtualClusterId": ev.VirtualClusterID,
		"id":               ev.JobRunID,
		"name":             name,
		"state":            ev.State,
		"releaseLabel":     ev.ReleaseLabel,
		"executionRoleArn": ev.ExecutionRoleArn,
	}
	if ev.ARN != "" {
		detail["arn"] = ev.ARN
	}
	if ev.StateDetails != "" {
		detail["stateDetails"] = ev.StateDetails
	}
	if ev.CreatedBy != "" {
		detail["createdBy"] = ev.CreatedBy
	}
	if !ev.CreatedAt.IsZero() {
		detail["createdAt"] = ev.CreatedAt.UTC().Format(time.RFC3339)
	}
	if !ev.UpdatedAt.IsZero() {
		detail["updatedAt"] = ev.UpdatedAt.UTC().Format(time.RFC3339)
	}
	if ev.FailureReason != "" {
		detail["failureReason"] = ev.FailureReason
	}
	eventTime := ev.UpdatedAt
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}
	return map[string]any{
		"version":     "0",
		"id":          newEventID(),
		"source":      awsSource(ev.Cloud, "emr-containers"),
		"account":     ev.AccountID,
		"time":        eventTime.UTC().Format(time.RFC3339),
		"region":      ev.Region,
		"resources":   []any{},
		"detail-type": "EMR Job Run State Change",
		"detail":      detail,
	}
}

// buildEMRClusterEnvelope builds an EventBridge envelope for an EMR cluster state change.
// Matches real AWS "EMR Cluster State Change" schema.
func buildEMRClusterEnvelope(ev events.EMRClusterStateEvent) map[string]any {
	severity := ev.Severity
	if severity == "" {
		severity = deriveSeverity(ev.State)
	}
	detail := map[string]any{
		"clusterId":         ev.ClusterID,
		"name":              ev.Name,
		"state":             ev.State,
		"severity":          severity,
		"message":           ev.Message,
		"stateChangeReason": stateChangeReasonObj(ev.StateChangeCode, ev.StateChangeReason),
	}
	eventTime := ev.OccurredAt
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}
	return map[string]any{
		"version":     "0",
		"id":          newEventID(),
		"source":      awsSource(ev.Cloud, "emr"),
		"account":     ev.AccountID,
		"time":        eventTime.UTC().Format(time.RFC3339),
		"region":      ev.Region,
		"resources":   []any{},
		"detail-type": "EMR Cluster State Change",
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

// awsSource returns "<cloud>.<service>" for use as the EventBridge envelope source.
// Falls back to "aws" when cloud is empty — defense against a missed handlerCtx
// threading that would produce source=".emr" and cause rule matches to silently drop.
// Scoped to AWS providers; GCP/Azure providers use their own narrow helpers.
func awsSource(cloud model.Cloud, service string) string {
	if cloud == "" {
		return "aws." + service
	}
	return string(cloud) + "." + service
}

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

// ─── Event Bus CRUD ───────────────────────────────────────────────────────────

type eventBusData struct {
	Name string `json:"Name"`
	Arn  string `json:"Arn"`
}

func (p *EventBridgeProvider) CreateEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	arn := nr.ResourceID("events-bus", name)
	bd := eventBusData{Name: name, Arn: arn}
	raw, _ := json.Marshal(bd)
	entry := store.ResourceEntry{Type: resTypeBus, ID: name, Data: raw}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "ResourceAlreadyExistsException", Message: "Event bus already exists: " + name, HTTPStatus: 400}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"EventBusArn": arn}), nil
}

func (p *EventBridgeProvider) DeleteEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "default" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Cannot delete default event bus", HTTPStatus: 400}
	}
	if err := p.resources.Delete(ctx, resTypeBus, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Event bus not found: "+name)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *EventBridgeProvider) DescribeEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		name = "default"
	}
	if name == "default" {
		arn := nr.ResourceID("events-bus", "default")
		return provider.OK(map[string]any{"Name": "default", "Arn": arn}), nil
	}
	e, err := p.resources.Get(ctx, resTypeBus, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Event bus not found: "+name)
	}
	var bd eventBusData
	if err := json.Unmarshal(e.Data, &bd); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Name": bd.Name, "Arn": bd.Arn}), nil
}

func (p *EventBridgeProvider) ListEventBuses(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	namePrefix := strParam(nr.Params, "NamePrefix")
	limit := 50
	if lv, ok := nr.Params["Limit"].(float64); ok && lv > 0 {
		limit = int(lv)
	}
	entries, _ := p.resources.List(ctx, resTypeBus, "")
	buses := make([]map[string]any, 0, len(entries)+1)
	// Include default bus if no prefix filter or "default" matches the prefix.
	if namePrefix == "" || strings.HasPrefix("default", namePrefix) {
		buses = append(buses, map[string]any{"Name": "default", "Arn": nr.ResourceID("events-bus", "default")})
	}
	for _, e := range entries {
		var bd eventBusData
		if json.Unmarshal(e.Data, &bd) == nil {
			if namePrefix != "" && !strings.HasPrefix(bd.Name, namePrefix) {
				continue
			}
			buses = append(buses, map[string]any{"Name": bd.Name, "Arn": bd.Arn})
		}
	}
	if len(buses) > limit {
		buses = buses[:limit]
	}
	return provider.OK(map[string]any{"EventBuses": buses}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *EventBridgeProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceARN")
	tags := p.loadTags(ctx, arn)
	if rawTags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range rawTags {
			if m, ok := t.(map[string]any); ok {
				k, _ := m["Key"].(string)
				v, _ := m["Value"].(string)
				if k != "" {
					tags[k] = v
				}
			}
		}
	}
	p.saveTags(ctx, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *EventBridgeProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceARN")
	tags := p.loadTags(ctx, arn)
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(tags, fmt.Sprintf("%v", k))
		}
	}
	p.saveTags(ctx, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *EventBridgeProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceARN")
	tags := p.loadTags(ctx, arn)
	out := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		out = append(out, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": out}), nil
}

func (p *EventBridgeProvider) loadTags(ctx context.Context, arn string) map[string]string {
	tags := make(map[string]string)
	if e, err := p.resources.Get(ctx, resTypeEBTags, arn); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	return tags
}

func (p *EventBridgeProvider) saveTags(ctx context.Context, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: resTypeEBTags, ID: arn, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		}
	}
}

// ─── Archive + Replay (13.6) ──────────────────────────────────────────────────

type ebArchive struct {
	Name           string    `json:"Name"`
	ARN            string    `json:"ARN"`
	EventSourceARN string    `json:"EventSourceArn"`
	Description    string    `json:"Description"`
	EventPattern   string    `json:"EventPattern"`
	RetentionDays  int       `json:"RetentionDays"`
	State          string    `json:"State"`
	StateReason    string    `json:"StateReason"`
	CreationTime   time.Time `json:"CreationTime"`
	SizeBytes      int64     `json:"SizeBytes"`
	EventCount     int64     `json:"EventCount"`
}

type ebReplay struct {
	Name               string     `json:"Name"`
	ARN                string     `json:"ARN"`
	EventSourceARN     string     `json:"EventSourceArn"`
	DestinationARN     string     `json:"DestinationArn"`
	FilterARNs         []string   `json:"FilterArns"`
	EventStartTime     time.Time  `json:"EventStartTime"`
	EventEndTime       time.Time  `json:"EventEndTime"`
	State              string     `json:"State"`
	StateReason        string     `json:"StateReason"`
	Description        string     `json:"Description"`
	ReplayStartTime    time.Time  `json:"ReplayStartTime"`
	ReplayEndTime      *time.Time `json:"ReplayEndTime,omitempty"`
}

func (p *EventBridgeProvider) CreateArchive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ArchiveName")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "ArchiveName is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, resTypeArchive, name); err == nil {
		return nil, &model.ProviderError{Code: "ResourceAlreadyExistsException", Message: "Archive already exists: " + name, HTTPStatus: http.StatusBadRequest}
	}
	arch := ebArchive{
		Name:           name,
		ARN:            nr.ResourceID("events-archive", name),
		EventSourceARN: strParam(nr.Params, "EventSourceArn"),
		Description:    strParam(nr.Params, "Description"),
		EventPattern:   strParam(nr.Params, "EventPattern"),
		State:          "ENABLED",
		CreationTime:   time.Now().UTC(),
	}
	if d, ok := nr.Params["RetentionDays"].(float64); ok {
		arch.RetentionDays = int(d)
	}
	data, _ := json.Marshal(arch)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: resTypeArchive, ID: name, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"ArchiveArn": arch.ARN, "CreationTime": arch.CreationTime.Unix(), "State": arch.State}), nil
}

func (p *EventBridgeProvider) DescribeArchive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ArchiveName")
	e, err := p.resources.Get(ctx, resTypeArchive, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Archive not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var arch ebArchive
	json.Unmarshal(e.Data, &arch)
	return provider.OK(map[string]any{
		"ArchiveName":    arch.Name,
		"ArchiveArn":     arch.ARN,
		"EventSourceArn": arch.EventSourceARN,
		"Description":    arch.Description,
		"EventPattern":   arch.EventPattern,
		"RetentionDays":  arch.RetentionDays,
		"State":          arch.State,
		"StateReason":    arch.StateReason,
		"CreationTime":   arch.CreationTime.Unix(),
		"SizeBytes":      arch.SizeBytes,
		"EventCount":     arch.EventCount,
	}), nil
}

func (p *EventBridgeProvider) ListArchives(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, resTypeArchive, "")
	namePrefix := strParam(nr.Params, "NamePrefix")
	srcFilter := strParam(nr.Params, "EventSourceArn")
	stateFilter := strParam(nr.Params, "State")
	out := []map[string]any{}
	for _, e := range entries {
		var arch ebArchive
		json.Unmarshal(e.Data, &arch)
		if namePrefix != "" && !strings.HasPrefix(arch.Name, namePrefix) {
			continue
		}
		if srcFilter != "" && arch.EventSourceARN != srcFilter {
			continue
		}
		if stateFilter != "" && arch.State != stateFilter {
			continue
		}
		out = append(out, map[string]any{
			"ArchiveName":    arch.Name,
			"ArchiveArn":     arch.ARN,
			"EventSourceArn": arch.EventSourceARN,
			"State":          arch.State,
			"RetentionDays":  arch.RetentionDays,
			"SizeBytes":      arch.SizeBytes,
			"EventCount":     arch.EventCount,
			"CreationTime":   arch.CreationTime.Unix(),
		})
	}
	return provider.OK(map[string]any{"Archives": out}), nil
}

func (p *EventBridgeProvider) UpdateArchive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ArchiveName")
	e, err := p.resources.Get(ctx, resTypeArchive, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Archive not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var arch ebArchive
	json.Unmarshal(e.Data, &arch)
	if v := strParam(nr.Params, "Description"); v != "" {
		arch.Description = v
	}
	if v := strParam(nr.Params, "EventPattern"); v != "" {
		arch.EventPattern = v
	}
	if d, ok := nr.Params["RetentionDays"].(float64); ok {
		arch.RetentionDays = int(d)
	}
	data, _ := json.Marshal(arch)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeArchive, ID: name, Data: data})
	return provider.OK(map[string]any{"ArchiveArn": arch.ARN, "CreationTime": arch.CreationTime.Unix(), "State": arch.State}), nil
}

func (p *EventBridgeProvider) DeleteArchive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ArchiveName")
	if _, err := p.resources.Get(ctx, resTypeArchive, name); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Archive not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	_ = p.resources.Delete(ctx, resTypeArchive, name)
	return provider.OK(map[string]any{}), nil
}

func (p *EventBridgeProvider) StartReplay(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ReplayName")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "ReplayName is required", HTTPStatus: http.StatusBadRequest}
	}
	dest, _ := nr.Params["Destination"].(map[string]any)
	destARN := ""
	var filterARNs []string
	if dest != nil {
		destARN, _ = dest["Arn"].(string)
		if fa, ok := dest["FilterArns"].([]any); ok {
			for _, f := range fa {
				if s, ok := f.(string); ok {
					filterARNs = append(filterARNs, s)
				}
			}
		}
	}
	replay := ebReplay{
		Name:            name,
		ARN:             nr.ResourceID("events-replay", name),
		EventSourceARN:  strParam(nr.Params, "EventSourceArn"),
		DestinationARN:  destARN,
		FilterARNs:      filterARNs,
		Description:     strParam(nr.Params, "Description"),
		State:           "COMPLETED",
		ReplayStartTime: time.Now().UTC(),
	}
	now := time.Now().UTC()
	replay.ReplayEndTime = &now
	data, _ := json.Marshal(replay)
	entry := store.ResourceEntry{Type: resTypeReplay, ID: name, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		} else {
			return nil, err
		}
	}
	return provider.OK(map[string]any{"ReplayArn": replay.ARN, "State": replay.State, "ReplayStartTime": replay.ReplayStartTime.Unix()}), nil
}

func (p *EventBridgeProvider) DescribeReplay(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ReplayName")
	e, err := p.resources.Get(ctx, resTypeReplay, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Replay not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var r ebReplay
	json.Unmarshal(e.Data, &r)
	out := map[string]any{
		"ReplayName":     r.Name,
		"ReplayArn":      r.ARN,
		"EventSourceArn": r.EventSourceARN,
		"Description":    r.Description,
		"State":          r.State,
		"StateReason":    r.StateReason,
		"ReplayStartTime": r.ReplayStartTime.Unix(),
	}
	if r.ReplayEndTime != nil {
		out["ReplayEndTime"] = r.ReplayEndTime.Unix()
	}
	return provider.OK(out), nil
}

func (p *EventBridgeProvider) ListReplays(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, resTypeReplay, "")
	namePrefix := strParam(nr.Params, "NamePrefix")
	stateFilter := strParam(nr.Params, "State")
	out := []map[string]any{}
	for _, e := range entries {
		var r ebReplay
		json.Unmarshal(e.Data, &r)
		if namePrefix != "" && !strings.HasPrefix(r.Name, namePrefix) {
			continue
		}
		if stateFilter != "" && r.State != stateFilter {
			continue
		}
		out = append(out, map[string]any{
			"ReplayName":     r.Name,
			"ReplayArn":      r.ARN,
			"EventSourceArn": r.EventSourceARN,
			"State":          r.State,
			"ReplayStartTime": r.ReplayStartTime.Unix(),
		})
	}
	return provider.OK(map[string]any{"Replays": out}), nil
}

func (p *EventBridgeProvider) CancelReplay(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ReplayName")
	e, err := p.resources.Get(ctx, resTypeReplay, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Replay not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var r ebReplay
	json.Unmarshal(e.Data, &r)
	if r.State != "RUNNING" && r.State != "STARTING" {
		return nil, &model.ProviderError{Code: "IllegalStatusException", Message: fmt.Sprintf("Replay %s is not in a cancellable state: %s", name, r.State), HTTPStatus: http.StatusBadRequest}
	}
	r.State = "CANCELLED"
	data, _ := json.Marshal(r)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeReplay, ID: name, Data: data})
	return provider.OK(map[string]any{"ReplayArn": r.ARN, "State": r.State}), nil
}

// ─── Connection + ApiDestination (13.7) ───────────────────────────────────────

type ebConnection struct {
	Name               string         `json:"Name"`
	ARN                string         `json:"ARN"`
	ID                 string         `json:"ID"`
	AuthorizationType  string         `json:"AuthorizationType"`
	AuthParameters     map[string]any `json:"AuthParameters,omitempty"`
	State              string         `json:"State"`
	Description        string         `json:"Description"`
	CreationTime       time.Time      `json:"CreationTime"`
	LastModifiedTime   time.Time      `json:"LastModifiedTime"`
	LastAuthorizedTime time.Time      `json:"LastAuthorizedTime"`
}

type ebApiDestination struct {
	Name                         string    `json:"Name"`
	ARN                          string    `json:"ARN"`
	ID                           string    `json:"ID"`
	ConnectionARN                string    `json:"ConnectionArn"`
	InvocationEndpoint           string    `json:"InvocationEndpoint"`
	HTTPMethod                   string    `json:"HttpMethod"`
	State                        string    `json:"State"`
	InvocationRateLimitPerSecond int       `json:"InvocationRateLimitPerSecond"`
	Description                  string    `json:"Description"`
	CreationTime                 time.Time `json:"CreationTime"`
	LastModifiedTime             time.Time `json:"LastModifiedTime"`
}

func ebConnID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[time.Now().UnixNano()%int64(len(chars))]
		time.Sleep(0)
	}
	return string(b)
}

func (p *EventBridgeProvider) CreateConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, resTypeConnection, name); err == nil {
		return nil, &model.ProviderError{Code: "ResourceAlreadyExistsException", Message: "Connection already exists: " + name, HTTPStatus: http.StatusBadRequest}
	}
	now := time.Now().UTC()
	conn := ebConnection{
		Name:               name,
		ARN:                nr.ResourceID("events-connection", name),
		ID:                 ebConnID(),
		AuthorizationType:  strParam(nr.Params, "AuthorizationType"),
		State:              "AUTHORIZED",
		Description:        strParam(nr.Params, "Description"),
		CreationTime:       now,
		LastModifiedTime:   now,
		LastAuthorizedTime: now,
	}
	if ap, ok := nr.Params["AuthParameters"].(map[string]any); ok {
		conn.AuthParameters = ap
	}
	data, _ := json.Marshal(conn)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: resTypeConnection, ID: name, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"ConnectionArn": conn.ARN, "ConnectionState": conn.State, "CreationTime": conn.CreationTime.Unix(), "LastModifiedTime": conn.LastModifiedTime.Unix()}), nil
}

func (p *EventBridgeProvider) DescribeConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeConnection, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Connection not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var conn ebConnection
	json.Unmarshal(e.Data, &conn)
	return provider.OK(map[string]any{
		"Name":               conn.Name,
		"ConnectionArn":      conn.ARN,
		"AuthorizationType":  conn.AuthorizationType,
		"ConnectionState":    conn.State,
		"Description":        conn.Description,
		"CreationTime":       conn.CreationTime.Unix(),
		"LastModifiedTime":   conn.LastModifiedTime.Unix(),
		"LastAuthorizedTime": conn.LastAuthorizedTime.Unix(),
	}), nil
}

func (p *EventBridgeProvider) UpdateConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeConnection, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Connection not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var conn ebConnection
	json.Unmarshal(e.Data, &conn)
	if v := strParam(nr.Params, "Description"); v != "" {
		conn.Description = v
	}
	if v := strParam(nr.Params, "AuthorizationType"); v != "" {
		conn.AuthorizationType = v
	}
	if ap, ok := nr.Params["AuthParameters"].(map[string]any); ok {
		conn.AuthParameters = ap
	}
	conn.LastModifiedTime = time.Now().UTC()
	data, _ := json.Marshal(conn)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeConnection, ID: name, Data: data})
	return provider.OK(map[string]any{"ConnectionArn": conn.ARN, "ConnectionState": conn.State, "CreationTime": conn.CreationTime.Unix(), "LastModifiedTime": conn.LastModifiedTime.Unix()}), nil
}

func (p *EventBridgeProvider) DeleteConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeConnection, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Connection not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var conn ebConnection
	json.Unmarshal(e.Data, &conn)
	_ = p.resources.Delete(ctx, resTypeConnection, name)
	return provider.OK(map[string]any{"ConnectionArn": conn.ARN, "ConnectionState": "DELETING", "CreationTime": conn.CreationTime.Unix()}), nil
}

func (p *EventBridgeProvider) ListConnections(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, resTypeConnection, "")
	namePrefix := strParam(nr.Params, "NamePrefix")
	stateFilter := strParam(nr.Params, "ConnectionState")
	out := []map[string]any{}
	for _, e := range entries {
		var conn ebConnection
		json.Unmarshal(e.Data, &conn)
		if namePrefix != "" && !strings.HasPrefix(conn.Name, namePrefix) {
			continue
		}
		if stateFilter != "" && conn.State != stateFilter {
			continue
		}
		out = append(out, map[string]any{
			"Name":              conn.Name,
			"ConnectionArn":     conn.ARN,
			"AuthorizationType": conn.AuthorizationType,
			"ConnectionState":   conn.State,
			"CreationTime":      conn.CreationTime.Unix(),
			"LastModifiedTime":  conn.LastModifiedTime.Unix(),
		})
	}
	return provider.OK(map[string]any{"Connections": out}), nil
}

func (p *EventBridgeProvider) DeauthorizeConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeConnection, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Connection not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var conn ebConnection
	json.Unmarshal(e.Data, &conn)
	conn.State = "DEAUTHORIZED"
	conn.LastModifiedTime = time.Now().UTC()
	data, _ := json.Marshal(conn)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeConnection, ID: name, Data: data})
	return provider.OK(map[string]any{"ConnectionArn": conn.ARN, "ConnectionState": conn.State, "CreationTime": conn.CreationTime.Unix()}), nil
}

func (p *EventBridgeProvider) CreateApiDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	connARN := strParam(nr.Params, "ConnectionArn")
	endpoint := strParam(nr.Params, "InvocationEndpoint")
	if _, err := p.resources.Get(ctx, resTypeApiDestination, name); err == nil {
		return nil, &model.ProviderError{Code: "ResourceAlreadyExistsException", Message: "ApiDestination already exists: " + name, HTTPStatus: http.StatusBadRequest}
	}
	rateLimit := 300
	if v, ok := nr.Params["InvocationRateLimitPerSecond"].(float64); ok && v > 0 {
		rateLimit = int(v)
	}
	now := time.Now().UTC()
	dest := ebApiDestination{
		Name:                         name,
		ARN:                          nr.ResourceID("events-api-destination", name),
		ID:                           ebConnID(),
		ConnectionARN:                connARN,
		InvocationEndpoint:           endpoint,
		HTTPMethod:                   strParam(nr.Params, "HttpMethod"),
		State:                        "ACTIVE",
		InvocationRateLimitPerSecond: rateLimit,
		Description:                  strParam(nr.Params, "Description"),
		CreationTime:                 now,
		LastModifiedTime:             now,
	}
	data, _ := json.Marshal(dest)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: resTypeApiDestination, ID: name, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"ApiDestinationArn": dest.ARN, "ApiDestinationState": dest.State, "CreationTime": dest.CreationTime.Unix(), "LastModifiedTime": dest.LastModifiedTime.Unix()}), nil
}

func (p *EventBridgeProvider) DescribeApiDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeApiDestination, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "ApiDestination not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var dest ebApiDestination
	json.Unmarshal(e.Data, &dest)
	return provider.OK(map[string]any{
		"Name":                         dest.Name,
		"ApiDestinationArn":            dest.ARN,
		"ConnectionArn":                dest.ConnectionARN,
		"InvocationEndpoint":           dest.InvocationEndpoint,
		"HttpMethod":                   dest.HTTPMethod,
		"ApiDestinationState":          dest.State,
		"InvocationRateLimitPerSecond": dest.InvocationRateLimitPerSecond,
		"Description":                  dest.Description,
		"CreationTime":                 dest.CreationTime.Unix(),
		"LastModifiedTime":             dest.LastModifiedTime.Unix(),
	}), nil
}

func (p *EventBridgeProvider) UpdateApiDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, resTypeApiDestination, name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "ApiDestination not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	var dest ebApiDestination
	json.Unmarshal(e.Data, &dest)
	if v := strParam(nr.Params, "Description"); v != "" {
		dest.Description = v
	}
	if v := strParam(nr.Params, "ConnectionArn"); v != "" {
		dest.ConnectionARN = v
	}
	if v := strParam(nr.Params, "InvocationEndpoint"); v != "" {
		dest.InvocationEndpoint = v
	}
	if v := strParam(nr.Params, "HttpMethod"); v != "" {
		dest.HTTPMethod = v
	}
	if v, ok := nr.Params["InvocationRateLimitPerSecond"].(float64); ok && v > 0 {
		dest.InvocationRateLimitPerSecond = int(v)
	}
	dest.LastModifiedTime = time.Now().UTC()
	data, _ := json.Marshal(dest)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: resTypeApiDestination, ID: name, Data: data})
	return provider.OK(map[string]any{"ApiDestinationArn": dest.ARN, "ApiDestinationState": dest.State, "CreationTime": dest.CreationTime.Unix(), "LastModifiedTime": dest.LastModifiedTime.Unix()}), nil
}

func (p *EventBridgeProvider) DeleteApiDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if _, err := p.resources.Get(ctx, resTypeApiDestination, name); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "ApiDestination not found: " + name, HTTPStatus: http.StatusBadRequest}
	}
	_ = p.resources.Delete(ctx, resTypeApiDestination, name)
	return provider.OK(map[string]any{}), nil
}

func (p *EventBridgeProvider) ListApiDestinations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, resTypeApiDestination, "")
	namePrefix := strParam(nr.Params, "NamePrefix")
	connFilter := strParam(nr.Params, "ConnectionArn")
	out := []map[string]any{}
	for _, e := range entries {
		var dest ebApiDestination
		json.Unmarshal(e.Data, &dest)
		if namePrefix != "" && !strings.HasPrefix(dest.Name, namePrefix) {
			continue
		}
		if connFilter != "" && dest.ConnectionARN != connFilter {
			continue
		}
		out = append(out, map[string]any{
			"Name":                         dest.Name,
			"ApiDestinationArn":            dest.ARN,
			"ConnectionArn":                dest.ConnectionARN,
			"InvocationEndpoint":           dest.InvocationEndpoint,
			"HttpMethod":                   dest.HTTPMethod,
			"ApiDestinationState":          dest.State,
			"InvocationRateLimitPerSecond": dest.InvocationRateLimitPerSecond,
			"CreationTime":                 dest.CreationTime.Unix(),
			"LastModifiedTime":             dest.LastModifiedTime.Unix(),
		})
	}
	return provider.OK(map[string]any{"ApiDestinations": out}), nil
}
