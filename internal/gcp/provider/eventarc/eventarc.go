// Package eventarc implements the Eventarc v1 provider
// (eventarc.googleapis.com/v1): trigger-based event routing metadata.
//
// Eventarc is the GCP analogue of AWS EventBridge in purpose, but a *different*
// model — this is deliberately NOT a port of EventBridge's bus + rule +
// event-pattern + target fan-out. A Trigger routes events from a source (a
// Pub/Sub topic or a Channel) to a destination (Cloud Run service, Cloud
// Functions v2, Workflows, GKE) with optional eventFilters, plus Channel
// (3rd-party source) and Provider (discovery) resources.
//
// This is metadata-only CRUD: the emulator never stands up an event-delivery
// engine, so a Trigger/Channel is a stored metadata record. Source/destination
// references are validated structurally (a transport.pubsub.topic must name an
// existing Pub/Sub topic; a destination.workflow must name an existing
// Workflow), and any real Eventarc method not implemented fails loud with
// Unimplemented.
package eventarc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// rtTopic is the Pub/Sub topic resource type in the shared ResourceStore
// (mirrors provider/pubsub's unexported constant). Eventarc validates
// transport.pubsub.topic references against it.
const rtTopic = "gcp_topic"

// Provider handles Eventarc v1 trigger/channel/provider resources.
type Provider struct {
	store     eventarcstore.Store
	resources store.ResourceStore  // shared control-plane store (Pub/Sub topic existence)
	workflows workflowsstore.Store // Cloud Workflows store (destination.workflow existence)
}

// New returns a Provider backed by the given stores.
func New(s eventarcstore.Store, resources store.ResourceStore, workflows workflowsstore.Store) *Provider {
	return &Provider{store: s, resources: resources, workflows: workflows}
}

// Reset wipes the store.
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Eventarc.CreateTrigger": p.CreateTrigger,
		"Eventarc.GetTrigger":    p.GetTrigger,
		"Eventarc.ListTriggers":  p.ListTriggers,
		"Eventarc.UpdateTrigger": p.UpdateTrigger,
		"Eventarc.DeleteTrigger": p.DeleteTrigger,

		"Eventarc.CreateChannel": p.CreateChannel,
		"Eventarc.GetChannel":    p.GetChannel,
		"Eventarc.ListChannels":  p.ListChannels,
		"Eventarc.UpdateChannel": p.UpdateChannel,
		"Eventarc.DeleteChannel": p.DeleteChannel,

		"Eventarc.ListProviders": p.ListProviders,
		"Eventarc.GetProvider":   p.GetProvider,

		// IAM on triggers/channels is not implemented by the emulator.
		"Eventarc.TriggerGetIamPolicy":       p.unimplemented("GetIamPolicy"),
		"Eventarc.TriggerSetIamPolicy":       p.unimplemented("SetIamPolicy"),
		"Eventarc.TriggerTestIamPermissions": p.unimplemented("TestIamPermissions"),
		"Eventarc.ChannelGetIamPolicy":       p.unimplemented("GetIamPolicy"),
		"Eventarc.ChannelSetIamPolicy":       p.unimplemented("SetIamPolicy"),
		"Eventarc.ChannelTestIamPermissions": p.unimplemented("TestIamPermissions"),
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	m := bodyMap(body, key)
	if m == nil {
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

func mapErr(err error) error {
	switch {
	case errors.Is(err, eventarcstore.ErrNoSuchTrigger):
		return model.NewProviderError("NotFound", "trigger not found", 404)
	case errors.Is(err, eventarcstore.ErrNoSuchChannel):
		return model.NewProviderError("NotFound", "channel not found", 404)
	case errors.Is(err, eventarcstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// randomHex returns n hex characters of crypto/rand randomness.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// lastSegment returns the final path segment of a resource name (the resource
// id), e.g. "projects/p/topics/t" -> "t".
func lastSegment(name string) string {
	seg := strings.Split(strings.Trim(name, "/"), "/")
	if len(seg) == 0 {
		return ""
	}
	return seg[len(seg)-1]
}

// locationOf returns the location segment of a projects/{p}/locations/{l}/...
// name, or "" when the name has no locations segment.
func locationOf(name string) string {
	seg := strings.Split(strings.Trim(name, "/"), "/")
	for i, s := range seg {
		if s == "locations" && i+1 < len(seg) {
			return seg[i+1]
		}
	}
	return ""
}

// --- Wire rendering ---

// triggerMap renders a store Trigger as an eventarc.v1.Trigger wire map. The
// stored request body (destination/transport/eventFilters/...) is echoed
// verbatim; name/uid/etag/createTime/updateTime are overlaid as output-only.
func (p *Provider) triggerMap(nr *model.NormalizedRequest, t eventarcstore.Trigger) map[string]any {
	out := map[string]any{}
	if len(t.Config) > 0 {
		_ = json.Unmarshal(t.Config, &out)
	}
	out["name"] = nr.ResourceID("eventarc-trigger", t.Location+"/"+t.Name)
	out["uid"] = t.UID
	out["etag"] = t.Etag
	out["createTime"] = formatTimestamp(t.CreateTime)
	out["updateTime"] = formatTimestamp(t.UpdateTime)
	labels := t.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// channelMap renders a store Channel as an eventarc.v1.Channel wire map. The
// stored request body (provider/cryptoKeyName/...) is echoed verbatim;
// name/uid/createTime/updateTime are overlaid and the output-only pubsubTopic,
// activationToken, and state are synthesized.
func (p *Provider) channelMap(nr *model.NormalizedRequest, c eventarcstore.Channel) map[string]any {
	out := map[string]any{}
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &out)
	}
	out["name"] = nr.ResourceID("eventarc-channel", c.Location+"/"+c.Name)
	out["uid"] = c.UID
	out["createTime"] = formatTimestamp(c.CreateTime)
	out["updateTime"] = formatTimestamp(c.UpdateTime)
	out["activationToken"] = c.ActivationToken
	out["pubsubTopic"] = fmt.Sprintf("projects/%s/topics/jc-eventarc-channel-%s", nr.AccountID, c.Name)
	out["state"] = "ACTIVE"
	labels := c.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// operationLRO wraps a trigger/channel mutation in the emulator's synchronous
// long-running-operation shape (done=true with the resource in response),
// mirroring the real eventarc.v1 google.longrunning.Operation.
func (p *Provider) operationLRO(nr *model.NormalizedRequest, location, resourceType, id, verb string, response map[string]any) *model.ProviderResponse {
	now := clock.Now().UTC()
	target := nr.ResourceID(resourceType, location+"/"+id)
	return provider.OK(map[string]any{
		"name": nr.ResourceID("eventarc-operation", location+"/"+randomHex(12)),
		"metadata": map[string]any{
			"@type":                 "type.googleapis.com/google.cloud.eventarc.v1.OperationMetadata",
			"createTime":            formatTimestamp(now),
			"endTime":               formatTimestamp(now),
			"target":                target,
			"verb":                  verb,
			"requestedCancellation": false,
			"apiVersion":            "v1",
		},
		"done":     true,
		"response": response,
	})
}

// --- Validation ---

// validateDestination enforces the destination oneof contract: exactly one of
// cloudRun / gke / workflow / httpEndpoint must be set, and the read-only
// cloudFunction field is rejected.
func validateDestination(dest map[string]any) error {
	if dest == nil {
		return model.NewProviderError("InvalidArgument", "destination is required", 400)
	}
	if cf, _ := dest["cloudFunction"].(string); cf != "" {
		return model.NewProviderError("InvalidArgument", "destination.cloudFunction is read-only; creating a Cloud Functions trigger is only supported via Cloud Functions", 400)
	}
	set := 0
	if m, _ := dest["cloudRun"].(map[string]any); m != nil {
		set++
	}
	if m, _ := dest["gke"].(map[string]any); m != nil {
		set++
	}
	if wf, _ := dest["workflow"].(string); wf != "" {
		set++
	}
	if m, _ := dest["httpEndpoint"].(map[string]any); m != nil {
		set++
	}
	if set == 0 {
		return model.NewProviderError("InvalidArgument", "destination must specify one of cloudRun, gke, workflow, or httpEndpoint", 400)
	}
	if set > 1 {
		return model.NewProviderError("InvalidArgument", "destination must specify exactly one of cloudRun, gke, workflow, or httpEndpoint", 400)
	}
	return nil
}

// validateFilters enforces the eventFilters contract: at least one filter, each
// with a non-empty attribute and value.
func validateFilters(body map[string]any) error {
	filters, ok := body["eventFilters"].([]any)
	if !ok || len(filters) == 0 {
		return model.NewProviderError("InvalidArgument", "eventFilters is required", 400)
	}
	for _, f := range filters {
		fm, ok := f.(map[string]any)
		if !ok {
			return model.NewProviderError("InvalidArgument", "eventFilters entries must be objects", 400)
		}
		if attr, _ := fm["attribute"].(string); attr == "" {
			return model.NewProviderError("InvalidArgument", "eventFilters[].attribute is required", 400)
		}
		if _, ok := fm["value"].(string); !ok {
			return model.NewProviderError("InvalidArgument", "eventFilters[].value is required", 400)
		}
	}
	return nil
}

// validateReferences enforces the source/destination reference contract: a
// transport.pubsub.topic must name an existing Pub/Sub topic, a
// destination.workflow must name an existing Workflow, and a channel must name
// an existing Channel.
func (p *Provider) validateReferences(ctx context.Context, account string, body map[string]any) error {
	if dest := bodyMap(body, "destination"); dest != nil {
		if wf, _ := dest["workflow"].(string); wf != "" {
			loc, id := locationOf(wf), lastSegment(wf)
			if loc == "" || id == "" {
				return model.NewProviderError("InvalidArgument", "invalid destination.workflow resource name", 400)
			}
			if _, err := p.workflows.GetWorkflow(ctx, account, loc, id); err != nil {
				return model.NewProviderError("NotFound", "destination.workflow not found: "+wf, 404)
			}
		}
	}
	if transport := bodyMap(body, "transport"); transport != nil {
		if pubsub := bodyMap(transport, "pubsub"); pubsub != nil {
			if topic, _ := pubsub["topic"].(string); topic != "" {
				topicID := lastSegment(topic)
				if _, err := p.resources.Get(ctx, account, store.GlobalRegion, rtTopic, topicID); err != nil {
					return model.NewProviderError("NotFound", "transport.pubsub.topic not found: "+topic, 404)
				}
			}
		}
	}
	if ch, _ := body["channel"].(string); ch != "" {
		loc, id := locationOf(ch), lastSegment(ch)
		if loc == "" || id == "" {
			return model.NewProviderError("InvalidArgument", "invalid channel resource name", 400)
		}
		if _, err := p.store.GetChannel(ctx, account, loc, id); err != nil {
			return model.NewProviderError("NotFound", "channel not found: "+ch, 404)
		}
	}
	return nil
}

func (p *Provider) validateTrigger(ctx context.Context, account string, body map[string]any) error {
	if body == nil {
		return model.NewProviderError("InvalidArgument", "missing trigger body", 400)
	}
	if err := validateFilters(body); err != nil {
		return err
	}
	if err := validateDestination(bodyMap(body, "destination")); err != nil {
		return err
	}
	return p.validateReferences(ctx, account, body)
}

func (p *Provider) validateChannel(ctx context.Context, account string, body map[string]any) error {
	if body == nil {
		return model.NewProviderError("InvalidArgument", "missing channel body", 400)
	}
	return p.validateReferences(ctx, account, body)
}

// --- Triggers ---

func (p *Provider) CreateTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	triggerID := strParam(nr, "triggerId")
	if location == "" || triggerID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or triggerId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	if err := p.validateTrigger(ctx, nr.AccountID, body); err != nil {
		return nil, err
	}
	now := clock.Now().UTC()
	t := eventarcstore.Trigger{
		Location:   location,
		Name:       triggerID,
		Labels:     bodyStringMap(body, "labels"),
		UID:        randomHex(32),
		Etag:       randomHex(32),
		CreateTime: now,
		UpdateTime: now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			t.Config = data
		}
	}
	if err := p.store.CreateTrigger(ctx, nr.AccountID, location, t); err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "create", p.triggerMap(nr, t)), nil
}

func (p *Provider) GetTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	triggerID := lastSegment(strParam(nr, "name"))
	if location == "" || triggerID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or trigger id", 400)
	}
	t, err := p.store.GetTrigger(ctx, nr.AccountID, location, triggerID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.triggerMap(nr, t)), nil
}

func (p *Provider) ListTriggers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	triggers, err := p.store.ListTriggers(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(triggers, func(t eventarcstore.Trigger) string { return t.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, p.triggerMap(nr, t))
	}
	resp := map[string]any{"triggers": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	triggerID := lastSegment(strParam(nr, "name"))
	if location == "" || triggerID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or trigger id", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	updated, err := p.store.UpdateTriggerAtomic(ctx, nr.AccountID, location, triggerID, func(t eventarcstore.Trigger) (eventarcstore.Trigger, error) {
		stored := map[string]any{}
		if len(t.Config) > 0 {
			_ = json.Unmarshal(t.Config, &stored)
		}
		for k, v := range body {
			stored[k] = v
		}
		if err := p.validateTrigger(ctx, nr.AccountID, stored); err != nil {
			return eventarcstore.Trigger{}, err
		}
		if labels := bodyStringMap(body, "labels"); labels != nil {
			t.Labels = labels
		}
		if data, err := json.Marshal(stored); err == nil {
			t.Config = data
		}
		t.UpdateTime = clock.Now().UTC()
		return t, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "update", p.triggerMap(nr, updated)), nil
}

func (p *Provider) DeleteTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	triggerID := lastSegment(strParam(nr, "name"))
	if location == "" || triggerID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or trigger id", 400)
	}
	if err := p.store.DeleteTrigger(ctx, nr.AccountID, location, triggerID); err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "delete", map[string]any{}), nil
}

// --- Channels ---

func (p *Provider) CreateChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	channelID := strParam(nr, "channelId")
	if location == "" || channelID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or channelId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	if err := p.validateChannel(ctx, nr.AccountID, body); err != nil {
		return nil, err
	}
	now := clock.Now().UTC()
	c := eventarcstore.Channel{
		Location:        location,
		Name:            channelID,
		Labels:          bodyStringMap(body, "labels"),
		UID:             randomHex(32),
		ActivationToken: randomHex(32),
		CreateTime:      now,
		UpdateTime:      now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			c.Config = data
		}
	}
	if err := p.store.CreateChannel(ctx, nr.AccountID, location, c); err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-channel", channelID, "create", p.channelMap(nr, c)), nil
}

func (p *Provider) GetChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	channelID := lastSegment(strParam(nr, "name"))
	if location == "" || channelID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or channel id", 400)
	}
	c, err := p.store.GetChannel(ctx, nr.AccountID, location, channelID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.channelMap(nr, c)), nil
}

func (p *Provider) ListChannels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	channels, err := p.store.ListChannels(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(channels, func(c eventarcstore.Channel) string { return c.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, p.channelMap(nr, c))
	}
	resp := map[string]any{"channels": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	channelID := lastSegment(strParam(nr, "name"))
	if location == "" || channelID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or channel id", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	updated, err := p.store.UpdateChannelAtomic(ctx, nr.AccountID, location, channelID, func(c eventarcstore.Channel) (eventarcstore.Channel, error) {
		stored := map[string]any{}
		if len(c.Config) > 0 {
			_ = json.Unmarshal(c.Config, &stored)
		}
		for k, v := range body {
			stored[k] = v
		}
		if err := p.validateChannel(ctx, nr.AccountID, stored); err != nil {
			return eventarcstore.Channel{}, err
		}
		if labels := bodyStringMap(body, "labels"); labels != nil {
			c.Labels = labels
		}
		if data, err := json.Marshal(stored); err == nil {
			c.Config = data
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-channel", channelID, "update", p.channelMap(nr, updated)), nil
}

func (p *Provider) DeleteChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	channelID := lastSegment(strParam(nr, "name"))
	if location == "" || channelID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or channel id", 400)
	}
	if err := p.store.DeleteChannel(ctx, nr.AccountID, location, channelID); err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-channel", channelID, "delete", map[string]any{}), nil
}

// --- Providers (read-only discovery) ---

// eventTypeDef is one catalogued provider event type.
type eventTypeDef struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

// providerDef is one catalogued Eventarc provider.
type providerDef struct {
	ID          string
	DisplayName string
	EventTypes  []eventTypeDef
}

// catalog is the fixed set of real, catalogued Eventarc providers surfaced by
// the emulator. Deliberately small: only real provider IDs and event types are
// used (no invented providers).
var catalog = []providerDef{
	{
		ID:          "pubsub.googleapis.com",
		DisplayName: "Cloud Pub/Sub",
		EventTypes: []eventTypeDef{
			{Type: "google.cloud.pubsub.topic.v1.messagePublished", Description: "A message is published to a Pub/Sub topic."},
		},
	},
	{
		ID:          "storage.googleapis.com",
		DisplayName: "Cloud Storage",
		EventTypes: []eventTypeDef{
			{Type: "google.cloud.storage.object.v1.finalized", Description: "An object is finalized (created or overwritten) in Cloud Storage."},
			{Type: "google.cloud.storage.object.v1.deleted", Description: "An object is deleted in Cloud Storage."},
		},
	},
}

func providerMap(nr *model.NormalizedRequest, location string, d providerDef) map[string]any {
	eventTypes := make([]any, 0, len(d.EventTypes))
	for _, et := range d.EventTypes {
		eventTypes = append(eventTypes, map[string]any{"type": et.Type, "description": et.Description})
	}
	return map[string]any{
		"name":        nr.ResourceID("eventarc-provider", location+"/"+d.ID),
		"displayName": d.DisplayName,
		"eventTypes":  eventTypes,
	}
}

func (p *Provider) ListProviders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	page, next := paging.Page(catalog, func(d providerDef) string { return d.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, d := range page {
		items = append(items, providerMap(nr, location, d))
	}
	resp := map[string]any{"providers": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	providerID := lastSegment(strParam(nr, "name"))
	if location == "" || providerID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or provider id", 400)
	}
	for _, d := range catalog {
		if d.ID == providerID {
			return provider.OK(providerMap(nr, location, d)), nil
		}
	}
	return nil, model.NewProviderError("NotFound", "provider not found: "+providerID, 404)
}

// unimplemented returns a handler that fails loud with Unimplemented for the
// deferred Eventarc operations (the trigger/channel IAM surface).
func (p *Provider) unimplemented(name string) provider.HandlerFunc {
	return func(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return nil, model.NewProviderError("Unimplemented", name+" is not supported by the emulator", 501)
	}
}
