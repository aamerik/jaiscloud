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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// rtTopic is the Pub/Sub topic resource type in the shared ResourceStore
// (mirrors provider/pubsub's unexported constant). Eventarc validates
// transport.pubsub.topic references against it.
const rtTopic = "gcp_topic"

// rtTriggerPolicy / rtChannelPolicy are the generic ResourceStore types for
// Eventarc trigger/channel IAM policies (mirrors provider/functions).
const (
	rtTriggerPolicy = "gcp_eventarc_trigger_policy"
	rtChannelPolicy = "gcp_eventarc_channel_policy"
)

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

		"Eventarc.TriggerGetIamPolicy":       p.TriggerGetIamPolicy,
		"Eventarc.TriggerSetIamPolicy":       p.TriggerSetIamPolicy,
		"Eventarc.TriggerTestIamPermissions": p.TriggerTestIamPermissions,
		"Eventarc.ChannelGetIamPolicy":       p.ChannelGetIamPolicy,
		"Eventarc.ChannelSetIamPolicy":       p.ChannelSetIamPolicy,
		"Eventarc.ChannelTestIamPermissions": p.ChannelTestIamPermissions,
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

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
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

// boolParam reports whether a query/body parameter is the boolean true. It
// accepts the "true" string the REST codec stores for ?validateOnly=true, and
// a native bool for in-process callers.
func boolParam(nr *model.NormalizedRequest, key string) bool {
	switch v := nr.Params[key].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// maskPaths splits a comma-separated updateMask query value into its non-empty
// field paths. An empty mask returns nil, meaning "apply every field present in
// the request body" (the historical merge behavior).
func maskPaths(mask string) []string {
	if mask == "" {
		return nil
	}
	parts := strings.Split(mask, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func maskRoot(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}

// normalizeMaskField folds a field-mask path segment to a case/underscore-
// insensitive form so both the proto snake_case (event_filters) and the JSON
// camelCase (eventFilters) spellings resolve to the same field.
func normalizeMaskField(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// triggerMaskCanonical maps a normalized update_mask root to the canonical
// camelCase JSON key of an updatable Trigger field. Output-only fields are
// intentionally absent so a mask naming one fails loud rather than silently
// echoing.
var triggerMaskCanonical = map[string]string{
	"destination":          "destination",
	"eventfilters":         "eventFilters",
	"serviceaccount":       "serviceAccount",
	"transport":            "transport",
	"channel":              "channel",
	"labels":               "labels",
	"eventdatacontenttype": "eventDataContentType",
}

// channelMaskCanonical maps a normalized update_mask root to the canonical
// camelCase JSON key of an updatable Channel field.
var channelMaskCanonical = map[string]string{
	"provider":      "provider",
	"cryptokeyname": "cryptoKeyName",
	"labels":        "labels",
}

// applyTriggerMask merges an incoming Trigger body into the stored body
// according to the updateMask paths: a masked path takes the incoming value,
// every unmasked path retains the stored value. This is the Eventarc analogue
// of applyAlertPolicyMask, applied inside the store's atomic mutate closure so a
// masked PATCH cannot clobber a concurrent PATCH's disjoint fields. An empty
// mask merges every body field (the pre-existing behavior). An unsupported path
// fails loud with Unimplemented.
func applyTriggerMask(stored, incoming map[string]any, paths []string) (map[string]any, error) {
	merged := make(map[string]any, len(stored)+len(incoming))
	for k, v := range stored {
		merged[k] = v
	}
	if len(paths) == 0 {
		for k, v := range incoming {
			merged[k] = v
		}
		return merged, nil
	}
	for _, path := range paths {
		canon, ok := triggerMaskCanonical[normalizeMaskField(maskRoot(path))]
		if !ok {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
		if v, present := incoming[canon]; present {
			merged[canon] = v
		}
	}
	return merged, nil
}

// applyChannelMask is applyTriggerMask for Channel bodies.
func applyChannelMask(stored, incoming map[string]any, paths []string) (map[string]any, error) {
	merged := make(map[string]any, len(stored)+len(incoming))
	for k, v := range stored {
		merged[k] = v
	}
	if len(paths) == 0 {
		for k, v := range incoming {
			merged[k] = v
		}
		return merged, nil
	}
	for _, path := range paths {
		canon, ok := channelMaskCanonical[normalizeMaskField(maskRoot(path))]
		if !ok {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
		if v, present := incoming[canon]; present {
			merged[canon] = v
		}
	}
	return merged, nil
}

// contentEtag derives a deterministic OCC etag from a resource's mutable
// content. It is recomputed on every create/update so a caller presenting a
// pre-mutation etag is rejected (409 ABORTED) on the next write.
func contentEtag(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
		h.Write([]byte{0})
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func triggerEtag(t eventarcstore.Trigger) string {
	labels, _ := json.Marshal(t.Labels)
	return contentEtag(t.Config, labels)
}

func channelEtag(c eventarcstore.Channel) string {
	labels, _ := json.Marshal(c.Labels)
	return contentEtag(c.Config, labels, []byte(c.ActivationToken))
}

// abortedEtagMismatch is the etag-precondition failure returned as HTTP 409
// with the ABORTED google.rpc status (the exact status is unverified against
// real Eventarc; ABORTED matches the sibling shared-IAM OCC contract).
func abortedEtagMismatch() error {
	return &model.ProviderError{
		Code:       "Aborted",
		Message:    "etag mismatch: optimistic concurrency control failed",
		HTTPStatus: 409,
		Status:     "ABORTED",
	}
}

// checkEtag enforces the request-supplied etag precondition against the stored
// value. An empty request etag is a no-op (the caller opted out of OCC).
func checkEtag(reqEtag, storedEtag string) error {
	if reqEtag != "" && reqEtag != storedEtag {
		return abortedEtagMismatch()
	}
	return nil
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
	out["etag"] = c.Etag
	out["createTime"] = formatTimestamp(c.CreateTime)
	out["updateTime"] = formatTimestamp(c.UpdateTime)
	out["activationToken"] = c.ActivationToken
	out["pubsubTopic"] = fmt.Sprintf("projects/%s/topics/jc-eventarc-channel-%s", nr.AccountID, c.Name)
	// A freshly created channel has no provider Connection yet, so it is
	// PENDING; it only becomes ACTIVE once a SaaS provider connects. The
	// emulator never delivers events or connects providers.
	out["state"] = "PENDING"
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

// validateFilters enforces the eventFilters contract: at least one filter,
// each with a non-empty attribute and value, and at least one filter whose
// attribute is "type" — real Eventarc requires a type filter on every trigger.
func validateFilters(body map[string]any) error {
	filters, ok := body["eventFilters"].([]any)
	if !ok || len(filters) == 0 {
		return model.NewProviderError("InvalidArgument", "eventFilters is required", 400)
	}
	hasType := false
	for _, f := range filters {
		fm, ok := f.(map[string]any)
		if !ok {
			return model.NewProviderError("InvalidArgument", "eventFilters entries must be objects", 400)
		}
		attr, _ := fm["attribute"].(string)
		if attr == "" {
			return model.NewProviderError("InvalidArgument", "eventFilters[].attribute is required", 400)
		}
		value, _ := fm["value"].(string)
		if _, ok := fm["value"].(string); !ok {
			return model.NewProviderError("InvalidArgument", "eventFilters[].value is required", 400)
		}
		if attr == "type" && value != "" {
			hasType = true
		}
	}
	if !hasType {
		return model.NewProviderError("InvalidArgument", `eventFilters must contain a filter with attribute "type"`, 400)
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
		UID:        uuid.NewString(),
		CreateTime: now,
		UpdateTime: now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			t.Config = data
		}
	}
	t.Etag = triggerEtag(t)
	if boolParam(nr, "validateOnly") {
		if _, err := p.store.GetTrigger(ctx, nr.AccountID, location, triggerID); err == nil {
			return nil, mapErr(eventarcstore.ErrAlreadyExists)
		} else if !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
			return nil, mapErr(err)
		}
		return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "create", p.triggerMap(nr, t)), nil
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
	paths := maskPaths(strParam(nr, "updateMask"))
	reqEtag := bodyString(body, "etag")

	// Reference validation reads the shared stores (e.g. a channel named in the
	// body), so it must run outside UpdateTriggerAtomic's locked closure: the
	// store mutex is not reentrant and validateReferences re-enters it.
	stored, err := p.store.GetTrigger(ctx, nr.AccountID, location, triggerID)
	if err != nil {
		return nil, mapErr(err)
	}
	if err := checkEtag(reqEtag, stored.Etag); err != nil {
		return nil, err
	}
	next, merged, err := mergeTriggerBody(stored, body, paths)
	if err != nil {
		return nil, mapErr(err)
	}
	if err := p.validateTrigger(ctx, nr.AccountID, merged); err != nil {
		return nil, mapErr(err)
	}
	if boolParam(nr, "validateOnly") {
		return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "update", p.triggerMap(nr, next)), nil
	}

	updated, err := p.store.UpdateTriggerAtomic(ctx, nr.AccountID, location, triggerID, func(t eventarcstore.Trigger) (eventarcstore.Trigger, error) {
		// The etag precondition and the masked merge both run inside the
		// store's locked mutate closure, so a stale etag can't slip past a
		// concurrent update and a masked PATCH can't clobber a concurrent
		// PATCH's disjoint fields.
		if err := checkEtag(reqEtag, t.Etag); err != nil {
			return eventarcstore.Trigger{}, err
		}
		next, _, err := mergeTriggerBody(t, body, paths)
		if err != nil {
			return eventarcstore.Trigger{}, err
		}
		next.UpdateTime = clock.Now().UTC()
		next.Etag = triggerEtag(next)
		return next, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "update", p.triggerMap(nr, updated)), nil
}

// mergeTriggerBody applies a Trigger PATCH body to the stored trigger under the
// given updateMask paths: it overlays only the masked (or, for an empty mask,
// every present) top-level body field, mirrors the merged labels onto the
// struct, and returns the merged wire body. It performs no reference validation
// and never touches the store, so it is safe to call from inside the store's
// locked mutate closure.
func mergeTriggerBody(t eventarcstore.Trigger, body map[string]any, paths []string) (eventarcstore.Trigger, map[string]any, error) {
	stored := map[string]any{}
	if len(t.Config) > 0 {
		_ = json.Unmarshal(t.Config, &stored)
	}
	merged, err := applyTriggerMask(stored, body, paths)
	if err != nil {
		return eventarcstore.Trigger{}, nil, err
	}
	if labels := bodyStringMap(merged, "labels"); labels != nil {
		t.Labels = labels
	}
	if data, err := json.Marshal(merged); err == nil {
		t.Config = data
	}
	return t, merged, nil
}

func (p *Provider) DeleteTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	triggerID := lastSegment(strParam(nr, "name"))
	if location == "" || triggerID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or trigger id", 400)
	}
	reqEtag := strParam(nr, "etag")
	if boolParam(nr, "validateOnly") {
		stored, err := p.store.GetTrigger(ctx, nr.AccountID, location, triggerID)
		if err != nil {
			return nil, mapErr(err)
		}
		if err := checkEtag(reqEtag, stored.Etag); err != nil {
			return nil, err
		}
		return p.operationLRO(nr, location, "eventarc-trigger", triggerID, "delete", map[string]any{}), nil
	}
	if err := p.store.DeleteTriggerAtomic(ctx, nr.AccountID, location, triggerID, func(t eventarcstore.Trigger) error {
		return checkEtag(reqEtag, t.Etag)
	}); err != nil {
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
		UID:             uuid.NewString(),
		ActivationToken: randomHex(32),
		CreateTime:      now,
		UpdateTime:      now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			c.Config = data
		}
	}
	c.Etag = channelEtag(c)
	if boolParam(nr, "validateOnly") {
		if _, err := p.store.GetChannel(ctx, nr.AccountID, location, channelID); err == nil {
			return nil, mapErr(eventarcstore.ErrAlreadyExists)
		} else if !errors.Is(err, eventarcstore.ErrNoSuchChannel) {
			return nil, mapErr(err)
		}
		return p.operationLRO(nr, location, "eventarc-channel", channelID, "create", p.channelMap(nr, c)), nil
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
	paths := maskPaths(strParam(nr, "updateMask"))
	reqEtag := bodyString(body, "etag")

	// See UpdateTrigger: reference validation must run outside the locked
	// closure.
	stored, err := p.store.GetChannel(ctx, nr.AccountID, location, channelID)
	if err != nil {
		return nil, mapErr(err)
	}
	if err := checkEtag(reqEtag, stored.Etag); err != nil {
		return nil, err
	}
	next, merged, err := mergeChannelBody(stored, body, paths)
	if err != nil {
		return nil, mapErr(err)
	}
	if err := p.validateChannel(ctx, nr.AccountID, merged); err != nil {
		return nil, mapErr(err)
	}
	if boolParam(nr, "validateOnly") {
		return p.operationLRO(nr, location, "eventarc-channel", channelID, "update", p.channelMap(nr, next)), nil
	}

	updated, err := p.store.UpdateChannelAtomic(ctx, nr.AccountID, location, channelID, func(c eventarcstore.Channel) (eventarcstore.Channel, error) {
		// See UpdateTrigger: etag check and masked merge are both inside the
		// locked mutate closure.
		if err := checkEtag(reqEtag, c.Etag); err != nil {
			return eventarcstore.Channel{}, err
		}
		next, _, err := mergeChannelBody(c, body, paths)
		if err != nil {
			return eventarcstore.Channel{}, err
		}
		next.UpdateTime = clock.Now().UTC()
		next.Etag = channelEtag(next)
		return next, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return p.operationLRO(nr, location, "eventarc-channel", channelID, "update", p.channelMap(nr, updated)), nil
}

// mergeChannelBody applies a Channel PATCH body to the stored channel under the
// given updateMask paths. It performs no reference validation and never touches
// the store, so it is safe to call from inside the store's locked mutate
// closure.
func mergeChannelBody(c eventarcstore.Channel, body map[string]any, paths []string) (eventarcstore.Channel, map[string]any, error) {
	stored := map[string]any{}
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &stored)
	}
	merged, err := applyChannelMask(stored, body, paths)
	if err != nil {
		return eventarcstore.Channel{}, nil, err
	}
	if labels := bodyStringMap(merged, "labels"); labels != nil {
		c.Labels = labels
	}
	if data, err := json.Marshal(merged); err == nil {
		c.Config = data
	}
	return c, merged, nil
}

func (p *Provider) DeleteChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	channelID := lastSegment(strParam(nr, "name"))
	if location == "" || channelID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or channel id", 400)
	}
	reqEtag := strParam(nr, "etag")
	if boolParam(nr, "validateOnly") {
		stored, err := p.store.GetChannel(ctx, nr.AccountID, location, channelID)
		if err != nil {
			return nil, mapErr(err)
		}
		if err := checkEtag(reqEtag, stored.Etag); err != nil {
			return nil, err
		}
		return p.operationLRO(nr, location, "eventarc-channel", channelID, "delete", map[string]any{}), nil
	}
	if err := p.store.DeleteChannelAtomic(ctx, nr.AccountID, location, channelID, func(c eventarcstore.Channel) error {
		return checkEtag(reqEtag, c.Etag)
	}); err != nil {
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

// --- IAM (triggers / channels) ---

// requireTrigger resolves the IAM policy resource id for a trigger and
// requires the trigger to exist (NotFound otherwise), mirroring
// functions.requireFunction.
func (p *Provider) requireTrigger(ctx context.Context, nr *model.NormalizedRequest) (string, error) {
	location := strParam(nr, "location")
	id := lastSegment(strParam(nr, "name"))
	if location == "" || id == "" {
		return "", model.NewProviderError("InvalidArgument", "missing trigger name or location", 400)
	}
	if _, err := p.store.GetTrigger(ctx, nr.AccountID, location, id); err != nil {
		return "", mapErr(err)
	}
	return location + "/" + id, nil
}

// requireChannel resolves the IAM policy resource id for a channel and
// requires the channel to exist (NotFound otherwise).
func (p *Provider) requireChannel(ctx context.Context, nr *model.NormalizedRequest) (string, error) {
	location := strParam(nr, "location")
	id := lastSegment(strParam(nr, "name"))
	if location == "" || id == "" {
		return "", model.NewProviderError("InvalidArgument", "missing channel name or location", 400)
	}
	if _, err := p.store.GetChannel(ctx, nr.AccountID, location, id); err != nil {
		return "", mapErr(err)
	}
	return location + "/" + id, nil
}

func (p *Provider) TriggerGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, err := p.requireTrigger(ctx, nr)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtTriggerPolicy, id))), nil
}

func (p *Provider) TriggerSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, err := p.requireTrigger(ctx, nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtTriggerPolicy, id, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TriggerTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := p.requireTrigger(ctx, nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func (p *Provider) ChannelGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, err := p.requireChannel(ctx, nr)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtChannelPolicy, id))), nil
}

func (p *Provider) ChannelSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, err := p.requireChannel(ctx, nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtChannelPolicy, id, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) ChannelTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := p.requireChannel(ctx, nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}
