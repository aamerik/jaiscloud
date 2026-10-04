package eventarc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/gcp/eventing"
)

// httpDoer is the subset of *http.Client the dispatcher needs. Tests inject a
// fake; a nil doer disables outbound delivery.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// eventarcConnectTimeout bounds the connect phase of an Eventarc delivery, and
// eventarcDeliveryTimeout bounds the whole request (a sink that accepts and then
// stalls would otherwise leak a goroutine). Delivery is fire-and-forget (the
// floci contract): the emulator never retries or dead-letters a non-function
// delivery, so a slow or unreachable sink is logged, never surfaced to the
// producer.
const (
	eventarcConnectTimeout  = 10 * time.Second
	eventarcDeliveryTimeout = 30 * time.Second
)

// eventarcHTTPClient is the default outbound client.
func eventarcHTTPClient() httpDoer {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: eventarcConnectTimeout}).DialContext,
			ResponseHeaderTimeout: eventarcConnectTimeout,
		},
		Timeout: eventarcDeliveryTimeout,
	}
}

// DispatchEvent is the eventing.Dispatcher entry point. It delivers a produced
// Pub/Sub event to every matching Eventarc trigger whose destination is not a
// Cloud Function — the functions delivery engine owns those, so skipping them
// here is what keeps a single event from being delivered twice.
//
// Delivery mirrors floci: a binary-mode CloudEvents POST to
// destination.httpEndpoint.uri, fire-and-forget on its own goroutine, failures
// logged and never retried. cloudRun (a later phase), gke and workflow
// destinations are logged and dropped. A non-Pub/Sub event is ignored until the
// Cloud Storage source phase lands.
func (s *Service) DispatchEvent(ctx context.Context, ev eventing.Event) {
	if s == nil || s.httpClient == nil {
		return
	}
	if ev.Source != eventing.SourcePubSub {
		return
	}
	triggers, err := s.store.ListTriggersAllLocations(ctx, ev.Project)
	if err != nil {
		slog.Warn("eventarc: list triggers for event", "project", ev.Project, "err", err)
		return
	}
	attrs := pubsubEventAttributes(ev)
	topic := eventing.ResourceID(ev.Resource)
	for _, t := range triggers {
		body := decodeBody(t.Config)
		dest := bodyMap(body, "destination")
		if dest == nil {
			continue
		}
		if cf, _ := dest["cloudFunction"].(string); cf != "" {
			continue // the Cloud Functions delivery engine owns this destination
		}
		uri := httpEndpointURI(dest)
		if uri == "" {
			// cloudRun is delivered by a later phase; floci logs-and-drops the
			// destinations it cannot reach (gke, workflow, and for now cloudRun).
			slog.Warn("eventarc: destination not deliverable; dropping event",
				"trigger", t.Name, "destination", dest)
			continue
		}
		if !eventarcSourceMatches(body, topic) {
			continue
		}
		filters, _ := body["eventFilters"].([]any)
		if !filtersMatch(filters, attrs, ev.EventType) {
			continue
		}
		payload, headers := buildPubSubCloudEvent(ev.Project, t.Location, t.Name, ev)
		s.deliver(uri, headers, payload)
	}
}

// pubsubEventAttributes synthesizes the attribute map floci matches a Pub/Sub
// event on: the message's own attributes plus the reserved type and topic
// attributes (topic carries the fully-qualified resource so a filter may use
// either the short id or the full name — see attributeValueMatches).
func pubsubEventAttributes(ev eventing.Event) map[string]string {
	attrs := make(map[string]string, len(ev.Attributes)+2)
	for k, v := range ev.Attributes {
		attrs[k] = v
	}
	attrs["type"] = eventing.TypePubSubPublishCloudEvent
	attrs["topic"] = ev.Resource
	return attrs
}

// httpEndpointURI returns the destination's httpEndpoint.uri, or "" when the
// destination has none (a cloudRun/gke/workflow destination).
func httpEndpointURI(dest map[string]any) string {
	ep, _ := dest["httpEndpoint"].(map[string]any)
	if ep == nil {
		return ""
	}
	uri, _ := ep["uri"].(string)
	return strings.TrimSpace(uri)
}

// buildPubSubCloudEvent builds the binary-mode CloudEvents request floci sends
// for a Pub/Sub message: the ce-* headers and the Pub/Sub push-delivery JSON
// body ({message:{data,attributes,messageId,publishTime},subscription}). The
// subscription is the emulator's own provisioned id
// (eventarc-{location}-{triggerId}), which is what subscriptions.list returns.
func buildPubSubCloudEvent(project, location, triggerID string, ev eventing.Event) ([]byte, map[string]string) {
	message := map[string]any{
		"messageId":   ev.EventID,
		"publishTime": ev.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	if len(ev.Data) > 0 {
		message["data"] = base64.StdEncoding.EncodeToString(ev.Data)
	}
	if len(ev.Attributes) > 0 {
		message["attributes"] = ev.Attributes
	}
	payload := map[string]any{
		"message":      message,
		"subscription": "projects/" + project + "/subscriptions/" + eventing.EventarcSubscriptionID(location, triggerID),
	}
	body, _ := json.Marshal(payload)
	headers := map[string]string{
		"Content-Type":   "application/json",
		"ce-id":          ev.EventID,
		"ce-source":      "//pubsub.googleapis.com/" + ev.Resource,
		"ce-specversion": "1.0",
		"ce-type":        eventing.TypePubSubPublishCloudEvent,
		"ce-time":        ev.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	return body, headers
}

// deliver POSTs one event on its own goroutine. The waitgroup lets tests await
// in-flight deliveries; production never waits on them.
func (s *Service) deliver(uri string, headers map[string]string, body []byte) {
	s.deliveryWG.Add(1)
	go func() {
		defer s.deliveryWG.Done()
		req, err := http.NewRequest(http.MethodPost, uri, bytes.NewReader(body))
		if err != nil {
			slog.Warn("eventarc: build delivery request", "uri", uri, "err", err)
			return
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			slog.Warn("eventarc: deliver event", "uri", uri, "err", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			slog.Warn("eventarc: delivery non-2xx", "uri", uri, "status", resp.StatusCode)
		}
	}()
}

// waitDeliveries blocks until in-flight deliveries finish. It exists for tests;
// a producer never waits on fire-and-forget delivery.
func (s *Service) waitDeliveries() { s.deliveryWG.Wait() }
