package eventarc

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/resource"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"

	"github.com/google/uuid"
)

// FunctionTriggerName is the deterministic trigger id the emulator provisions
// for a Cloud Functions Pub/Sub event trigger (real GCP materializes one
// Eventarc trigger per Pub/Sub-triggered function and sets the function's
// output-only eventTrigger.trigger to it).
func FunctionTriggerName(functionID string) string { return "functions-" + functionID }

// EnsureFunctionTrigger materializes (or updates) the backing Eventarc trigger
// for a function's Pub/Sub event trigger and provisions its transport Pub/Sub
// subscription. It returns the trigger's resource name and the short id of its
// subscription, which is the surface a user configures a deadLetterPolicy on.
func (s *Service) EnsureFunctionTrigger(ctx context.Context, spec eventing.FunctionTriggerSpec) (string, string, error) {
	if spec.Project == "" || spec.Location == "" || spec.FunctionID == "" {
		return "", "", invalidArgument("function trigger spec is incomplete")
	}
	topicID := lastSegment(spec.Topic)
	if topicID == "" {
		return "", "", invalidArgument("missing trigger topic")
	}
	triggerID := FunctionTriggerName(spec.FunctionID)
	cfg := map[string]any{
		"destination": map[string]any{
			"cloudFunction": resource.ResourceID(spec.Project)("cloud-function", spec.Location+"/"+spec.FunctionID),
		},
		"transport": map[string]any{
			"pubsub": map[string]any{"topic": resource.ResourceID(spec.Project)("pubsub-topic", topicID)},
		},
		"eventFilters": []any{
			map[string]any{"attribute": "type", "value": eventing.TypePubSubPublishCloudEvent},
		},
		"eventDataContentType": "application/json",
	}
	raw, _ := json.Marshal(cfg)

	// Provision the transport subscription first: it validates that the topic
	// exists, so a function whose trigger names a missing topic does not leave a
	// dangling backing trigger behind.
	sub := ""
	if s.subscriptions != nil {
		name, err := s.subscriptions.EnsureEventarcSubscription(ctx, spec.Project, spec.Location, triggerID, topicID)
		if err != nil {
			return "", "", err
		}
		sub = name
	}

	if _, err := s.store.GetTrigger(ctx, spec.Project, spec.Location, triggerID); err == nil {
		if _, err := s.store.UpdateTriggerAtomic(ctx, spec.Project, spec.Location, triggerID, func(t eventarcstore.Trigger) (eventarcstore.Trigger, error) {
			t.Config = raw
			t.UpdateTime = clock.Now().UTC()
			t.Etag = triggerEtag(t)
			return t, nil
		}); err != nil {
			return "", "", mapErr(err)
		}
	} else if errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		now := clock.Now().UTC()
		t := eventarcstore.Trigger{
			Location:   spec.Location,
			Name:       triggerID,
			Config:     raw,
			UID:        uuid.NewString(),
			CreateTime: now,
			UpdateTime: now,
		}
		t.Etag = triggerEtag(t)
		if err := s.store.CreateTrigger(ctx, spec.Project, spec.Location, t); err != nil && !errors.Is(err, eventarcstore.ErrAlreadyExists) {
			return "", "", mapErr(err)
		}
	} else {
		return "", "", mapErr(err)
	}

	return TriggerName(spec.Project, spec.Location, triggerID), sub, nil
}

// DeleteFunctionTrigger removes a function's backing Eventarc trigger and its
// transport subscription, tolerating absence.
func (s *Service) DeleteFunctionTrigger(ctx context.Context, project, location, functionID string) error {
	if project == "" || location == "" || functionID == "" {
		return nil
	}
	triggerID := FunctionTriggerName(functionID)
	if s.subscriptions != nil {
		_ = s.subscriptions.DeleteEventarcSubscription(ctx, project, eventing.EventarcSubscriptionID(location, triggerID))
	}
	if err := s.store.DeleteTrigger(ctx, project, location, triggerID); err != nil && !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		return mapErr(err)
	}
	return nil
}

// SyncTriggerSubscription provisions the transport subscription of a trigger
// whose destination is a Cloud Functions function, and removes it for any other
// destination (the emulator only executes functions). It is called after a
// trigger create or update, so a changed transport topic re-points the
// subscription and a destination change tears it down.
func (s *Service) SyncTriggerSubscription(ctx context.Context, project string, t eventarcstore.Trigger) error {
	if s.subscriptions == nil {
		return nil
	}
	body := decodeBody(t.Config)
	topic := triggerTransportTopic(body)
	dest := bodyMap(body, "destination")
	cf, _ := dest["cloudFunction"].(string)
	if topic == "" || cf == "" {
		s.DeleteTriggerSubscription(ctx, project, t)
		return nil
	}
	_, err := s.subscriptions.EnsureEventarcSubscription(ctx, project, t.Location, t.Name, lastSegment(topic))
	return err
}

// DeleteTriggerSubscription removes a user-created Eventarc trigger's transport
// subscription, tolerating absence.
func (s *Service) DeleteTriggerSubscription(ctx context.Context, project string, t eventarcstore.Trigger) {
	if s.subscriptions == nil {
		return
	}
	_ = s.subscriptions.DeleteEventarcSubscription(ctx, project, eventing.EventarcSubscriptionID(t.Location, t.Name))
}

// triggerTransportTopic returns a trigger config's transport.pubsub.topic, or "".
func triggerTransportTopic(body map[string]any) string {
	transport := bodyMap(body, "transport")
	if transport == nil {
		return ""
	}
	pubsub := bodyMap(transport, "pubsub")
	if pubsub == nil {
		return ""
	}
	topic, _ := pubsub["topic"].(string)
	return topic
}
