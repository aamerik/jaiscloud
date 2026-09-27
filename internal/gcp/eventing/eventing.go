// Package eventing carries the transport-neutral event payload that GCP
// producers hand to the Cloud Functions event-delivery engine, plus the small
// interfaces that keep the producer and consumer packages decoupled.
//
// A producer (Pub/Sub publish, GCS object finalize/delete, an Eventarc source)
// raises an Event and hands it to a Dispatcher. The Cloud Functions core
// implements Dispatcher and delivers the event to every matching event-triggered
// function, retrying per the trigger's failure policy. Producers hold the
// Dispatcher as an interface so they never import another service's core, and
// the functions core consults a TargetIndex (implemented by the Eventarc core)
// so a trigger whose destination is a cloudFunction also routes to its function
// without importing the Eventarc core.
package eventing

import (
	"context"
	"strings"
	"time"
)

// Canonical event types emitted by the emulator's producers. Cloud Functions
// event triggers name either these v2 forms or the legacy v1
// "providers/{service}/eventTypes/{type}" forms (and the Eventarc
// "google.cloud.*" CloudEvent forms); NormalizeEventType reconciles them.
const (
	TypePubSubPublish   = "google.pubsub.topic.publish"
	TypeStorageFinalize = "google.storage.object.finalize"
	TypeStorageDelete   = "google.storage.object.delete"

	// legacyStorageObjectChange is the v1 catch-all storage event type: a
	// trigger declaring it receives every storage object event.
	legacyStorageObjectChange = "storage.object.change"
)

// Source identifies the producer that raised an Event.
const (
	SourcePubSub   = "pubsub"
	SourceStorage  = "storage"
	SourceEventarc = "eventarc"
)

// Event is one event raised by a producer for delivery to any subscribed Cloud
// Functions. Project is the producing project (the emulator maps a project onto
// the account scope). Resource is the normalized source resource:
// "projects/{project}/topics/{topic}" for Pub/Sub or
// "projects/_/buckets/{bucket}" for Cloud Storage.
type Event struct {
	Project    string
	EventType  string
	Resource   string
	EventID    string
	Source     string
	Data       []byte
	Attributes map[string]string
	OccurredAt time.Time
}

// Dispatcher accepts a produced event for delivery to subscribed functions. The
// Cloud Functions core implements it; producers hold it as an interface so they
// never import another service's core. A nil Dispatcher disables delivery.
type Dispatcher interface {
	DispatchEvent(ctx context.Context, ev Event)
}

// Target is a Cloud Functions function that an external trigger (an Eventarc
// trigger whose destination is a cloudFunction) routes matching events to.
type Target struct {
	Project    string
	Location   string
	FunctionID string
	// Retry mirrors the trigger's retryPolicy: true when the trigger retries a
	// failed delivery.
	Retry bool
}

// TargetIndex resolves external trigger targets for an event. The Eventarc core
// implements it; the Cloud Functions delivery engine consults it so a trigger
// created directly (or by another client) routes to its function.
type TargetIndex interface {
	TargetsForEvent(ctx context.Context, ev Event) []Target
}

// NormalizeEventType maps a declared Cloud Functions or Eventarc event type onto
// the canonical form emitted by the emulator's producers. Recognized aliases:
//
//	google.pubsub.topic.publish                                → TypePubSubPublish
//	providers/cloud.pubsub/eventTypes/topic.publish            → TypePubSubPublish
//	google.cloud.pubsub.topic.v1.messagePublished             → TypePubSubPublish
//	google.storage.object.finalize                             → TypeStorageFinalize
//	google.cloud.storage.object.v1.finalized                   → TypeStorageFinalize
//	google.storage.object.delete                               → TypeStorageDelete
//	google.cloud.storage.object.v1.deleted                     → TypeStorageDelete
//	providers/cloud.storage/eventTypes/object.change           → legacyStorageObjectChange
//
// Anything else is returned unchanged (lower-cased and trimmed), so an exact
// match between a producer and a trigger still works.
func NormalizeEventType(t string) string {
	t = strings.TrimSpace(t)
	switch t {
	case TypePubSubPublish, "providers/cloud.pubsub/eventTypes/topic.publish",
		"google.cloud.pubsub.topic.v1.messagePublished":
		return TypePubSubPublish
	case TypeStorageFinalize, "google.cloud.storage.object.v1.finalized":
		return TypeStorageFinalize
	case TypeStorageDelete, "google.cloud.storage.object.v1.deleted":
		return TypeStorageDelete
	case "providers/cloud.storage/eventTypes/object.change":
		return legacyStorageObjectChange
	}
	return t
}

// TypeMatches reports whether a trigger declaring declaredType subscribes to an
// event of actualType. The v1 object.change catch-all subscribes to every
// storage object event.
func TypeMatches(declaredType, actualType string) bool {
	d := NormalizeEventType(declaredType)
	a := NormalizeEventType(actualType)
	if d == "" || a == "" {
		return false
	}
	if d == a {
		return true
	}
	return d == legacyStorageObjectChange && strings.HasPrefix(a, "google.storage.object.")
}

// ResourceID returns the last non-empty path segment of a resource name
// ("projects/p/topics/t" → "t", "projects/_/buckets/b" → "b", "b" → "b"). It is
// used to compare a trigger's declared resource against an event's normalized
// resource, tolerating the short and fully-qualified forms real GCP accepts.
func ResourceID(name string) string {
	name = strings.TrimRight(strings.TrimSpace(name), "/")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}
