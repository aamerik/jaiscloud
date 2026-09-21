package monitoring

import (
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"

	"jaiscloud/internal/gcp/rescatalog"
	"jaiscloud/internal/gcp/resource"
)

// monitoredResourceDescriptorName is the Cloud Monitoring resource name for a
// canonical monitored resource type.
func monitoredResourceDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("monitored-resource-descriptor", typ)
}

// monitoredResourceDescriptors returns the canonical catalog of well-known
// monitored resource descriptors (rescatalog), scoped to project.
func monitoredResourceDescriptors(project string) []*monitoredrespb.MonitoredResourceDescriptor {
	entries := rescatalog.Catalog()
	out := make([]*monitoredrespb.MonitoredResourceDescriptor, 0, len(entries))
	for _, e := range entries {
		out = append(out, rescatalog.Proto(e, monitoredResourceDescriptorName(project, e.Type())))
	}
	return out
}

// lookupMonitoredResourceDescriptor returns the canonical descriptor for typ,
// or ok=false when the type is not in the catalog.
func lookupMonitoredResourceDescriptor(project, typ string) (*monitoredrespb.MonitoredResourceDescriptor, bool) {
	e, ok := rescatalog.Lookup(typ)
	if !ok {
		return nil, false
	}
	return rescatalog.Proto(e, monitoredResourceDescriptorName(project, typ)), true
}

// ─── notification channel descriptor catalog ─────────────────────────────────

// notificationChannelDescriptor is one entry in the static catalog of
// well-known notification channel types. Unlike monitored resource descriptors
// (shared with Logging via rescatalog), this catalog is Monitoring-only, so it
// lives here. Channel types are globally published by Cloud Monitoring and
// carry no per-project state, so the emulator returns the catalog without
// persisting it.
type notificationChannelDescriptor struct {
	typ         string
	displayName string
	description string
	labels      []*labelpb.LabelDescriptor
}

// notificationChannelCatalog is the fixed set of notification channel types
// the emulator advertises. It mirrors the shape (not the exhaustive list) of
// the descriptors Cloud Monitoring publishes: each carries a type, a
// human-readable display name/description, and the labels a channel of that
// type must define.
var notificationChannelCatalog = []notificationChannelDescriptor{
	{
		typ:         "email",
		displayName: "Email",
		description: "A notification channel that sends email to one or more addresses.",
		labels: []*labelpb.LabelDescriptor{
			{Key: "email_address", ValueType: labelpb.LabelDescriptor_STRING, Description: "The email address to send notifications to."},
		},
	},
	{
		typ:         "sms",
		displayName: "SMS",
		description: "A notification channel that sends an SMS text message to one or more phone numbers.",
		labels: []*labelpb.LabelDescriptor{
			{Key: "number", ValueType: labelpb.LabelDescriptor_STRING, Description: "The phone number to send notifications to, in E.164 format."},
		},
	},
	{
		typ:         "pubsub",
		displayName: "Pub/Sub",
		description: "A notification channel that publishes notifications to a Google Cloud Pub/Sub topic.",
		labels: []*labelpb.LabelDescriptor{
			{Key: "topic", ValueType: labelpb.LabelDescriptor_STRING, Description: "The full resource name of the Pub/Sub topic, projects/{project}/topics/{topic}."},
		},
	},
	{
		typ:         "webhook_tokenauth",
		displayName: "Webhook (token auth)",
		description: "A notification channel that POSTs a JSON payload to an HTTPS endpoint, authenticated with a URL token.",
		labels: []*labelpb.LabelDescriptor{
			{Key: "url", ValueType: labelpb.LabelDescriptor_STRING, Description: "The HTTPS URL to POST notifications to."},
		},
	},
	{
		typ:         "slack",
		displayName: "Slack",
		description: "A notification channel that posts notifications to a Slack workspace via a webhook.",
		labels: []*labelpb.LabelDescriptor{
			{Key: "channel_name", ValueType: labelpb.LabelDescriptor_STRING, Description: "The Slack channel to post to."},
			{Key: "auth_token", ValueType: labelpb.LabelDescriptor_STRING, Description: "The Slack authentication token."},
		},
	},
}

// notificationChannelDescriptorName is the Cloud Monitoring resource name for a
// notification channel type descriptor.
func notificationChannelDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("notification-channel-descriptor", typ)
}

// notificationChannelDescriptors returns the static catalog of notification
// channel descriptors, scoped to project and sorted by type.
func notificationChannelDescriptors(project string) []*monitoringpb.NotificationChannelDescriptor {
	out := make([]*monitoringpb.NotificationChannelDescriptor, 0, len(notificationChannelCatalog))
	for _, e := range notificationChannelCatalog {
		out = append(out, notificationChannelDescriptorToProto(project, e))
	}
	return out
}

// lookupNotificationChannelDescriptor returns the descriptor for typ, or
// ok=false when the type is not in the catalog. An unknown type is NotFound,
// matching real Cloud Monitoring.
func lookupNotificationChannelDescriptor(project, typ string) (*monitoringpb.NotificationChannelDescriptor, bool) {
	for _, e := range notificationChannelCatalog {
		if e.typ == typ {
			return notificationChannelDescriptorToProto(project, e), true
		}
	}
	return nil, false
}

// notificationChannelDescriptorToProto builds the wire descriptor for e, with
// label descriptors copied so callers cannot mutate the shared catalog.
func notificationChannelDescriptorToProto(project string, e notificationChannelDescriptor) *monitoringpb.NotificationChannelDescriptor {
	labels := make([]*labelpb.LabelDescriptor, 0, len(e.labels))
	for _, l := range e.labels {
		labels = append(labels, &labelpb.LabelDescriptor{
			Key:         l.GetKey(),
			ValueType:   l.GetValueType(),
			Description: l.GetDescription(),
		})
	}
	return &monitoringpb.NotificationChannelDescriptor{
		Name:        notificationChannelDescriptorName(project, e.typ),
		Type:        e.typ,
		DisplayName: e.displayName,
		Description: e.description,
		Labels:      labels,
	}
}
