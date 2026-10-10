package pubsub

import (
	"testing"
	"time"
)

func TestDeadLetterAttributes(t *testing.T) {
	// Publish time with a non-UTC instant to prove the attribute is rendered in
	// UTC with real GCP's millisecond + numeric-offset layout.
	published := time.Date(2026, 10, 10, 4, 24, 54, 929_000_000, time.UTC)
	got := DeadLetterAttributes("parity-proj", "src-sub", 5, published)

	want := map[string]string{
		"CloudPubSubDeadLetterSourceSubscription":        "src-sub",
		"CloudPubSubDeadLetterSourceSubscriptionProject": "parity-proj",
		"CloudPubSubDeadLetterSourceDeliveryCount":       "5",
		"CloudPubSubDeadLetterSourceTopicPublishTime":    "2026-10-10T04:24:54.929+00:00",
	}
	if len(got) != len(want) {
		t.Fatalf("attribute count = %d, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestDeadLetterAttributesUTCConversion(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	published := time.Date(2026, 10, 10, 4, 24, 54, 0, loc)
	got := DeadLetterAttributes("p", "s", 1, published)
	if want := "2026-10-10T09:24:54.000+00:00"; got[AttrDeadLetterSourceTopicPublishTime] != want {
		t.Errorf("publish time = %q, want %q", got[AttrDeadLetterSourceTopicPublishTime], want)
	}
}

func TestDeadLetterMessageAttributesMergesOriginals(t *testing.T) {
	published := time.Date(2026, 10, 10, 4, 24, 54, 929_000_000, time.UTC)
	original := map[string]string{"k": "v", AttrDeadLetterSourceDeliveryCount: "stale"}
	got := DeadLetterMessageAttributes("proj", "sub", 5, published, original)

	if got["k"] != "v" {
		t.Errorf("original attribute k = %q, want v", got["k"])
	}
	// The source attributes win on collision (the forwarded copy is authoritative).
	if got[AttrDeadLetterSourceDeliveryCount] != "5" {
		t.Errorf("delivery count = %q, want 5", got[AttrDeadLetterSourceDeliveryCount])
	}
	if len(got) != 5 {
		t.Errorf("attribute count = %d, want 5 (%v)", len(got), got)
	}
}
