package sdkpubsub_test

import (
	"context"
	"testing"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
)

// TestPubSubStreamingPullReceive drives the official client's high-level
// Subscriber (the StreamingPull machinery) end to end: five messages are
// published, received through Receive, and acked; all five are observed.
func TestPubSubStreamingPullReceive(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("stream-topic")
	sub := subName("stream-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, nil)

	want := map[string]bool{"s0": false, "s1": false, "s2": false, "s3": false, "s4": false}
	for body := range want {
		publish(t, c, ctx, topic, body, nil)
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	subscriber := c.Subscriber(sub)
	subscriber.ReceiveSettings.NumGoroutines = 1
	subscriber.ReceiveSettings.MaxOutstandingMessages = 10

	got := make(chan string, len(want))
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Receive(rctx, func(ctx context.Context, m *pubsub.Message) {
			got <- string(m.Data)
			m.Ack()
		})
	}()

	seen := map[string]bool{}
	timeout := time.After(25 * time.Second)
	for len(seen) < len(want) {
		select {
		case body := <-got:
			seen[body] = true
		case <-timeout:
			t.Fatalf("received %v over streaming pull, want all of %v", seen, want)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Subscriber.Receive did not return after cancel")
	}
	for body := range want {
		if !seen[body] {
			t.Fatalf("streaming pull never delivered %q (got %v)", body, seen)
		}
	}
}
