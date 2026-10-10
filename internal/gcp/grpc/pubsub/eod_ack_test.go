package pubsub

import (
	"context"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"jaiscloud/internal/clock"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// eodErrorInfo extracts the google.rpc.ErrorInfo detail from an exactly-once
// ack failure.
func eodErrorInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	st, _ := status.FromError(err)
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			return ei
		}
	}
	t.Fatalf("no ErrorInfo detail on %v (details=%v)", err, st.Details())
	return nil
}

// pullOne polls a subscription until it delivers exactly one message.
func pullOne(t *testing.T, ctx context.Context, subc pubsubpb.SubscriberClient, sub string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		resp, err := subc.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
		if err != nil {
			t.Fatalf("Pull: %v", err)
		}
		if len(resp.GetReceivedMessages()) == 1 {
			return resp.GetReceivedMessages()[0].GetAckId()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no message delivered")
	return ""
}

func TestExactlyOnceAckIDVersioning(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-topic"
	const sub = "projects/test/subscriptions/eod-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("m")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ack1 := pullOne(t, ctx, subc, sub)

	// Supersede: nack then re-pull so a newer delivery version exists.
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("nack: %v", err)
	}
	ack2 := pullOne(t, ctx, subc, sub)
	if ack1 == ack2 {
		t.Fatal("re-pull returned the same ack id; version was not encoded")
	}

	// Superseded ack id → INVALID_ARGUMENT + ErrorInfo(EXACTLY_ONCE_ACKID_FAILURE).
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}}); err == nil {
		t.Fatal("Acknowledge(superseded) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge(superseded) code = %v, want InvalidArgument", st.Code())
	} else if st.Message() != pubsubstore.EODAckFailureMessage {
		t.Fatalf("Acknowledge(superseded) message = %q", st.Message())
	}
	ei := eodErrorInfo(t, func() error {
		_, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}})
		return err
	}())
	if ei.Reason != pubsubstore.EODAckFailureReason || ei.Domain != pubsubstore.EODAckFailureDomain {
		t.Fatalf("ErrorInfo reason/domain = (%q,%q)", ei.Reason, ei.Domain)
	}
	if ei.Metadata[ack1] != pubsubstore.InvalidAckIDValue {
		t.Fatalf("ErrorInfo metadata = %v, want [%s]=%s", ei.Metadata, ack1, pubsubstore.InvalidAckIDValue)
	}

	// Superseded modack is rejected the same way.
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 30,
	}); err == nil {
		t.Fatal("ModifyAckDeadline(superseded) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("ModifyAckDeadline(superseded) code = %v, want InvalidArgument", st.Code())
	}

	// The latest ack id succeeds, and a re-ack is an idempotent OK.
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack2}}); err != nil {
		t.Fatalf("Acknowledge(latest): %v", err)
	}
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack2}}); err != nil {
		t.Fatalf("Acknowledge(re-ack) = %v, want OK", err)
	}
}

func TestExactlyOnceAckIDExpired(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(nil)

	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/eod-exp-topic"
	const sub = "projects/test/subscriptions/eod-exp-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 10, EnableExactlyOnceDelivery: true,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("exp")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ack := pullOne(t, ctx, subc, sub)
	// Advance past the 10s ack deadline so the granted ack id has expired.
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(11 * time.Second)})
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack}}); err == nil {
		t.Fatal("Acknowledge(expired) = nil, want InvalidArgument")
	} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Fatalf("Acknowledge(expired) code = %v, want InvalidArgument", st.Code())
	}
}

// TestPlainSubscriptionStaleAckIDOK pins the out-of-scope rule: without
// exactly-once delivery a superseded ack id is still accepted.
func TestPlainSubscriptionStaleAckIDOK(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/plain-topic"
	const sub = "projects/test/subscriptions/plain-sub"
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subc.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 60,
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("p")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ack1 := pullOne(t, ctx, subc, sub)
	if _, err := subc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{ack1}, AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("nack: %v", err)
	}
	_ = pullOne(t, ctx, subc, sub)
	if _, err := subc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ack1}}); err != nil {
		t.Fatalf("plain Acknowledge(stale) = %v, want OK", err)
	}
}
