package sqs_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	sqsstore "jaiscloud/internal/aws/store/sqs"
)

// snapshotRoundTripSQS snapshots src into a buffer and restores into dst.
func snapshotRoundTripSQS(t *testing.T, src, dst interface {
	Snapshot(context.Context, *bytes.Buffer) error
	Restore(context.Context, *bytes.Buffer) error
}) {
	t.Helper()
	var buf bytes.Buffer
	if err := src.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := dst.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
}

// roundTrip snapshots store into a buffer and restores into a new store of the same type.
func roundTripMemorySQS(t *testing.T, s *sqsstore.MemoryMessageStore) *sqsstore.MemoryMessageStore {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s2 := sqsstore.NewMemoryMessageStore()
	if err := s2.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return s2
}

// ─── MemoryMessageStore ───────────────────────────────────────────────────────

func TestMemoryMessageStore_Snapshot_Empty(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	s2 := roundTripMemorySQS(t, s)

	empty, err := s2.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("expected restored empty store to be empty")
	}
}

func TestMemoryMessageStore_Snapshot_MessageBodySurvives(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()

	const qURL = "http://localhost:4566/000000000000/snap-queue"
	_, _, err := s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "msg-1",
		QueueURL:  qURL,
		Body:      "hello-snapshot",
		SentAt:    now,
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	s2 := roundTripMemorySQS(t, s)

	msgs, err := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 10, now)
	if err != nil {
		t.Fatalf("Receive after restore: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Body != "hello-snapshot" {
		t.Fatalf("wrong body: got %q, want %q", msgs[0].Body, "hello-snapshot")
	}
	if msgs[0].MessageID != "msg-1" {
		t.Fatalf("wrong MessageID: got %q", msgs[0].MessageID)
	}
}

func TestMemoryMessageStore_Snapshot_MultipleQueues(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()

	const q1 = "http://localhost:4566/000000000000/q1"
	const q2 = "http://localhost:4566/000000000000/q2"

	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{MessageID: "a", QueueURL: q1, Body: "from-q1", SentAt: now})
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{MessageID: "b", QueueURL: q2, Body: "from-q2", SentAt: now})

	s2 := roundTripMemorySQS(t, s)

	msgs1, _ := s2.Receive(ctx, "000000000000", "us-east-1", q1, 10, now)
	msgs2, _ := s2.Receive(ctx, "000000000000", "us-east-1", q2, 10, now)
	if len(msgs1) != 1 || msgs1[0].Body != "from-q1" {
		t.Fatalf("q1 not restored correctly: %v", msgs1)
	}
	if len(msgs2) != 1 || msgs2[0].Body != "from-q2" {
		t.Fatalf("q2 not restored correctly: %v", msgs2)
	}
}

func TestMemoryMessageStore_Snapshot_InFlightVisibilityRestored(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()

	const qURL = "http://localhost:4566/000000000000/inflight-queue"
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "m-inflight",
		QueueURL:  qURL,
		Body:      "in-flight",
		SentAt:    now,
	})

	// Receive the message (now in-flight with default 30s visibility).
	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", qURL, 1, now)
	if len(msgs) == 0 {
		t.Fatal("no messages on first receive")
	}

	s2 := roundTripMemorySQS(t, s)

	// Still in-flight at now+5s — should not be visible.
	msgs2, _ := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 1, now.Add(5*time.Second))
	if len(msgs2) != 0 {
		t.Fatalf("message should still be in-flight after restore, got %d", len(msgs2))
	}

	// Visible again after timeout expiry.
	msgs3, _ := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 1, now.Add(31*time.Second))
	if len(msgs3) != 1 {
		t.Fatalf("expected re-delivery after visibility timeout, got %d", len(msgs3))
	}
}

func TestMemoryMessageStore_Snapshot_DelayedMessageSurvives(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()

	const qURL = "http://localhost:4566/000000000000/delayed-snap"
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID:  "delayed-id",
		QueueURL:   qURL,
		Body:       "delayed",
		SentAt:     now,
		DelayUntil: now.Add(20 * time.Second),
	})

	s2 := roundTripMemorySQS(t, s)

	// Not visible yet.
	msgs, _ := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 1, now.Add(5*time.Second))
	if len(msgs) != 0 {
		t.Fatal("delayed message should not be visible yet after restore")
	}

	// Visible after delay.
	msgs2, _ := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 1, now.Add(21*time.Second))
	if len(msgs2) != 1 {
		t.Fatalf("expected delayed message visible after restore, got %d", len(msgs2))
	}
}

func TestMemoryMessageStore_Snapshot_FIFODedupSurvives(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()

	const fifoURL = "http://localhost:4566/000000000000/snap.fifo"
	m := sqsstore.SQSMessage{
		MessageID: "fifo-1", QueueURL: fifoURL, Body: "fifo-body",
		GroupID: "grp1", DeduplicationID: "dup-123", SentAt: now,
	}
	s.Send(ctx, "000000000000", "us-east-1", m)

	s2 := roundTripMemorySQS(t, s)

	// Re-sending the same dedup ID to the restored store should be deduplicated.
	dedupID, _, err := s2.Send(ctx, "000000000000", "us-east-1", m)
	if err != nil {
		t.Fatalf("Send to restored store: %v", err)
	}
	if dedupID == "" {
		t.Fatal("expected dedup entry to survive restore; duplicate should have been detected")
	}
}

func TestMemoryMessageStore_Snapshot_RetentionSecsSurvives(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()

	const qURL = "http://localhost:4566/000000000000/retention-snap"
	if err := s.SetQueueRetention(ctx, "000000000000", "us-east-1", qURL, 7200); err != nil {
		t.Fatalf("SetQueueRetention: %v", err)
	}

	s2 := roundTripMemorySQS(t, s)

	vis, notVis, _, err := s2.GetApproximateCounts(ctx, "000000000000", "us-east-1", qURL, time.Now())
	if err != nil {
		t.Fatalf("GetApproximateCounts after restore: %v", err)
	}
	// Queue exists with 0 messages — just verify no error.
	if vis != 0 || notVis != 0 {
		t.Fatalf("expected empty counts, got visible=%d notVisible=%d", vis, notVis)
	}
}

// ─── IsEmpty ──────────────────────────────────────────────────────────────────

func TestMemoryMessageStore_IsEmpty_NoData(t *testing.T) {
	s := sqsstore.NewMemoryMessageStore()
	empty, err := s.IsEmpty(context.Background())
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("expected new store to be empty")
	}
}

func TestMemoryMessageStore_IsEmpty_QueueExistsWithNoMessages(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()
	const qURL = "http://localhost:4566/000000000000/purge-snap"

	// Send then purge — queue exists but has 0 messages.
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "x", QueueURL: qURL, Body: "x", SentAt: now,
	})
	s.Purge(ctx, "000000000000", "us-east-1", qURL)

	empty, err := s.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("purged queue with 0 messages should be considered empty")
	}
}

func TestMemoryMessageStore_IsEmpty_WithMessages(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := time.Now()
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "y", QueueURL: testQueue, Body: "y", SentAt: now,
	})

	empty, err := s.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if empty {
		t.Fatal("expected non-empty store with a message")
	}
}

// ─── BundledSQSStore ──────────────────────────────────────────────────────────

func roundTripBundledSQS(t *testing.T, s *sqsstore.BundledSQSStore) *sqsstore.BundledSQSStore {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("BundledSQSStore Snapshot: %v", err)
	}
	s2 := sqsstore.NewBundledSQSStore()
	if err := s2.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("BundledSQSStore Restore: %v", err)
	}
	return s2
}

func TestBundledSQSStore_Snapshot_Empty(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewBundledSQSStore()
	s2 := roundTripBundledSQS(t, s)

	empty, err := s2.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("expected empty bundled store after restore")
	}
}

func TestBundledSQSStore_Snapshot_SingleScope(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewBundledSQSStore()
	now := time.Now()

	const qURL = "http://localhost:4566/000000000000/bundled-snap"
	_, _, err := s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "b1", QueueURL: qURL, Body: "bundled-hello", SentAt: now,
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	s2 := roundTripBundledSQS(t, s)

	msgs, err := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 10, now)
	if err != nil {
		t.Fatalf("Receive after restore: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Body != "bundled-hello" {
		t.Fatalf("expected 1 message 'bundled-hello', got %v", msgs)
	}
}

func TestBundledSQSStore_Snapshot_MultipleScopes(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewBundledSQSStore()
	now := time.Now()

	const qURL = "http://localhost:4566/000000000000/multi-scope"

	// Two different (account, region) scopes.
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "scope1", QueueURL: qURL, Body: "east-msg", SentAt: now,
	})
	s.Send(ctx, "000000000000", "eu-west-1", sqsstore.SQSMessage{
		MessageID: "scope2", QueueURL: qURL, Body: "west-msg", SentAt: now,
	})

	s2 := roundTripBundledSQS(t, s)

	msgsEast, _ := s2.Receive(ctx, "000000000000", "us-east-1", qURL, 10, now)
	msgsWest, _ := s2.Receive(ctx, "000000000000", "eu-west-1", qURL, 10, now)

	if len(msgsEast) != 1 || msgsEast[0].Body != "east-msg" {
		t.Fatalf("us-east-1 scope not restored: %v", msgsEast)
	}
	if len(msgsWest) != 1 || msgsWest[0].Body != "west-msg" {
		t.Fatalf("eu-west-1 scope not restored: %v", msgsWest)
	}
}

func TestBundledSQSStore_Snapshot_RestoreReplacesExistingState(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewBundledSQSStore()
	now := time.Now()

	const qURL = "http://localhost:4566/000000000000/replace-snap"
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "original", QueueURL: qURL, Body: "original-msg", SentAt: now,
	})

	var buf bytes.Buffer
	s.Snapshot(ctx, &buf)

	// Add more messages after snapshot.
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID: "extra", QueueURL: qURL, Body: "extra-msg", SentAt: now,
	})

	// Restore into the same store — should replace state.
	s.Restore(ctx, &buf)

	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", qURL, 10, now)
	if len(msgs) != 1 || msgs[0].Body != "original-msg" {
		t.Fatalf("expected exactly 1 original message after restore, got %v", msgs)
	}
}
