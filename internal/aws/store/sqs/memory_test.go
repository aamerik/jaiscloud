package sqs_test

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
		sqsstore "jaiscloud/internal/aws/store/sqs"
)

const testQueue = "http://localhost:4566/000000000000/test-queue"

func newMsg(body string) sqsstore.SQSMessage {
	return sqsstore.SQSMessage{
		MessageID: body + "-id",
		QueueURL:  testQueue,
		Body:      body,
		SentAt:    clock.RealNow(),
	}
}

func TestMemoryMessageStore_SendReceiveDelete(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	if _, _, err := s.Send(ctx, "000000000000", "us-east-1", newMsg("hello")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	msgs, err := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Body != "hello" {
		t.Fatalf("wrong body: %s", msgs[0].Body)
	}

	// Message is now in-flight — should not be returned again
	msgs2, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs2) != 0 {
		t.Fatalf("expected 0 (in-flight), got %d", len(msgs2))
	}

	// Delete
	if err := s.Delete(ctx, "000000000000", "us-east-1", testQueue, msgs[0].ReceiptHandle); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Queue should be empty
	msgs3, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now.Add(time.Minute))
	if len(msgs3) != 0 {
		t.Fatalf("expected empty after delete, got %d", len(msgs3))
	}
}

func TestMemoryMessageStore_VisibilityTimeout(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	s.Send(ctx, "000000000000", "us-east-1", newMsg("vis-test"))
	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs) != 1 {
		t.Fatal("no messages received")
	}

	// Still in-flight 5s later
	msgs2, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now.Add(5*time.Second))
	if len(msgs2) != 0 {
		t.Fatal("message should still be in-flight")
	}

	// Visible again after timeout (30s default set by Receive)
	msgs3, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now.Add(31*time.Second))
	if len(msgs3) != 1 {
		t.Fatalf("expected re-delivery after timeout, got %d", len(msgs3))
	}
	if msgs3[0].ReceiveCount != 2 {
		t.Fatalf("expected ReceiveCount=2, got %d", msgs3[0].ReceiveCount)
	}
}

func TestMemoryMessageStore_ChangeVisibility(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	s.Send(ctx, "000000000000", "us-east-1", newMsg("change-vis"))
	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs) != 1 {
		t.Fatal("no messages")
	}

	// Extend visibility to 60s
	if err := s.ChangeVisibility(ctx, "000000000000", "us-east-1", testQueue, msgs[0].ReceiptHandle, 60, now); err != nil {
		t.Fatalf("ChangeVisibility: %v", err)
	}

	// Still invisible at 45s
	msgs2, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now.Add(45*time.Second))
	if len(msgs2) != 0 {
		t.Fatal("should still be invisible")
	}

	// Visible at 61s
	msgs3, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now.Add(61*time.Second))
	if len(msgs3) != 1 {
		t.Fatal("should be visible again")
	}
}

func TestMemoryMessageStore_ChangeVisibilityZero(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	s.Send(ctx, "000000000000", "us-east-1", newMsg("zero-vis"))
	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs) != 1 {
		t.Fatal("no messages")
	}

	// Set visibility to 0 — immediately visible
	s.ChangeVisibility(ctx, "000000000000", "us-east-1", testQueue, msgs[0].ReceiptHandle, 0, now)
	msgs2, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs2) != 1 {
		t.Fatal("message should be immediately visible after timeout=0")
	}
}

func TestMemoryMessageStore_Purge(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	s.Send(ctx, "000000000000", "us-east-1", newMsg("m1"))
	s.Send(ctx, "000000000000", "us-east-1", newMsg("m2"))
	s.Purge(ctx, "000000000000", "us-east-1", testQueue)

	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 10, now)
	if len(msgs) != 0 {
		t.Fatalf("expected empty after purge, got %d", len(msgs))
	}
}

func TestMemoryMessageStore_FIFODeduplication(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	fifoQueue := "http://localhost:4566/000000000000/test.fifo"
	m := sqsstore.SQSMessage{
		MessageID:       "dedup-id",
		QueueURL:        fifoQueue,
		Body:            "dedup-body",
		GroupID:         "group1",
		DeduplicationID: "dedup-123",
		SentAt:          now,
	}

	s.Send(ctx, "000000000000", "us-east-1", m)
	s.Send(ctx, "000000000000", "us-east-1", m) // duplicate — should be dropped

	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", fifoQueue, 10, now)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 (dedup), got %d", len(msgs))
	}
}

func TestMemoryMessageStore_DelayedMessage(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	m := sqsstore.SQSMessage{
		MessageID:  "delayed",
		QueueURL:   testQueue,
		Body:       "delayed-body",
		SentAt:     now,
		DelayUntil: now.Add(10 * time.Second),
	}
	s.Send(ctx, "000000000000", "us-east-1", m)

	// Not visible yet
	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs) != 0 {
		t.Fatalf("message should be delayed, got %d", len(msgs))
	}

	// Visible after delay
	msgs2, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now.Add(11*time.Second))
	if len(msgs2) != 1 {
		t.Fatalf("expected visible after delay, got %d", len(msgs2))
	}
}

func TestMemoryMessageStore_ApproximateCounts(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	s.Send(ctx, "000000000000", "us-east-1", newMsg("m1"))
	s.Send(ctx, "000000000000", "us-east-1", newMsg("m2"))
	s.Send(ctx, "000000000000", "us-east-1", sqsstore.SQSMessage{
		MessageID:  "m3",
		QueueURL:   testQueue,
		Body:       "m3",
		SentAt:     now,
		DelayUntil: now.Add(60 * time.Second),
	})

	// Receive one (it becomes in-flight)
	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 1, now)
	if len(msgs) != 1 {
		t.Fatal("expected 1 received")
	}

	visible, notVisible, delayed, err := s.GetApproximateCounts(ctx, "000000000000", "us-east-1", testQueue, now)
	if err != nil {
		t.Fatalf("GetApproximateCounts: %v", err)
	}
	if visible != 1 {
		t.Fatalf("expected visible=1, got %d", visible)
	}
	if notVisible != 1 {
		t.Fatalf("expected notVisible=1, got %d", notVisible)
	}
	if delayed != 1 {
		t.Fatalf("expected delayed=1, got %d", delayed)
	}
}

func TestMemoryMessageStore_Reset(t *testing.T) {
	ctx := context.Background()
	s := sqsstore.NewMemoryMessageStore()
	now := clock.RealNow()

	s.Send(ctx, "000000000000", "us-east-1", newMsg("x"))
	s.Reset(context.Background())

	msgs, _ := s.Receive(ctx, "000000000000", "us-east-1", testQueue, 10, now)
	if len(msgs) != 0 {
		t.Fatalf("expected empty after reset, got %d", len(msgs))
	}
}
