package dynamodb

import (
	"context"
	"strings"
	"testing"
)

// TestDDBProvisionedThroughputExceeded verifies that a table created with WCU=1
// eventually starts rejecting writes when the token bucket is exhausted.
func TestDDBProvisionedThroughputExceeded(t *testing.T) {
	store := NewMemoryDynamoDBItemStore()
	ctx := context.Background()

	// Create table with very low WCU (1 WCU = 1 token initial capacity).
	schema := TableSchema{
		TableName:   "ThrottleTest",
		PKAttr:      "id",
		PKType:      "S",
		BillingMode: "PROVISIONED",
		WCU:         1,
	}
	if err := store.CreateTableSchema(ctx, schema); err != nil {
		t.Fatalf("CreateTableSchema: %v", err)
	}

	// Attempt 50 PutItem calls rapidly — with WCU=1 and refill rate of 1/s,
	// all tokens are consumed almost immediately.
	throttledCount := 0
	for i := 0; i < 50; i++ {
		item := map[string]any{
			"id": map[string]any{"S": "key"},
		}
		_, err := store.PutItem(ctx, "ThrottleTest", "id=key", item, ConditionSpec{})
		if err != nil {
			if strings.Contains(err.Error(), "ProvisionedThroughput") {
				throttledCount++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}
	}

	if throttledCount == 0 {
		t.Error("expected some PutItem calls to be throttled with WCU=1, but none were")
	}
	t.Logf("throttled %d out of 50 PutItem calls", throttledCount)
}

// TestTokenBucket_BasicConsumption verifies the token bucket mechanics directly.
func TestTokenBucket_BasicConsumption(t *testing.T) {
	// capacity=2 means only 2 tokens start available.
	b := newTokenBucket(2, 0.001) // very slow refill
	if !b.TryConsume(1) {
		t.Error("expected first consume to succeed")
	}
	if !b.TryConsume(1) {
		t.Error("expected second consume to succeed")
	}
	// Third consume should fail (bucket empty, refill rate too slow to matter).
	if b.TryConsume(1) {
		t.Error("expected third consume to fail (bucket empty)")
	}
}

// TestTokenBucket_PayPerRequest verifies PAY_PER_REQUEST tables have effectively unlimited capacity.
func TestTokenBucket_PayPerRequest(t *testing.T) {
	store := NewMemoryDynamoDBItemStore()
	ctx := context.Background()

	schema := TableSchema{
		TableName:   "PPRTable",
		PKAttr:      "id",
		PKType:      "S",
		BillingMode: "PAY_PER_REQUEST",
	}
	if err := store.CreateTableSchema(ctx, schema); err != nil {
		t.Fatalf("CreateTableSchema: %v", err)
	}

	// All writes should succeed — PAY_PER_REQUEST has 40000 capacity.
	for i := 0; i < 100; i++ {
		item := map[string]any{"id": map[string]any{"S": "k"}}
		if _, err := store.PutItem(ctx, "PPRTable", "id=k", item, ConditionSpec{}); err != nil {
			t.Fatalf("PutItem %d failed unexpectedly: %v", i, err)
		}
	}
}

// TestDropTableSchema_CleansThrottle verifies that dropping a table removes its throttle bucket.
func TestDropTableSchema_CleansThrottle(t *testing.T) {
	store := NewMemoryDynamoDBItemStore()
	ctx := context.Background()

	schema := TableSchema{
		TableName:   "DropTest",
		PKAttr:      "id",
		PKType:      "S",
		BillingMode: "PROVISIONED",
		WCU:         5,
	}
	if err := store.CreateTableSchema(ctx, schema); err != nil {
		t.Fatalf("CreateTableSchema: %v", err)
	}
	if store.throttles["DropTest"] == nil {
		t.Error("expected throttle bucket after CreateTableSchema")
	}
	if err := store.DropTableSchema(ctx, "DropTest"); err != nil {
		t.Fatalf("DropTableSchema: %v", err)
	}
	if store.throttles["DropTest"] != nil {
		t.Error("expected throttle bucket to be removed after DropTableSchema")
	}
}
