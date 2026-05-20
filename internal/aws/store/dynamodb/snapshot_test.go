package dynamodb_test

import (
	"bytes"
	"context"
	"testing"

	dynamostore "jaiscloud/internal/aws/store/dynamodb"
)

func roundTripMemoryDynamo(t *testing.T, s *dynamostore.MemoryDynamoDBItemStore) *dynamostore.MemoryDynamoDBItemStore {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s2 := dynamostore.NewMemoryDynamoDBItemStore()
	if err := s2.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return s2
}

// ─── IsEmpty ──────────────────────────────────────────────────────────────────

func TestMemoryDynamoDBItemStore_IsEmpty_NewStore(t *testing.T) {
	s := dynamostore.NewMemoryDynamoDBItemStore()
	empty, err := s.IsEmpty(context.Background())
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("new store must be empty")
	}
}

func TestMemoryDynamoDBItemStore_IsEmpty_AfterCreateSchema(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	schema := dynamostore.TableSchema{
		TableName: "test-table", PKAttr: "id", PKType: "S",
		BillingMode: "PAY_PER_REQUEST",
	}
	if err := s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema); err != nil {
		t.Fatalf("CreateTableSchema: %v", err)
	}

	empty, err := s.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if empty {
		t.Fatal("store with a schema must not be empty")
	}
}

// ─── MemoryDynamoDBItemStore snapshot round-trips ────────────────────────────

func TestMemoryDynamoDBItemStore_Snapshot_Empty(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()
	s2 := roundTripMemoryDynamo(t, s)

	empty, err := s2.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("restored empty store must still be empty")
	}
}

func TestMemoryDynamoDBItemStore_Snapshot_SchemaSurvives(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	schema := dynamostore.TableSchema{
		TableName: "orders",
		PKAttr:    "order_id", PKType: "S",
		SKAttr: "ts", SKType: "N",
		BillingMode: "PROVISIONED", WCU: 5, RCU: 5,
	}
	if err := s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema); err != nil {
		t.Fatalf("CreateTableSchema: %v", err)
	}

	s2 := roundTripMemoryDynamo(t, s)

	// After restore, the schema must be queryable (putting an item must work).
	_, err := s2.PutItem(ctx, "000000000000", "us-east-1", "orders", "pk#o1",
		map[string]any{"order_id": map[string]any{"S": "o1"}, "ts": map[string]any{"N": "100"}},
		dynamostore.ConditionSpec{Schema: &schema},
	)
	if err != nil {
		t.Fatalf("PutItem into restored store: %v", err)
	}
}

func TestMemoryDynamoDBItemStore_Snapshot_ItemsSurvive(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	schema := dynamostore.TableSchema{
		TableName: "items", PKAttr: "id", PKType: "S",
		BillingMode: "PAY_PER_REQUEST",
	}
	s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema)

	for i, body := range []string{"alpha", "beta", "gamma"} {
		item := map[string]any{
			"id":   map[string]any{"S": body},
			"val":  map[string]any{"S": body + "-val"},
			"rank": map[string]any{"N": string(rune('1' + i))},
		}
		if _, err := s.PutItem(ctx, "000000000000", "us-east-1", "items", "pk#"+body, item,
			dynamostore.ConditionSpec{Schema: &schema}); err != nil {
			t.Fatalf("PutItem %s: %v", body, err)
		}
	}

	s2 := roundTripMemoryDynamo(t, s)

	// Scan must return all 3 items.
	results, _, _, err := s2.Scan(ctx, "000000000000", "us-east-1", "items", dynamostore.ScanSpec{})
	if err != nil {
		t.Fatalf("Scan after restore: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 items after restore, got %d", len(results))
	}

	// GetItem for a specific key must work.
	got, err := s2.GetItem(ctx, "000000000000", "us-east-1", "items", "pk#alpha")
	if err != nil {
		t.Fatalf("GetItem after restore: %v", err)
	}
	if got == nil {
		t.Fatal("expected item 'alpha' to be present after restore")
	}
	valAttr, ok := got["val"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected type for 'val': %T", got["val"])
	}
	if valAttr["S"] != "alpha-val" {
		t.Fatalf("wrong val: %v", valAttr)
	}
}

func TestMemoryDynamoDBItemStore_Snapshot_GSIIndexSurvives(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	schema := dynamostore.TableSchema{
		TableName: "products",
		PKAttr:    "product_id", PKType: "S",
		GSIs: []dynamostore.IndexDef{{
			IndexName:  "category-index",
			PKAttr:     "category",
			PKType:     "S",
			Projection: dynamostore.ProjectionDef{Type: "ALL"},
		}},
		BillingMode: "PAY_PER_REQUEST",
	}
	s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema)

	items := []struct {
		id, category, name string
	}{
		{"p1", "electronics", "Laptop"},
		{"p2", "electronics", "Phone"},
		{"p3", "clothing", "Shirt"},
	}
	for _, it := range items {
		_, err := s.PutItem(ctx, "000000000000", "us-east-1", "products", "pk#"+it.id,
			map[string]any{
				"product_id": map[string]any{"S": it.id},
				"category":   map[string]any{"S": it.category},
				"name":       map[string]any{"S": it.name},
			},
			dynamostore.ConditionSpec{Schema: &schema},
		)
		if err != nil {
			t.Fatalf("PutItem %s: %v", it.id, err)
		}
	}

	s2 := roundTripMemoryDynamo(t, s)

	// Query via GSI — must return only the 2 electronics items.
	idxRef := &dynamostore.IndexKeyRef{
		IndexName: "category-index",
		PKAttr:    "category",
		PKType:    "S",
	}
	results, _, _, err := s2.Query(ctx, "000000000000", "us-east-1", "products",
		dynamostore.QuerySpec{
			IndexSchema:               idxRef,
			KeyConditionExpression:    "category = :cat",
			ExpressionAttributeValues: map[string]any{":cat": map[string]any{"S": "electronics"}},
			ScanIndexForward:          true,
		},
	)
	if err != nil {
		t.Fatalf("GSI Query after restore: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 electronics items via GSI after restore, got %d", len(results))
	}

	// Also verify clothing returns exactly 1.
	clothing, _, _, err := s2.Query(ctx, "000000000000", "us-east-1", "products",
		dynamostore.QuerySpec{
			IndexSchema:               idxRef,
			KeyConditionExpression:    "category = :cat",
			ExpressionAttributeValues: map[string]any{":cat": map[string]any{"S": "clothing"}},
			ScanIndexForward:          true,
		},
	)
	if err != nil {
		t.Fatalf("GSI Query clothing after restore: %v", err)
	}
	if len(clothing) != 1 {
		t.Fatalf("expected 1 clothing item via GSI after restore, got %d", len(clothing))
	}
}

func TestMemoryDynamoDBItemStore_Snapshot_LSIQueryAfterRestore(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	schema := dynamostore.TableSchema{
		TableName: "scores",
		PKAttr:    "game", PKType: "S",
		SKAttr: "player", SKType: "S",
		LSIs: []dynamostore.IndexDef{{
			IndexName:  "score-index",
			PKAttr:     "game",
			PKType:     "S",
			SKAttr:     "score",
			SKType:     "N",
			IsLSI:      true,
			Projection: dynamostore.ProjectionDef{Type: "ALL"},
		}},
		BillingMode: "PAY_PER_REQUEST",
	}
	s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema)

	rows := []struct {
		game, player, score string
	}{
		{"chess", "alice", "95"},
		{"chess", "bob", "72"},
		{"chess", "carol", "88"},
	}
	for _, r := range rows {
		_, err := s.PutItem(ctx, "000000000000", "us-east-1", "scores", "pk#"+r.game+"#"+r.player,
			map[string]any{
				"game":   map[string]any{"S": r.game},
				"player": map[string]any{"S": r.player},
				"score":  map[string]any{"N": r.score},
			},
			dynamostore.ConditionSpec{Schema: &schema},
		)
		if err != nil {
			t.Fatalf("PutItem %s/%s: %v", r.game, r.player, err)
		}
	}

	s2 := roundTripMemoryDynamo(t, s)

	// LSI query: all chess items. LSI is rebuilt from items, not index structures.
	idxRef := &dynamostore.IndexKeyRef{
		IndexName: "score-index",
		PKAttr:    "game", PKType: "S",
		SKAttr: "score", SKType: "N",
		IsLSI: true,
	}
	results, _, _, err := s2.Query(ctx, "000000000000", "us-east-1", "scores",
		dynamostore.QuerySpec{
			IndexSchema:               idxRef,
			KeyConditionExpression:    "game = :g",
			ExpressionAttributeValues: map[string]any{":g": map[string]any{"S": "chess"}},
			ScanIndexForward:          true,
		},
	)
	if err != nil {
		t.Fatalf("LSI Query after restore: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 chess items via LSI after restore, got %d", len(results))
	}
}

func TestMemoryDynamoDBItemStore_Snapshot_ThrottleRebuildFromSchema(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	// PAY_PER_REQUEST tables get a token bucket on restore.
	schema := dynamostore.TableSchema{
		TableName: "throttled", PKAttr: "id", PKType: "S",
		BillingMode: "PAY_PER_REQUEST",
	}
	s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema)

	_, err := s.PutItem(ctx, "000000000000", "us-east-1", "throttled", "pk#1",
		map[string]any{"id": map[string]any{"S": "1"}},
		dynamostore.ConditionSpec{Schema: &schema},
	)
	if err != nil {
		t.Fatalf("PutItem before snapshot: %v", err)
	}

	s2 := roundTripMemoryDynamo(t, s)

	// Write to the restored store — throttle must not block normal writes.
	_, err = s2.PutItem(ctx, "000000000000", "us-east-1", "throttled", "pk#2",
		map[string]any{"id": map[string]any{"S": "2"}},
		dynamostore.ConditionSpec{Schema: &schema},
	)
	if err != nil {
		t.Fatalf("PutItem after restore (throttle test): %v", err)
	}
}

func TestMemoryDynamoDBItemStore_Snapshot_MultipleTablesSurvive(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewMemoryDynamoDBItemStore()

	for _, tbl := range []string{"table-a", "table-b", "table-c"} {
		sc := dynamostore.TableSchema{TableName: tbl, PKAttr: "id", PKType: "S", BillingMode: "PAY_PER_REQUEST"}
		s.CreateTableSchema(ctx, "000000000000", "us-east-1", sc)
		s.PutItem(ctx, "000000000000", "us-east-1", tbl, "pk#1",
			map[string]any{"id": map[string]any{"S": "1"}, "tbl": map[string]any{"S": tbl}},
			dynamostore.ConditionSpec{Schema: &sc},
		)
	}

	s2 := roundTripMemoryDynamo(t, s)

	for _, tbl := range []string{"table-a", "table-b", "table-c"} {
		results, _, _, err := s2.Scan(ctx, "000000000000", "us-east-1", tbl, dynamostore.ScanSpec{})
		if err != nil {
			t.Fatalf("Scan %s: %v", tbl, err)
		}
		if len(results) != 1 {
			t.Fatalf("%s: expected 1 item, got %d", tbl, len(results))
		}
	}
}

// ─── BundledDynamoDBItemStore ─────────────────────────────────────────────────

func roundTripBundledDynamo(t *testing.T, s *dynamostore.BundledDynamoDBItemStore) *dynamostore.BundledDynamoDBItemStore {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("BundledDynamoDBItemStore Snapshot: %v", err)
	}
	s2 := dynamostore.NewBundledDynamoDBItemStore()
	if err := s2.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("BundledDynamoDBItemStore Restore: %v", err)
	}
	return s2
}

func TestBundledDynamoDBItemStore_Snapshot_Empty(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewBundledDynamoDBItemStore()
	s2 := roundTripBundledDynamo(t, s)
	empty, err := s2.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("expected empty bundled store after restore")
	}
}

func TestBundledDynamoDBItemStore_Snapshot_MultipleScopes(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewBundledDynamoDBItemStore()

	schemaA := dynamostore.TableSchema{TableName: "tbl", PKAttr: "id", PKType: "S", BillingMode: "PAY_PER_REQUEST"}
	schemaB := schemaA

	s.CreateTableSchema(ctx, "000000000001", "us-east-1", schemaA)
	s.PutItem(ctx, "000000000001", "us-east-1", "tbl", "pk#account1",
		map[string]any{"id": map[string]any{"S": "account1"}},
		dynamostore.ConditionSpec{Schema: &schemaA},
	)

	s.CreateTableSchema(ctx, "000000000002", "eu-west-1", schemaB)
	s.PutItem(ctx, "000000000002", "eu-west-1", "tbl", "pk#account2",
		map[string]any{"id": map[string]any{"S": "account2"}},
		dynamostore.ConditionSpec{Schema: &schemaB},
	)

	s2 := roundTripBundledDynamo(t, s)

	got1, err := s2.GetItem(ctx, "000000000001", "us-east-1", "tbl", "pk#account1")
	if err != nil || got1 == nil {
		t.Fatalf("scope 1 item not restored: err=%v item=%v", err, got1)
	}

	got2, err := s2.GetItem(ctx, "000000000002", "eu-west-1", "tbl", "pk#account2")
	if err != nil || got2 == nil {
		t.Fatalf("scope 2 item not restored: err=%v item=%v", err, got2)
	}
}

func TestBundledDynamoDBItemStore_IsEmpty(t *testing.T) {
	ctx := context.Background()
	s := dynamostore.NewBundledDynamoDBItemStore()

	empty, err := s.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("new bundled store must be empty")
	}

	schema := dynamostore.TableSchema{TableName: "t", PKAttr: "id", PKType: "S", BillingMode: "PAY_PER_REQUEST"}
	s.CreateTableSchema(ctx, "000000000000", "us-east-1", schema)
	s.PutItem(ctx, "000000000000", "us-east-1", "t", "pk#1",
		map[string]any{"id": map[string]any{"S": "1"}},
		dynamostore.ConditionSpec{Schema: &schema},
	)

	empty2, err := s.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty after insert: %v", err)
	}
	if empty2 {
		t.Fatal("bundled store with an item must not be empty")
	}
}
