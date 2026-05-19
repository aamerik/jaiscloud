package stream_test

import (
	"bytes"
	"context"
	"testing"

	streamstore "jaiscloud/internal/aws/store/stream"
)

func roundTripMemoryStream(t *testing.T, s *streamstore.MemoryStreamStore) *streamstore.MemoryStreamStore {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(context.Background(), &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s2 := streamstore.NewMemoryStreamStore()
	if err := s2.Restore(context.Background(), &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return s2
}

// ─── IsEmpty ──────────────────────────────────────────────────────────────────

func TestMemoryStreamStore_IsEmpty_NewStore(t *testing.T) {
	s := streamstore.NewMemoryStreamStore()
	empty, err := s.IsEmpty(context.Background())
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("new store must be empty")
	}
}

func TestMemoryStreamStore_IsEmpty_AfterEnable(t *testing.T) {
	s := streamstore.NewMemoryStreamStore()
	s.Enable("my-table", "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024")
	empty, err := s.IsEmpty(context.Background())
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if empty {
		t.Fatal("store with enabled stream must not be empty")
	}
}

// ─── Snapshot round-trips ─────────────────────────────────────────────────────

func TestMemoryStreamStore_Snapshot_Empty(t *testing.T) {
	ctx := context.Background()
	s := streamstore.NewMemoryStreamStore()
	s2 := roundTripMemoryStream(t, s)
	empty, err := s2.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("restored empty store must still be empty")
	}
}

func TestMemoryStreamStore_Snapshot_StreamARNSurvives(t *testing.T) {
	const tableName = "orders"
	const streamARN = "arn:aws:dynamodb:us-east-1:000000000000:table/orders/stream/2024-01-01T00:00:00.000"

	s := streamstore.NewMemoryStreamStore()
	s.Enable(tableName, streamARN)

	s2 := roundTripMemoryStream(t, s)

	if !s2.IsEnabled(tableName) {
		t.Fatal("stream must be enabled after restore")
	}
	if got := s2.GetStreamARN(tableName); got != streamARN {
		t.Fatalf("StreamARN not restored: got %q, want %q", got, streamARN)
	}
}

func TestMemoryStreamStore_Snapshot_RecordsSurvive(t *testing.T) {
	const tableName = "events"
	const streamARN = "arn:aws:dynamodb:us-east-1:000000000000:table/events/stream/2024"

	s := streamstore.NewMemoryStreamStore()
	s.Enable(tableName, streamARN)

	s.Append(tableName, streamstore.Record{
		EventID:   "e1",
		EventName: "INSERT",
		Keys:      map[string]any{"id": map[string]any{"S": "k1"}},
		NewImage:  map[string]any{"id": map[string]any{"S": "k1"}, "val": map[string]any{"S": "v1"}},
	})
	s.Append(tableName, streamstore.Record{
		EventID:   "e2",
		EventName: "MODIFY",
		Keys:      map[string]any{"id": map[string]any{"S": "k1"}},
		OldImage:  map[string]any{"id": map[string]any{"S": "k1"}, "val": map[string]any{"S": "v1"}},
		NewImage:  map[string]any{"id": map[string]any{"S": "k1"}, "val": map[string]any{"S": "v2"}},
	})

	s2 := roundTripMemoryStream(t, s)

	// Records after seq -1 means "all records".
	records, nextSeq := s2.GetRecords(tableName, -1)
	if len(records) != 2 {
		t.Fatalf("expected 2 records after restore, got %d", len(records))
	}
	if nextSeq < 2 {
		t.Fatalf("nextSeq should be >= 2, got %d", nextSeq)
	}

	// Verify event names survived.
	if records[0].EventName != "INSERT" {
		t.Fatalf("first record EventName: got %q", records[0].EventName)
	}
	if records[1].EventName != "MODIFY" {
		t.Fatalf("second record EventName: got %q", records[1].EventName)
	}
}

func TestMemoryStreamStore_Snapshot_SequenceNumbersSurvive(t *testing.T) {
	const tableName = "seq-table"
	s := streamstore.NewMemoryStreamStore()
	s.Enable(tableName, "arn:seq")

	s.Append(tableName, streamstore.Record{EventID: "r1", EventName: "INSERT"})
	s.Append(tableName, streamstore.Record{EventID: "r2", EventName: "INSERT"})
	s.Append(tableName, streamstore.Record{EventID: "r3", EventName: "INSERT"})

	// Cursor at seq 1 — get records with seq > 1.
	beforeRestore, _ := s.GetRecords(tableName, 1)
	if len(beforeRestore) != 1 {
		t.Fatalf("before restore: expected 1 record after seq 1, got %d", len(beforeRestore))
	}

	s2 := roundTripMemoryStream(t, s)

	// Same cursor must produce the same result after restore.
	afterRestore, _ := s2.GetRecords(tableName, 1)
	if len(afterRestore) != 1 {
		t.Fatalf("after restore: expected 1 record after seq 1, got %d", len(afterRestore))
	}
	if beforeRestore[0].SequenceNumber != afterRestore[0].SequenceNumber {
		t.Fatalf("sequence number changed: before=%d after=%d",
			beforeRestore[0].SequenceNumber, afterRestore[0].SequenceNumber)
	}
}

func TestMemoryStreamStore_Snapshot_MultipleTablesSurvive(t *testing.T) {
	s := streamstore.NewMemoryStreamStore()

	tables := []string{"table-a", "table-b", "table-c"}
	for _, tbl := range tables {
		arn := "arn:aws:dynamodb:us-east-1:000000000000:table/" + tbl + "/stream/2024"
		s.Enable(tbl, arn)
		s.Append(tbl, streamstore.Record{EventID: tbl + "-r1", EventName: "INSERT"})
	}

	s2 := roundTripMemoryStream(t, s)

	for _, tbl := range tables {
		if !s2.IsEnabled(tbl) {
			t.Fatalf("table %s stream must be enabled after restore", tbl)
		}
		records, _ := s2.GetRecords(tbl, -1)
		if len(records) != 1 {
			t.Fatalf("table %s: expected 1 record after restore, got %d", tbl, len(records))
		}
	}

	streams := s2.ListStreams()
	if len(streams) != 3 {
		t.Fatalf("expected 3 streams listed, got %d", len(streams))
	}
}

func TestMemoryStreamStore_Snapshot_DisabledStreamSurvives(t *testing.T) {
	const tableName = "disabled-table"
	s := streamstore.NewMemoryStreamStore()
	s.Enable(tableName, "arn:disabled")
	s.Append(tableName, streamstore.Record{EventID: "r1", EventName: "INSERT"})
	s.Disable(tableName)

	s2 := roundTripMemoryStream(t, s)

	if s2.IsEnabled(tableName) {
		t.Fatal("disabled stream must remain disabled after restore")
	}
	// Records must still exist even though stream is disabled.
	records, _ := s2.GetRecords(tableName, -1)
	if len(records) != 1 {
		t.Fatalf("expected 1 record in disabled stream after restore, got %d", len(records))
	}
}

func TestMemoryStreamStore_Snapshot_StreamInfoSurvives(t *testing.T) {
	const tableName = "info-table"
	const streamARN = "arn:aws:dynamodb:us-east-1:000000000000:table/info-table/stream/2024-06-01"

	s := streamstore.NewMemoryStreamStore()
	s.Enable(tableName, streamARN)

	s2 := roundTripMemoryStream(t, s)

	info, ok := s2.GetStreamInfo(tableName)
	if !ok {
		t.Fatal("stream info must be present after restore")
	}
	if info.StreamArn != streamARN {
		t.Fatalf("StreamArn not restored: got %q, want %q", info.StreamArn, streamARN)
	}
	if info.TableName != tableName {
		t.Fatalf("TableName not restored: got %q", info.TableName)
	}
	if !info.Enabled {
		t.Fatal("stream must be enabled in restored info")
	}
}

func TestMemoryStreamStore_Snapshot_AppendAfterRestoreIncrementsSeq(t *testing.T) {
	const tableName = "seq-inc"
	s := streamstore.NewMemoryStreamStore()
	s.Enable(tableName, "arn:seq-inc")
	s.Append(tableName, streamstore.Record{EventID: "r1", EventName: "INSERT"})

	s2 := roundTripMemoryStream(t, s)

	// Append a new record — its sequence number must be greater than the last restored one.
	s2.Append(tableName, streamstore.Record{EventID: "r2", EventName: "INSERT"})
	all, _ := s2.GetRecords(tableName, -1)
	if len(all) != 2 {
		t.Fatalf("expected 2 records, got %d", len(all))
	}
	if all[1].SequenceNumber <= all[0].SequenceNumber {
		t.Fatalf("new record seq %d must be > restored seq %d",
			all[1].SequenceNumber, all[0].SequenceNumber)
	}
}
