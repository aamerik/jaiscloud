package logging

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func testEntry(logName, text string, severity int, ts time.Time) LogEntry {
	return LogEntry{LogName: logName, Severity: severity, PayloadType: "text", TextPayload: text, Timestamp: ts}
}

func TestMemoryStoreCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	base := time.Now().UTC()

	if got, err := s.List(ctx, "p1"); err != nil || len(got) != 0 {
		t.Fatalf("initial list = %v, %v", got, err)
	}

	if err := s.Write(ctx, "p1", testEntry("projects/p1/logs/a", "a1", 200, base)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, "p1", testEntry("projects/p1/logs/b", "b1", 500, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, "p2", testEntry("projects/p2/logs/c", "c1", 400, base)); err != nil {
		t.Fatal(err)
	}

	got, err := s.List(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].TextPayload != "a1" || got[1].TextPayload != "b1" {
		t.Fatalf("list = %+v", got)
	}

	logs, err := s.ListLogs(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || logs[0] != "projects/p1/logs/a" || logs[1] != "projects/p1/logs/b" {
		t.Fatalf("logs = %v", logs)
	}

	if err := s.DeleteLog(ctx, "p1", "projects/p1/logs/a"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.List(ctx, "p1")
	if len(got) != 1 || got[0].TextPayload != "b1" {
		t.Fatalf("after delete = %+v", got)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Write(ctx, "p1", testEntry("projects/p1/logs/a", "a", 200, time.Now()))
	s.Reset(ctx)
	if got, _ := s.List(ctx, "p1"); len(got) != 0 {
		t.Fatalf("after reset = %+v", got)
	}
	if logs, _ := s.ListLogs(ctx, "p1"); len(logs) != 0 {
		t.Fatalf("logs after reset = %v", logs)
	}
}

func TestMemoryStoreSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Write(ctx, "p1", testEntry("projects/p1/logs/a", "a", 200, time.Now().UTC()))

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	got, _ := s2.List(ctx, "p1")
	if len(got) != 1 || got[0].TextPayload != "a" {
		t.Fatalf("restored = %+v", got)
	}
	// nextID must advance past the restored entry's ID.
	if err := s2.Write(ctx, "p1", testEntry("projects/p1/logs/b", "b", 300, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	got, _ = s2.List(ctx, "p1")
	if len(got) != 2 {
		t.Fatalf("after append = %+v", got)
	}
}
