package logging

import (
	"testing"
	"time"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// TestEntryToWireTimestampSpelling pins the protojson spelling of the LogEntry
// timestamp/receiveTimestamp google-datetime fields.
func TestEntryToWireTimestampSpelling(t *testing.T) {
	out := entryToWire(loggingstore.LogEntry{
		Timestamp:        time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
		ReceiveTimestamp: time.Date(2024, 3, 1, 12, 0, 1, 123_456_789, time.UTC),
	})
	if got := out["timestamp"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("timestamp = %v, want 2024-03-01T12:00:00.500Z", got)
	}
	if got := out["receiveTimestamp"]; got != "2024-03-01T12:00:01.123456Z" {
		t.Fatalf("receiveTimestamp = %v, want 2024-03-01T12:00:01.123456Z", got)
	}
}
