package pubsub

import (
	"strings"
	"testing"
	"time"
)

func TestAckIDRoundTrip(t *testing.T) {
	id := EncodeAckID("my-sub", "42", 3)
	if id == "my-sub/42/3" {
		t.Fatalf("ack id %q is not opaque", id)
	}
	sub, msg, attempt, ok := DecodeAckID(id)
	if !ok || sub != "my-sub" || msg != "42" || attempt != 3 {
		t.Fatalf("DecodeAckID(%q) = (%q,%q,%d,%v), want (my-sub,42,3,true)", id, sub, msg, attempt, ok)
	}
}

func TestDecodeAckIDRejectsMalformed(t *testing.T) {
	for _, s := range []string{"", "not-base64!!", "!!!!", "!!!", "aGVsbG8", "aGVsbG8v", "aGVsbG8vLw", "c3ViL21zZy9ub3RhbnVt"} {
		if _, _, _, ok := DecodeAckID(s); ok {
			t.Errorf("DecodeAckID(%q) ok = true, want false", s)
		}
	}
}

func TestAckIDCurrent(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inFlight := Message{MessageID: "1", DeliveryAttempt: 2, VisibleAt: now.Add(time.Minute)}
	lapsed := Message{MessageID: "1", DeliveryAttempt: 1, VisibleAt: now.Add(-time.Second)}
	released := Message{MessageID: "1", DeliveryAttempt: 1} // VisibleAt zero: nacked/never claimed

	cases := []struct {
		name        string
		m           Message
		version     int
		wantCurrent bool
	}{
		{"latest in-flight", inFlight, 2, true},
		{"superseded version", inFlight, 1, false},
		{"expired deadline", lapsed, 1, false},
		{"released by nack", released, 1, true},
	}
	for _, tc := range cases {
		if got := AckIDCurrent(tc.m, tc.version, now); got != tc.wantCurrent {
			t.Errorf("%s: AckIDCurrent = %v, want %v", tc.name, got, tc.wantCurrent)
		}
	}
}

func TestEODAckFailureMetadata(t *testing.T) {
	md := EODAckFailureMetadata([]string{"a", "b"})
	if len(md) != 2 || md["a"] != InvalidAckIDValue || md["b"] != InvalidAckIDValue {
		t.Fatalf("metadata = %v", md)
	}
	if !strings.HasPrefix(EODAckFailureMessage, "Some acknowledgement ids") {
		t.Fatalf("unexpected EOD message: %q", EODAckFailureMessage)
	}
}
