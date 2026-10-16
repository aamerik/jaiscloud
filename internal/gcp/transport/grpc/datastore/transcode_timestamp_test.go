package datastore

import (
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestTimestampProtoCanonicalUTC pins that the gRPC transport writes a timestamp
// value in the shared canonical form (RFC 3339 UTC, the same form the REST
// boundary normalizes to) and reads it back as the same instant. (TD41)
func TestTimestampProtoCanonicalUTC(t *testing.T) {
	// 2019-12-31T19:00:00.5-05:00 == 2020-01-01T00:00:00.5Z, spelled in a
	// non-UTC location to prove the transcode canonicalizes to UTC.
	loc := time.FixedZone("EST", -5*3600)
	instant := time.Date(2019, 12, 31, 19, 0, 0, 500000000, loc)

	neutral, err := valueFromProto(&datastorepb.Value{
		ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(instant)},
	})
	if err != nil {
		t.Fatalf("valueFromProto: %v", err)
	}
	if neutral.TimestampValue == nil || *neutral.TimestampValue != "2020-01-01T00:00:00.5Z" {
		t.Fatalf("TimestampValue = %v, want 2020-01-01T00:00:00.5Z", neutral.TimestampValue)
	}

	back := valueToProto(neutral, "proj").GetTimestampValue()
	if got := back.AsTime(); !got.Equal(instant) {
		t.Fatalf("round-trip instant = %v, want %v", got, instant)
	}
}
