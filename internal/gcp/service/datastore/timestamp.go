package datastore

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/timestamp"
)

// microsecond is Datastore's timestamp resolution. Real Datastore stores a
// timestamp "precise only to microseconds; any additional precision is rounded
// down" (Datastore REST reference, Value.timestampValue):
// https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value
// so a value carrying sub-microsecond digits is held and echoed without them.
const microsecond = time.Microsecond

// truncateTimestamp returns t at Datastore's timestamp resolution (microsecond),
// so a response-level timestamp — read_time, commit_time — carries no
// sub-microsecond digit real GCP would not send. Real Datastore holds every
// timestamp at microseconds, exactly as CanonicalTimestamp renders a stored
// value and as the store already truncates an entity's create/update stamps, so
// both transports hand a client the same instant.
func truncateTimestamp(t time.Time) time.Time {
	return timestamp.Truncate(t)
}

// ParseTimestamp parses the store's timestamp form: an RFC 3339 string. A
// Datastore timestamp is an instant (the wire proto Timestamp is seconds+nanos
// since the epoch), so the string's UTC offset and fractional-second spelling
// are presentation only; the parser accepts any RFC 3339 offset.
func ParseTimestamp(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

// CanonicalTimestamp renders an instant in the store's canonical timestamp
// form: RFC 3339 in UTC, truncated to Datastore's microsecond resolution, and
// spelled the way real GCP's protojson transcoder spells a proto Timestamp.
//
// Real Datastore normalizes every timestamp to UTC on write and drops
// sub-microsecond precision; both transports hold this one form so a value
// written over one transport reads back byte-identical over the other, and a
// REST read echoes exactly what real GCP would (a value that differs only below
// the microsecond is the same stored value).
func CanonicalTimestamp(t time.Time) string {
	// The one protojson speller shared by every service that renders a
	// google-datetime field; Datastore's metadata timestamps use it verbatim
	// (RFC 3339 in UTC, 0/3/6-digit fraction, microsecond resolution).
	return timestamp.Canonical(t)
}

// NormalizeTimestamp parses an RFC 3339 timestamp and returns its canonical UTC
// spelling. An unparseable string is an error: real GCP rejects a malformed
// timestampValue with INVALID_ARGUMENT rather than storing it, and the store
// must never hold a value neither transport can render.
func NormalizeTimestamp(s string) (string, error) {
	// RFC 3339 requires a '.' as the fractional-second separator; Go's parser
	// also accepts a comma, which protojson (real GCP) rejects.
	if strings.ContainsRune(s, ',') {
		return "", fmt.Errorf("invalid timestamp %q", s)
	}
	t, err := ParseTimestamp(s)
	if err != nil {
		return "", err
	}
	return CanonicalTimestamp(t), nil
}

// timestampInstant parses a timestamp value to the instant Datastore compares
// at — truncated to the store's microsecond resolution. Real Datastore stores a
// timestamp truncated to microseconds, so two values that differ only below the
// microsecond are the same stored value and compare equal (and order-equal).
// Both the stored form and a query operand are canonicalized on write, so this
// is a backstop for a value that reached the store by another path.
func timestampInstant(s string) (time.Time, error) {
	t, err := ParseTimestamp(s)
	if err != nil {
		return time.Time{}, err
	}
	return t.Truncate(microsecond), nil
}
