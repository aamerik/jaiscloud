package datastore

import (
	"fmt"
	"strings"
	"time"
)

// ParseTimestamp parses the store's timestamp form: an RFC 3339 string. A
// Datastore timestamp is an instant (the wire proto Timestamp is seconds+nanos
// since the epoch), so the string's UTC offset and fractional-second spelling
// are presentation only; the parser accepts any RFC 3339 offset.
func ParseTimestamp(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

// CanonicalTimestamp renders an instant in the store's canonical timestamp
// form: RFC 3339 in UTC (a trailing Z). Real Datastore normalizes every
// timestamp to UTC on write, and both transports hold this form so a value
// written over one transport reads back byte-identical over the other.
func CanonicalTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
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
