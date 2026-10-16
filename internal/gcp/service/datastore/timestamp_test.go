package datastore

import (
	"testing"
)

// TestNormalizeTimestamp pins the canonical form: an RFC 3339 instant with any
// UTC offset is rendered in UTC (a trailing Z), the fractional-second digits are
// Datastore's microsecond precision, and the fraction is spelled the way real
// GCP's protojson transcoder spells a proto Timestamp (0, 3, or 6 digits — a
// group of three is never trimmed below its boundary the way RFC3339Nano trims).
// (TD41, TD42)
func TestNormalizeTimestamp(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"utc unchanged", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"},
		{"negative offset to utc", "2019-12-31T19:00:00-05:00", "2020-01-01T00:00:00Z"},
		{"positive offset to utc", "2020-01-01T05:30:00+05:30", "2020-01-01T00:00:00Z"},
		{"fraction preserved", "2020-01-01T00:00:00.123456Z", "2020-01-01T00:00:00.123456Z"},
		{"fraction with offset to utc", "2019-12-31T19:00:00.5-05:00", "2020-01-01T00:00:00.500Z"},
		// protojson spells a fraction with 0, 3, 6 (here) digits: a value needing
		// one or two digits is padded to the next group of three, so it is not
		// RFC3339Nano's ".5"/".12". (TD42)
		{"one-digit fraction padded to three", "2020-01-01T00:00:00.5Z", "2020-01-01T00:00:00.500Z"},
		{"two-digit fraction padded to three", "2020-01-01T00:00:00.12Z", "2020-01-01T00:00:00.120Z"},
		{"milliseconds", "2020-01-01T00:00:00.123Z", "2020-01-01T00:00:00.123Z"},
		// Sub-microsecond precision is rounded down on write. (TD42)
		{"nanoseconds truncated to microseconds", "2020-01-01T00:00:00.123456789Z", "2020-01-01T00:00:00.123456Z"},
		{"nanoseconds below a microsecond dropped", "2020-01-01T00:00:00.123000456Z", "2020-01-01T00:00:00.123Z"},
		// Truncation happens before the fraction is grouped, not after.
		{"sub-microsecond does not round up", "2020-01-01T00:00:00.9999999Z", "2020-01-01T00:00:00.999999Z"},
		// "Rounded down" is floor at either side of the epoch and at the
		// Datastore year boundary (0001..9999).
		{"pre-epoch truncates down", "1969-12-31T23:59:59.9999995Z", "1969-12-31T23:59:59.999999Z"},
		{"upper year boundary", "9999-12-31T23:59:59.9999999Z", "9999-12-31T23:59:59.999999Z"},
	}
	for _, tc := range cases {
		got, err := NormalizeTimestamp(tc.in)
		if err != nil {
			t.Fatalf("%s: NormalizeTimestamp(%q): %v", tc.name, tc.in, err)
		}
		if got != tc.want {
			t.Errorf("%s: NormalizeTimestamp(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}

// TestNormalizeTimestampRejectsMalformed asserts a value neither transport can
// render as a proto Timestamp is an error, not a value silently held by the
// store (real GCP answers INVALID_ARGUMENT). (TD41)
func TestNormalizeTimestampRejectsMalformed(t *testing.T) {
	for _, s := range []string{
		"",
		"not-a-time",
		"2020-01-01",
		"2020-01-01T00:00:00",
		"12345",
		// Go's parser accepts a comma fractional separator; RFC 3339/protojson
		// do not, so the boundary must reject it.
		"2020-01-01T00:00:00,5Z",
	} {
		if got, err := NormalizeTimestamp(s); err == nil {
			t.Errorf("NormalizeTimestamp(%q) = %q, nil error; want a parse error", s, got)
		}
	}
}
