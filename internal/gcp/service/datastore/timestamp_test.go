package datastore

import (
	"testing"
)

// TestNormalizeTimestamp pins the canonical form: an RFC 3339 instant with any
// UTC offset is rendered in UTC (a trailing Z), and the fractional-second
// digits are preserved. (TD41)
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
		{"fraction with offset to utc", "2019-12-31T19:00:00.5-05:00", "2020-01-01T00:00:00.5Z"},
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
