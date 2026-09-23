package gcp

import (
	"sort"
	"testing"
)

// TestKnownServiceNamesIncludesGRPConly guards the transport-selection set: the
// gRPC-only services must be present, otherwise a default "grpc" selection
// would silently disable their gRPC surface.
func TestKnownServiceNamesIncludesGRPConly(t *testing.T) {
	names := KnownServiceNames()
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	for _, want := range []string{"storage", "datastore", "logging", "monitoring"} {
		if !set[want] {
			t.Errorf("KnownServiceNames() missing %q", want)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("KnownServiceNames() not sorted: %v", names)
	}
}
