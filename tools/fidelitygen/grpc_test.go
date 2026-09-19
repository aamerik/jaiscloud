//go:build gcp_conformance

package main

import (
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

// TestGRPCFactsEveryMethod checks the acceptance criterion: one fact per
// enumerated gRPC method, all on the grpc transport and implemented.
func TestGRPCFactsEveryMethod(t *testing.T) {
	services := conf.EnumerateGRPC()
	if len(services) == 0 {
		t.Fatal("EnumerateGRPC returned no services")
	}

	want := 0
	for _, s := range services {
		if s.WireService == "" || s.Service == "" {
			t.Errorf("service with empty wire/service name: %+v", s)
		}
		if len(s.Methods) == 0 {
			t.Errorf("%s (%s): no methods", s.WireService, s.Service)
		}
		want += len(s.Methods)
	}

	facts := GRPCFacts(services, &Overrides{})
	if len(facts) != want {
		t.Fatalf("got %d facts, want one per enumerated gRPC method (%d)", len(facts), want)
	}

	seen := map[string]bool{}
	for _, f := range facts {
		if f.Transport != "grpc" {
			t.Errorf("%s/%s: transport = %q, want grpc", f.Service, f.Operation, f.Transport)
		}
		if !f.Implemented {
			t.Errorf("%s/%s: Implemented = false, want true", f.Service, f.Operation)
		}
		if f.Service == "" || f.Operation == "" {
			t.Errorf("fact with empty service/operation: %+v", f)
		}
		if f.DiscoveryMethod != "" {
			t.Errorf("%s/%s: DiscoveryMethod = %q, want empty", f.Service, f.Operation, f.DiscoveryMethod)
		}
		key := f.Service + "/" + f.Operation
		if seen[key] {
			t.Errorf("%s: duplicate fact", key)
		}
		seen[key] = true
	}
	t.Logf("grpc facts: %d methods across %d registered services", len(facts), len(services))
	for _, s := range services {
		t.Logf("  %-50s -> %-14s %3d methods", s.WireService, s.Service, len(s.Methods))
	}
}

// TestGRPCFactDefaultsToLimited checks that a cell with no override classifies
// to limited through Classify, via the injected default override.
func TestGRPCFactDefaultsToLimited(t *testing.T) {
	services := []conf.GRPCService{{
		WireService: "google.storage.v2.Storage",
		Service:     "storage",
		Methods:     []string{"GetObject"},
	}}

	for _, ov := range []*Overrides{nil, {}} {
		facts := GRPCFacts(services, ov)
		if len(facts) != 1 {
			t.Fatalf("got %d facts, want 1", len(facts))
		}
		if facts[0].Override == nil {
			t.Fatalf("no default override attached")
		}
		cell := Classify(facts[0])
		if cell.State != StateLimited {
			t.Errorf("state = %q, want %q (reason %q)", cell.State, StateLimited, cell.Reason)
		}
		if cell.Reason != grpcDefaultReason {
			t.Errorf("reason = %q, want %q", cell.Reason, grpcDefaultReason)
		}
	}
}

// TestGRPCFactOverrideWins checks that a curated override replaces the default
// limited classification, in both directions (upgrade to ga with
// allow_upgrade, and downgrade to preview).
func TestGRPCFactOverrideWins(t *testing.T) {
	services := []conf.GRPCService{{
		WireService: "google.storage.v2.Storage",
		Service:     "storage",
		Methods:     []string{"GetObject"},
	}}

	upgrade := &Overrides{byOp: map[string]Override{
		"storage/GetObject": {State: StateGA, Reason: "verified against proto", AllowUpgrade: true},
	}}
	facts := GRPCFacts(services, upgrade)
	if got := facts[0].Override; got == nil || got.State != StateGA || !got.AllowUpgrade {
		t.Fatalf("Override = %+v, want ga with AllowUpgrade", got)
	}
	if cell := Classify(facts[0]); cell.State != StateGA {
		t.Errorf("state = %q, want ga", cell.State)
	}

	downgrade := &Overrides{byService: map[string]Override{
		"storage": {State: StatePreview, Reason: "not yet conformance-tested"},
	}}
	facts = GRPCFacts(services, downgrade)
	if cell := Classify(facts[0]); cell.State != StatePreview {
		t.Errorf("state = %q, want preview", cell.State)
	}
}

// TestGRPCOnlyServices checks the services reachable only over gRPC.
func TestGRPCOnlyServices(t *testing.T) {
	ops := conf.Enumerate()
	facts := GRPCFacts(conf.EnumerateGRPC(), &Overrides{})
	only := GRPCOnlyServices(ops, facts)

	got := map[string]bool{}
	for _, s := range only {
		got[s] = true
	}
	for _, want := range []string{"datastore", "logging", "monitoring"} {
		if !got[want] {
			t.Errorf("GRPCOnlyServices missing %q (got %v)", want, only)
		}
	}
	for i := 1; i < len(only); i++ {
		if only[i-1] >= only[i] {
			t.Errorf("GRPCOnlyServices not sorted/unique: %v", only)
			break
		}
	}
	t.Logf("gRPC-only services: %v", only)
}

// TestGRPCOnlyServicesDoesNotClaimRestServices guards against a service that
// has both transports being reported as gRPC-only.
func TestGRPCOnlyServicesDoesNotClaimRestServices(t *testing.T) {
	ops := conf.Enumerate()
	facts := GRPCFacts(conf.EnumerateGRPC(), &Overrides{})
	for _, s := range GRPCOnlyServices(ops, facts) {
		if s == "storage" || s == "pubsub" || s == "firestore" || s == "kms" || s == "secretmanager" || s == "iam" {
			t.Errorf("%q has REST operations but was reported gRPC-only", s)
		}
	}
}
