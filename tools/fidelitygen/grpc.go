//go:build gcp_conformance

package main

import (
	"sort"

	conf "jaiscloud/tests/gcpconformance"
)

// grpcDefaultReason is the reason attached to every gRPC cell that has no
// curated override. Per the plan (T4), gRPC starts at "limited": the emulator
// implements the RPCs, but there is no proto-driven conformance harness yet, so
// the cells are verified against proto descriptors only.
const grpcDefaultReason = "gRPC verified against proto descriptors only (pending proto-conformance)"

// grpcDefaultOverride is the classification injected when the overrides file
// says nothing about a gRPC cell. It is a downgrade (limited <= ga), so
// Classify accepts it without allow_upgrade.
func grpcDefaultOverride() *Override {
	return &Override{State: StateLimited, Reason: grpcDefaultReason}
}

// GRPCFacts builds one Facts per gRPC method from the enumerated service
// surface (conf.EnumerateGRPC) and the curated overrides (ov).
//
// Every method yields exactly one fact with Transport "grpc" and Implemented
// true. DiscoveryMethod is always empty: Discovery documents describe REST, not
// gRPC. PersistentBackend and Mutating reuse the REST derivation
// (persistentBackends and isMutating); a PascalCase RPC method name splits into
// the same CamelCase words, so "GetObject" is not mistaken for a "Set".
//
// Override resolution: if the overrides file has an entry for (service, method)
// it wins verbatim (including allow_upgrade); otherwise the default limited
// override is attached so the cell starts "limited" per the plan.
//
// nil ov is tolerated (no overrides) and still yields the default limited cells,
// which keeps the function testable without I/O.
func GRPCFacts(services []conf.GRPCService, ov *Overrides) []Facts {
	var facts []Facts
	for _, svc := range services {
		for _, method := range svc.Methods {
			override := grpcDefaultOverride()
			if ov != nil {
				if o := ov.For(svc.Service, method); o != nil {
					override = o
				}
			}
			facts = append(facts, Facts{
				Service:           svc.Service,
				Operation:         method,
				Transport:         "grpc",
				Implemented:       true,
				DiscoveryMethod:   "",
				PersistentBackend: persistentBackends[svc.Service],
				Mutating:          isMutating(method),
				Override:          override,
			})
		}
	}
	return facts
}

// GRPCOnlyServices returns the sorted set of services that appear in the gRPC
// facts but have no operation in the REST registry — i.e. they can only be
// reached over gRPC. T5 renders those services' REST transport as
// "unsupported". No fake REST operations are synthesized here; the caller
// generates the unsupported cells from this list.
func GRPCOnlyServices(ops []conf.Operation, grpcFacts []Facts) []string {
	rest := make(map[string]bool, len(ops))
	for _, op := range ops {
		if op.Service != "" {
			rest[op.Service] = true
		}
	}

	seen := map[string]bool{}
	var only []string
	for _, f := range grpcFacts {
		if f.Service == "" || rest[f.Service] || seen[f.Service] {
			continue
		}
		seen[f.Service] = true
		only = append(only, f.Service)
	}
	sort.Strings(only)
	return only
}
