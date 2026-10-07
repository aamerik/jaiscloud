//go:build gcp_parity

// Package gcpparity is the REST↔gRPC cross-transport parity harness for the
// jaiscloud GCP emulator (audit workstream AUD3).
//
// Structural dual-protocol parity is already closed: every service that is dual
// in real GCP shares one transport-neutral core and exposes both a REST and a
// gRPC adapter. What no existing gate proved is that the *same logical
// operation* returns equivalent data over both transports. A field that the
// gRPC adapter drops while transcoding (or that the REST adapter invents) passes
// every existing gate, because each transport's conformance check only asserts a
// key field.
//
// This harness drives a canonical create → get → list → mutate → delete flow
// per dual service, reads the resource back over BOTH transports at each read
// step, normalizes the two responses to logical fields, and diffs them. The
// per-service scenarios live in cases_*.go; the runner, normalizer and diff
// rules live in case.go, normalize.go and differ.go.
//
// The suite runs against a live emulator (REST :8080, gRPC :8081), like the REST
// wire-conformance and gRPC message-conformance harnesses, and is wired into
// `make test-gcp-rest-grpc-parity` / `make ga-check` and CI. Run it with
// `-tags gcp_parity,gcp_differential` (it reuses the differential normalizer/diff
// engine) plus `gcp_conformance` for the coverage gate, which reads the
// emulator's registries; the Makefile passes all three.
//
// Coverage boundary: the cross-transport body diff currently covers the Get and
// List responses. Create/Update/Delete are exercised for state setup but their
// response bodies are not yet diffed (a mutation is driven over one transport
// and the read-back is compared); extending the diff to mutation responses is
// tracked as AUD3-12.
package gcpparity
