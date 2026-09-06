# Pending GCP service tests

This nested module holds SDK test suites for the GCP services jaiscloud-gcp does
**not yet implement**: Cloud Logging, Datastore, Managed Kafka, and Cloud Storage
gRPC v2.

**These tests are expected to FAIL.** They are real, complete test bodies (real
SDK calls + assertions) written ahead of the implementation, serving as the
acceptance spec to build against — not placeholders. Each is gated behind
`//go:build gcp_pending`, so they are excluded from normal `go build`/`go test`
and have no CI impact.

To run them (and see the current failures):

```bash
go test -tags gcp_pending -count=1 ./...
```

against a running `jaiscloud-gcp` (REST `http://localhost:8080`, gRPC
`localhost:8081`; env vars `GCP_EMULATOR_ENDPOINT` / `GCP_EMULATOR_PROJECT`).

When a service is implemented, flip its test into the active suite (drop the
`gcp_pending` build tag and move it alongside the other `sdk*` modules).
