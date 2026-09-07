# Pending GCP service tests

This nested module holds SDK test suites that live outside the main `sdk*`
modules. As services get implemented, their tests here are "flipped active"
(the `gcp_pending` build tag is removed) and run in CI via the
`test-gcp-integration` job.

**Currently active (untagged, run in CI):** Cloud Datastore
(`datastore_test.go`), Cloud Storage gRPC v2 (`gcs_grpc_test.go`), and Cloud
Logging (`logging_test.go`).

**Currently pending (tagged `//go:build gcp_pending`, excluded from normal
`go build`/`go test` and CI):** Managed Kafka (`kafka_test.go`).

## Running

Against a running `jaiscloud-gcp` (REST `http://localhost:8080`, gRPC
`localhost:8081`):

```bash
# Active tests (no build tag)
STORAGE_EMULATOR_HOST_GRPC=localhost:8081 \
DATASTORE_EMULATOR_HOST=localhost:8081 \
LOGGING_EMULATOR_HOST=localhost:8081 \
GCP_EMULATOR_PROJECT=test-project \
go test -count=1 ./...

# Include the still-pending (kafka) test to see its current failures
go test -tags gcp_pending -count=1 ./...
```

Client factories live in `internal/testutil/fixtures.go`. Env-var contract:

- `STORAGE_EMULATOR_HOST_GRPC` — Storage v2 gRPC endpoint (default `localhost:8081`)
- `DATASTORE_EMULATOR_HOST` — Datastore gRPC endpoint (native SDK var)
- `LOGGING_EMULATOR_HOST` — Logging v2 gRPC endpoint (default `localhost:8081`)
- `GCP_EMULATOR_PROJECT` — project id (default `test-project`)

When the pending service is implemented, drop its `gcp_pending` build tag (and
optionally move the file alongside the other `sdk*` modules).
