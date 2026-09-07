# GCP service tests

This nested module holds SDK test suites that live outside the main `sdk*`
modules. Tests here are run in CI via the `test-gcp-integration` job.

**Active (untagged, run in CI):** Cloud Datastore (`datastore_test.go`),
Cloud Storage gRPC v2 (`gcs_grpc_test.go`), Cloud Logging (`logging_test.go`),
and Managed Kafka (`kafka_test.go`).

There are no pending services left: every test in this module is now active,
so the `gcp_pending` build tag is no longer used by any file here.

## Running

Against a running `jaiscloud-gcp` (REST `http://localhost:8080`, gRPC
`localhost:8081`):

```bash
STORAGE_EMULATOR_HOST_GRPC=localhost:8081 \
DATASTORE_EMULATOR_HOST=localhost:8081 \
LOGGING_EMULATOR_HOST=localhost:8081 \
GCP_EMULATOR_PROJECT=test-project \
go test -count=1 ./...
```

Client factories live in `internal/testutil/fixtures.go`. Env-var contract:

- `STORAGE_EMULATOR_HOST_GRPC` — Storage v2 gRPC endpoint (default `localhost:8081`)
- `DATASTORE_EMULATOR_HOST` — Datastore gRPC endpoint (native SDK var)
- `LOGGING_EMULATOR_HOST` — Logging v2 gRPC endpoint (default `localhost:8081`)
- `GCP_EMULATOR_PROJECT` — project id (default `test-project`)
- `GCP_EMULATOR_ENDPOINT` — REST endpoint (default `http://localhost:8080`)
