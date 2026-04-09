# JaisCloud — Developer Reference

## Project

JaisCloud is a local multi-cloud emulator written in Go. It speaks native AWS wire protocols (Query/XML, JSON/Target, REST) so any AWS SDK can point at it without modification.

**Phase 0 (complete):** SQS — all 32 integration tests pass.  
**Phase 1 (complete):** IAM/STS, SNS, DynamoDB, S3, Lambda; BlobFS; PostgreSQL stores; export/import; Prometheus metrics.

---

## Module

```
module jaiscloud   # go.mod
go 1.25
```

---

## Directory layout

```
cmd/jaiscloud/          # main.go — wires everything together, Cobra CLI
docs/                   # Architecture, LLD, phase plan documents
internal/
  adapter/              # AWS wire-protocol layer (no business logic)
    adapter.go          # Codec interface
    aws/
      aws.go            # AWSAdapter.DetectAndDecode
      router.go         # DetectService — X-Amz-Target / SigV4 / Action param
      services/
        sqs.go          # SQSCodec: JSON + Query/XML
        iam.go          # IAMCodec: Query/XML (handles STS too)
        sns.go          # SNSCodec: Query/XML
        dynamodb.go     # DynamoDBCodec: JSON/Target
        s3.go           # S3Codec: REST path-style, XML responses
        lambda.go       # LambdaCodec: REST JSON
  admin/                # /_jaiscloud/* endpoints
                        # Resetter, Snapshotter interfaces
  blobfs/               # BlobStore interface: MemoryBlobStore, LocalFSBlobStore
  clock/                # Clock interface: RealClock, FixedClock, OffsetClock
  config/               # Config struct; Viper loading; env prefix JAISCLOUD_
  events/               # In-process EventBus (subscribe/publish)
  gateway/              # HTTP server (Chi), middleware, request dispatch
    middleware/         # Recovery, RequestID, Logging, Metrics (Prometheus)
  model/                # Shared types: NormalizedRequest, ProviderResponse, ProviderError
  provider/             # Business logic layer
    provider.go         # HandlerFunc type, OK() helper
    registry.go         # Registry.Dispatch
    function/           # FunctionProvider — Lambda (echo invoke mock)
    iam/                # IAMProvider + STS (roles, policies, users, access keys)
    notification/       # SNSProvider (topics, subscriptions, fan-out to SQS)
    object/             # ObjectProvider — S3 (buckets, objects, multipart)
    queue/              # QueueProvider — SQS (all 17 operations)
    table/              # TableProvider — DynamoDB (tables, items, expressions)
  store/                # Resource metadata
    store.go            # ResourceStore interface
    memory.go           # MemoryResourceStore (sync.RWMutex, Snapshot/Restore)
    postgres.go         # PostgresResourceStore (pgx/v5)
    migrate.go          # RunMigrations — go:embed migrations/*.sql
    migrations/         # SQL migration files (001–005)
    aws/
      dynamodb/         # DynamoDBItemStore interface + memory + postgres
      s3/               # S3ObjectMetaStore interface + memory + postgres
      sqs/              # SQSMessageStore interface + memory + postgres
tests/
  integration/          # End-to-end tests using aws-sdk-go-v2
```

---

## Architecture

Request flow:

```
HTTP request
  → gateway.Server.handleCloudRequest
      → AWSAdapter.DetectAndDecode   (detect protocol, decode params)
          → <ServiceCodec>.Decode    (SQS/IAM/SNS → Query/XML; DynamoDB → JSON/Target;
                                      S3/Lambda → REST)
      → Registry.Dispatch(service.Action, NormalizedRequest)
          → <ServiceProvider>.<action>
              → ResourceStore        (control-plane metadata)
              → <service>Store       (data-plane: messages, items, blobs)
      → <ServiceCodec>.Encode        (XML or JSON or raw bytes)
  → HTTP response
```

Key design rule: **no layer imports its caller**. The `model` package exists solely to break the cycle between `gateway` and `adapter`.

### Service → Provider mapping

| Wire service | Provider prefix | Codec |
|---|---|---|
| `sqs` | `Queue` | SQSCodec (JSON + Query) |
| `iam` | `IAM` | IAMCodec (Query/XML) |
| `sts` | `STS` | IAMCodec (Query/XML) |
| `sns` | `Notification` | SNSCodec (Query/XML) |
| `dynamodb` | `Table` | DynamoDBCodec (JSON/Target) |
| `s3` | `Object` | S3Codec (REST/XML) |
| `lambda` | `Function` | LambdaCodec (REST/JSON) |

---

## Build & run

```bash
# Build binary — always rebuild after code changes, never run a stale binary
go build -o jaiscloud ./cmd/jaiscloud/

# Run server (default port 4566, lite mode)
./jaiscloud start

# Run in full mode (PostgreSQL persistence)
./jaiscloud start --mode full --dsn "postgres://user:pass@localhost:5432/jaiscloud"

# Run with options
./jaiscloud start --port 4566 --region us-east-1 --metrics

# Enable Prometheus metrics
./jaiscloud start --metrics        # served at /metrics

# Environment variables (all JAISCLOUD_ prefixed)
JAISCLOUD_PORT=4566
JAISCLOUD_MODE=lite          # or "full"
JAISCLOUD_DSN=               # required when MODE=full
JAISCLOUD_REGION=us-east-1
JAISCLOUD_ACCOUNT_ID=000000000000
JAISCLOUD_LOG_LEVEL=info
JAISCLOUD_METRICS=true
```

> **Config loading:** `config.Load()` reads from the global Viper instance. All `viper.BindPFlag(...)` calls in `startCmd` must use the global `viper` package (not a local `viper.New()` instance) or flags will be silently ignored and defaults used.

### CLI commands

| Command | Description |
|---|---|
| `start` | Start the emulator |
| `version` | Print version |
| `env` | Print effective config as env vars |
| `doctor` | Check emulator reachability |
| `reset` | Wipe all emulator state via HTTP |
| `export [-o file]` | Export state snapshot to JSON |
| `import [-i file]` | Restore state from JSON snapshot |

---

## Tests

```bash
# Unit + store tests (no server needed)
go test -race ./internal/...

# Integration tests — lite mode (no external deps)
./jaiscloud start &
go test -race ./tests/integration/

# Integration tests — full mode (requires postgres)
./jaiscloud start --mode full --dsn "postgres://..." &
go test -race ./tests/integration/

# Point at a different host
JAISCLOUD_HOST=http://localhost:9000 go test ./tests/integration/
```

Integration tests call `POST /_jaiscloud/reset` between each test via `resetState(t)` in [tests/integration/helpers_test.go](tests/integration/helpers_test.go).

Current integration test coverage: **SQS, IAM/STS, SNS, DynamoDB, S3, Lambda**.

### Full mode reset behaviour

`POST /_jaiscloud/reset` wipes all registered stores. In full mode this covers every postgres table:

| Store | Table(s) wiped |
|---|---|
| `PostgresResourceStore` | `jc_resources` (queues, topics, tables, functions, IAM) |
| `PostgresSQSMessageStore` | `jc_sqs_messages`, `jc_sqs_dedup` |
| `PostgresDynamoDBItemStore` | `jc_dynamodb_items` |
| `PostgresS3ObjectMetaStore` | `jc_s3_objects` |
| `MemoryBlobStore` | in-memory blob bytes |

All five are registered as `Resetter` in `main.go`.

> **Note:** Even in full mode, blob bytes (`BlobStore`) use `MemoryBlobStore`. `LocalFSBlobStore` is implemented (its `Reset()` removes and recreates the base directory) but is not yet wired into `main.go`. Swap the `NewMemoryBlobStore()` call in `startCmd` for `NewLocalFSBlobStore(dir)` to enable on-disk blob persistence.

---

## Key conventions

### Service detection order

In [internal/adapter/aws/router.go](internal/adapter/aws/router.go):
1. `X-Amz-Target` header (SQS JSON, DynamoDB)
2. SigV4 `Authorization` scope (all services)
3. `Action` param in query/body (SQS, IAM, STS, SNS Query protocol)

S3 and Lambda are always detected via SigV4 (they use REST, no `Action` param).

### S3 action detection

`S3Codec.Decode` maps `(HTTP method, bucket, key, query params, headers)` to action names. Key rules:
- No bucket → `ListBuckets`
- No key, `GET ?list-type=2` → `ListObjectsV2`
- `X-Amz-Copy-Source` header + `PUT` → `CopyObject`
- `?uploads` on POST → `CreateMultipartUpload`, on GET → `ListMultipartUploads`
- `?delete` on POST → `DeleteObjects` (XML body parsed for keys)

### Lambda invoke

`InvokeFunction` echoes the payload back unchanged — no subprocess is spawned. Useful for testing fan-out pipelines that invoke Lambda as a sink.

### FIFO deduplication (SQS)

`MemoryMessageStore.Send` returns `(dedupMessageID string, err error)`. A non-empty `dedupMessageID` means duplicate; the original `MessageId` is returned.

### Visibility / DLQ (SQS)

`MemoryMessageStore.Receive` sets `VisibleAt = now + 30s`. The provider then calls `ChangeVisibility` with the queue's configured timeout. When `ReceiveCount >= maxReceiveCount`, `checkDLQ` copies the message to the dead-letter queue with zeroed timers.

### SNS fan-out

`SNSProvider.Publish` wraps the message in a JSON envelope and calls `SQSMessageStore.Send` for each SQS subscription. Each SQS delivery is assigned a **new unique `MessageID`**; the SNS notification ID is embedded in the envelope body only. Reusing the SNS ID as the SQS row key would conflict on the `(id, queue_url)` primary key when delivering to N queues.

### DynamoDB pk hash

`TableProvider` computes a stable hash from key attributes only (in schema-defined order) and passes it explicitly to `DynamoDBItemStore`. The store never auto-computes hashes — the provider is the sole authority.

### DynamoDB wire protocol: x-amz-crc32

Every DynamoDB response **must** include `x-amz-crc32: <crc32_of_body>`. AWS SDK v2 validates this header; without it the SDK does not cleanly drain the response body, causing a "failed to close HTTP response body" warning and potential connection-reuse problems. `DynamoDBCodec.Encode` computes and sets this header on every response.

### Postgres SQS: composite primary key

`jc_sqs_messages` uses a composite primary key `(id, queue_url)` so the same `MessageID` can appear in multiple queues (e.g. SNS fan-out). Migration `005_sqs_fix_pk.sql` upgrades existing installs that have the old single-column `id` primary key.

`MessageAttributes` are stored in the `msg_attributes JSONB` column. The postgres `Send` serialises them; `Receive` deserialises them. The column exists in migration 002 but was unused before Phase 1 fixes.

### Postgres ResourceStore: prefix matching

`PostgresResourceStore.List` uses `LIKE '%prefix%'` (contains) to match the behaviour of `MemoryResourceStore.List` (`strings.Contains`). Queue IDs are full URLs (`http://localhost:4566/000000000000/<name>`), so a queue-name prefix must be matched as a substring, not a prefix of the full URL.

### Admin endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | Liveness check |
| `/_jaiscloud/reset` | POST | Wipe all state (used by integration tests) |
| `/_jaiscloud/export` | GET | JSON snapshot of all registered Snapshotter stores |
| `/_jaiscloud/import` | POST | Restore state from JSON snapshot |
| `/metrics` | GET | Prometheus metrics (requires `--metrics` flag) |

### Snapshot / Restore

Stores implement `admin.Snapshotter` (`Snapshot() (json.RawMessage, error)` + `Restore(json.RawMessage) error`) and are registered with `adminHandler.RegisterSnapshotter(name, store)`. Currently `MemoryResourceStore` is snapshotted under key `"resources"`.

### HTTP response: Content-Length

`gateway.writeResponse` explicitly sets `Content-Length` before calling `WriteHeader`. Without this, Go's HTTP server uses chunked transfer encoding, which can cause the AWS SDK's response body close to fail and log a connection-reuse warning.

### Clock abstraction

All time-sensitive code receives a `clock.Clock` from `NormalizedRequest.Clock`. Integration tests use `RealClock`. Unit tests can use `FixedClock` or `OffsetClock` for deterministic control.

### Multi-cloud extensibility: dependency injection for cloud-specific formatting

**Rule: providers must never hard-code cloud-specific resource identifier formats (AWS ARNs, Azure resource IDs, etc.).**

When a provider needs a cloud-specific resource ID, it must use the injected function on `NormalizedRequest` rather than calling `fmt.Sprintf("arn:aws:...")` directly.

**Pattern — use `NormalizedRequest.ResourceID`:**
```go
// WRONG — couples provider to AWS:
arn := fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", nr.Region, nr.AccountID, name)

// CORRECT — delegate to injected formatter:
arn := nr.ResourceID("dynamodb-table", name)
```

**Where each piece lives:**
- `internal/model/model.go` — declares `ResourceID func(resourceType, name string) string` on `NormalizedRequest`
- `internal/config/config.go` — `AWSResourceID(region, accountID)` returns the AWS ARN implementation
- `internal/gateway/server.go` — injects `nr.ResourceID = config.AWSResourceID(...)` alongside `nr.Region`/`nr.AccountID`
- `internal/provider/*/` — calls `nr.ResourceID("type", name)` only; no `"arn:aws:"` literals

**Adding a new resource type:** add a `case "my-service-resource":` to `AWSResourceID` in `config.go`. A hypothetical Azure adapter would inject a different function that formats Azure resource IDs.

**Fallback:** providers may include a nil-guard fallback for unit tests that don't go through the gateway:
```go
func myArn(nr *model.NormalizedRequest, name string) string {
    if nr.ResourceID != nil {
        return nr.ResourceID("my-resource", name)
    }
    return fmt.Sprintf("arn:aws:...", nr.Region, nr.AccountID, name) // test fallback only
}
```

The same DI principle applies to any other cloud-specific customisation point: define an interface or function type in `internal/model/`, implement it per cloud in the adapter/config layer, and inject it via `NormalizedRequest` or the provider constructor.

---

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/go-chi/chi/v5` | HTTP router + middleware |
| `github.com/spf13/cobra` | CLI framework |
| `github.com/spf13/viper` | Config / env loading |
| `github.com/jackc/pgx/v5` | PostgreSQL driver (full mode) |
| `github.com/prometheus/client_golang` | Prometheus metrics (opt-in) |
| `github.com/aws/aws-sdk-go-v2` | Integration test client |
| `github.com/stretchr/testify` | Test assertions |
