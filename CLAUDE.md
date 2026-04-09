# JaisCloud — Developer Reference

## Project

JaisCloud is a local multi-cloud emulator written in Go. It speaks native AWS wire protocols (Query/XML and JSON/Target) so any AWS SDK can point at it without modification.

**Phase 0 (complete):** SQS — all 32 MiniStack integration tests pass.  
**Phase 1 (planned):** S3, IAM, DynamoDB, Lambda; PostgreSQL message store; OIDC auth.

---

## Module

```
module jaiscloud   # go.mod
go 1.24
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
        sqs.go          # SQSCodec: JSON decode, Query decode, XML encode
  admin/                # /_jaiscloud/health and /_jaiscloud/reset endpoints
  clock/                # Clock interface: RealClock, FixedClock, OffsetClock
  config/               # Config struct; Viper loading; env prefix JAISCLOUD_
  events/               # In-process EventBus (subscribe/publish)
  gateway/              # HTTP server (Chi), middleware, request dispatch
    middleware/         # Recovery, RequestID, Logging
  model/                # Shared types: NormalizedRequest, ProviderResponse, ProviderError
  provider/             # Business logic layer
    provider.go         # HandlerFunc type, OK() helper
    registry.go         # Registry.Dispatch
    queue/              # QueueProvider — all 17 SQS operations
  store/                # Resource metadata (queues, topics, …)
    store.go            # ResourceStore interface
    memory.go           # MemoryResourceStore (sync.RWMutex)
    aws/sqs/
      store.go          # SQSMessageStore interface
      memory.go         # MemoryMessageStore (per-queue slices, FIFO, DLQ)
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
          → SQSCodec.Decode
      → Registry.Dispatch(service.Action, NormalizedRequest)
          → QueueProvider.<action>
              → MemoryResourceStore  (queue metadata)
              → MemoryMessageStore   (message data plane)
      → SQSCodec.Encode             (XML response)
  → HTTP response
```

Key design rule: **no layer imports its caller**. The `model` package exists solely to break the cycle between `gateway` and `adapter` — both import `model`, neither imports the other.

---

## Build & run

```bash
# Build binary
go build -o jaiscloud ./cmd/jaiscloud/

# Run server (default port 4566)
./jaiscloud start

# Run with options
./jaiscloud start --port 4566 --region us-east-1 --account-id 000000000000

# Environment variables (all JAISCLOUD_ prefixed)
JAISCLOUD_PORT=4566
JAISCLOUD_REGION=us-east-1
JAISCLOUD_ACCOUNT_ID=000000000000
JAISCLOUD_LOG_LEVEL=info
```

---

## Tests

```bash
# Unit + store tests (no server needed)
go test -race ./internal/...

# Integration tests (requires running server on localhost:4566)
./jaiscloud start &
go test -race ./tests/integration/

# Point at a different host
JAISCLOUD_HOST=http://localhost:9000 go test ./tests/integration/
```

Integration tests call `POST /_jaiscloud/reset` between each test via `resetState(t)` in [tests/integration/helpers_test.go](tests/integration/helpers_test.go).

---

## Key conventions

### SQS codec — dual protocol

SQS clients send either:
- **JSON**: `Content-Type: application/x-amz-json-1.0` + `X-Amz-Target: AmazonSQS.<Action>`
- **Query/XML**: `Content-Type: application/x-www-form-urlencoded`, `Action=<Action>` in body or URL

Detection order in [internal/adapter/aws/router.go](internal/adapter/aws/router.go):
1. `X-Amz-Target` header
2. SigV4 `Authorization` scope
3. `Action` param (URL query, then POST body)

### FIFO deduplication

`MemoryMessageStore.Send` returns `(dedupMessageID string, err error)`. A non-empty `dedupMessageID` means the message was a duplicate; the original MessageId is returned. Callers in `QueueProvider` use this to return the correct ID in send responses.

### Visibility / DLQ

`MemoryMessageStore.Receive` sets `VisibleAt = now + 30s` as a default. The provider immediately calls `ChangeVisibility` with the queue's configured timeout. When `ReceiveCount >= maxReceiveCount`, `checkDLQ` copies the message to the dead-letter queue, resetting `VisibleAt`, `DelayUntil`, and `ReceiveCount` to zero so the DLQ copy is immediately visible.

### Admin reset

Both `MemoryResourceStore` and `MemoryMessageStore` implement the `admin.Resetter` interface. They are registered with `admin.Handler.RegisterResetter` in `main.go`. `POST /_jaiscloud/reset` calls `Reset()` on all registered resetters — used by integration tests to isolate state.

### Clock abstraction

All time-sensitive code receives a `clock.Clock` from `NormalizedRequest.Clock`. Integration tests use `RealClock`. Unit tests can use `FixedClock` or `OffsetClock` for deterministic time control.

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/go-chi/chi/v5` | HTTP router + middleware |
| `github.com/spf13/cobra` | CLI framework |
| `github.com/spf13/viper` | Config / env loading |
| `github.com/aws/aws-sdk-go-v2` | Integration test client |
| `github.com/stretchr/testify` | Test assertions |
