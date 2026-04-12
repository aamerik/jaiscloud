# JaisCloud — Developer Reference

## Project

JaisCloud is a local multi-cloud emulator written in Go. It speaks native AWS wire protocols (Query/XML, JSON/Target, REST) so any AWS SDK can point at it without modification.

**Phase 0 (complete):** SQS — all 32 integration tests pass.  
**Phase 1 (complete):** IAM/STS, SNS, DynamoDB, S3, Lambda; BlobFS; PostgreSQL stores; export/import; Prometheus metrics.  
**Phase 2 (complete):** Plugin system; Plugin SDK (`sdk/`); ResourceManager with deletion guards; Multi-cloud adapter model (AWS default, Azure/GCP stubs); EMR/EMR-on-EKS via `aws-emr-spark` plugin; Prometheus cloud label.

---

## Module

```
module jaiscloud   # go.mod
go 1.26.2
```

The plugin SDK is a separate standalone module at `sdk/` with no jaiscloud core dependencies:

```
module github.com/jaiscloud/plugin-sdk   # sdk/go.mod
go 1.26.2
```

The host module references the SDK via a `replace` directive in `go.mod`:
```
replace github.com/jaiscloud/plugin-sdk => ./sdk
```

---

## Directory layout

```
cmd/jaiscloud/          # main.go — wires everything together, Cobra CLI
docs/                   # Architecture, LLD, phase plan documents
sdk/                    # Standalone plugin SDK module (github.com/jaiscloud/plugin-sdk)
  sdk.go                # SparkPlugin interface, ManifestInfo, HandleRequest/Response, PluginError
  store.go              # ResourceStore interface (sdk-facing, stdlib only)
  rm.go                 # ResourceManager interface, DeleteGuardRule, DeletionPolicy, DeletionHandle
  events.go             # EventBus interface, Event, NoopEventBus
plugins/
  aws-emr-spark/        # EMR + EMR-on-EKS plugin (standalone module: github.com/jaiscloud/plugin-aws-emr-spark)
    main.go             # var Plugin sdk.SparkPlugin = plugin.New()  (package main, buildmode=plugin)
    Makefile            # build / test / clean targets
    Dockerfile          # multi-stage build → aws-emr-spark.so
    internal/
      executor/spark/
        executor.go     # SparkExecutor interface, SparkJob, SparkState, NewExecutor factory
        config.go       # SparkConfig, ClusterSize (Small/Medium/Large), SparkConfigFrom
        mock.go         # MockExecutor — immediate COMPLETED, ForceState, Reset
        k8s.go          # K8sExecutor — wraps MockExecutor, logs spark-submit args
        command.go      # SparkSubmitArgs — builds --master k8s:// arg list
        poller.go       # StatusPoller — background goroutine, OnStateChange callback
      provider/
        emr/            # EMRProvider — RunJobFlow, DescribeCluster, ListClusters,
                        #   TerminateJobFlows, AddJobFlowSteps, DescribeStep, ListSteps,
                        #   CancelSteps, tag operations
        emrcontainers/  # EMRContainersProvider — virtual clusters, job runs
      plugin/
        emrsparkplugin.go  # EMRSparkPlugin struct — Init, Manifest, Handle, Shutdown, Reset
internal/
  adapter/              # Cloud wire-protocol layer (no business logic)
    adapter.go          # CloudAdapter interface (Cloud, DetectAndDecode, ServiceToProvider); Codec interface
    aws/
      aws.go            # AWSAdapter — Cloud(), DetectAndDecode(), ServiceToProvider()
      router.go         # DetectService — data-driven via services.go (X-Amz-Target / SigV4 / Action)
      services.go       # ServiceDescriptor + awsServices registry — single source of truth for all
                        #   service metadata (SigV4Name, TargetPrefix, QueryActions, ProviderPrefix).
                        #   Derived maps built at init(): targetPrefixToService, knownSigV4Services,
                        #   actionToService, serviceProviderMap. Add one entry here to register a service.
      services/
        sqs.go          # SQSCodec: JSON + Query/XML
        iam.go          # IAMCodec: Query/XML (handles STS too)
        sns.go          # SNSCodec: Query/XML
        dynamodb.go     # DynamoDBCodec: JSON/Target
        s3.go           # S3Codec: REST path-style, XML responses
        lambda.go       # LambdaCodec: REST JSON
        glue.go         # GlueCodec
        ec2.go          # EC2Codec
        route53.go      # Route53Codec
        rds.go          # RDSCodec
        elasticache.go  # ElastiCacheCodec
        ecs.go          # ECSCodec
        dynamodbstreams.go # DynamoDBStreamsCodec
        cloudformation.go  # CloudFormationCodec
        emr.go          # EMRCodec
        emrcontainers.go   # EMRContainersCodec
        eventbridge.go  # EventBridgeCodec: JSON/Target (X-Amz-Target: AWSEvents.*)
    azure/
      azure.go          # AzureAdapter stub — Cloud(), DetectAndDecode() (501), ServiceToProvider() (passthrough)
    gcp/
      gcp.go            # GCPAdapter stub — Cloud(), DetectAndDecode() (501), ServiceToProvider() (passthrough)
  admin/                # /_jaiscloud/* endpoints
                        # Resetter, Snapshotter interfaces
  blobfs/               # BlobStore interface: MemoryBlobStore, LocalFSBlobStore
  clock/                # Clock interface: RealClock, FixedClock, OffsetClock
  config/               # Config struct; Viper loading; env prefix JAISCLOUD_
  events/               # In-process EventBus (subscribe/publish)
  gateway/              # HTTP server (Chi), middleware, request dispatch
    server.go           # Server — holds single CloudAdapter; handleCloudRequest
    middleware/         # Recovery, RequestID, Logging, Metrics (Prometheus + cloud label)
  model/                # Shared types: NormalizedRequest, ProviderResponse, ProviderError
  plugin/               # Host-side plugin management
    manager.go          # PluginManager — LoadAll, Shutdown, Reset, InjectForTest
    routes.go           # registerPluginRoutes, serviceToProviderPrefix
    adapters.go         # Bridge adapters: NewSDKStoreAdapter, NewSDKResourceManager, convertRule
  provider/             # Business logic layer
    provider.go         # HandlerFunc type, OK() helper
    registry.go         # Registry — Dispatch (exact match → plugin wildcard → error)
                        # RegisterPlugin(prefix, handler) for plugin wildcard fallback
    cache/              # ElastiCache provider
    catalog/            # Glue Data Catalog provider
    compute/            # EC2 provider
    container/          # ECS provider
    dns/                # Route53 provider
    emr/                # EMR provider (built-in stub)
    emroneks/           # EMR-on-EKS provider (built-in stub)
    function/           # FunctionProvider — Lambda (echo invoke)
    iam/                # IAMProvider + STS (roles, policies, users, access keys)
    notification/       # SNSProvider (topics, subscriptions, fan-out to SQS)
    object/             # ObjectProvider — S3 (buckets, objects, multipart)
    queue/              # QueueProvider — SQS (all 17 operations)
    rds/                # RDS provider
    stack/              # CloudFormation provider
    table/              # TableProvider — DynamoDB (tables, items, expressions, streams)
    events/             # EventBridgeProvider — rules, targets, event delivery to SQS
  resourcemgr/          # Deletion guards and parent-existence checks
    manager.go          # Manager — CheckParent, AcquireDelete, RegisterRules, Reset
    deletionlock.go     # DeletionLock — thread-safe per-resource deletion marks
    adapter.go          # StoreAdapter — bridges store.ResourceStore → resourcemgr.ResourceStore
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
      stream/           # MemoryStreamStore (DynamoDB Streams)
tests/
  integration/          # End-to-end tests using aws-sdk-go-v2
```

---

## Architecture

### Request flow

```
HTTP request
  → gateway.Server.handleCloudRequest
      → cloudAdapter.DetectAndDecode     (single adapter selected at startup from cfg.Cloud)
          → <ServiceCodec>.Decode        (SQS/IAM/SNS → Query/XML; DynamoDB → JSON/Target;
                                          S3/Lambda → REST)
      → inject: nr.Clock, nr.Region, nr.AccountID, nr.Cloud, nr.ResourceID (all clouds)
      → middleware.WithRequestLabels(ctx, cloud, service, action)
      → Registry.Dispatch("ProviderPrefix.Action", nr)
          → exact match: built-in provider handler
          → plugin wildcard fallback: PluginManager → plugin.Handle
              → plugin.EMRSparkPlugin.Handle → EMRProvider / EMRContainersProvider
                  → sdk.ResourceStore (store bridge)
                  → SparkExecutor (mock or k8s)
                  → StatusPoller
      → <ServiceCodec>.Encode            (XML or JSON or raw bytes)
  → HTTP response
```

Key design rules:
- **No layer imports its caller.** The `model` package breaks the cycle between `gateway` and `adapter`.
- **Single cloud per instance.** `cfg.Cloud` is set once at startup; one `CloudAdapter` is constructed; no per-request cloud detection.
- **Plugins never import the host module.** They import only `github.com/jaiscloud/plugin-sdk` (stdlib only). The host bridges host types to SDK interfaces at the boundary.

### Single-cloud adapter model

An instance of JaisCloud emulates exactly one cloud. The cloud is selected from `cfg.Cloud` (default `"aws"`) once at startup in `buildAdapter(cfg)` in `main.go`. There is no per-request detection loop.

```go
// main.go
func buildAdapter(cfg *config.Config) (adapter.CloudAdapter, error) {
    switch cfg.Cloud {
    case "aws":  return buildAWSAdapter(), nil
    case "azure": return azureadapter.New(), nil
    case "gcp":   return gcpadapter.New(), nil
    default:      return nil, fmt.Errorf("unknown cloud %q", cfg.Cloud)
    }
}
```

`config.Load()` validates `cfg.Cloud` (allowlist: `aws`, `azure`, `gcp`) before any stores are initialised, so an invalid `--cloud` flag fails fast.

### CloudAdapter interface

```go
type CloudAdapter interface {
    Cloud() model.Cloud
    DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, Codec, error)
    ServiceToProvider(service string) string
}
```

`Detect()` was deliberately omitted — cloud identity is a startup config decision, not a per-request inference.

`ServiceToProvider` maps a wire service name (e.g. `"sqs"`) to the provider registry prefix (e.g. `"Queue"`). The gateway calls this instead of maintaining its own switch. AWS delegates to `serviceProviderMap` built from `awsServices` in `services.go`; Azure/GCP stubs return the service name unchanged.

### Service → Provider mapping

This mapping is defined once in `internal/adapter/aws/services.go` (`awsServices` slice) and derived automatically — no switch statement anywhere. The gateway calls `cloudAdapter.ServiceToProvider(nr.Service)` to get the provider prefix.

| Wire service | Provider prefix | Codec |
|---|---|---|
| `sqs` | `Queue` | SQSCodec (JSON + Query) |
| `iam` | `IAM` | IAMCodec (Query/XML) |
| `sts` | `STS` | IAMCodec (Query/XML) |
| `sns` | `Notification` | SNSCodec (Query/XML) |
| `dynamodb` | `Table` | DynamoDBCodec (JSON/Target) |
| `dynamodbstreams` | `Streams` | DynamoDBStreamsCodec (JSON/Target) |
| `s3` | `Object` | S3Codec (REST/XML) |
| `lambda` | `Function` | LambdaCodec (REST/JSON) |
| `glue` | `Glue` | GlueCodec (JSON/Target) |
| `ec2` | `Compute` | EC2Codec (Query/XML) |
| `route53` | `DNS` | Route53Codec (REST/XML) |
| `rds` | `RDS` | RDSCodec (Query/XML) |
| `elasticache` | `ElastiCache` | ElastiCacheCodec (Query/XML) |
| `ecs` | `ECS` | ECSCodec (JSON/Target) |
| `cloudformation` | `CloudFormation` | CloudFormationCodec (Query/XML) |
| `emr` | `EMR` | EMRCodec (JSON/Target) |
| `emr-containers` | `EMRContainers` | EMRContainersCodec (REST/JSON) |
| `events` | `EventBridge` | EventBridgeCodec (JSON/Target) |

---

## Build & run

```bash
# Build binary — always rebuild after code changes, never run a stale binary
go build -o jaiscloud ./cmd/jaiscloud/

# Run server (default port 4566, lite mode, AWS)
./jaiscloud start

# Run in full mode (PostgreSQL persistence)
./jaiscloud start --mode full --dsn "postgres://user:pass@localhost:5432/jaiscloud"

# Run with options
./jaiscloud start --port 4566 --region us-east-1 --metrics

# Run in GCP cloud mode (stub — returns 501 for all requests)
./jaiscloud start --cloud gcp

# Load EMR Spark plugin from a directory
./jaiscloud start --mode full --plugin-dir /path/to/plugins

# Build the EMR Spark plugin .so
cd plugins/aws-emr-spark && make build
# Produces aws-emr-spark.so in the root of the repo

# Enable Prometheus metrics
./jaiscloud start --metrics        # served at /metrics

# Environment variables (all JAISCLOUD_ prefixed)
JAISCLOUD_PORT=4566
JAISCLOUD_MODE=lite          # or "full"
JAISCLOUD_CLOUD=aws          # or "azure", "gcp"
JAISCLOUD_DSN=               # required when MODE=full
JAISCLOUD_REGION=us-east-1
JAISCLOUD_ACCOUNT_ID=000000000000
JAISCLOUD_LOG_LEVEL=info
JAISCLOUD_METRICS=true
JAISCLOUD_PLUGIN_DIR=        # path to directory containing .so plugin files
```

> **Config loading:** `config.Load()` reads from the global Viper instance. All `viper.BindPFlag(...)` calls in `startCmd` must use the global `viper` package (not a local `viper.New()` instance) or flags will be silently ignored and defaults used.

### CLI commands

| Command | Description |
|---|---|
| `start` | Start the emulator |
| `version` | Print version |
| `env` | Print effective config as env vars (includes JAISCLOUD_CLOUD) |
| `doctor` | Check emulator reachability |
| `reset` | Wipe all emulator state via HTTP |
| `export [-o file]` | Export state snapshot to JSON |
| `import [-i file]` | Restore state from JSON snapshot |

---

## Tests

```bash
# Unit + store tests (no server needed)
go test -race ./internal/...

# Plugin SDK tests
cd sdk && go test -race ./...

# Plugin unit tests (does not require -buildmode=plugin)
cd plugins/aws-emr-spark && go test -race ./internal/...

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
| `PluginManager` | calls `Reset()` on every loaded plugin |

> **Note:** Even in full mode, blob bytes (`BlobStore`) use `MemoryBlobStore`. `LocalFSBlobStore` is implemented but not yet wired into `main.go`. Swap the `NewMemoryBlobStore()` call in `startCmd` for `NewLocalFSBlobStore(dir)` to enable on-disk blob persistence.

---

## Plugin system

### Overview

Plugins are Go `.so` files compiled with `-buildmode=plugin`. The host discovers them at startup by scanning `--plugin-dir` for `*.so` files. Each plugin must export a package-level variable:

```go
var Plugin sdk.SparkPlugin = plugin.New()
```

The host lifecycle:

```
plugin.Open(.so) → Lookup("Plugin") → Init(ctx, rm, store) → Manifest() → RegisterPlugin routes
                                                                          ↓ on request
                                                            Handle(ctx, req) → HandleResponse
                                                                          ↓ on shutdown
                                                            Shutdown(ctx)
                                                                          ↓ on reset
                                                            Reset()
```

### Plugin SDK (`sdk/`)

The SDK module (`github.com/jaiscloud/plugin-sdk`) uses only the Go standard library. Plugins import it; they never import the host module `jaiscloud`.

Key interfaces:

```go
type SparkPlugin interface {
    Init(ctx context.Context, rm ResourceManager, store ResourceStore) error
    Manifest() ManifestInfo
    Handle(ctx context.Context, req HandleRequest) HandleResponse
    Shutdown(ctx context.Context) error
    Reset()
}

type ResourceStore interface {
    Exists(ctx, resourceType, id string) (bool, error)
    Get(ctx, resourceType, id string) (ResourceEntry, error)
    List(ctx, resourceType, prefix string) ([]ResourceEntry, error)
    Create(ctx context.Context, e ResourceEntry) error
    Update(ctx context.Context, e ResourceEntry) error
    Delete(ctx, resourceType, id string) error
}

type ResourceManager interface {
    CheckParent(ctx, parentType, parentID, notFoundCode, notFoundMsg string, httpStatus int) error
    AcquireDelete(ctx, resourceType, resourceID string) (DeletionHandle, error)
    RegisterRules(rules []DeleteGuardRule)
}
```

### Bridge adapters (`internal/plugin/adapters.go`)

The host bridges its concrete types to SDK interfaces at the plugin boundary:

| Function | Bridges |
|---|---|
| `NewSDKStoreAdapter(store.ResourceStore)` | `store.ResourceStore` → `sdk.ResourceStore` |
| `NewSDKResourceManager(*resourcemgr.Manager)` | `*resourcemgr.Manager` → `sdk.ResourceManager` |
| `convertRule(sdk.DeleteGuardRule)` | `sdk.DeleteGuardRule` → `resourcemgr.DeleteGuardRule` (translates all callback fields) |

### Registry plugin wildcard

`provider.Registry` supports a two-tier dispatch:
1. **Exact match** — `"EMR.RunJobFlow"` hits a built-in handler.
2. **Plugin wildcard** — `registry.RegisterPlugin("EMR", handler)` registers a catch-all for all `EMR.*` actions not matched exactly. The plugin's `Handle` method dispatches internally.

```go
registry.RegisterPlugin("EMR", func(ctx, nr) (*ProviderResponse, error) {
    resp := plugin.Handle(ctx, toSDKRequest(nr))
    ...
})
```

### EMR Spark plugin (`plugins/aws-emr-spark/`)

The plugin is a self-contained module. Its structure:

- **`EMRSparkPlugin`** (`internal/plugin/emrsparkplugin.go`) — extracted to a testable non-main package; `main.go` is a one-liner.
- **`SparkExecutor`** interface — `Submit`, `Status`, `Cancel`, `Close`. Implementations:
  - `MockExecutor` — immediate `COMPLETED`, supports `ForceState` and `Reset` for tests.
  - `K8sExecutor` — wraps `MockExecutor`; logs `spark-submit` args via `SparkSubmitArgs(job)`. Real client-go integration is a future phase.
- **`StatusPoller`** — single background goroutine; polls non-terminal jobs at a configurable interval; fires `OnStateChange` callbacks. `Stop()` is safe to call multiple times (uses `sync.Once`).
- **`SparkSubmitArgs`** — builds the full `spark-submit` argument list including `--master k8s://`, `--deploy-mode cluster`, container image, namespace, service account, resource profile, and S3 event-log args.
- **`EMRProvider`** — handles `RunJobFlow`, `DescribeCluster`, `ListClusters`, `TerminateJobFlows`, `AddJobFlowSteps`, `DescribeStep`, `ListSteps`, `CancelSteps`, and tag operations. Steps are stored under key `clusterID/stepID`.
- **`EMRContainersProvider`** — handles virtual clusters and job runs. `pathOrParam` helper reads `_path_<key>` REST path parameters before falling back to body params.

Spark mode is controlled by `JAISCLOUD_SPARK_MODE` env var (default `"mock"`). Set to `"k8s"` to use the K8s executor path.

---

## ResourceManager (`internal/resourcemgr/`)

The ResourceManager provides two guards that prevent invalid state during concurrent deletions.

### CheckParent

Verifies that a parent resource exists and is not being deleted. Uses `m.mu.RLock()` so concurrent checks do not block each other, but a concurrent `AcquireDelete` cannot set `IsDeleting` between the `Exists` and `IsDeleting` checks (TOCTOU window closed).

```go
err := mgr.CheckParent(ctx, "emrc_virtual_cluster", clusterID,
    "ResourceNotFoundException", "cluster not found", 404)
```

### AcquireDelete

Acquires a deletion lock and runs all applicable `DeleteGuardRule`s. Steps:

1. Acquires lock under `m.mu.Lock()` (briefly). Concurrent `CheckParent` now sees `IsDeleting=true`.
2. Releases `m.mu`. `FindChildren` runs outside the mutex.
3. Sorts results by policy priority: `PolicyFail(0)` → `PolicyForceTerminate(1)` → `PolicyCascade(2)`. Fail fires before any irreversible operation.
4. Returns `*DeletionHandle`. Caller **must** call `handle.Release()` (not via `defer` in loops).

```go
handle, err := mgr.AcquireDelete(ctx, "emr_cluster", clusterID)
if err != nil { return toHTTPError(err) }
defer handle.Release()
// ... perform the actual delete ...
```

### DeleteGuardRule policies

| Policy constant | Priority | Behaviour |
|---|---|---|
| `PolicyFail` | 0 (highest) | Returns error with configurable code/status/message. Releases lock. |
| `PolicyForceTerminate` | 1 | Calls `ForceTerminate(ctx, store, child)` for each child. |
| `PolicyCascade` | 2 (lowest) | Calls `CascadeDelete(ctx, store, child)` or falls back to `store.Delete`. |

### RegisterRules

Plugins call `rm.RegisterRules([]sdk.DeleteGuardRule{...})` during `Init` to register their own resource dependency rules. Thread-safe.

### StoreAdapter (`internal/resourcemgr/adapter.go`)

Bridges `store.ResourceStore` (host type) to `resourcemgr.ResourceStore` (internal interface). `Exists` maps `store.ErrNotFound` to `false` rather than returning an error.

---

## Key conventions

### AWS service detection order

Detection is data-driven. All service metadata (target prefixes, SigV4 names, Query-protocol actions) is declared once in [internal/adapter/aws/services.go](internal/adapter/aws/services.go) as `ServiceDescriptor` entries in `awsServices`. Four lookup maps are derived at `init()` time. `DetectService` in [router.go](internal/adapter/aws/router.go) consults those maps — no hardcoded strings.

Priority:
1. `X-Amz-Target` header — looked up in `targetPrefixToService` map (JSON/Target services: SQS, DynamoDB, Glue, ECS, EMR, EventBridge, DynamoDB Streams)
2. SigV4 `Authorization` scope — service token checked against `knownSigV4Services` set (all services)
3. `Action=` query/body param — looked up in `actionToService` map (Query-protocol services: SQS, IAM, STS, SNS)

S3 and Lambda are always detected via SigV4 (REST, no `Action` param).

**Adding a new service:** add one `ServiceDescriptor` entry to `awsServices` in `services.go`. Service detection, SigV4 allow-list, Action routing, and gateway provider mapping all update automatically.

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

### SNS MessageAttributes pass-through

`SNS.Publish` extracts `MessageAttributes` from the Query protocol form (`MessageAttributes.entry.N.{Name,Value.DataType,Value.StringValue}`) via `SNSCodec.Decode`. The provider passes them through in the SQS envelope JSON under `"MessageAttributes"` so downstream consumers can read them after receiving from SQS.

### DynamoDB pk hash

`TableProvider` computes a stable hash from key attributes only (in schema-defined order) and passes it explicitly to `DynamoDBItemStore`. The store never auto-computes hashes — the provider is the sole authority.

### DynamoDB pagination determinism

Both `MemoryDynamoDBItemStore` and `PostgresDynamoDBItemStore` sort all matching items by `itemPKHash` (a stable string key derived from sorted attribute name=value pairs) before applying `ExclusiveStartKey` / `Limit`. This guarantees consistent cursor behaviour across requests regardless of map iteration order or Postgres heap scan order. The postgres implementation uses `ORDER BY pk_hash` in SQL and delegates page slicing to the same `paginateItems` helper used by the memory store.

### DynamoDB wire protocol: x-amz-crc32

Every DynamoDB response **must** include `x-amz-crc32: <crc32_of_body>`. AWS SDK v2 validates this header; without it the SDK does not cleanly drain the response body, causing a "failed to close HTTP response body" warning and potential connection-reuse problems. `DynamoDBCodec.Encode` computes and sets this header on every response.

### Postgres SQS: composite primary key

`jc_sqs_messages` uses a composite primary key `(id, queue_url)` so the same `MessageID` can appear in multiple queues (e.g. SNS fan-out). Migration `005_sqs_fix_pk.sql` upgrades existing installs that have the old single-column `id` primary key.

`MessageAttributes` are stored in the `msg_attributes JSONB` column. The postgres `Send` serialises them; `Receive` deserialises them. The column exists in migration 002 but was unused before Phase 1 fixes.

### Postgres connection pool (HikariCP equivalent)

`NewPostgresResourceStore` configures `pgxpool` with production-ready defaults before the first ping:

| Setting | Value | Purpose |
|---|---|---|
| `MaxConns` | 40 | Hard cap on open connections |
| `MinConns` | 2 | Keep-alive connections |
| `MaxConnLifetime` | 30 min | Recycle connections to avoid server-side stale limits |
| `MaxConnIdleTime` | 10 min | Release idle connections under low load |
| `HealthCheckPeriod` | 30 s | Proactive dead-connection detection |
| `ConnectTimeout` | 5 s | Per-attempt TCP timeout |

**Startup retry:** the server retries the initial `Ping` up to 10 times with exponential backoff (500 ms → 8 s) and logs a `WARN` on each attempt. This lets JaisCloud start before the database is ready (e.g., `docker-compose` spin-up order, Kubernetes init ordering). Context cancellation (SIGINT during startup) stops the retry loop immediately.

### Postgres error classification

`wrapPgError` in `store/postgres.go` maps raw pgx errors to typed store sentinels:

| pgx error | Store sentinel |
|---|---|
| `pgx.ErrNoRows` | `store.ErrNotFound` |
| Unique violation (23505) | `store.ErrAlreadyExists` |
| Connection class 08xx / 57xx, `net.Error` | `store.ErrStorageUnavailable` |

Providers call `provider.StoreNotFoundError(err, code, msg)` after every store read. This helper returns a 400 `ProviderError` only when `errors.Is(err, store.ErrNotFound)` is true. Any other error (including `ErrStorageUnavailable`) is returned unwrapped so the gateway emits a **500 Internal Error** instead of a misleading 404.

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

### Prometheus metrics: cloud label

The Metrics middleware (`internal/gateway/middleware/metrics.go`) records `jaiscloud_requests_total{cloud,service,action,status}` and `jaiscloud_request_duration_seconds{cloud,service,action}`.

Labels are injected by the gateway after decoding each request:

```go
r = r.WithContext(middleware.WithRequestLabels(r.Context(), string(nr.Cloud), nr.Service, nr.Action))
```

Requests without labels (admin endpoints, unrecognised paths) fall back to `cloud="unknown"`, `service="unknown"`, `action=<HTTP method>`.

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
- `internal/config/config.go` — `awsARNFormatters` map + `AWSResourceID(region, accountID)` returns the AWS implementation; `AzureResourceID` / `GCPResourceID` return stub functions that return the name unchanged
- `internal/gateway/server.go` — injects `nr.ResourceID` for **all clouds** via a switch (AWS gets full ARN formatters; Azure/GCP get their stub functions); `nr.ResourceID` is therefore always non-nil after the gateway
- `internal/provider/*/` — calls `nr.ResourceID("type", name)` unconditionally; no `"arn:aws:"` literals, no nil checks

**Adding a new resource type:** add one entry to `awsARNFormatters` in `config.go`:
```go
"my-service-resource": func(r, a, n string) string {
    return fmt.Sprintf("arn:aws:myservice:%s:%s:resource/%s", r, a, n)
},
```
An Azure adapter injects its own function that formats Azure resource IDs; providers don't need to change.

The same DI principle applies to any other cloud-specific customisation point: define an interface or function type in `internal/model/`, implement it per cloud in the adapter/config layer, and inject it via `NormalizedRequest` or the provider constructor.

---

### EventBridge conventions

**Wire protocol:** `X-Amz-Target: AmazonCloudWatchEvents.<Action>` (JSON/Target). Detected via the `TargetPrefix` in `services.go`.

**Rule storage:** `eb_rule` resource type. Key = rule name.

**Target storage:** `eb_target` resource type. Key = `"<ruleName>/<targetId>"` so all targets for a rule are listed with a single `List(ctx, "eb_target", ruleName+"/")` call.

**Target type resolution (`resolveTargetMeta`):** ARN parsing happens **once at `PutTargets` time** where `nr.Cloud` is available. The resolved `TargetType` (`"sqs"`) and `QueueName` are stored in `targetData`. The delivery path (`deliverToTarget`) is cloud-agnostic — it reads only pre-resolved fields.

**Event delivery:** `deliverEvent` lists all `ENABLED` rules, matches the event envelope against `EventPattern` using `matchesPattern`, then calls `deliverToTarget` for each matching target.

**EMR integration:** `EMRProvider.CancelSteps` and `EMRContainersProvider.CancelJobRun` publish `EventEMRStepState` / `EventEMRJobRunState` on the `EventBus`. `EventBridgeProvider.subscribeToEventBus` subscribes at construction time and converts these domain events into EventBridge envelopes. The event `source` is derived as `string(ev.Cloud) + ".emr"` — not hardcoded — so it is correct for any cloud.

**`PutEvents` action:** allows tests and callers to inject arbitrary events directly into the rule-matching pipeline without triggering EMR state changes.

**`atomic.Uint64` event counter:** `newEventID()` uses `eventCounter.Add(1)` — thread-safe without a mutex.

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
| `github.com/aws/aws-sdk-go-v2/service/eventbridge` | EventBridge integration test client |
| `github.com/stretchr/testify` | Test assertions |
| `github.com/jaiscloud/plugin-sdk` | Plugin SDK (local `replace` → `./sdk`) |
