# JaisCloud — Developer Reference

## Project

JaisCloud is a local multi-cloud emulator written in Go. It speaks native AWS wire protocols (Query/XML, JSON/Target, REST) so any AWS SDK can point at it without modification.

**Implemented services:** SQS, SNS, IAM/STS, DynamoDB (+ Streams), S3, Lambda, KMS, SecretsManager, SSM, API Gateway, CloudFormation, EMR (on EC2), EMR on EKS, EventBridge, CloudWatch, EKS, EC2, Route53, RDS, ElastiCache, ECS, Glue.

---

## Module

```
module jaiscloud   # go.mod
go 1.26.2
```

---

## Directory layout

```
cmd/jaiscloud/          # main.go — wires everything together, Cobra CLI
docs/                   # Architecture, LLD, design documents
internal/
  adapter/              # Cloud wire-protocol layer (no business logic)
    adapter.go          # CloudAdapter interface; Codec interface
    aws/
      aws.go            # AWSAdapter
      router.go         # DetectService — data-driven via services.go
      services.go       # ServiceDescriptor + awsServices registry (single source of truth)
                        #   Add one entry here to register a new service.
                        #   Derived maps at init(): targetPrefixToService, knownSigV4Services,
                        #   actionToService, serviceProviderMap
      services/         # Per-service Codec implementations (Decode/Encode)
    azure/azure.go      # AzureAdapter stub (501)
    gcp/gcp.go          # GCPAdapter stub (501)
  admin/                # /_jaiscloud/* endpoints; Resetter, Snapshotter interfaces
  blobfs/               # BlobStore (Memory/LocalFS); BlobFetcher (S3BlobFetcher)
  clock/                # Clock interface: RealClock, FixedClock, OffsetClock
  config/               # Config struct; Viper loading; env prefix JAISCLOUD_
  events/               # In-process EventBus (subscribe/publish)
  gateway/              # HTTP server (Chi), middleware, request dispatch
    server.go           # Server — holds single CloudAdapter; handleCloudRequest
    middleware/         # Recovery, RequestID, Logging, Metrics
  k8shelpers/           # Generic K8s helpers (BuildPodSpec, IdentityMutator, OwnershipPatcher)
  model/                # Shared types: NormalizedRequest, ProviderResponse, ProviderError
  platform/             # PlatformConfig — TLS init containers, env fragments, volume mounts
  provider/             # Business logic layer
    provider.go         # HandlerFunc type, OK() helper
    registry.go         # Registry — Dispatch (exact match → error)
    aws/
      apigw/            # APIGatewayProvider
      cache/            # ElastiCache (metadata only)
      cloudwatch/       # CloudWatchProvider — metrics ring, alarms
      compute/          # EC2 (metadata only)
      container/        # ECS (metadata only)
      dns/              # Route53 (metadata only)
      eks/              # EKS (metadata only)
      emr/              # EMRProvider — RunJobFlow, steps, bootstrap, Spark K8s/Docker
      emroneks/         # EMRContainersProvider — virtual clusters, job runs
      iam/              # IAMProvider + STS
      rds/              # RDS (metadata only)
      sparkaws/         # AWS emulator wiring injected into Spark driver pods
      stack/            # CloudFormation — CreateStack, intrinsics, topsort, dispatch
    catalog/            # Glue Data Catalog provider
    events/             # EventBridgeProvider
    function/           # FunctionProvider — Lambda
    notification/       # SNSProvider
    object/             # ObjectProvider — S3
    queue/              # QueueProvider — SQS (all 17 operations)
    table/              # TableProvider — DynamoDB
    key/                # KeyProvider — KMS
    secret/             # SecretProvider — SecretsManager
    param/              # ParameterProvider — SSM
  resourcemgr/          # Deletion guards: CheckParent, AcquireDelete, DeleteGuardRule
  sparkhelpers/         # Spark-specific K8s helpers: SubmitClientMode, ClientModeJob,
                        # BuildExecutorTemplate, OwnershipPatcher, ClassifyTerminal
  store/                # ResourceStore interface + memory/postgres implementations
    migrations/         # SQL migration files (001–013)
    aws/
      dynamodb/         # DynamoDBItemStore (memory + postgres)
      s3/               # S3ObjectMetaStore (memory + postgres)
      sqs/              # SQSMessageStore (memory + postgres)
      stream/           # MemoryStreamStore (DynamoDB Streams)
tests/
  integration/          # End-to-end tests using aws-sdk-go-v2 (SQS, IAM, SNS, DynamoDB, S3, Lambda)
  full_mode/aws/        # Full-mode e2e tests (build-tagged): lambda, cfn, kms, emr, emrcontainers,
                        # eventbridge, dpc, iceberg
```

---

## Architecture

### Request flow

```
HTTP request
  → gateway.Server.handleCloudRequest
      → cloudAdapter.DetectAndDecode     (single adapter selected at startup from cfg.Cloud)
          → <ServiceCodec>.Decode
      → inject: nr.Clock, nr.Region, nr.AccountID, nr.Cloud, nr.ResourceID
      → middleware.WithRequestLabels(ctx, cloud, service, action)
      → Registry.Dispatch("ProviderPrefix.Action", nr)
      → <ServiceCodec>.Encode
  → HTTP response
```

Key design rules:
- **No layer imports its caller.** `model` package breaks the cycle between `gateway` and `adapter`.
- **Single cloud per instance.** `cfg.Cloud` set once at startup; no per-request cloud detection.
- **Executors are wired at startup.** `JAISCLOUD_EXECUTOR_MODE` controls the container orchestrator for Spark and Lambda.
- **AWS providers live under `internal/provider/aws/`.** Cloud-agnostic providers live directly under `internal/provider/`.

### CloudAdapter interface

```go
type CloudAdapter interface {
    Cloud() model.Cloud
    DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, Codec, error)
    ServiceToProvider(service string) string
}
```

`ServiceToProvider` maps wire service name (e.g. `"sqs"`) to provider registry prefix (e.g. `"Queue"`). AWS delegates to `serviceProviderMap` from `services.go`; Azure/GCP stubs return the service name unchanged.

### Service → Provider mapping

Defined once in `internal/adapter/aws/services.go` — no switch statement anywhere.

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
| `eks` | `EKS` | EKSCodec (REST/JSON) |
| `cloudformation` | `CloudFormation` | CloudFormationCodec (Query/XML) |
| `monitoring` | `CloudWatch` | CloudWatchCodec (form-body + Granite path) |
| `emr` | `EMR` | EMRCodec (JSON/Target) |
| `emr-containers` | `EMRContainers` | EMRContainersCodec (REST/JSON) |
| `events` | `EventBridge` | EventBridgeCodec (JSON/Target) |
| `kms` | `KMS` | KMSCodec (JSON/Target) |
| `secretsmanager` | `SecretsManager` | SecretsManagerCodec (JSON/Target) |
| `ssm` | `SSM` | SSMCodec (JSON/Target) |
| `apigateway` | `APIGateway` | APIGatewayCodec (REST/JSON) |

**Adding a new service:** add one `ServiceDescriptor` entry to `awsServices` in `services.go`. Detection, SigV4 allow-list, Action routing, and provider mapping all update automatically.

### Service detection order (router.go)

1. `X-Amz-Target` header → `targetPrefixToService` (JSON/Target services)
2. SigV4 `Authorization` scope → `knownSigV4Services`
3. `Action=` query/body param → `actionToService` (Query-protocol services)
4. Granite path `/service/<sigv4name>/operation/<action>` — AWS SDK v2 CloudWatch

---

## Backend resources by mode

`JAISCLOUD_EXECUTOR_MODE`: `""` / `mock` / `docker` / `k8s` — applies to both Spark (EMR) and Lambda.

| Service | Lite (default) | Full (`--mode full`) | `docker` executor | `k8s` executor |
|---|---|---|---|---|
| SQS / SNS / IAM / STS / KMS / SecretsManager / SSM | In-memory | PostgreSQL | PostgreSQL | PostgreSQL |
| DynamoDB + Streams | In-memory | PostgreSQL | PostgreSQL | PostgreSQL |
| S3 | In-memory + MemoryBlobStore | PostgreSQL + LocalFSBlobStore (`~/.jaiscloud/blobs`) | same | same |
| Lambda | Echo (mock) | Echo (mock) | Docker container per function (warm pool) | K8s Pod + Service per function |
| EMR on EC2 steps | Instant COMPLETED | Instant COMPLETED | Docker container per step | K8s batch/v1 Job per step |
| EMR on EKS job runs | Instant COMPLETED | Instant COMPLETED | Docker container per job | K8s batch/v1 Job per job |
| CloudWatch | In-memory ring + alarms | In-memory ring + PostgreSQL alarms | — | — |
| EC2 / Route53 / EKS / RDS / ElastiCache / ECS | Metadata only | Metadata only | — | — |
| API Gateway | In-memory | PostgreSQL | — | — |
| CloudFormation | In-memory + real dispatch | PostgreSQL + real dispatch | — | — |
| Glue / EventBridge | In-memory | PostgreSQL | — | — |

---

## Build & run

```bash
# Always rebuild after code changes
go build -o jaiscloud ./cmd/jaiscloud/

# Lite mode (default, port 4566)
./jaiscloud start

# Full mode (PostgreSQL persistence)
./jaiscloud start --mode full --dsn "postgres://user:pass@localhost:5433/jaiscloud"

# Docker-compose (postgres on 5433, jaiscloud on 4566)
make up-docker
make down-docker

# K8s executors (Spark + Lambda)
JAISCLOUD_EXECUTOR_MODE=k8s ./jaiscloud start --mode full --dsn "postgres://..."

# Docker executors
JAISCLOUD_EXECUTOR_MODE=docker ./jaiscloud start --mode full --dsn "postgres://..."

# Prometheus metrics (at /metrics)
./jaiscloud start --metrics
```

### Key environment variables

```bash
JAISCLOUD_PORT=4566
JAISCLOUD_MODE=lite                  # or "full"
JAISCLOUD_CLOUD=aws                  # or "azure", "gcp"
JAISCLOUD_DSN=                       # required when MODE=full
JAISCLOUD_REGION=us-east-1
JAISCLOUD_ACCOUNT_ID=000000000000
JAISCLOUD_LOG_LEVEL=info
JAISCLOUD_EXECUTOR_MODE=             # "" | mock | docker | k8s
JAISCLOUD_LAMBDA_IMAGE=              # override default Lambda runtime image
JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300
JAISCLOUD_KMS_MASTER_KEY=            # 32-byte hex KEK; if unset DEK stored plaintext
JAISCLOUD_S3_VIRTUAL_HOST_BASES=     # comma-separated host suffixes
JAISCLOUD_IMDS_ENABLED=false
JAISCLOUD_AWS_EMULATOR_ENDPOINT=     # JaisCloud endpoint reachable from Spark pods
JAISCLOUD_SPARK_K8S_CLUSTER_MODE=auto   # auto | always | never
JAISCLOUD_INSTANCE_ID=               # override auto-generated instance UUID
```

> **Config loading:** all `viper.BindPFlag(...)` calls in `startCmd` must use the global `viper` package (not `viper.New()`) or flags are silently ignored.

### CLI commands

| Command | Description |
|---|---|
| `start` | Start the emulator |
| `version` | Print version |
| `env` | Print effective config as env vars |
| `doctor` | Check emulator reachability |
| `reset` | Wipe all state via HTTP |
| `export [-o file] [--strip-kek]` | Export state snapshot |
| `import [-i file]` | Restore state from snapshot |
| `rotate-master-key --new-key <hex>` | Re-wrap KMS DEK with new KEK |
| `services` | Print service implementation levels |

---

## Tests

```bash
# Unit + store tests
go test -race ./internal/...

# Integration tests — lite mode
./jaiscloud start &
go test -race ./tests/integration/

# Full-mode e2e (docker-compose handles server + postgres)
make test-e2e-lambda-docker      # tag: lambda_e2e
make test-e2e-lambda-k8s         # tag: lambda_e2e
make test-e2e-emr-docker         # tag: spark_e2e
make test-e2e-emrcontainers-k8s  # tag: spark_e2e
make test-e2e-cloudformation     # tag: cfn_fullmode
make test-e2e-kms                # tag: kms_fullmode
make test-e2e-eventbridge        # tag: spark_e2e
make test-e2e-iceberg            # tag: iceberg_e2e
```

Full-mode e2e tests live under `tests/full_mode/aws/{lambda,cloudformation,kms,emr,emrcontainers,eventbridge,dpc,iceberg}/`.

Integration tests call `POST /_jaiscloud/reset` between each test via `resetState(t)`.

---

## Key conventions

### Multi-cloud: never hardcode ARN formats

Providers must use `nr.ResourceID("type", name)` — never `fmt.Sprintf("arn:aws:...")`.

- `model.NormalizedRequest.ResourceID` — injected by gateway, always non-nil
- `config.awsARNFormatters` — add one entry per new resource type
- Azure/GCP adapters inject their own formatters at startup

### DynamoDB x-amz-crc32

Every DynamoDB response **must** include `x-amz-crc32: <crc32_of_body>`. AWS SDK v2 validates it; missing → SDK fails to drain body. Set in `DynamoDBCodec.Encode`.

### S3 delete ordering

`DeleteObject`/`DeleteObjects` delete **metadata before blob**. `GetObject` rechecks metadata when blob is missing: if metadata gone → 404 (concurrent delete); metadata present + blob absent → 500 (corruption).

### HTTP Content-Length

`gateway.writeResponse` sets `Content-Length` explicitly before `WriteHeader`. Without it, Go uses chunked encoding, causing AWS SDK body-close warnings.

### SQS FIFO deduplication

`MemoryMessageStore.Send` returns `(dedupMessageID string, err error)`. Non-empty `dedupMessageID` means duplicate — return the original `MessageId`.

### SNS fan-out

Each SQS delivery gets a **new unique `MessageID`**. The SNS notification ID is in the envelope body only.

### DynamoDB pagination

Both memory and postgres stores sort items by `itemPKHash` before applying cursor/limit. `LastEvaluatedKey` is set when `len(page) == limit` (page full), not when pre-filter count equals limit.

### EMR goroutine lifecycle

EMR/EMRContainers providers capture `handlerCtx{cloud, region, accountID}` at handler entry so background goroutines can publish events without holding `NormalizedRequest`. All step/job goroutines use `p.wg.Add(1)` + `defer p.wg.Done()`; `Shutdown()` calls `p.cancel()` then `p.wg.Wait()`.

### Spark conf precedence

`BuildClientModeArgs` emits: fixed JaisCloud confs → `ExtraSparkConfs` → `SparkSubmitArgs`. Spark last-value-wins, so user `SparkSubmitArgs` always override JaisCloud defaults.

### Admin endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | Liveness check |
| `/_jaiscloud/reset` | POST | Wipe all state |
| `/_jaiscloud/export` | GET | JSON state snapshot |
| `/_jaiscloud/import` | POST | Restore from snapshot |
| `/metrics` | GET | Prometheus (requires `--metrics`) |
