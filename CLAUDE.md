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
    adapter.go          # CloudAdapter interface (Cloud, DetectAndDecode, ServiceToProvider); Codec interface
    aws/
      aws.go            # AWSAdapter — Cloud(), DetectAndDecode(), ServiceToProvider()
      router.go         # DetectService — data-driven via services.go
                        #   Priority 1: X-Amz-Target header
                        #   Priority 2: SigV4 Authorization scope
                        #   Priority 3: Action= query/body param
                        #   Priority 4: Granite path /service/<ver>/operation/<Action> (CloudWatch SDK v2)
      services.go       # ServiceDescriptor + awsServices registry — single source of truth for all
                        #   service metadata (SigV4Name, TargetPrefix, QueryActions, ProviderPrefix).
                        #   Derived maps built at init(): targetPrefixToService, knownSigV4Services,
                        #   actionToService, serviceProviderMap. Add one entry here to register a service.
      services/
        sqs.go          # SQSCodec: JSON + Query/XML
        iam.go          # IAMCodec: Query/XML (handles STS too)
        sns.go          # SNSCodec: Query/XML
        dynamodb.go     # DynamoDBCodec: JSON/Target
        s3.go           # S3Codec: REST path-style + virtual-hosted-style, XML responses
        lambda.go       # LambdaCodec: REST JSON
        glue.go         # GlueCodec: JSON/Target
        ec2.go          # EC2Codec: Query/XML
        route53.go      # Route53Codec: REST/XML
        rds.go          # RDSCodec: Query/XML
        elasticache.go  # ElastiCacheCodec: Query/XML
        ecs.go          # ECSCodec: JSON/Target
        eks.go          # EKSCodec: REST/JSON
        dynamodbstreams.go # DynamoDBStreamsCodec: JSON/Target
        cloudformation.go  # CloudFormationCodec: Query/XML
        cloudwatch.go   # CloudWatchCodec: form-body + Granite path Action extraction
        emr.go          # EMRCodec: JSON/Target
        emrcontainers.go   # EMRContainersCodec: REST/JSON
        eventbridge.go  # EventBridgeCodec: JSON/Target (X-Amz-Target: AWSEvents.*)
        kms.go          # KMSCodec: JSON/Target
        secretsmanager.go  # SecretsManagerCodec: JSON/Target
        ssm.go          # SSMCodec: JSON/Target
        apigateway.go   # APIGatewayCodec: REST JSON (management plane path routing)
        executeapi.go   # ExecuteAPICodec: execute-api invoke plane
    azure/
      azure.go          # AzureAdapter stub — Cloud(), DetectAndDecode() (501), ServiceToProvider() (passthrough)
    gcp/
      gcp.go            # GCPAdapter stub — Cloud(), DetectAndDecode() (501), ServiceToProvider() (passthrough)
  admin/                # /_jaiscloud/* endpoints
                        # Resetter, Snapshotter interfaces
  blobfs/               # BlobStore interface: MemoryBlobStore, LocalFSBlobStore
                        # BlobFetcher interface: S3BlobFetcher (fetches bootstrap scripts by s3:// URI)
  clock/                # Clock interface: RealClock, FixedClock, OffsetClock
  config/               # Config struct; Viper loading; env prefix JAISCLOUD_
  events/               # In-process EventBus (subscribe/publish)
  gateway/              # HTTP server (Chi), middleware, request dispatch
    server.go           # Server — holds single CloudAdapter; handleCloudRequest
    middleware/         # Recovery, RequestID, Logging, Metrics (Prometheus + cloud label)
  k8shelpers/           # Generic K8s helpers (no Spark/EMR concepts)
    platform_overlay.go # BuildPodSpec — layered pod spec assembly; IdentityMutator callback type
    ownership_patcher.go # StartOwnershipPatcher — watches executor pods, patches ownerReferences
  model/                # Shared types: NormalizedRequest, ProviderResponse, ProviderError
  platform/             # PlatformConfig — TLS init containers, env fragments, volume mounts
  provider/             # Business logic layer
    provider.go         # HandlerFunc type, OK() helper
    registry.go         # Registry — Dispatch (exact match → error)
    aws/                # AWS-specific provider implementations
      apigw/            # APIGatewayProvider — REST API management plane + execute-api invoke
      cache/            # ElastiCache provider (metadata only)
      cloudwatch/       # CloudWatchProvider — metrics (in-memory ring), alarms
      compute/          # EC2 provider (metadata only)
      container/        # ECS provider (metadata only)
      dns/              # Route53 provider (metadata only)
      eks/              # EKS provider (metadata only)
      emr/              # EMRProvider — RunJobFlow, steps, tags
                        #   bootstrap.go  — Resolve() fetches + scrubs bootstrap scripts → init containers
                        #   events.go     — handlerCtx, emitStepStateChange, emitClusterStateChange
                        #   spark_step.go — runSparkSubmitStep via sparkhelpers.SubmitClientMode
                        #   step_dispatch.go — runStep dispatcher (spark vs. generic stub)
                        #   rehydrate.go  — rehydratePoller() re-tracks non-terminal steps on startup
      emroneks/         # EMRContainersProvider — virtual clusters, job runs
                        #   events.go  — handlerCtx, emitJobRunStateChange
                        #   jobrun.go  — runJobRun via sparkhelpers.SubmitClientMode
      iam/              # IAMProvider + STS (roles, policies, users, access keys)
      rds/              # RDS provider (metadata only)
      stack/            # CloudFormation provider
        cloudformation.go # StackProvider — CreateStack, UpdateStack, DeleteStack, Describe, List
        intrinsics.go   # CloudFormation intrinsic function resolver (Ref, Fn::*, conditions)
        topsort.go      # Kahn's topological sort for DependsOn + implicit Ref/GetAtt deps
        dispatch.go     # CFNResourceDispatcher — per-resource-type create/delete handlers
    catalog/            # Glue Data Catalog provider
    events/             # EventBridgeProvider — rules, targets, event delivery to SQS
    function/           # FunctionProvider — Lambda (echo/Docker/K8s invoke)
    notification/       # SNSProvider (topics, subscriptions, fan-out to SQS)
    object/             # ObjectProvider — S3 (buckets, objects, multipart)
    queue/              # QueueProvider — SQS (all 17 operations)
    table/              # TableProvider — DynamoDB (tables, items, expressions, streams)
    key/                # KeyProvider — KMS (keys, aliases, grants, envelope crypto, rotation)
    secret/             # SecretProvider — SecretsManager (secrets, versions, rotation)
    param/              # ParameterProvider — SSM Parameter Store (put/get/history/path)
  resourcemgr/          # Deletion guards and parent-existence checks
    manager.go          # Manager — CheckParent, AcquireDelete, RegisterRules, Reset
    deletionlock.go     # DeletionLock — thread-safe per-resource deletion marks
    adapter.go          # StoreAdapter — bridges store.ResourceStore → resourcemgr.ResourceStore
  sparkhelpers/         # Spark-specific helpers built on top of k8shelpers
    client_mode.go      # SubmitClientMode, WaitTerminal, ClientModeJob, Final
    emr_yarn_translate.go # TranslateEMREC2YarnArgs — YARN master → k8s client mode
    executor_template.go  # BuildExecutorTemplate — merged executor pod-template YAML
    patcher.go          # MakeExecutorOwnerResolver — closure for executor pod ownership lookup
    terminal_classify.go  # ClassifyTerminal — maps pod exit codes to SparkReason strings
    types.go            # Handle, OwnerRefHint, shared types
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
  full_mode/aws/        # Full-mode e2e tests (build-tagged)
```

---

## Architecture

### Request flow

```
HTTP request
  → gateway.Server.handleCloudRequest
      → cloudAdapter.DetectAndDecode     (single adapter selected at startup from cfg.Cloud)
          → <ServiceCodec>.Decode        (SQS/IAM/SNS → Query/XML; DynamoDB → JSON/Target;
                                          S3/Lambda → REST; CloudWatch → form-body or Granite path)
      → inject: nr.Clock, nr.Region, nr.AccountID, nr.Cloud, nr.ResourceID (all clouds)
      → middleware.WithRequestLabels(ctx, cloud, service, action)
      → Registry.Dispatch("ProviderPrefix.Action", nr)
          → exact match: built-in provider handler
              → EMRProvider / EMRContainersProvider
                  → sparkhelpers.SubmitClientMode (K8s executor)
                  → k8shelpers.BuildPodSpec (platform overlay + identity mutator)
      → <ServiceCodec>.Encode            (XML or JSON or raw bytes)
  → HTTP response
```

Key design rules:
- **No layer imports its caller.** The `model` package breaks the cycle between `gateway` and `adapter`.
- **Single cloud per instance.** `cfg.Cloud` is set once at startup; one `CloudAdapter` is constructed; no per-request cloud detection.
- **Executors are wired at startup.** `JAISCLOUD_EXECUTOR_MODE` controls the container orchestrator for both Spark and Lambda executors.
- **AWS providers live under `internal/provider/aws/`.** Cloud-agnostic providers (SQS, DynamoDB, S3, Lambda, EventBridge, etc.) live directly under `internal/provider/`.

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
| `eks` | `EKS` | EKSCodec (REST/JSON) |
| `cloudformation` | `CloudFormation` | CloudFormationCodec (Query/XML) |
| `monitoring` | `CloudWatch` | CloudWatchCodec (form-body + Granite path) |
| `emr` | `EMR` | EMRCodec (JSON/Target) |
| `emr-containers` | `EMRContainers` | EMRContainersCodec (REST/JSON) |
| `events` | `EventBridge` | EventBridgeCodec (JSON/Target) |
| `kms` | `KMS` | KMSCodec (JSON/Target) |
| `secretsmanager` | `SecretsManager` | SecretsManagerCodec (JSON/Target) |
| `ssm` | `SSM` | SSMCodec (JSON/Target) |
| `apigateway` | `APIGateway` | APIGatewayCodec (REST/JSON, path-based routing) |

---

## Backend resources by mode

This section documents what real infrastructure resources JaisCloud creates for each service depending on the runtime mode and executor configuration. "Metadata only" means the service stores resource definitions in-memory or in PostgreSQL but does **not** spin up any real compute.

### Mode matrix

Executor behaviour is controlled by a single env var: `JAISCLOUD_EXECUTOR_MODE` (`""` / `mock` / `docker` / `k8s`). It applies globally to both Spark (EMR) and Lambda.

| Service | Lite (default) | Full (`--mode full`) | `EXECUTOR_MODE=docker` | `EXECUTOR_MODE=k8s` |
|---|---|---|---|---|
| SQS / SNS / IAM / STS / KMS / SecretsManager / SSM | In-memory maps | PostgreSQL rows | PostgreSQL rows | PostgreSQL rows |
| DynamoDB + Streams | In-memory maps | PostgreSQL rows | PostgreSQL rows | PostgreSQL rows |
| S3 | In-memory maps + `MemoryBlobStore` | PostgreSQL rows + `LocalFSBlobStore` (default `~/.jaiscloud/blobs`) | same | same |
| Lambda | Echo response (mock) | Echo response (mock) | **Docker container** per function (warm pool) | **K8s Pod + ClusterIP Service** per function (warm pool) |
| EMR on EC2 steps | Instant `COMPLETED` (mock) | Instant `COMPLETED` (mock) | **Docker container** per step | **K8s `batch/v1 Job`** per step |
| EMR on EKS job runs | Instant `COMPLETED` (mock) | Instant `COMPLETED` (mock) | **Docker container** per job run | **K8s `batch/v1 Job`** per job run |
| CloudWatch | In-memory metric ring + alarm store | In-memory metric ring + PostgreSQL alarms | — | — |
| EC2 / VPC / Route53 / EKS | Metadata only | Metadata only | — | — |
| RDS | Metadata only | Metadata only | — | — |
| ElastiCache | Metadata only | Metadata only | — | — |
| ECS | Metadata only | Metadata only | — | — |
| API Gateway | In-memory resource store | PostgreSQL rows | — | — |
| CloudFormation | In-memory stack store + real resource dispatch | PostgreSQL rows + real resource dispatch | — | — |
| Glue / EventBridge | In-memory maps | PostgreSQL rows | — | — |

> \* `LocalFSBlobStore` is implemented and wired in `main.go` when `--blob-dir` is set. Without `--blob-dir`, `MemoryBlobStore` is used even in full mode and S3 blobs are lost on restart.

---

### Lambda — Docker executor (`JAISCLOUD_EXECUTOR_MODE=docker`)

Each distinct function gets **one warm Docker container** that is reused across invocations until it is idle beyond the keep-alive timeout.

```
docker run -d
  --name jc-lambda-{shortInstanceID}-{functionName}
  --network jaiscloud-net
  -p {hostPort}:{INVOCATION_PORT}
  -e AWS_LAMBDA_FUNCTION_NAME={name}
  -e AWS_DEFAULT_REGION={region}
  -e JAISCLOUD_ENDPOINT=http://host.docker.internal:{port}
  -e {user env vars...}
  --memory {memMB}m
  {runtimeImage}
```

**Container lifecycle:**

| Trigger | Action |
|---|---|
| First `InvokeFunction` for a function | `docker run` (cold start) |
| Subsequent invocations | HTTP POST to warm container port (reuse) |
| `DeleteFunction` | `docker stop` + `docker rm` |
| Idle > `JAISCLOUD_LAMBDA_KEEPALIVE_SECS` (default 300 s) | GC goroutine stops container |
| Server shutdown (`Close()`) | All warm containers stopped |

**Runtime → image mapping** (override with `JAISCLOUD_LAMBDA_IMAGE` or per-function `ImageUri`):

| Runtime | Default image |
|---|---|
| `python3.12` | `public.ecr.aws/lambda/python:3.12` |
| `nodejs20.x` | `public.ecr.aws/lambda/nodejs:20` |
| `java21` | `public.ecr.aws/lambda/java:21` |
| `go1.x` / `provided.al2` | `public.ecr.aws/lambda/provided:al2` |

---

### Lambda — K8s executor (`JAISCLOUD_EXECUTOR_MODE=k8s`)

Each distinct function gets **one warm Pod + ClusterIP Service** that is reused across invocations until idle beyond the keep-alive timeout. Invocations POST to the Lambda RIE endpoint (`/2015-03-31/functions/function/invocations`, port 8080) on the ClusterIP Service.

```
Pod name:     jc-lambda-{instanceID[:8]}-{sanitized-name}   (namespace: jaiscloud)
Service name: jc-lambda-{instanceID[:8]}-{sanitized-name}   (ClusterIP, port 8080)
Labels:       app=jaiscloud-lambda, function={name}, jaiscloud.io/instance-id={instanceID}
```

**Pod/Service lifecycle:**

| Trigger | Action |
|---|---|
| First `InvokeFunction` for a function | Create Pod + ClusterIP Service (cold start); wait Ready (90 s timeout) |
| Subsequent invocations | HTTP POST to ClusterIP endpoint (reuse) |
| `DeleteFunction` | Delete Pod + Service |
| Idle > `JAISCLOUD_LAMBDA_KEEPALIVE_SECS` (default 300 s) | GC goroutine deletes Pod + Service |
| Server shutdown (`Close()`) | All warm Pods + Services deleted |
| Server restart (startup `cleanupOrphans`) | Instance-scoped `app=jaiscloud-lambda` Pods + Services deleted |

---

### EMR on EC2 steps — K8s executor (`JAISCLOUD_EXECUTOR_MODE=k8s`)

Each EMR step (`AddJobFlowSteps`) that reaches `RUNNING` state submits a **`batch/v1 Job`** via `spark-submit` in cluster deploy mode:

```yaml
kind: Job
metadata:
  name: jc-spark-{clusterID[:8]}-{stepID[:8]}
  namespace: {SparkConfig.Namespace}   # default: "jaiscloud"
  labels:
    app: jaiscloud-spark
    jaiscloud.io/instance-id: {instanceID}
  annotations:
    jaiscloud.io/job-id-raw: {stepID}
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: {SparkConfig.ServiceAccount}
      containers:
        - name: spark-submit
          image: {SparkConfig.Image}
          command: ["spark-submit"]
          args:
            - --master k8s://{apiServer}
            - --deploy-mode cluster
            - --conf spark.kubernetes.container.image={SparkConfig.Image}
            - --conf spark.kubernetes.namespace={namespace}
            - {user --conf entries from step args}
            - {jar URI}
            - {step args...}
```

The `StatusPoller` goroutine polls the Job at a configurable interval and fires `OnStateChange` to update the EMR step state (`PENDING → RUNNING → COMPLETED / FAILED`).

**Bootstrap actions:** When `RunJobFlow` includes `BootstrapActions` and `EXECUTOR_MODE=k8s`, the EMR provider calls `bootstrap.Resolve()` before submitting. Each bootstrap action becomes a K8s init container that runs before `spark-submit`:

```yaml
initContainers:
  - name: bootstrap-{sanitized-action-name}
    image: amazon/aws-cli:2.18          # JAISCLOUD_BOOTSTRAP_IMAGE
    command: ["/bin/sh", "-c"]
    args: ["printf '%s' '<b64-script>' | base64 -d | /bin/sh -s -- <args>"]
    securityContext:
      runAsUser: 0
    volumeMounts:
      - name: bootstrap-prefix-etc-pki
        mountPath: /etc/pki
      - name: bootstrap-prefix-home-hadoop
        mountPath: /home/hadoop
```

One `emptyDir` volume is created per prefix (`JAISCLOUD_BOOTSTRAP_RELOCATE_PREFIXES`, default `/etc/pki,/home/hadoop`) and mounted into both init containers and the main `spark-submit` container. Host-only commands (`yum`, `apt-get`, `dnf`, `rpm`, `systemctl`, `service`, `chkconfig`, `update-rc.d`) are automatically commented out with `# [jaiscloud-skip]`.

**Suspend/resume lifecycle:** `K8sExecutor.Close()` suspends all tracked Jobs (`spec.suspend: true`) instead of deleting them, preserving partial progress across server restarts. On startup, `cleanupOrphans()` lists all `app.kubernetes.io/managed-by=jaiscloud` Jobs: terminal Jobs are deleted, suspended Jobs are unsuspended and re-adopted, running Jobs are adopted directly.

**Cluster state transitions:** `RunJobFlow` emits `STARTING → BOOTSTRAPPING → WAITING` (or `TERMINATED` on error). `TerminateJobFlows` emits `TERMINATING → TERMINATED`. All transitions publish `EventEMRClusterState` on the EventBus, which EventBridgeProvider converts to "EMR Cluster State Change" envelopes.

---

### EMR on EKS job runs — K8s executor (`JAISCLOUD_EXECUTOR_MODE=k8s`)

Each `StartJobRun` on a virtual cluster submits the same `batch/v1 Job` pattern as EMR steps (above), with the job named `jc-emrc-{virtualClusterID[:8]}-{jobRunID[:8]}` and labels `app: jaiscloud-emrc`.

`CancelJobRun` emits `CANCEL_PENDING` before `CANCELLED` to match the real EMR on EKS state machine.

---

### RDS, ElastiCache, ECS, EKS — metadata only (current)

These services currently store resource definitions (instance configs, cluster configs, task definitions) as JSON blobs in `jc_resources`. **No real compute is started.** Real container provisioning is a planned future enhancement.

---

### S3 blob storage

S3 object **bodies** are stored in `BlobStore`, separate from the metadata in PostgreSQL:

| Mode | BlobStore | Where blobs live |
|---|---|---|
| Lite | `MemoryBlobStore` | In-process heap; lost on restart |
| Full | `LocalFSBlobStore` | `JAISCLOUD_BLOB_DIR` (default `~/.jaiscloud/blobs`); survives restarts |

`LocalFSBlobStore` is wired automatically in full mode via `blobfs.NewLocalFSBlobStore(cfg.BlobDir)` in `initStores`. The default directory is `~/.jaiscloud/blobs` (set in `config.go`). Override with `--blob-dir /path/to/dir` or `JAISCLOUD_BLOB_DIR`.

**`BlobFetcher` interface** (`internal/blobfs/blobfetch.go`) provides URI-addressed read-only access to the blob store. `S3BlobFetcher` parses `s3://` and `s3a://` URIs and delegates to the underlying `BlobStore`. Used by the EMR bootstrap resolver to fetch bootstrap scripts by their S3 path without importing the S3 provider.

---

## Build & run

```bash
# Build binary — always rebuild after code changes, never run a stale binary
go build -o jaiscloud ./cmd/jaiscloud/

# Run server (default port 4566, lite mode, AWS)
./jaiscloud start

# Run in full mode (PostgreSQL persistence)
./jaiscloud start --mode full --dsn "postgres://user:pass@localhost:5433/jaiscloud"

# Run via docker-compose (postgres on 5433, jaiscloud on 4566)
make up-docker      # start detached (builds image first)
make down-docker    # stop and remove

# Enable all executors via K8s (Spark + Lambda)
JAISCLOUD_EXECUTOR_MODE=k8s ./jaiscloud start --mode full --dsn "postgres://..."

# Enable all executors via Docker (Spark + Lambda)
JAISCLOUD_EXECUTOR_MODE=docker ./jaiscloud start --mode full --dsn "postgres://..."

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
JAISCLOUD_EXECUTOR_MODE=     # "" (default mock) | "mock" | "docker" | "k8s" — applies to Spark + Lambda
JAISCLOUD_LAMBDA_IMAGE=      # override default Lambda runtime image
JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300  # Docker warm container idle timeout
JAISCLOUD_KMS_MASTER_KEY=    # 32-byte hex KEK; if unset DEK stored plaintext (dev only)
JAISCLOUD_APIGW_PROXY_TIMEOUT=30s    # HTTP_PROXY integration timeout
JAISCLOUD_APIGW_ALLOW_PRIVATE_HOSTS= # true to allow RFC-1918 HTTP_PROXY targets
# S3 virtual-hosted style routing
JAISCLOUD_S3_VIRTUAL_HOST_BASES=     # comma-separated host suffixes, e.g. "jaiscloud.devbox.local,s3.local"
# IMDS emulator (AWS Cloud only)
JAISCLOUD_IMDS_ENABLED=false         # true to expose /_/latest/meta-data/* IMDS endpoints
# AWS emulator wiring for Spark driver pods (K8s executor only)
JAISCLOUD_AWS_EMULATOR_ENDPOINT=     # JaisCloud S3/API endpoint reachable from Spark pods, e.g. http://jaiscloud.jaiscloud.svc:4566
JAISCLOUD_K8S_SPARK_SA=             # K8s service account for spark-submit pods (default: "default")
# EMR bootstrap actions (K8s executor only)
JAISCLOUD_BOOTSTRAP_IMAGE=amazon/aws-cli:2.18  # init container image for bootstrap scripts
JAISCLOUD_BOOTSTRAP_SCRIPT_MAX_BYTES=1048576   # max size per bootstrap script (default 1 MiB)
JAISCLOUD_BOOTSTRAP_RELOCATE_PREFIXES=/etc/pki,/home/hadoop  # comma-separated emptyDir mount paths
# Spark K8s (EXECUTOR_MODE=k8s only)
JAISCLOUD_SPARK_K8S_CLUSTER_MODE=auto          # auto | always | never — when to engage cluster deploy-mode
JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN=leave     # leave | delete — what Close() does to cluster-mode Jobs
JAISCLOUD_SPARK_K8S_CLUSTER_RESTART_POLICY=adopt  # adopt | reap — what cleanupOrphans does to cluster-mode Jobs on restart
JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT=10m     # how long a Job may be missing before the poller marks it FAILED
# Multi-instance isolation (Spark + Lambda)
JAISCLOUD_INSTANCE_ID=                        # override the auto-generated instance UUID (dev only)
```

> **Config loading:** `config.Load()` reads from the global Viper instance. All `viper.BindPFlag(...)` calls in `startCmd` must use the global `viper` package (not a local `viper.New()` instance) or flags will be silently ignored and defaults used.

> **Startup executor log:** on startup the server logs the active executor mode. An unset `JAISCLOUD_EXECUTOR_MODE` silently defaults to mock — the startup log is the first place to check:
> ```
> INFO  executor    lambda=mock  spark=mock
> INFO  blob storage  dir=/home/user/.jaiscloud/blobs
> INFO  kms         master-key=unset  dek=plaintext [WARN: dev mode only]
> INFO  jaiscloud started  port=4566  mode=full
> ```

### CLI commands

| Command | Description |
|---|---|
| `start` | Start the emulator |
| `version` | Print version |
| `env` | Print effective config as env vars (includes JAISCLOUD_CLOUD) |
| `doctor` | Check emulator reachability |
| `reset` | Wipe all emulator state via HTTP |
| `export [-o file] [--strip-kek] [--export-key hex]` | Export state snapshot; use `--strip-kek` when moving to a different instance |
| `import [-i file] [--export-key hex]` | Restore state from snapshot |
| `rotate-master-key --new-key <hex>` | Re-wrap KMS DEK with new KEK; zero re-encryption of key material |
| `services` | Print each service with its implementation level: `full \| partial \| metadata-only \| stub` |

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

# Full-mode e2e via Makefile (docker-compose handles server + postgres)
make test-e2e-lambda-docker      # Lambda Docker warm-pool (tag: lambda_e2e)
make test-e2e-lambda-k8s         # Lambda K8s warm-pod (tag: lambda_e2e)
make test-e2e-emr-docker         # EMR Spark via Docker (tag: spark_e2e)
make test-e2e-emrcontainers-k8s  # EMR Containers K8s (tag: spark_e2e)
make test-e2e-cloudformation     # CloudFormation (tag: cfn_fullmode)
make test-e2e-kms                # KMS/SecretsManager/SSM (tag: kms_fullmode)
make test-e2e-eventbridge        # EventBridge (tag: spark_e2e)
make test-e2e-iceberg            # Iceberg Glue (tag: iceberg_e2e)
```

Integration tests call `POST /_jaiscloud/reset` between each test via `resetState(t)` in [tests/integration/helpers_test.go](tests/integration/helpers_test.go).

Current integration test coverage: **SQS, IAM/STS, SNS, DynamoDB, S3, Lambda**.

### Full-mode test layout

All full-mode e2e tests live under `tests/full_mode/aws/`:

| Directory | Build tag | Description |
|---|---|---|
| `tests/full_mode/aws/lambda/` | `lambda_e2e` | Lambda Docker + K8s warm-pool / warm-pod |
| `tests/full_mode/aws/cloudformation/` | `cfn_fullmode` | CloudFormation stacks, real resource dispatch |
| `tests/full_mode/aws/kms/` | `kms_fullmode` | KMS, SecretsManager, SSM cross-service |
| `tests/full_mode/aws/emr/` | `spark_e2e` | EMR on EC2 steps (Docker + K8s lifecycle) |
| `tests/full_mode/aws/emrcontainers/` | `spark_e2e` | EMR on EKS job runs |
| `tests/full_mode/aws/eventbridge/` | `spark_e2e` | EventBridge rule matching + SQS delivery |
| `tests/full_mode/aws/dpc/` | `spark_e2e` | DPC Spark tests |
| `tests/full_mode/aws/iceberg/` | `iceberg_e2e` | Apache Iceberg via Glue Catalog |

### Full mode reset behaviour

`POST /_jaiscloud/reset` wipes all registered stores. In full mode this covers every postgres table:

| Store | Table(s) wiped |
|---|---|
| `PostgresResourceStore` | `jc_resources` (queues, topics, tables, functions, IAM) |
| `PostgresSQSMessageStore` | `jc_sqs_messages`, `jc_sqs_dedup` |
| `PostgresDynamoDBItemStore` | `jc_dynamodb_items` |
| `PostgresS3ObjectMetaStore` | `jc_s3_objects` |
| `LocalFSBlobStore` | blob files under `JAISCLOUD_BLOB_DIR` (full mode) or in-memory (lite mode) |

---

## sparkhelpers / k8shelpers design decisions

### EMR execution is cloud-specific — no abstract executor interface

`internal/k8shelpers` is a generic K8s helper library (no EMR/Spark concepts). `internal/sparkhelpers` adds Spark-specific logic on top. EMR and EMR-on-EKS providers wire these directly — there is no `SparkExecutor` interface or factory abstraction. If a future provider (e.g. Dataproc) needs Spark execution, it creates its own wiring in its own package, not a shared interface.

### `sparkhelpers.SubmitClientMode` and `ClientModeJob`

`ClientModeJob` is the input to `SubmitClientMode`. Key fields:

| Field | Purpose |
|---|---|
| `Image` | spark-submit container image (provider's `sparkImage` / `emrImage`) |
| `IdentityMutator` | `k8shelpers.IdentityMutator` — cloud-specific pod identity (IRSA, Azure MI, GCP WI) |
| `PlatformOverlay` | `*platform.PlatformConfig` — TLS init containers, CA env vars, volume mounts |
| `EntryPoint` | JAR or Python file URI |
| `SparkSubmitArgs` | user-supplied `--conf` entries, `--num-executors`, etc. (appended last — wins over `ExtraSparkConfs`) |
| `ExtraSparkConfs` | JaisCloud-injected `--conf` tokens (e.g. from `sparkaws.DriverSparkConfsFromEnv`), prepended before `SparkSubmitArgs` |
| `ExtraDriverEnv` | extra `corev1.EnvVar` entries added to the spark-submit container |
| `ServiceAccountName` | Kubernetes service account for the spark-submit pod (falls back to `"default"`) |
| `JarArgs` | arguments passed after the entry point |

**Spark conf precedence:** `BuildClientModeArgs` emits confs in order: fixed JaisCloud confs → `ExtraSparkConfs` → `SparkSubmitArgs`. Because Spark uses last-value-wins semantics, user-supplied `SparkSubmitArgs` always override JaisCloud defaults for the same key.

**Built-in confs emitted by `BuildClientModeArgs`:**
- `spark.kubernetes.namespace` — from `ClientModeJob.Namespace`
- `spark.kubernetes.driver.pod.name` — `$(SPARK_DRIVER_POD_NAME)` env-var ref
- `spark.kubernetes.executor.podTemplateFile` — `file:///jaiscloud/spark/executor-template.yaml`
- `spark.kubernetes.executor.podTemplateContainerName` — `spark-kubernetes-executor`
- `spark.kubernetes.container.image` — from `ClientModeJob.Image`
- `spark.kubernetes.authenticate.executor.serviceAccountName` — from `ServiceAccountName` (default `"default"`)
- `spark.driver.bindAddress` — `0.0.0.0`

`SubmitClientMode` creates a ConfigMap for the executor pod template before creating the Job. If `createJob` fails, the ConfigMap is explicitly deleted in a deferred cleanup, preventing orphaned ConfigMaps.

### `sparkaws` — AWS emulator wiring for Spark driver pods

`internal/provider/aws/sparkaws` injects JaisCloud's own endpoint coordinates into Spark driver pods so they can reach S3, IMDS, and AWS credentials inside the cluster.

`AWSEmulatorConfig` fields:
- `S3Endpoint` — `spark.hadoop.fs.s3a.endpoint` value (and `HADOOP_AWS_S3_ENDPOINT` env var)
- `IMDSEndpoint` — `AWS_EC2_METADATA_SERVICE_ENDPOINT` env var (empty → `AWS_EC2_METADATA_DISABLED=true`)
- `AccessKeyID`, `SecretAccessKey` — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars

Key functions:
- `DriverEnv(cfg)` — returns `[]corev1.EnvVar` for the driver container
- `DriverSparkConfs(cfg)` — returns `--conf` tokens including `spark.hadoop.fs.s3a.impl`, `spark.hadoop.fs.s3a.endpoint`, credentials provider, and `spark.executorEnv.*` mirrors
- `DriverSparkConfsFromEnv(cfg, driverEnv)` — same but accepts pre-computed env to avoid calling `DriverEnv` twice per job submission

Both EMR providers compute `driverEnv := sparkaws.DriverEnv(p.awsEmulator)` once, then pass it to both `ClientModeJob.ExtraDriverEnv` and `sparkaws.DriverSparkConfsFromEnv(...)`.

### Executor pod template — `BuildExecutorPodTemplate`

`sparkhelpers.BuildExecutorPodTemplate` produces YAML for `spark.kubernetes.executor.podTemplateFile`. The base container is always named `"spark-kubernetes-executor"` (the canonical name Spark requires to inject its image and command). `ImagePullPolicy` is set to `corev1.PullIfNotPresent` on the base container; it is **not** in the `BuildPodSpec` merge table, so callerTpl cannot override it.

### `BuildPodSpec` and `IdentityMutator`

`k8shelpers.BuildPodSpec` accepts `ctx context.Context` and `k8s kubernetes.Interface` and passes them to the `IdentityMutator` callback. This is required because real cloud identity wiring (IRSA, Azure Managed Identity, GCP Workload Identity) needs to make K8s API calls and must respect request deadlines. Passing `nil` for both is safe only when `IdentityMutator` is `nil`.

```go
type IdentityMutator func(ctx context.Context, k8s kubernetes.Interface, tpl *corev1.PodTemplateSpec) error
```

Both EMR providers accept the following constructor options:
- `WithIdentityMutator(m k8shelpers.IdentityMutator)` — cloud-specific pod identity wiring
- `WithAWSEmulator(cfg *sparkaws.AWSEmulatorConfig)` — inject JaisCloud's S3/IMDS/credentials endpoint into driver pods
- `WithServiceAccountName(sa string)` — set the Kubernetes service account for spark-submit pods
- `WithInstanceID(id string)` — override the instance ID stamped on managed K8s resources

### `OwnershipPatcher` — executor pod ownership

Spark executor pods are created by the Spark driver (inside the batch/v1 Job pod) and have no ownerReference. `k8shelpers.StartOwnershipPatcher` watches pods with label `spark-role=executor` and patches ownerReferences back to the parent Job using a caller-supplied resolver.

`sparkhelpers.MakeExecutorOwnerResolver` implements the resolver: it reads the pod's `spark-app-selector` label, lists driver pods matching `spark-app-id=<selector>`, and returns an `OwnerRefHint` pointing at the driver pod's owning Job.

Both EMR providers start the OwnershipPatcher in `New()` when a k8s client is present, and stop it via `patcherStop()` in `Shutdown()`.

### Provider goroutine lifecycle — `handlerCtx` + `WaitGroup`

`EMRProvider` and `EMRContainersProvider` capture a `handlerCtx` at handler entry:

```go
type handlerCtx struct {
    cloud     model.Cloud
    region    string
    accountID string
}

func newHandlerCtx(nr *model.NormalizedRequest) handlerCtx {
    return handlerCtx{cloud: nr.Cloud, region: nr.Region, accountID: nr.AccountID}
}
```

This lets goroutines publish state-change events with correct cloud provenance without holding a reference to `NormalizedRequest`.

All `runStep` / `runJobRun` goroutines use `p.wg.Add(1)` + `defer p.wg.Done()`. `Shutdown()` calls `p.cancel()` then `p.wg.Wait()`, ensuring all in-flight goroutines complete before the server exits.

```go
func (p *EMRProvider) Shutdown(_ context.Context) {
    if p.patcherStop != nil { p.patcherStop() }
    p.cancel()
    p.wg.Wait()
}
```

### Best-effort vs. critical error handling

Operations that must not silently lose state (cluster/step persistence) log at `WARN` on error. Operations that are inherently best-effort (log collection for Spark exit classification, terminal snapshot persistence) log at `WARN` but do not fail the parent operation.

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

### StoreAdapter (`internal/resourcemgr/adapter.go`)

Bridges `store.ResourceStore` (host type) to `resourcemgr.ResourceStore` (internal interface). `Exists` maps `store.ErrNotFound` to `false` rather than returning an error.

---

## Key conventions

### AWS service detection order

Detection is data-driven. All service metadata is declared once in [internal/adapter/aws/services.go](internal/adapter/aws/services.go) as `ServiceDescriptor` entries in `awsServices`. Four lookup maps are derived at `init()` time. `DetectService` in [router.go](internal/adapter/aws/router.go) consults those maps — no hardcoded strings.

Priority:
1. `X-Amz-Target` header — `targetPrefixToService` map (JSON/Target services: SQS, DynamoDB, Glue, ECS, EMR, EventBridge, DynamoDB Streams, KMS, SecretsManager, SSM)
2. SigV4 `Authorization` scope — `knownSigV4Services` set (all services)
3. `Action=` query/body param — `actionToService` map (Query-protocol services: SQS, IAM, STS, SNS, EC2, CloudFormation, RDS, ElastiCache)
4. Granite path `/service/<ver>/operation/<Action>` — matched when `SigV4Name` resolves but action is empty (AWS SDK v2 CloudWatch)

**Adding a new service:** add one `ServiceDescriptor` entry to `awsServices` in `services.go`. Service detection, SigV4 allow-list, Action routing, and gateway provider mapping all update automatically.

### CloudWatch Granite URL routing

AWS SDK v2 routes CloudWatch requests to `/service/monitoring/operation/<Action>` instead of using `Action=` in the body. `router.go` priority 4 handles this: when the URL path matches `/service/<sigv4name>/operation/<action>`, the action is extracted from the path and the service is resolved from the SigV4 name.

`CloudWatchCodec.Decode` extracts the `Action` from either the form body (SDK v1 style) or the Granite path (SDK v2 style) using the same fallback logic.

### S3 action detection

`S3Codec.Decode` maps `(HTTP method, bucket, key, query params, headers)` to action names. Key rules:
- No bucket → `ListBuckets`
- No key, `GET ?list-type=2` → `ListObjectsV2`
- `X-Amz-Copy-Source` header + `PUT` → `CopyObject`
- `?uploads` on POST → `CreateMultipartUpload`, on GET → `ListMultipartUploads`
- `?delete` on POST → `DeleteObjects` (XML body parsed for keys)

S3 handles two virtual-hosted-style URL forms, in priority order:

1. **AWS SDK form** — host matches `*.s3.<region>.amazonaws.com` or `*.s3.amazonaws.com`: the bucket is extracted from the Host header prefix before path parsing.
2. **Custom base form** — `S3Codec.VirtualHostBases []string`: `extractVirtualHostedBucket` strips any `:port` suffix then checks whether `host` ends with `.<base>`. If it does, the prefix before `.<base>` is the bucket. Configured via `--s3-virtual-host-bases` / `JAISCLOUD_S3_VIRTUAL_HOST_BASES` (comma-separated). Empty base list → path-style only.

Both forms fall back to **path-style** (`/<bucket>/<key>`) when neither matches.

### S3 object store: race-condition ordering

`DeleteObject` and `DeleteObjects` delete **metadata before blob** so `GetObject` never observes a dangling metadata row with a missing blob. If metadata deletion succeeds but blob deletion fails, the object is gone (metadata was the authoritative existence check).

`GetObject` distinguishes a clean concurrent delete from storage corruption: when the blob fetch returns not-found, the provider rechecks metadata. If metadata is also gone (concurrent delete), it returns 404. If metadata is still present but blob is absent, it returns 500 (storage inconsistency).

### Lambda invoke

`InvokeFunction` echoes the payload back unchanged in echo mode. Useful for testing fan-out pipelines that invoke Lambda as a sink.

### FIFO deduplication (SQS)

`MemoryMessageStore.Send` returns `(dedupMessageID string, err error)`. A non-empty `dedupMessageID` means duplicate; the original `MessageId` is returned.

### Visibility / DLQ (SQS)

`MemoryMessageStore.Receive` sets `VisibleAt = now + 30s`. The provider then calls `ChangeVisibility` with the queue's configured timeout. When `ReceiveCount >= maxReceiveCount`, `checkDLQ` copies the message to the dead-letter queue with zeroed timers.

### SNS fan-out

`SNSProvider.Publish` wraps the message in a JSON envelope and calls `SQSMessageStore.Send` for each SQS subscription. Each SQS delivery is assigned a **new unique `MessageID`**; the SNS notification ID is embedded in the envelope body only.

### SNS MessageAttributes pass-through

`SNS.Publish` extracts `MessageAttributes` from the Query protocol form and passes them through in the SQS envelope JSON under `"MessageAttributes"` so downstream consumers can read them after receiving from SQS.

### DynamoDB pk hash

`TableProvider` computes a stable hash from key attributes only (in schema-defined order) and passes it explicitly to `DynamoDBItemStore`. The store never auto-computes hashes.

### DynamoDB pagination determinism

Both `MemoryDynamoDBItemStore` and `PostgresDynamoDBItemStore` sort all matching items by `itemPKHash` before applying `ExclusiveStartKey` / `Limit`. This guarantees consistent cursor behaviour regardless of map iteration order or Postgres heap scan order.

### DynamoDB `LastEvaluatedKey` — page-full check

`MemoryDynamoDBItemStore.Query` sets `LastEvaluatedKey` when the returned **page** is full (`len(all) == q.Limit`), not when the pre-pagination match count equals the limit.

### DynamoDB wire protocol: x-amz-crc32

Every DynamoDB response **must** include `x-amz-crc32: <crc32_of_body>`. AWS SDK v2 validates this header; without it the SDK does not cleanly drain the response body. `DynamoDBCodec.Encode` computes and sets this header on every response.

### Postgres SQS: composite primary key

`jc_sqs_messages` uses a composite primary key `(id, queue_url)` so the same `MessageID` can appear in multiple queues (e.g. SNS fan-out). Migration `005_sqs_fix_pk.sql` upgrades existing installs.

### Postgres connection pool

`NewPostgresResourceStore` configures `pgxpool` with production-ready defaults:

| Setting | Value | Purpose |
|---|---|---|
| `MaxConns` | 40 | Hard cap on open connections |
| `MinConns` | 2 | Keep-alive connections |
| `MaxConnLifetime` | 30 min | Recycle connections to avoid server-side stale limits |
| `MaxConnIdleTime` | 10 min | Release idle connections under low load |
| `HealthCheckPeriod` | 30 s | Proactive dead-connection detection |
| `ConnectTimeout` | 5 s | Per-attempt TCP timeout |

**Startup retry:** the server retries the initial `Ping` up to 10 times with exponential backoff (500 ms → 8 s).

### Postgres error classification

`wrapPgError` in `store/postgres.go` maps raw pgx errors to typed store sentinels:

| pgx error | Store sentinel |
|---|---|
| `pgx.ErrNoRows` | `store.ErrNotFound` |
| Unique violation (23505) | `store.ErrAlreadyExists` |
| Connection class 08xx / 57xx, `net.Error` | `store.ErrStorageUnavailable` |

### Admin endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | Liveness check |
| `/_jaiscloud/reset` | POST | Wipe all state (used by integration tests) |
| `/_jaiscloud/export` | GET | JSON snapshot of all registered Snapshotter stores |
| `/_jaiscloud/import` | POST | Restore state from JSON snapshot |
| `/metrics` | GET | Prometheus metrics (requires `--metrics` flag) |

### Snapshot / Restore

Stores implement `admin.Snapshotter` (`Snapshot() (json.RawMessage, error)` + `Restore(json.RawMessage) error`) and are registered with `adminHandler.RegisterSnapshotter(name, store)`. Export envelope is schema v2 (`schema_version`, `instance_id`, `cloud`, `region`, `account_id`); import validates cloud identity and accepts both v1 and v2 envelopes.

### HTTP response: Content-Length

`gateway.writeResponse` explicitly sets `Content-Length` before calling `WriteHeader`. Without this, Go's HTTP server uses chunked transfer encoding, which can cause the AWS SDK's response body close to fail and log a connection-reuse warning.

### Clock abstraction

All time-sensitive code receives a `clock.Clock` from `NormalizedRequest.Clock`. Integration tests use `RealClock`. Unit tests can use `FixedClock` or `OffsetClock` for deterministic control.

### Prometheus metrics: cloud label

The Metrics middleware (`internal/gateway/middleware/metrics.go`) records `jaiscloud_requests_total{cloud,service,action,status}` and `jaiscloud_request_duration_seconds{cloud,service,action}`.

### Multi-cloud extensibility: dependency injection for cloud-specific formatting

**Rule: providers must never hard-code cloud-specific resource identifier formats (AWS ARNs, Azure resource IDs, etc.).**

When a provider needs a cloud-specific resource ID, it must use the injected function on `NormalizedRequest` rather than calling `fmt.Sprintf("arn:aws:...")` directly.

```go
// WRONG — couples provider to AWS:
arn := fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", nr.Region, nr.AccountID, name)

// CORRECT — delegate to injected formatter:
arn := nr.ResourceID("dynamodb-table", name)
```

**Where each piece lives:**
- `internal/model/model.go` — declares `ResourceID func(resourceType, name string) string` on `NormalizedRequest`
- `internal/config/config.go` — `awsARNFormatters` map + `AWSResourceID(region, accountID)` returns the AWS implementation
- `internal/gateway/server.go` — injects `nr.ResourceID` for all clouds; always non-nil after the gateway
- `internal/provider/*/` — calls `nr.ResourceID("type", name)` unconditionally; no `"arn:aws:"` literals

**Adding a new resource type:** add one entry to `awsARNFormatters` in `config.go`.

---

### EventBridge conventions

**Wire protocol:** `X-Amz-Target: AmazonCloudWatchEvents.<Action>` (JSON/Target). Detected via the `TargetPrefix` in `services.go`.

**Rule storage:** `eb_rule` resource type. Key = rule name.

**Target storage:** `eb_target` resource type. Key = `"<ruleName>/<targetId>"` so all targets for a rule are listed with a single `List(ctx, "eb_target", ruleName+"/")` call.

**Target type resolution (`resolveTargetMeta`):** ARN parsing happens **once at `PutTargets` time** where `nr.Cloud` is available. The resolved `TargetType` (`"sqs"`) and `QueueName` are stored in `targetData`. The delivery path (`deliverToTarget`) is cloud-agnostic.

**EMR envelope format (real-AWS parity):**

- **Step state change** (`EventEMRStepState`): `detail-type: "EMR Step Status Change"`, `source: "<cloud>.emr"`, `detail` contains `stepId`, `clusterId`, `state`, `severity` (derived from terminal state), `actionOnFailure`, `stateChangeReason: {code, message}` (nested object), `createdTime`, `endTime`.
- **Job run state change** (`EventEMRJobRunState`): `detail-type: "EMR Containers Job Run State Change"`, `detail` contains `jobRunId`, `virtualClusterId`, `state`, `releaseLabel`, `executionRoleArn`, `stateChangeReason: {code, message}`.
- **Cluster state change** (`EventEMRClusterState`): `detail-type: "EMR Cluster State Change"`, `detail` contains `clusterId`, `name`, `state`, `severity`, `message`, `stateChangeCode`, `stateChangeReason: {code, message}`.

`severity` values: `"ERROR"` for FAILED/TERMINATED_WITH_ERRORS, `"WARN"` for CANCELLED/INTERRUPTED, `"INFO"` otherwise.

The event `source` is `string(ev.Cloud) + ".emr"` — not hardcoded — so it is correct for any cloud.

**`PutEvents` action:** allows tests and callers to inject arbitrary events directly into the rule-matching pipeline.

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
| `k8s.io/client-go` | Kubernetes API client (k8shelpers, sparkhelpers) |
| `sigs.k8s.io/yaml` | YAML marshaling for pod templates |
