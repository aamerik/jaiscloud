# JaisCloud — Developer Reference

## Project

JaisCloud is a local multi-cloud emulator written in Go. It speaks native AWS wire protocols (Query/XML, JSON/Target, REST) so any AWS SDK can point at it without modification.

**Phase 0 (complete):** SQS — all 32 integration tests pass.  
**Phase 1 (complete):** IAM/STS, SNS, DynamoDB, S3, Lambda; BlobFS; PostgreSQL stores; export/import; Prometheus metrics.  
**Phase 2 (complete):** ResourceManager with deletion guards; Multi-cloud adapter model (AWS default, Azure/GCP stubs); EMR/EMR-on-EKS built-in providers with SparkExecutor (mock + k8s); Prometheus cloud label.  
**Phase 2.5 (complete):** KMS (envelope crypto, grants, rotation); SecretsManager (AES-GCM at rest via KMS); SSM Parameter Store (SecureString via KMS); API Gateway REST management plane + execute-api invoke plane (MOCK, AWS_PROXY, HTTP_PROXY); Lambda Docker/K8s executors; CloudFormation with full intrinsics engine (Ref, Fn::GetAtt, Fn::Sub, Fn::Join, conditions, mappings), topological sort, and real resource dispatch for 9 AWS resource types.  
**Phase 2.5 patch (complete):** Lambda K8s executor rewritten to warm-pod-per-function model (matching Docker executor); Spark K8s executor `Close()` suspends tracked Jobs instead of deleting them; `cleanupOrphans()` re-adopts running/suspended Jobs on restart; HTTPS server with auto-generated cloud-aware TLS cert (ECDSA P-256, 10-year, DNS SANs per cloud); S3 virtual-hosted-style parsing (`mybucket.s3.<region>.amazonaws.com`); IAM `AssumeRole`/`GetFederationToken` `PackedPolicySize` computed correctly; SSM `GetParameterHistory` now decrypts SecureString values; `DeleteFunction` tears down warm container/pod; `LambdaExecutor` interface extended with `DeleteFunction` + `Reset`; test suite reorganised under `tests/full_mode/aws/`; `docker-compose.yml` + K8s RBAC manifest + Lambda echo image added.  
**Phase 2.5 patch 2 (complete):** EMR classic bootstrap actions (`RunJobFlow.BootstrapActions`) materialised as Kubernetes init containers when `EXECUTOR_MODE=k8s`; `BlobFetcher` interface + `S3BlobFetcher` in `blobfs` so bootstrap scripts are fetched from the S3 store; `k8stypes.EnvVar.ValueFrom` support (SecretKeyRef, ConfigMapKeyRef, FieldRef); `SparkJob` extended with `ExtraInitContainers`, `ExtraVolumes`, `ExtraMainMounts` fragment fields so the EMR provider can inject bootstrap fragments without importing the executor package; host-package-manager commands (`yum`, `apt-get`, `systemctl`, etc.) automatically commented out from bootstrap scripts; DynamoDB `Query` `LastEvaluatedKey` pagination bug fixed (`len(matched)` → `len(all)`).  
**Phase 2.5 patch 3 (complete):** Spark K8s cluster deploy-mode with per-cloud pod-template merging. `CloudSparkTransform` extended with `CloudExecutorTemplateIO` sub-interface (`UploadTemplate`, `DeleteTemplate`, `DriverFetchEnv`); AWS implementation uploads merged executor pod-template YAML to the S3 store and injects the S3 URI as `spark.kubernetes.executor.podTemplateFile`. `buildJobManifest` returns `(batchJob, CloudSparkTransform, cleanupKey, error)` — transform is returned so `Submit` can clean up the uploaded blob if `createJob` fails. `jobEntry` struct stores `isClusterMode bool` so `Close()` detects cluster-mode Jobs without K8s API calls. `JAISCLOUD_SPARK_K8S_CLUSTER_MODE` controls policy (`auto`/`always`/`never`). `resolveMasterArgs` whitelists `--master` values for cluster mode and logs a structured `WARN` when none is found. Diagnostic `WARN` logs emitted at submission time for missing service account, default image with `ImagePullPolicy=Never`, and missing S3 endpoint with an `s3://` JAR URI. `rewriteSparkMaster` handles both `--master X` and `--master=X` forms. `pollAll` logs failed-state transitions at `WARN` level (not `INFO`). `OnClusterModeOrphanDelete` callback wired in `main.go` so orphaned cluster-mode Jobs propagate `FAILED` state to both EMR and EMR-on-EKS providers. `ApplyResourceProfile` uses executor CPU/memory when `args == nil` (executor-side merge). `AWSSparkTransform.PodEnv` uses a deterministic slice instead of a map to avoid non-deterministic env-var ordering.  
**Phase 2.5 patch 4 (complete):** Multi-instance restart recovery. Spark `K8sExecutor.cleanupOrphans()` is now synchronous (completes before `New()` returns); paginates Job lists with continuation tokens (safety cap 10 K); stamps every managed Job with `jaiscloud.io/job-id-raw` annotation + `jaiscloud.io/instance-id` label; `ClusterRestartPolicy` (`adopt`/`reap`) controls what happens to cluster-mode Jobs on restart; `StatusPoller` reconciles stale-missing Jobs after `ReconcileTimeout` (default 10 min); `Reset()` performs a label-filtered K8s sweep for escaped Jobs. Lambda K8s executor stamps all pods and services with `jaiscloud.io/instance-id`; `cleanupOrphans` and `Reset` filter by instance ID; service names are instance-scoped (`jc-lambda-<id[:8]>-<fn>`); pod/service list uses pagination. Lambda Docker executor includes `<instanceID[:8]>` in container names so instances on a shared daemon don't cross-reap. EMR and EMR-on-EKS providers call `rehydratePoller()` on startup to re-track non-terminal steps/job-runs into the `StatusPoller`. Export envelope upgraded to schema v2 (`schema_version`, `instance_id`, `cloud`, `region`, `account_id`); import validates cloud identity and accepts both v1 and v2 envelopes. `OnJobAdopted` callback lets providers re-track adopted Jobs into the poller without restarting.

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
docs/                   # Architecture, LLD, phase plan documents
internal/
  executor/spark/
    executor.go         # SparkExecutor interface, SparkJob, SparkState, NewExecutor factory
    config.go           # SparkConfig, ClusterSize (Small/Medium/Large), SparkConfigFrom
    mock.go             # MockExecutor — immediate COMPLETED, ForceState, Reset
    k8s.go              # K8sExecutor — submits batch/v1 Jobs to Kubernetes
    k8sclient.go        # stdlib K8s HTTP client (no client-go)
    command.go          # SparkSubmitArgs — builds --master k8s:// arg list
    poller.go           # StatusPoller — background goroutine, OnStateChange callback
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
        kms.go          # KMSCodec: JSON/Target
        secretsmanager.go  # SecretsManagerCodec: JSON/Target
        ssm.go          # SSMCodec: JSON/Target
        apigateway.go   # APIGatewayCodec: REST JSON (management plane path routing)
    azure/
      azure.go          # AzureAdapter stub — Cloud(), DetectAndDecode() (501), ServiceToProvider() (passthrough)
    gcp/
      gcp.go            # GCPAdapter stub — Cloud(), DetectAndDecode() (501), ServiceToProvider() (passthrough)
  admin/                # /_jaiscloud/* endpoints
                        # Resetter, Snapshotter interfaces
  blobfs/               # BlobStore interface: MemoryBlobStore, LocalFSBlobStore
                        # BlobFetcher interface: S3BlobFetcher (fetches bootstrap scripts by s3:// URI)
                        # URI scheme support is AWS-only (s3://, s3a://); Azure (abfss://) and GCP (gs://) require future fetcher implementations.
  clock/                # Clock interface: RealClock, FixedClock, OffsetClock
  config/               # Config struct; Viper loading; env prefix JAISCLOUD_
  events/               # In-process EventBus (subscribe/publish)
  gateway/              # HTTP server (Chi), middleware, request dispatch
    server.go           # Server — holds single CloudAdapter; handleCloudRequest
    middleware/         # Recovery, RequestID, Logging, Metrics (Prometheus + cloud label)
  model/                # Shared types: NormalizedRequest, ProviderResponse, ProviderError
  provider/             # Business logic layer
    provider.go         # HandlerFunc type, OK() helper
    registry.go         # Registry — Dispatch (exact match → error)
    cache/              # ElastiCache provider
    catalog/            # Glue Data Catalog provider
    compute/            # EC2 provider
    container/          # ECS provider
    dns/                # Route53 provider
    emr/                # EMRProvider — RunJobFlow, steps, tags; wires SparkExecutor + StatusPoller
                        # bootstrap.go — Resolve() fetches + scrubs bootstrap scripts → init containers
    emroneks/           # EMRContainersProvider — virtual clusters, job runs; wires SparkExecutor
    function/           # FunctionProvider — Lambda (echo/Docker/K8s invoke)
    iam/                # IAMProvider + STS (roles, policies, users, access keys)
    notification/       # SNSProvider (topics, subscriptions, fan-out to SQS)
    object/             # ObjectProvider — S3 (buckets, objects, multipart)
    queue/              # QueueProvider — SQS (all 17 operations)
    rds/                # RDS provider
    stack/              # CloudFormation provider
      cloudformation.go # StackProvider — CreateStack, UpdateStack, DeleteStack, Describe, List
      intrinsics.go     # CloudFormation intrinsic function resolver (Ref, Fn::*, conditions)
      topsort.go        # Kahn's topological sort for DependsOn + implicit Ref/GetAtt deps
      dispatch.go       # CFNResourceDispatcher — per-resource-type create/delete handlers
    table/              # TableProvider — DynamoDB (tables, items, expressions, streams)
    events/             # EventBridgeProvider — rules, targets, event delivery to SQS
    apigw/              # APIGatewayProvider — REST API management plane + execute-api invoke
    key/                # KeyProvider — KMS (keys, aliases, grants, envelope crypto, rotation)
    secret/             # SecretProvider — SecretsManager (secrets, versions, rotation)
    param/              # ParameterProvider — SSM Parameter Store (put/get/history/path)
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
          → exact match: built-in provider handler (EMR, EMRContainers, etc.)
              → EMRProvider / EMRContainersProvider
                  → SparkExecutor (mock or k8s, wired at startup)
                  → StatusPoller
      → <ServiceCodec>.Encode            (XML or JSON or raw bytes)
  → HTTP response
```

Key design rules:
- **No layer imports its caller.** The `model` package breaks the cycle between `gateway` and `adapter`.
- **Single cloud per instance.** `cfg.Cloud` is set once at startup; one `CloudAdapter` is constructed; no per-request cloud detection. For multi-cloud applications that need both AWS and GCP simultaneously, run two instances on different ports (e.g. `--port 4566 --cloud aws` and `--port 4567 --cloud gcp`). A `docker-compose` example shipping in Phase 5 documents this pattern.
- **Executors are wired at startup.** `JAISCLOUD_EXECUTOR_MODE` controls the container orchestrator for both Spark and Lambda executors; providers receive it via the `WithExecutor` option.

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
| S3 | In-memory maps + `MemoryBlobStore` | PostgreSQL rows + `LocalFSBlobStore`* | same | same |
| Lambda | Echo response (mock) | Echo response (mock) | **Docker container** per function (warm pool) | **K8s Pod + ClusterIP Service** per function (warm pool) |
| EMR on EC2 steps | Instant `COMPLETED` (mock) | Instant `COMPLETED` (mock) | **Docker container** per step | **K8s `batch/v1 Job`** per step |
| EMR on EKS job runs | Instant `COMPLETED` (mock) | Instant `COMPLETED` (mock) | **Docker container** per job run | **K8s `batch/v1 Job`** per job run |
| EC2 / VPC / Route53 | Metadata only | Metadata only | — | — |
| RDS | Metadata only | Metadata only | — | — |
| ElastiCache | Metadata only | Metadata only | — | — |
| ECS | Metadata only | Metadata only | — | — |
| API Gateway | In-memory resource store | PostgreSQL rows | — | — |
| CloudFormation | In-memory stack store + real resource dispatch | PostgreSQL rows + real resource dispatch | — | — |
| Glue / EventBridge | In-memory maps | PostgreSQL rows | — | — |
| EC2 / VPC / Route53 | Metadata only | Metadata only | — | — |
| RDS | Metadata only | Metadata only | — | — |
| ElastiCache | Metadata only | Metadata only | — | — |
| ECS | Metadata only | Metadata only | — | — |

> \* `LocalFSBlobStore` is implemented and wired in `main.go` when `--blob-dir` is set. Without `--blob-dir`, `MemoryBlobStore` is used even in full mode and S3 blobs are lost on restart.

---

### Lambda — Docker executor (`JAISCLOUD_EXECUTOR_MODE=docker`)

Each distinct function gets **one warm Docker container** that is reused across invocations until it is idle beyond the keep-alive timeout.

```
docker run -d
  --name jc-lambda-{functionName}-{shortID}
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

Each distinct function gets **one warm Pod + ClusterIP Service** that is reused across invocations until idle beyond the keep-alive timeout — matching the Docker executor pattern. Invocations POST to the Lambda RIE endpoint (`/2015-03-31/functions/function/invocations`, port 8080) on the ClusterIP Service.

```
Pod name:    jc-lambda-{sanitized-name}-{shortID}   (namespace: jaiscloud)
Service name: jc-lambda-{sanitized-name}             (ClusterIP, port 8080)
Labels:      app=jaiscloud-lambda, function={name}
```

**Pod/Service lifecycle:**

| Trigger | Action |
|---|---|
| First `InvokeFunction` for a function | Create Pod + ClusterIP Service (cold start); wait Ready (90 s timeout) |
| Subsequent invocations | HTTP POST to ClusterIP endpoint (reuse) |
| `DeleteFunction` | Delete Pod + Service |
| Idle > `JAISCLOUD_LAMBDA_KEEPALIVE_SECS` (default 300 s) | GC goroutine deletes Pod + Service |
| Server shutdown (`Close()`) | All warm Pods + Services deleted |
| Server restart (startup `cleanupOrphans`) | Orphaned `app=jaiscloud-lambda` Pods + Services deleted |

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
    cluster: {clusterID}
    step: {stepID}
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

The `StatusPoller` goroutine polls the Job every `SparkConfig.PollInterval` (default 5 s) and fires `OnStateChange` to update the EMR step state (`PENDING → RUNNING → COMPLETED / FAILED`).

**Bootstrap actions:** When `RunJobFlow` includes `BootstrapActions` and `EXECUTOR_MODE=k8s`, the EMR provider calls `bootstrap.Resolve()` before submitting. Each bootstrap action becomes a K8s init container that runs before `spark-submit`:

```yaml
initContainers:
  - name: bootstrap-{sanitized-action-name}
    image: amazon/aws-cli:2.18          # JAISCLOUD_BOOTSTRAP_IMAGE
    command: ["/bin/sh", "-c"]
    args: ["printf '%s' '<b64-script>' | base64 -d | /bin/sh -s -- <args>"]
    securityContext:
      runAsUser: 0                       # root so scripts can write to /etc/pki, /home/hadoop
    volumeMounts:
      - name: bootstrap-prefix-etc-pki
        mountPath: /etc/pki
      - name: bootstrap-prefix-home-hadoop
        mountPath: /home/hadoop
```

One `emptyDir` volume is created per prefix (`JAISCLOUD_BOOTSTRAP_RELOCATE_PREFIXES`, default `/etc/pki,/home/hadoop`) and mounted into both init containers and the main `spark-submit` container. Host-only commands (`yum`, `apt-get`, `dnf`, `rpm`, `systemctl`, `service`, `chkconfig`, `update-rc.d`) are automatically commented out with `# [jaiscloud-skip]`. If bootstrap script fetch or resolution fails the step is immediately marked `FAILED`.

**Suspend/resume lifecycle:** `K8sExecutor.Close()` suspends all tracked Jobs (strategic-merge-patch `spec.suspend: true`) instead of deleting them, preserving partial progress across server restarts. On startup, `cleanupOrphans()` lists all `app.kubernetes.io/managed-by=jaiscloud` Jobs: terminal Jobs are deleted, suspended Jobs are unsuspended and re-adopted into the live jobs map, running Jobs are adopted directly.

---

### EMR on EKS job runs — K8s executor (`JAISCLOUD_EXECUTOR_MODE=k8s`)

Each `StartJobRun` on a virtual cluster submits the same `batch/v1 Job` pattern as EMR steps (above), with the job named `jc-emrc-{virtualClusterID[:8]}-{jobRunID[:8]}` and labels `app: jaiscloud-emrc`.

---

### RDS, ElastiCache, ECS — metadata only (current)

These services currently store resource definitions (instance configs, cluster configs, task definitions) as JSON blobs in `jc_resources` (PostgreSQL in full mode, in-memory map in lite mode). **No real database processes or containers are started.** This is the same pattern as LocalStack's basic tier for these services.

Real container provisioning (e.g. spinning up a PostgreSQL container for each `CreateDBInstance` call, or a Redis container for each `CreateCacheCluster`) is planned as a future enhancement triggered by a dedicated executor flag (e.g. `JAISCLOUD_RDS_MODE=docker`), following the same executor pattern used for Lambda and Spark.

---

### S3 blob storage

S3 object **bodies** are stored in `BlobStore`, separate from the metadata in PostgreSQL:

| Mode | BlobStore | Where blobs live |
|---|---|---|
| Lite | `MemoryBlobStore` | In-process heap; lost on restart |
| Full (current) | `MemoryBlobStore` | In-process heap; lost on restart |
| Full (when wired) | `LocalFSBlobStore` | `JAISCLOUD_BLOB_DIR` on local filesystem |

`LocalFSBlobStore` is fully implemented at [internal/blobfs/](internal/blobfs/) but the `main.go` wire-up still uses `MemoryBlobStore`. Swap `NewMemoryBlobStore()` for `NewLocalFSBlobStore(cfg.BlobDir)` in `startCmd` to enable persistent blob storage.

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

# Run with options
./jaiscloud start --port 4566 --region us-east-1 --metrics

# Run in GCP cloud mode (stub — returns 501 for all requests)
./jaiscloud start --cloud gcp

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
> INFO  executor  lambda=mock  spark=mock
> INFO  store     mode=full  blob=memory [WARN: LocalFSBlobStore not wired — S3 blobs lost on restart]
> INFO  kms       master-key=unset  dek=plaintext [WARN: dev mode only]
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

# Point at a different host
JAISCLOUD_HOST=http://localhost:9000 go test ./tests/integration/

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
| `MemoryBlobStore` | in-memory blob bytes |

> **Note:** Even in full mode, blob bytes (`BlobStore`) use `MemoryBlobStore`. `LocalFSBlobStore` is implemented but not yet wired into `main.go`. Swap the `NewMemoryBlobStore()` call in `startCmd` for `NewLocalFSBlobStore(dir)` to enable on-disk blob persistence.

---

## Spark executor (`internal/executor/spark/`)

The `SparkExecutor` interface drives EMR step execution and EMR-on-EKS job runs:

- **`MockExecutor`** — immediate `COMPLETED`, supports `ForceState` and `Reset` for tests.
- **`K8sExecutor`** — submits real `batch/v1 Jobs` to Kubernetes via stdlib HTTP (no client-go). Reads auth from in-cluster service account or `JAISCLOUD_K8S_*` env vars. `Close()` **suspends** (not deletes) tracked Jobs; `cleanupOrphans()` on startup re-adopts or deletes orphaned Jobs.
- **`StatusPoller`** — single background goroutine; polls non-terminal jobs at a configurable interval; fires `OnStateChange` callbacks. `Stop()` is safe to call multiple times (`sync.Once`). Failed-state transitions are logged at `WARN` level; all other transitions at `INFO`.
- **`SparkSubmitArgs`** — builds the full `spark-submit` argument list including `--master k8s://`, `--deploy-mode cluster`, container image, namespace, service account, resource profile, and S3 event-log args. When `job.Config.Mode == "k8s"` and `job.AllowClusterMode == true`, Pattern 3 generates `--master k8s://...` so the Spark driver runs as a real K8s Pod.
- **`SparkJob` fragment fields** — `ExtraInitContainers`, `ExtraVolumes`, `ExtraMainMounts` carry pre-built K8s fragments from the EMR provider (bootstrap init containers + emptyDir volumes). The executor injects them into the pod spec in `buildJobManifest` after cloud/platform layers are applied. Volume name conflicts are detected via `checkVolumeConflicts` and cause `buildJobManifest` to return an error.
- **`buildJobManifest`** — returns `(batchJob, CloudSparkTransform, cleanupKey string, error)`. The transform is returned so `Submit` can call `transform.DeleteTemplate` if `createJob` fails.
- **`jobEntry`** — tracks a submitted Job with `name`, `cleanupKey`, `transform`, `isClusterMode bool`, and the raw `jobID`. `isClusterMode` lets `Close()` choose shutdown behaviour without K8s API calls; the raw jobID is stored in the `jaiscloud.io/job-id-raw` annotation so `cleanupOrphans` can reconstruct the map key exactly on restart.
- **`CloudSparkTransform`** — per-cloud contributions to the K8s Job manifest (env vars, `--conf` entries). Executor pod-template merging and `CloudExecutorTemplateIO` were removed in Phase 2.5 patch 4; callers supply templates verbatim and JaisCloud passes them through unchanged.

EMR and EMRContainers providers accept a `WithExecutor(exec, cfg)` option. When an executor is wired, `AddJobFlowSteps` / `StartJobRun` call `Submit`, and state changes from the `StatusPoller` feed back via `OnStateChange`. Without an executor, steps complete instantly (mock behaviour).

`JAISCLOUD_EXECUTOR_MODE` controls which executor is created at startup for both Spark and Lambda: `""` / unset = instant mock completion, `"mock"` = MockExecutor, `"docker"` = DockerExecutor, `"k8s"` = K8sExecutor.

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

Providers call `rm.RegisterRules([]resourcemgr.DeleteGuardRule{...})` at construction time to register their own resource dependency rules. Thread-safe.

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

### DynamoDB `LastEvaluatedKey` — page-full check

`MemoryDynamoDBItemStore.Query` sets `LastEvaluatedKey` when the returned **page** is full (`len(all) == q.Limit`), not when the pre-pagination match count equals the limit. The distinction matters: if 5 items match a key condition and `Limit=2`, `paginateItems` returns 2 items (`all`); the pre-pagination slice (`matched`) has 5. Checking `len(matched) == q.Limit` would be `5 == 2 → false`, suppressing `LastEvaluatedKey` incorrectly. `Scan` and both Postgres paths already used `len(all)` — only the memory `Query` path had the bug.

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
