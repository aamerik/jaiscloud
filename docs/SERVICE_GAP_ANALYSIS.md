# JaisCloud vs LocalStack — Service Gap Analysis & Expansion Plan

> Generated: 2026-04-13  
> LocalStack source: `~/Code/localstack` (open-source community edition)  
> JaisCloud source: `~/Code/JaisCloud` (Phases 0–2 complete)

---

## 1. Side-by-Side Comparison

### 1A. Services Present in Both

| Service | JaisCloud Depth | LocalStack Depth | Notes |
|---|---|---|---|
| **SQS** | Full (17 ops, FIFO, DLQ, batch, visibility) | Full | JaisCloud has parity |
| **IAM** | Full (roles, policies, users, access keys, groups, sim) | Full | JaisCloud has parity |
| **STS** | Full (AssumeRole, GetCallerIdentity, GetSessionToken) | Full | JaisCloud has parity |
| **SNS** | Full (topics, subs, fan-out, MessageAttributes) | Full | JaisCloud has parity |
| **DynamoDB** | Full (tables, items, expressions, GSI, streams, paginate) | Full | JaisCloud has parity |
| **DynamoDB Streams** | Full (ring buffer, shard iter, sequence nums) | Full | JaisCloud has parity |
| **S3** | Full (buckets, objects, multipart, copy, versioning-ready) | Full | JaisCloud has parity |
| **Lambda** | Echo invoke only | Real execution (runtimes) | LocalStack far deeper |
| **EC2 / VPC** | Control-plane CRUD + VPC/Subnet/SG/IGW/NGW | Full | LocalStack has more ops |
| **Route 53** | Hosted zones, record sets, health checks | Full | Similar depth |
| **CloudFormation** | Basic (Create/Update/Delete/Describe/Validate) | Full (resource graph, change sets, drift detection) | LocalStack far deeper |
| **EventBridge** | Rules, targets, PutEvents, EMR integration | Full | Similar depth |
| **ECS** | Full control-plane (clusters, tasks, services) | Control-plane (paid tier) | JaisCloud has parity; ECS paid in LS |

### 1B. Services in JaisCloud — NOT in LocalStack Open-Source (JaisCloud advantage)

| Service | JaisCloud | LocalStack | Notes |
|---|---|---|---|
| **EMR on EC2** | Full (RunJobFlow, steps, Spark executor, EventBridge integration) | Paid tier only | JaisCloud unique in OSS |
| **EMR on EKS** | Full (virtual clusters, job runs, managed endpoints) | Paid tier only | JaisCloud unique in OSS |
| **Glue Data Catalog** | Full (databases, tables, partitions, Iceberg CAS pointer) | Paid tier only | JaisCloud unique in OSS |
| **RDS** | Control-plane CRUD (instances, clusters, subnet groups) | Paid tier only | JaisCloud has control plane |
| **ElastiCache** | Control-plane CRUD (clusters, replication groups) | Paid tier only | JaisCloud has control plane |

### 1C. Services in LocalStack Open-Source — NOT in JaisCloud (gaps to fill)

| Service | LocalStack Dir | Priority | Cloud-Agnostic Mapping |
|---|---|---|---|
| **Secrets Manager** | `secretsmanager/` | P0 — extremely common | `SecretStore` (KV + versioning) |
| **SSM Parameter Store** | `ssm/` | P0 — extremely common | `ParameterStore` (hierarchical KV) |
| **KMS** | `kms/` | P0 — required by many services | `KeyStore` (key material + encrypt/decrypt) |
| **API Gateway** | `apigateway/` | P0 — REST + HTTP + WebSocket APIs | `GatewayProvider` → `FunctionProvider` |
| **CloudWatch Metrics** | `cloudwatch/` | P1 — observability sink | `MetricStore` (time series) |
| **CloudWatch Logs** | `logs/` | P1 — observability sink | `LogStore` (log groups/streams) |
| **Step Functions** | `stepfunctions/` | P1 — orchestration | `WorkflowProvider` (state machine ASL) |
| **Kinesis** | `kinesis/` | P1 — streaming | `StreamProvider` (shards, PutRecord, GetRecords) |
| **EventBridge Scheduler** | `scheduler/` | P1 — cron/one-time triggers | `SchedulerProvider` |
| **SES** | `ses/` | P1 — email sink for tests | `EmailProvider` |
| **ECR** | `ecr/` | P1 — container image registry | `RegistryProvider` |
| **Kinesis Firehose** | `firehose/` | P2 — delivery streams → S3/ES | `DeliveryProvider` → `ObjectProvider` |
| **Redshift** | `redshift/` | P2 — data warehouse control plane | `WarehouseProvider` |
| **Elasticsearch / OpenSearch** | `es/`, `opensearch/` | P2 — search | `SearchProvider` (proxy to embedded ES) |
| **Secrets Rotation** | (part of secretsmanager) | P2 — Lambda-driven rotation | Extends `SecretStore` |
| **ACM** | `acm/` | P2 — TLS certificates | `CertProvider` |
| **Route 53 Resolver** | `route53resolver/` | P2 — DNS resolver rules | Extends `DNSProvider` |
| **S3 Control** | `s3control/` | P2 — S3 batch ops, access points | Extends `ObjectProvider` |
| **Resource Groups** | `resource_groups/` | P3 — grouping/tagging | `GroupProvider` |
| **Resource Groups Tagging API** | `resourcegroupstaggingapi/` | P3 — cross-service tag queries | `TagQueryProvider` |
| **Config** | `configservice/` | P3 — configuration recording | `ConfigProvider` |
| **SWF** | `swf/` | P3 — legacy workflows | `WorkflowProvider` (SWF dialect) |
| **Support** | `support/` | P3 — mostly stub | Stub |
| **Transcribe** | `transcribe/` | P3 — audio transcription stub | Stub |

---

## 2. Service Priority Matrix

```
P0 — Blockers (required by most real workloads today)
  ├── Secrets Manager   (apps read secrets at boot)
  ├── SSM               (Parameter Store used everywhere)
  ├── KMS               (S3/DynamoDB/Secrets encryption)
  └── API Gateway       (Lambda + REST API pattern ubiquitous)

P1 — High value (common in CI pipelines, microservices)
  ├── CloudWatch Metrics + Logs   (observability)
  ├── Step Functions               (orchestration)
  ├── Kinesis                      (event streaming)
  ├── EventBridge Scheduler        (cron triggers)
  ├── SES                          (email in tests)
  └── ECR                          (container registry)

P2 — Medium (analytics, search, delivery pipelines)
  ├── Kinesis Firehose   (→ S3 delivery)
  ├── Redshift           (control plane)
  ├── Elasticsearch / OpenSearch
  ├── ACM
  ├── Route 53 Resolver
  └── S3 Control

P3 — Low / Stubs (niche, legacy, or stub-sufficient)
  ├── Resource Groups + Tagging API
  ├── Config
  ├── SWF
  ├── Support
  └── Transcribe
```

---

## 3. Multicloud & Cloud-Agnostic Design Plan

### 3.1 Core Design Principles (Already Established)

JaisCloud's existing architecture is already well-positioned for cloud-agnostic expansion:

- **Single cloud per instance** — `cfg.Cloud` selects adapter at startup; no per-request switching
- **`NormalizedRequest.ResourceID`** — injected formatter keeps ARN/Azure-resource-ID/GCP resource names out of providers
- **`ServiceToProvider`** on `CloudAdapter` — wire service name → provider prefix, no switch in gateway
- **Data-driven `services.go`** — add one `ServiceDescriptor` entry to register a service end-to-end

New services must follow these same rules. Violations create technical debt that blocks GCP/Azure phases.

### 3.2 Cloud-Agnostic Provider Interfaces

Each new service should expose a **semantic provider interface**, not an AWS-shaped one. The AWS codec translates AWS wire → semantic call; a future GCP codec translates GCP wire → the same semantic call.

#### Pattern: Provider Interface Design

```
internal/provider/<domain>/
  provider.go      — HandlerFunc registrations + semantic interface
  store.go         — Store interface (cloud-neutral operations)
  memory.go        — MemoryStore implementation
  postgres.go      — PostgresStore implementation (for full mode)
```

**DO NOT** put AWS ARN formats, AWS action names, or AWS error codes inside providers. Those belong in the codec layer.

#### Recommended Provider Groupings

| Provider Name | Handles (AWS) | Handles (GCP) | Handles (Azure) |
|---|---|---|---|
| `SecretProvider` | Secrets Manager | Secret Manager | Key Vault (secrets) |
| `ParameterProvider` | SSM Parameter Store | Runtime Configurator | App Configuration |
| `KeyProvider` | KMS | Cloud KMS | Key Vault (keys) |
| `GatewayProvider` | API Gateway v1/v2 | Cloud Endpoints / Apigee | API Management |
| `MetricProvider` | CloudWatch Metrics | Cloud Monitoring | Azure Monitor |
| `LogProvider` | CloudWatch Logs | Cloud Logging | Log Analytics |
| `WorkflowProvider` | Step Functions | Cloud Workflows | Logic Apps |
| `StreamProvider` | Kinesis | Pub/Sub (pull) | Event Hubs |
| `SchedulerProvider` | EventBridge Scheduler | Cloud Scheduler | Scheduler |
| `EmailProvider` | SES | — | Communication Services (email) |
| `RegistryProvider` | ECR | Artifact Registry | Container Registry |
| `DeliveryProvider` | Kinesis Firehose | Dataflow | Event Hubs Capture |
| `SearchProvider` | OpenSearch / ES | Cloud Search | Azure AI Search |
| `WarehouseProvider` | Redshift | BigQuery | Synapse |
| `CertProvider` | ACM | Certificate Manager | App Service Certificates |
| `GroupProvider` | Resource Groups | Resource Manager | Resource Groups |
| `ConfigProvider` | AWS Config | Asset Inventory | Azure Policy |

### 3.3 Store Design Considerations

#### Naming Convention

Store types use semantic names, never AWS service names:

```go
// WRONG — couples store to AWS:
type SecretsManagerStore interface { ... }

// CORRECT — cloud-neutral:
type SecretStore interface {
    Create(ctx, id string, value []byte, tags map[string]string) error
    Get(ctx, id string) (*Secret, error)
    PutVersion(ctx, id string, value []byte) (versionID string, error)
    ListVersions(ctx, id string) ([]*SecretVersion, error)
    Delete(ctx, id string) error
    List(ctx, prefix string) ([]*SecretMeta, error)
}
```

#### Versioned Secrets Store

Secrets Manager and Parameter Store both need versioning. Design one `VersionedStore` interface and share it:

```go
type VersionedSecretStore interface {
    SecretStore
    GetVersion(ctx, id, versionID string) (*SecretVersion, error)
    DeleteVersion(ctx, id, versionID string) error
    RestoreVersion(ctx, id, versionID string) error
}
```

#### KMS Key Store

KMS requires in-process crypto (for local dev) without real HSM:

```go
type KeyStore interface {
    CreateKey(ctx, id string, spec KeySpec, usage KeyUsage) (*Key, error)
    DescribeKey(ctx, id string) (*Key, error)
    Encrypt(ctx, keyID string, plaintext []byte, ctx map[string]string) ([]byte, error)
    Decrypt(ctx, keyID string, ciphertext []byte, ctx map[string]string) ([]byte, error)
    GenerateDataKey(ctx, keyID string, bits int) (*DataKey, error)
    ScheduleKeyDeletion(ctx, id string, pendingDays int) error
}
```

`MemoryKeyStore` uses AES-GCM with an in-process key map — never contacts a real KMS. `PostgresKeyStore` persists key material (encrypted with a master key derived from `JAISCLOUD_KMS_MASTER_KEY` env var).

#### Log / Metric Store

CloudWatch Logs and Metrics are write-heavy time-series data. Use append-only in-memory ring buffers in lite mode:

```go
type LogStore interface {
    CreateGroup(ctx, name string, retention int) error
    CreateStream(ctx, group, stream string) error
    PutEvents(ctx, group, stream string, events []LogEvent) (nextSeqToken string, error)
    FilterLogEvents(ctx, group string, opts FilterOpts) ([]*LogEvent, string, error) // (events, nextToken, err)
}

type MetricStore interface {
    PutMetricData(ctx, namespace string, data []MetricDatum) error
    GetMetricStatistics(ctx, namespace, metric string, dims []Dimension, opts StatOpts) ([]Datapoint, error)
    ListMetrics(ctx, namespace string) ([]*Metric, error)
}
```

Do NOT use PostgreSQL for log/metric stores — the write amplification is too high for local dev workloads. Use in-memory ring buffers with configurable retention.

#### Stream Store (Kinesis)

Kinesis shards are append-only ordered sequences. Reuse and extend the `MemoryStreamStore` pattern from DynamoDB Streams:

```go
type StreamStore interface {
    CreateStream(ctx, name string, shards int) error
    PutRecord(ctx, name, partitionKey string, data []byte) (shardID, seqNum string, error)
    PutRecords(ctx, name string, records []StreamRecord) ([]StreamResult, error)
    GetShardIterator(ctx, name, shardID string, iterType IteratorType, seqNum string) (string, error)
    GetRecords(ctx, iterToken string, limit int) ([]*StreamRecord, nextIter string, error)
    DescribeStream(ctx, name string) (*StreamDescription, error)
}
```

#### API Gateway Store

API Gateway has a deep control-plane object model (RestApi → Resource → Method → Integration → Deployment → Stage). Store the full model graph in `ResourceStore` as typed JSON blobs rather than individual tables:

```go
// Use ResourceStore with structured JSON values — no new table needed
// Key scheme: "apigw_rest_api:<apiId>" / "apigw_resource:<apiId>/<resourceId>" / etc.
// Integration detail is embedded in the Method resource blob
```

Request routing at invoke time resolves the path against the stored resource tree.

### 3.4 Wire Protocol Codec Strategy

When adding a new AWS service, follow the existing pattern in `internal/adapter/aws/services/`:

```go
// internal/adapter/aws/services/secretsmanager.go
type SecretsManagerCodec struct{}

func (c *SecretsManagerCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
    // JSON/Target: X-Amz-Target: secretsmanager.GetSecretValue
    // Extract action, params
}

func (c *SecretsManagerCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) ([]byte, int, http.Header, error) {
    // Marshal response JSON
}
```

Then add one entry to `internal/adapter/aws/services.go`:

```go
{
    Name:         "secretsmanager",
    SigV4Name:    "secretsmanager",
    TargetPrefix: "secretsmanager",
    ProviderPrefix: "Secret",
    Codec:        func() Codec { return &services.SecretsManagerCodec{} },
},
```

For GCP equivalents: GCP Secret Manager uses REST (`GET /v1/projects/{project}/secrets/{secret}/versions/{version}:access`). The GCP adapter would route this to the same `SecretProvider` via a GCP-specific codec — no provider changes required.

### 3.5 ResourceManager Integration

All new services with parent-child relationships must register `DeleteGuardRule`s at construction:

```go
// Example: deleting a KMS key that still has aliases
rm.RegisterRules([]resourcemgr.DeleteGuardRule{
    {
        ParentType: "kms_key",
        ChildType:  "kms_alias",
        Policy:     resourcemgr.PolicyFail,
        ErrorCode:  "KMSInvalidStateException",
        StatusCode: 400,
        Message:    "key has pending aliases; delete aliases first",
    },
})
```

---

## 4. Implementation Phases (New Services)

### Phase 3-A: Security Foundation (P0 gaps)

**Goal:** Unblock real application workloads that depend on secrets, config, and encryption.

| Service | Provider | Store | Codec | ARN Type |
|---|---|---|---|---|
| **Secrets Manager** | `SecretProvider` | `SecretStore` (memory + postgres) | `SecretsManagerCodec` (JSON/Target) | `secretsmanager-secret` |
| **SSM Parameter Store** | `ParameterProvider` | `ParameterStore` (memory + postgres) | `SSMCodec` (JSON/Target) | `ssm-parameter` |
| **KMS** | `KeyProvider` | `KeyStore` (memory + postgres) | `KMSCodec` (JSON/Target) | `kms-key` |

**Integration:** Wire KMS into S3 (SSE-KMS), DynamoDB (encrypted tables), and Secrets Manager (secret encryption). Use a passthrough-AES implementation — no real HSM.

**Tests:** Secrets Manager CRUD + versioning; SSM GetParameter/PutParameter/GetParametersByPath; KMS Encrypt/Decrypt/GenerateDataKey round-trips.

### Phase 3-B: API Gateway (P0 gap)

| Component | Scope |
|---|---|
| `GatewayProvider` | REST APIs v1: Create/Get/Delete RestApi, Resource, Method, Integration, Deployment, Stage |
| `APIGatewayV2Provider` | HTTP APIs v2: CreateApi, CreateRoute, CreateIntegration, CreateStage |
| Invoke path | `POST /{stage}/{resource+}` resolves to Lambda invoke via `FunctionProvider` |
| `APIGatewayCodec` | `X-Amz-Target: APIGateway.*` (JSON/Target, control plane) |
| `APIGatewayV2Codec` | JSON/Target for HTTP API management |
| Invoke codec | REST path routing (no target header; detected by SigV4 service name `execute-api`) |

**ARN types:** `apigateway-restapi`, `apigateway-resource`, `apigateway-stage`

### Phase 3-C: Observability (P1 gaps)

| Service | Provider | Store Design | Notes |
|---|---|---|---|
| **CloudWatch Metrics** | `MetricProvider` | In-memory ring buffer per namespace, 14-day window | Prometheus scrape from `/metrics` reads same data |
| **CloudWatch Logs** | `LogProvider` | In-memory ring buffer per stream, configurable retention | FilterLogEvents supports basic pattern matching |
| **EventBridge Scheduler** | `SchedulerProvider` | `ResourceStore` + in-process ticker | Fires `PutEvents` to EventBridge at scheduled time |

### Phase 3-D: Streaming & Orchestration (P1 gaps)

| Service | Provider | Store Design | Notes |
|---|---|---|---|
| **Kinesis** | `StreamProvider` | Extend `MemoryStreamStore`; new postgres impl | PutRecord, GetRecords, shard splitting/merging stub |
| **Kinesis Firehose** | `DeliveryProvider` | State in `ResourceStore`; delivery to `ObjectProvider` | Buffer flush → S3 PutObject; ES delivery stub |
| **Step Functions** | `WorkflowProvider` | `ResourceStore` (state machine def) + execution store | ASL interpreter: Task, Choice, Parallel, Wait, Map |

**Step Functions ASL Interpreter scope:**  
- States: `Task` (Lambda invoke), `Choice`, `Pass`, `Succeed`, `Fail`, `Wait` (wall-clock or timestamp), `Parallel`, `Map`  
- Execution runs in a goroutine; state updates polled via `DescribeExecution`  
- `Task` state delegates to `FunctionProvider` (Lambda) or `QueueProvider` (SQS) based on resource ARN

### Phase 3-E: Identity & Email (P1 gaps)

| Service | Provider | Notes |
|---|---|---|
| **SES** | `EmailProvider` | Send/verify email addresses; store sent messages in memory for test assertions; SMTP passthrough optional |
| **ECR** | `RegistryProvider` | CreateRepository, DescribeRepositories, GetAuthorizationToken; image manifest stored in `ObjectProvider` (S3 bucket) |

### Phase 4 (Already Planned): Athena + Full Export/Import

*(Unchanged from existing ROADMAP.md)*

### Phase 5 (Already Planned): GCP Layer

The providers added in Phases 3-A through 3-E map cleanly to GCP equivalents:

| JaisCloud Provider | AWS Service | GCP Equivalent |
|---|---|---|
| `SecretProvider` | Secrets Manager | Secret Manager |
| `ParameterProvider` | SSM | Runtime Configurator |
| `KeyProvider` | KMS | Cloud KMS |
| `GatewayProvider` | API Gateway | Cloud Endpoints / Apigee |
| `MetricProvider` | CloudWatch Metrics | Cloud Monitoring |
| `LogProvider` | CloudWatch Logs | Cloud Logging |
| `WorkflowProvider` | Step Functions | Cloud Workflows |
| `StreamProvider` | Kinesis | Pub/Sub |
| `SchedulerProvider` | EventBridge Scheduler | Cloud Scheduler |
| `EmailProvider` | SES | (no direct equivalent — SendGrid often used) |
| `RegistryProvider` | ECR | Artifact Registry |

---

## 5. Store Architecture Summary

### Lite Mode (default)

All stores are in-memory. Ring buffers for append-only data (logs, metrics, streams). No external deps.

### Full Mode (PostgreSQL)

New tables follow the existing migration pattern (`internal/store/migrations/`):

| Store | Table(s) | Notes |
|---|---|---|
| `SecretStore` | `jc_secrets`, `jc_secret_versions` | Versioned; soft-delete with pending window |
| `ParameterStore` | `jc_parameters`, `jc_parameter_history` | Hierarchical path prefix queries via `LIKE '/path/%'` |
| `KeyStore` | `jc_kms_keys`, `jc_kms_aliases` | Key material stored as encrypted BYTEA |
| `GatewayStore` | Uses `jc_resources` with typed JSON blobs | No separate table — embed in ResourceStore |
| `MetricStore` | `jc_metrics` (time series, partitioned by day) | Consider TimescaleDB hypertable for full mode perf |
| `LogStore` | `jc_log_groups`, `jc_log_events` | Append-only; BRIN index on `timestamp` |
| `WorkflowStore` | `jc_state_machines`, `jc_executions`, `jc_execution_events` | JSON column for ASL definition and history |
| `StreamStore` | `jc_kinesis_streams`, `jc_kinesis_records` | Sequence number as bigint; shard as partition key |
| `RegistryStore` | Uses `jc_resources` + `ObjectProvider` (S3) | Image layers stored as S3 objects |

### Cloud-Neutral Store Rule

Store interfaces accept and return **domain types**, not AWS-shaped structs:

```go
// WRONG — AWS-coupled:
type SecretStore interface {
    GetSecretValue(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error)
}

// CORRECT — cloud-neutral:
type SecretStore interface {
    Get(ctx context.Context, id string) (*Secret, error)
    GetVersion(ctx context.Context, id, versionID string) (*SecretVersion, error)
}
```

The codec layer translates AWS request → `Get(ctx, id)` call. A GCP codec translates GCP request → the same `Get(ctx, id)` call.

---

## 6. File Creation Checklist (per new service)

For each service in the gap list, the minimal set of files to create:

```
internal/
  adapter/aws/services/<service>.go        # Codec (Decode + Encode)
  provider/<domain>/
    provider.go                            # HandlerFunc registrations
    store.go                               # Store interface
    memory.go                              # MemoryStore
    postgres.go                            # PostgresStore (full mode)
  store/migrations/
    00N_<service>.sql                      # CREATE TABLE statements

internal/adapter/aws/services.go           # +1 ServiceDescriptor entry
internal/config/config.go                  # +1 ARN formatter entry
cmd/jaiscloud/main.go                      # Wire provider + store at startup
tests/integration/
  <service>_test.go                        # Integration test suite
```

**Total files per service:** ~7–9 files. Existing infrastructure (codec dispatch, ResourceManager, gateway, CLI, Prometheus) requires zero changes when the above pattern is followed.
