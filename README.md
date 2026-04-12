# JaisCloud

**A lightweight, zero-dependency AWS emulator that runs anywhere — laptop, CI, or Kubernetes.**

JaisCloud speaks the exact same wire protocols as AWS — no SDK shims, no proxy rewrites. Point any `aws-sdk-go-v2`, `boto3`, or `aws-sdk-js` client at `http://localhost:4566` and it works, unmodified.

```bash
# Start in seconds
go run ./cmd/jaiscloud/ start

# Or with Docker
docker run -p 4566:4566 ghcr.io/jaisraj/jaiscloud:latest
```

```python
# Works with any AWS SDK — no code changes
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:4566",
                  aws_access_key_id="test", aws_secret_access_key="test")
s3.create_bucket(Bucket="my-bucket")
```

---

## Why JaisCloud?

| | JaisCloud | LocalStack (Community) | Moto | Fake GCS |
|---|---|---|---|---|
| **Single static binary** | ✅ | ❌ (Python + Docker) | ❌ (Python library) | ❌ |
| **Zero runtime deps (lite mode)** | ✅ | ❌ | ❌ | ✅ |
| **Postgres persistence (full mode)** | ✅ | 💰 Pro | ❌ | ❌ |
| **Exact AWS wire protocol** | ✅ | ✅ | Partial | N/A |
| **Kubernetes-native** | ✅ | Partial | ❌ | ✅ |
| **State export / import** | ✅ | ❌ | ❌ | ❌ |
| **Prometheus metrics** | ✅ | 💰 Pro | ❌ | ❌ |
| **Plugin system** | ✅ | ❌ | ❌ | ❌ |
| **Spark / EMR emulation** | ✅ | ❌ | ❌ | ❌ |
| **Apache Iceberg (Glue Catalog)** | ✅ | ❌ | ❌ | ❌ |
| **Written in Go** | ✅ | ❌ | ❌ | ❌ |
| **License** | Apache-2.0 | Apache-2.0 | Apache-2.0 | Apache-2.0 |

> **Fidelity over features.** JaisCloud implements fewer services than LocalStack, but the ones it does implement pass the full AWS SDK integration test suite with no patching.

---

## Supported Services

### ✅ Amazon S3
| Operation | Supported |
|---|---|
| CreateBucket / DeleteBucket / ListBuckets / HeadBucket | ✅ |
| PutObject / GetObject / HeadObject / DeleteObject | ✅ |
| ListObjectsV1 / ListObjectsV2 (prefix, delimiter, pagination) | ✅ |
| CopyObject | ✅ |
| DeleteObjects (batch) | ✅ |
| CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload | ✅ |
| GetBucketLocation | ✅ |
| Object/Bucket tagging, ACLs, versioning stubs | ✅ (stubs — no error) |
| AWS chunked transfer encoding (`x-amz-content-sha256: STREAMING-*`) | ✅ |
| S3A Hadoop connector compatibility (FileOutputCommitter, flat-key semantics) | ✅ |

### ✅ Amazon SQS
| Operation | Supported |
|---|---|
| CreateQueue / DeleteQueue / ListQueues / GetQueueUrl | ✅ |
| GetQueueAttributes / SetQueueAttributes | ✅ |
| SendMessage / ReceiveMessage / DeleteMessage | ✅ |
| SendMessageBatch / DeleteMessageBatch | ✅ |
| ChangeMessageVisibility / ChangeMessageVisibilityBatch | ✅ |
| PurgeQueue | ✅ |
| TagQueue / UntagQueue / ListQueueTags | ✅ |
| FIFO queues (deduplication, message groups) | ✅ |
| Dead-letter queues | ✅ |
| JSON protocol (`X-Amz-Target`) + Query/XML protocol | ✅ |

### ✅ Amazon DynamoDB
| Operation | Supported |
|---|---|
| CreateTable / DescribeTable / DeleteTable / ListTables | ✅ |
| PutItem / GetItem / DeleteItem | ✅ |
| UpdateItem (SET, REMOVE, ADD expressions) | ✅ |
| Scan (with FilterExpression) | ✅ |
| Query (KeyConditionExpression, begins_with, between) | ✅ |
| BatchWriteItem / BatchGetItem | ✅ |
| Composite primary keys (hash + range) | ✅ |
| Conditional writes (ConditionExpression) | ✅ |
| DynamoDB Streams (GetShardIterator, GetRecords) | ✅ |

### ✅ Amazon SNS
| Operation | Supported |
|---|---|
| CreateTopic / DeleteTopic / ListTopics | ✅ |
| GetTopicAttributes / SetTopicAttributes | ✅ |
| Subscribe / Unsubscribe / ListSubscriptions | ✅ |
| Publish (fan-out to SQS subscriptions with MessageAttributes) | ✅ |
| DeleteTopic removes all subscriptions | ✅ |

### ✅ Amazon EventBridge
| Operation | Supported |
|---|---|
| PutRule / DescribeRule / DeleteRule / ListRules | ✅ |
| PutTargets / RemoveTargets / ListTargetsByRule | ✅ |
| EnableRule / DisableRule | ✅ |
| PutEvents (inject arbitrary events into the matching pipeline) | ✅ |
| Event delivery to SQS targets | ✅ |
| EventPattern matching | ✅ |

### ✅ AWS IAM + STS
| Operation | Supported |
|---|---|
| CreateRole / GetRole / DeleteRole / ListRoles | ✅ |
| CreatePolicy / GetPolicy / DeletePolicy / ListPolicies | ✅ |
| AttachRolePolicy / DetachRolePolicy / ListAttachedRolePolicies | ✅ |
| PutRolePolicy / GetRolePolicy / DeleteRolePolicy (inline) | ✅ |
| CreateUser / GetUser / DeleteUser / ListUsers | ✅ |
| CreateAccessKey / DeleteAccessKey / ListAccessKeys | ✅ |
| GetCallerIdentity | ✅ |
| AssumeRole (returns mock credentials) | ✅ |

### ✅ AWS Lambda (echo mode)
| Operation | Supported |
|---|---|
| CreateFunction / GetFunction / DeleteFunction / ListFunctions | ✅ |
| UpdateFunctionConfiguration / UpdateFunctionCode | ✅ |
| Invoke (echo mode — returns payload unchanged) | ✅ |

> Lambda Invoke uses **echo mode**: the payload you send is returned as the response. No subprocess is spawned. This is intentional — it lets you test fan-out pipelines, event routing, and infrastructure wiring without needing real function code.

### ✅ AWS Glue Data Catalog
| Operation | Supported |
|---|---|
| CreateDatabase / GetDatabase / GetDatabases / UpdateDatabase / DeleteDatabase | ✅ |
| CreateTable / GetTable / GetTables / UpdateTable / DeleteTable | ✅ |
| CreatePartition / GetPartition / GetPartitions / UpdatePartition / DeletePartition | ✅ |
| BatchCreatePartition / BatchDeletePartition | ✅ |
| Iceberg `metadata_location` CAS (conditional update used by Iceberg commits) | ✅ |

> **Apache Iceberg support:** JaisCloud passes as a Glue Catalog endpoint for real Apache Iceberg 1.5+ workloads running in Spark. Iceberg reads and writes table metadata via the Glue API and stores data files in S3 — both backed by JaisCloud. This enables full Iceberg integration testing locally, including schema evolution, time travel, partitioning, and multi-batch appends.

### ✅ Amazon EMR (on EC2)
| Operation | Supported |
|---|---|
| RunJobFlow / DescribeCluster / ListClusters / TerminateJobFlows | ✅ |
| ModifyCluster / SetTerminationProtection / SetVisibleToAllUsers | ✅ |
| AddJobFlowSteps / DescribeStep / ListSteps / CancelSteps | ✅ |
| AddInstanceFleet / ListInstanceFleets / ModifyInstanceFleet | ✅ |
| AddInstanceGroups / ListInstanceGroups / ModifyInstanceGroups | ✅ |
| ListBootstrapActions | ✅ |
| AddTags / RemoveTags | ✅ |
| GetBlockPublicAccessConfiguration / PutBlockPublicAccessConfiguration | ✅ |
| PutManagedScalingPolicy / GetManagedScalingPolicy / RemoveManagedScalingPolicy | ✅ |

### ✅ Amazon EMR on EKS (EMR Containers)
| Operation | Supported |
|---|---|
| CreateVirtualCluster / DescribeVirtualCluster / DeleteVirtualCluster / ListVirtualClusters | ✅ |
| StartJobRun / DescribeJobRun / CancelJobRun / ListJobRuns | ✅ |
| CreateManagedEndpoint / DescribeManagedEndpoint / DeleteManagedEndpoint / ListManagedEndpoints | ✅ |
| TagResource / UntagResource / ListTagsForResource | ✅ |

### ⚙️ Stub services (wire protocol only — no business logic)
The following services are registered and respond with well-formed empty responses so SDK calls don't fail during infrastructure setup. Full implementations are planned.

| Service | Status |
|---|---|
| Amazon EC2 | Stub |
| Amazon Route 53 | Stub |
| Amazon RDS | Stub |
| Amazon ElastiCache | Stub |
| Amazon ECS | Stub |
| AWS CloudFormation | Stub |

---

## Full Mode (PostgreSQL Persistence)

Start with `--mode full --dsn <postgres DSN>` to persist all state across restarts.

```bash
./jaiscloud start --mode full \
  --dsn "postgres://user:pass@localhost:5432/jaiscloud" \
  --blob-dir /var/lib/jaiscloud/blobs
```

### What persists

| Service | PostgreSQL table(s) | Blob storage |
|---|---|---|
| All resource metadata (queues, topics, tables, roles, functions, Glue, EMR…) | `jc_resources` | — |
| SQS messages | `jc_sqs_messages`, `jc_sqs_dedup` | — |
| DynamoDB items | `jc_dynamodb_items` | — |
| S3 object metadata | `jc_s3_objects` | `--blob-dir` (LocalFSBlobStore) |
| S3 object bytes | — | `--blob-dir` (LocalFSBlobStore) |
| Lambda deployment packages | — | `--blob-dir` (MemoryBlobStore by default) |

> **S3 blob storage:** Pass `--blob-dir <path>` to persist S3 object bytes to disk. Without it, blobs are held in memory and lost on restart even in full mode. `LocalFSBlobStore` implements flat S3-key semantics on a local filesystem — keys like `foo/bar` and `foo/bar/baz` coexist correctly, with atomic writes and crash recovery.

### Startup retry

JaisCloud retries the initial database ping up to 10 times with exponential backoff (500 ms → 8 s), so it starts cleanly before Postgres is ready — useful in `docker-compose` or Kubernetes init ordering.

---

## Spark Plugin (`aws-emr-spark`)

The `aws-emr-spark` plugin provides full EMR and EMR-on-EKS API emulation. It is shipped as a separate Go `.so` plugin and loaded at startup via `--plugin-dir`.

```bash
# Build the plugin
cd plugins/aws-emr-spark && make build
# Load at startup
./jaiscloud start --mode full --plugin-dir ./plugins
```

### Executor modes

The plugin selects a Spark executor via the `JAISCLOUD_SPARK_MODE` environment variable:

| Mode | Description |
|---|---|
| `mock` (default) | Jobs complete immediately with `COMPLETED` state. No external process. Ideal for unit tests and CI where you only need EMR API correctness, not actual computation. |
| `k8s` | **Planned — Phase 3.5.** Builds the full `spark-submit --master k8s://...` argument list and logs it to stderr. Job execution is currently delegated to `mock` until `client-go` is wired in. Use this mode to verify your `spark-submit` configuration is generated correctly. |

```bash
# Default: jobs complete immediately
JAISCLOUD_SPARK_MODE=mock ./jaiscloud start --plugin-dir ./plugins

# Log spark-submit args (k8s execution not yet wired)
JAISCLOUD_SPARK_MODE=k8s ./jaiscloud start --plugin-dir ./plugins
```

### Running against a real Spark cluster

The current plugin routes all job execution through the `SparkExecutor` interface. Real cluster support (Spark Standalone, YARN, or K8s) is the next planned milestone:

- **K8s executor (Phase 3.5):** `client-go` will be added to the plugin's `go.mod`. `K8sExecutor.Submit` will launch a `batch/v1 Job` and `K8sExecutor.Status` will map pod phase → `SparkState`.
- **Remote/Standalone (planned):** `SparkConfig.RemoteURL` is already wired into the config layer for a Spark Standalone REST API executor.

Until then, `mock` is the correct mode for all automated testing, and `k8s` is useful only to verify argument generation.

### Resource profiles

Three named sizes control driver/executor CPU and memory for the K8s executor:

| Size | Driver | Executors |
|---|---|---|
| `small` | 500m / 1Gi | 1 × 500m / 1Gi |
| `medium` | 1 / 2Gi | 2 × 1 / 2Gi |
| `large` | 2 / 4Gi | 4 × 2 / 4Gi |

---

## What's New in Phase 2

### Plugin System

JaisCloud supports loadable plugins compiled as Go `.so` files. Plugins extend the emulator with new services without modifying the core binary.

```bash
cd plugins/aws-emr-spark && make build
./jaiscloud start --plugin-dir ./plugins
```

### Multi-Cloud Mode

Each JaisCloud instance runs in one cloud mode. The default is AWS. Azure and GCP adapters are scaffolded (return `501 Not Implemented`) and will be filled in future phases.

```bash
./jaiscloud start --cloud aws    # default — full AWS wire protocol
./jaiscloud start --cloud azure  # stub — returns 501
./jaiscloud start --cloud gcp    # stub — returns 501
```

### Resource Dependency Manager

The `ResourceManager` prevents invalid deletions. When a plugin registers a `DeleteGuardRule`, attempts to delete a parent resource while children exist are blocked, forcibly terminated, or cascaded — depending on the configured policy.

### Prometheus Cloud Label

All metrics include a `cloud` label (`aws` / `azure` / `gcp`) so you can differentiate traffic in mixed-environment dashboards.

---

## Fidelity Notes

JaisCloud prioritises **protocol correctness** over breadth:

- All responses use the exact XML/JSON envelope the AWS SDK expects.
- Error codes match AWS (`NoSuchBucket`, `ResourceNotFoundException`, etc.).
- `Last-Modified` headers use RFC 1123 GMT format, not UTC — the SDK parses these strictly.
- SQS supports both **JSON** (`X-Amz-Target`) and **Query/XML** protocols in the same server.
- DynamoDB key hash is computed from key attributes only (in schema order), matching AWS semantics.
- DynamoDB `x-amz-crc32` header is computed and returned on every response.
- S3 ETag is the MD5 of the stored bytes, including correct handling of AWS chunked transfer encoding.
- S3 implements flat-key semantics: `foo/bar` and `foo/bar/baz` coexist as independent objects, matching real S3 behaviour.

**Known limitations:**
- No IAM policy evaluation — all requests are accepted regardless of attached policies.
- Lambda runs in echo mode only; no actual function execution.
- S3 versioning and object locking are stubbed (no error, no actual versioning).
- No cross-region or cross-account semantics.
- Azure and GCP cloud modes are scaffolded only (Phase 3+).
- EMR K8s executor delegates to mock until Phase 3.5 (client-go not yet wired).

---

## Quick Start

### Binary

```bash
go build -o jaiscloud ./cmd/jaiscloud/
./jaiscloud start
```

### Docker

```bash
docker build -t jaiscloud .
docker run -p 4566:4566 jaiscloud
```

### Docker Compose

```yaml
services:
  jaiscloud:
    image: ghcr.io/jaisraj/jaiscloud:latest
    ports:
      - "4566:4566"
    environment:
      JAISCLOUD_REGION: us-east-1
      JAISCLOUD_LOG_LEVEL: info
```

### Docker Compose with full mode (Postgres + blob storage)

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: jaiscloud
      POSTGRES_PASSWORD: jaiscloud
      POSTGRES_DB: jaiscloud
    volumes:
      - pg_data:/var/lib/postgresql/data

  jaiscloud:
    image: ghcr.io/jaisraj/jaiscloud:latest
    ports:
      - "4566:4566"
    depends_on:
      - postgres
    environment:
      JAISCLOUD_MODE: full
      JAISCLOUD_DSN: postgres://jaiscloud:jaiscloud@postgres:5432/jaiscloud
      JAISCLOUD_REGION: us-east-1
    volumes:
      - blob_data:/var/lib/jaiscloud/blobs
    command: ["start", "--blob-dir", "/var/lib/jaiscloud/blobs"]

volumes:
  pg_data:
  blob_data:
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaiscloud
  labels:
    app: jaiscloud
spec:
  replicas: 1
  selector:
    matchLabels:
      app: jaiscloud
  template:
    metadata:
      labels:
        app: jaiscloud
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "4566"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: jaiscloud
          image: ghcr.io/jaisraj/jaiscloud:latest
          args: ["start", "--metrics"]
          ports:
            - containerPort: 4566
          env:
            - name: JAISCLOUD_REGION
              value: us-east-1
            - name: JAISCLOUD_LOG_LEVEL
              value: info
          readinessProbe:
            httpGet:
              path: /_jaiscloud/health
              port: 4566
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /_jaiscloud/health
              port: 4566
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 500m
              memory: 256Mi
---
apiVersion: v1
kind: Service
metadata:
  name: jaiscloud
spec:
  selector:
    app: jaiscloud
  ports:
    - port: 4566
      targetPort: 4566
```

Point your SDK at `http://jaiscloud:4566` from within the cluster.

---

## Configuration

All flags have an equivalent `JAISCLOUD_*` environment variable.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `JAISCLOUD_PORT` | `4566` | Listen port |
| `--mode` | `JAISCLOUD_MODE` | `lite` | `lite` (memory) or `full` (postgres) |
| `--cloud` | `JAISCLOUD_CLOUD` | `aws` | Cloud to emulate: `aws`, `azure`, `gcp` |
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |
| `--plugin-dir` | `JAISCLOUD_PLUGIN_DIR` | _(empty)_ | Directory containing `.so` plugin files |
| `--dsn` | `JAISCLOUD_DSN` | _(empty)_ | PostgreSQL DSN (required when `--mode full`) |
| `--blob-dir` | `JAISCLOUD_BLOB_DIR` | _(empty)_ | Directory for S3 blob bytes (full mode, optional) |

### Modes

**`lite` (default)** — all state in memory. Zero external dependencies. State is lost on restart. Ideal for unit tests and CI.

**`full`** — resource metadata persisted in PostgreSQL. S3 object bytes optionally persisted to `--blob-dir`. Survives restarts. Ideal for integration environments and long-running dev clusters.

---

## CLI Commands

```bash
jaiscloud start          # start the emulator
jaiscloud version        # print version
jaiscloud env            # print effective config as env vars
jaiscloud doctor         # verify the emulator is reachable
jaiscloud reset          # wipe all state
jaiscloud export -o snapshot.json   # save state to file
jaiscloud import -i snapshot.json   # restore state from file
```

### State snapshots

Export/import lets you seed a fresh emulator with pre-built state — useful for integration test fixtures:

```bash
# Seed your test environment once, capture the state
jaiscloud export -o fixtures/baseline.json

# In CI: restore the baseline before each test run
jaiscloud import -i fixtures/baseline.json
```

---

## Admin API

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | `{"status":"ok"}` liveness check |
| `/_jaiscloud/reset` | POST | Wipe all state (used by integration tests) |
| `/_jaiscloud/export` | GET | JSON snapshot of all state |
| `/_jaiscloud/import` | POST | Restore state from JSON snapshot |
| `/metrics` | GET | Prometheus metrics (requires `--metrics`) |

---

## Connecting AWS SDKs

All SDKs need three things: a custom endpoint, a region, and dummy credentials (any non-empty string works).

### Go (aws-sdk-go-v2)
```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
    o.UsePathStyle = true
})
```

### Python (boto3)
```python
import boto3
client = boto3.client(
    "sqs",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
```

### Node.js (aws-sdk v3)
```javascript
import { S3Client } from "@aws-sdk/client-s3";
const client = new S3Client({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  forcePathStyle: true,
});
```

### AWS CLI
```bash
aws --endpoint-url http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url http://localhost:4566 dynamodb list-tables
aws --endpoint-url http://localhost:4566 glue create-database --database-input '{"Name":"mydb"}'
aws --endpoint-url http://localhost:4566 emr list-clusters
```

---

## Running Tests

```bash
# Unit tests (no server required)
go test -race ./internal/...

# Plugin unit tests
cd plugins/aws-emr-spark && go test -race ./internal/...

# Integration tests (server must be running on :4566)
./jaiscloud start &
go test -race -count=1 ./tests/integration/

# Target a specific service
go test -race -run TestS3       ./tests/integration/
go test -race -run TestSQS      ./tests/integration/
go test -race -run TestDynamo   ./tests/integration/
go test -race -run TestLambda   ./tests/integration/
go test -race -run TestEMR      ./tests/integration/
go test -race -run TestEMRC     ./tests/integration/

# Iceberg e2e tests (requires Docker + Spark image + Postgres)
SPARK_E2E_ICEBERG_IMAGE=spark-iceberg-test \
./jaiscloud start --mode full --dsn "postgres://..." --blob-dir /tmp/blobs &
go test -tags iceberg_e2e -timeout 60m ./tests/full_mode/iceberg/
```

---

## Architecture

```
HTTP request
  → gateway (Chi router + middleware)
      → CloudAdapter.DetectAndDecode        (selected once at startup from --cloud)
          (X-Amz-Target → SQS/DynamoDB/Glue/EMR JSON)
          (Authorization SigV4 scope → all services)
          (Action param → SQS/IAM/STS/SNS Query protocol)
      → inject: Clock, Region, AccountID, Cloud, ResourceID
      → Registry.Dispatch("Service.Action", NormalizedRequest)
          → exact match: built-in Provider (business logic, pure Go)
              → ResourceStore  (queue/table/function/Glue metadata)
              → ServiceStore   (messages/items/objects)
          → plugin wildcard: PluginManager → plugin.Handle
              → EMRProvider / EMRContainersProvider
                  → SparkExecutor (mock | k8s-planned)
                  → StatusPoller
      → Codec.Encode (XML / JSON / raw bytes)
  → HTTP response
```

Each JaisCloud instance runs in one cloud mode (`--cloud`). There is no per-request cloud detection — the adapter is selected once at startup.

---

## Contributing

Contributions welcome. Please open an issue before starting large changes.

```bash
git clone https://github.com/jaisraj/jaiscloud
cd jaiscloud
go test -race ./...          # must pass
go vet ./...                 # must pass
```

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for how to run in full mode, set up the Spark cluster, and write custom plugins.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
