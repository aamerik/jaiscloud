# JaisCloud

<p align="center">
  <img src="docs/images/jaiscloud-hero.png" alt="JaisCloud — AI Agent to multi-cloud emulation" width="100%"/>
</p>

> **Early Development Notice**
> JaisCloud is under active development. While core services are functional and tested, some operations may have incomplete implementations, behavioural differences from AWS, or known bugs. If you encounter an issue, please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) with a minimal reproduction — your report directly shapes what gets fixed next.

**⚡ JaisCloud — Fast, Local, Realistic Cloud for AI-Driven Development that runs anywhere — laptop, CI, or Kubernetes**

JaisCloud is a free and open-source, lightweight multi-cloud emulator designed to enable AI agents and developers to rapidly deploy and validate code changes for enterprise applications that depend on cloud resources—without interacting with real cloud environments. It goes beyond basic emulation by providing high-fidelity, scaled-down implementations of cloud services, including real execution backends for systems like EMR (Spark), Lambda, and container-based workloads. This allows workloads to behave much closer to actual cloud environments while still running locally or in isolated infrastructure.

By combining protocol-level compatibility with real execution semantics, JaisCloud enables end-to-end validation of cloud-dependent workflows in complete isolation, eliminating the latency, cost, and risk associated with real deployments. **This dramatically shortens the feedback loop: AI agents can provision resources, execute workflows, and verify behavior in seconds rather than minutes. At the same time, it significantly reduces cloud costs by removing the need to provision real cloud infrastructure for development and testing workflows.**

As an open-source project, JaisCloud offers transparency, extensibility, and community-driven innovation—allowing teams to customize, audit, and evolve the platform to fit their specific needs. It plays a critical role in accelerating AI-driven Software Development Lifecycle (AI-SDLC) by enabling fast, reliable, and repeatable validation of code changes. By empowering AI agents with tight feedback cycles and realistic execution environments, JaisCloud becomes a foundational tool for building, testing, and evolving cloud-native systems with high velocity.

**One binary per cloud.** JaisCloud ships separate, self-contained binaries for each cloud provider. Each binary speaks the exact wire protocol for that cloud — no runtime flag to switch clouds.

| Cloud | Binary | Status |
|---|---|---|
| [AWS](#aws) | `jaiscloud-aws` | Full implementation |
| [Azure](#azure) | `jaiscloud-azure` | Stub (501 — in progress) |
| [GCP](#gcp) | `jaiscloud-gcp` | Stub (501 — in progress) |

---

## AWS

`jaiscloud-aws` speaks the exact same wire protocols as AWS — no SDK shims, no proxy rewrites. Point any `aws-sdk-go-v2`, `boto3`, or `aws-sdk-js` client at `http://localhost:4566` and it works, unmodified.

```bash
# Start in seconds
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/ && ./jaiscloud-aws start

# Or with Docker
docker run -p 4566:4566 ghcr.io/jaisrajms/jaiscloud-aws:latest
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

| | JaisCloud | LocalStack (Community) | Moto |
|---|---|---|---|
| **Single static binary** | ✅ | ❌ (Python + Docker) | ❌ (Python library) |
| **Zero runtime deps (lite mode)** | ✅ | ❌ | ❌ |
| **Postgres persistence (full mode)** | ✅ | 💰 Pro | ❌ |
| **Exact AWS wire protocol** | ✅ | ✅ | Partial |
| **Kubernetes-native** | ✅ | Partial | ❌ |
| **State export / import** | &#x231B; | ❌ | ❌ |
| **Prometheus metrics** | ✅ | 💰 Pro | ❌ |
| **Spark / EMR real execution** | ✅ | ❌ | ❌ |
| **Apache Iceberg (Glue Catalog)** | ✅ | ❌ | ❌ |
| **Written in Go** | ✅ | ❌ | ❌ |
| **Multi Cloud** | &#x231B; | Partial | ❌ |
| **License** | Apache-2.0 | Apache-2.0 | Apache-2.0 |

---

## Supported Services

**Legend:**
- ✅ **Full** — real business logic; passes the AWS SDK integration test suite
- ⚙️ **Metadata-only** — wire protocol + resource CRUD; no execution engine (e.g. EC2 instances don't run)
- 🔌 **Stub** — endpoint exists, returns plausible responses; limited operation coverage

### Full implementations

| Service | Operations / Notes |
|---|---|
| Amazon S3 | Buckets, objects, multipart upload, batch delete, chunked encoding, versioning, policies, website, lifecycle, notifications; Iceberg / S3A compatible |
| Amazon SQS | All 17 operations; FIFO + deduplication; DLQ; message-move task; long-poll; JSON + Query/XML protocols |
| Amazon DynamoDB | CRUD, condition/update/filter expressions, batch ops, parallel scan, composite keys, cursor pagination, TTL |
| Amazon DynamoDB Streams | Stream creation, shard iterators, record delivery to Lambda via ESM |
| Amazon SNS | Topics, subscriptions, publish, SQS fan-out with MessageAttributes and filter policies |
| Amazon EventBridge | Rules, targets, event pattern matching, SQS / Lambda delivery; paginated ListRules / ListTargetsByRule |
| AWS IAM | Roles, policies, users, groups, access keys, instance profiles, policy attachments |
| AWS STS | AssumeRole, GetCallerIdentity, GetSessionToken, federation tokens |
| AWS Lambda | Echo (mock) / Docker warm pool / K8s warm pod per function; event source mappings (SQS, DynamoDB Streams, Kinesis); aliases, versions, layers (Docker /opt mount), concurrency, function URLs; X-Amz-Log-Result tail |
| AWS Glue Data Catalog | Databases, tables, partitions, table versions, Iceberg metadata CAS, crawlers |
| Amazon Kinesis | Streams, shards, records (PutRecord / PutRecords / GetRecords), shard iterators, shard split/merge, consumers, retention, tags |
| Amazon EMR (on EC2) | Clusters, steps, instance fleets/groups; bootstrap actions as K8s init containers; mock or real Docker/K8s Spark |
| Amazon EMR on EKS | Virtual clusters, job runs, managed endpoints; mock or real Docker/K8s Spark |
| AWS KMS | Keys, aliases, grants, Encrypt/Decrypt/GenerateDataKey, envelope crypto (AES-256-GCM), key rotation |
| AWS Secrets Manager | Secrets, versions, binary secrets, rotation, KMS-encrypted at rest |
| AWS SSM Parameter Store | String / StringList / SecureString, versioning, labels, history, path queries, tags; paginated Describe/GetByPath |
| AWS API Gateway (REST) | Resources, methods, integrations, deployments, stages, request validators, domain names, usage plans, API keys, base path mappings; MOCK / AWS_PROXY / HTTP_PROXY invocation |
| AWS CloudFormation | Intrinsics (Ref, Fn::Sub, Fn::Join, Fn::Select, Fn::If, Fn::GetAtt, Fn::ImportValue), topological sort, real resource dispatch, change sets, SAM Transform |
| Amazon CloudWatch | PutMetricData, GetMetricData, GetMetricStatistics, PutMetricAlarm, DescribeAlarms; in-memory metric ring |
| Amazon CloudWatch Logs | Log groups, log streams, PutLogEvents, GetLogEvents, FilterLogEvents, metric filters, subscription filters, retention policy, export tasks |
| AWS Step Functions | Real ASL engine — all 8 state types (Pass, Task, Choice, Wait, Map, Parallel, Succeed, Fail); retry with backoff; catch; input/output processing (InputPath, OutputPath, ResultPath, Parameters) |

### Metadata-only implementations

These services implement the full wire protocol and resource CRUD (create, describe, delete, tag) but do not have an underlying execution engine — instances don't run, clusters don't provision real VMs, etc.

| Service | Supported operations |
|---|---|
| Amazon EC2 | Instances, AMIs, security groups, key pairs, VPCs, subnets, route tables, internet gateways, snapshots, volumes, placement groups |
| Amazon Route 53 | Hosted zones, record sets, health checks, tags |
| Amazon RDS | DB instances, DB clusters, parameter groups, subnet groups, snapshots |
| Amazon ElastiCache | Clusters, replication groups, subnet groups, parameter groups |
| Amazon ECS | Clusters, task definitions (with validation), services (RunningCount reconciliation), tasks |
| Amazon EKS | Clusters, node groups, Fargate profiles, add-ons |
| AWS ELBv2 | Application and Network Load Balancers, target groups, listeners, rules, tags |
| Amazon ECR | Repositories, images, lifecycle policies, registry scanning, image tags |
| AWS ACM | Certificates, certificate details, tags; request/import/describe/delete |
| Amazon Kinesis Data Firehose | Delivery streams, S3/Redshift/Elasticsearch destinations, tags |
| AWS Config | Configuration recorders, delivery channels, config rules, compliance evaluation status |
| AWS Resource Groups | Groups, group queries, group resources, tags |
| Amazon Redshift | Clusters, parameter groups, subnet groups, snapshots |
| Amazon Athena | Workgroups, named queries, query executions |

### Stub implementations

These services accept requests and return plausible responses but have limited operation coverage.

| Service | Notes |
|---|---|
| Amazon SES | SendEmail, SendRawEmail, verified identities, templates; messages are logged, not delivered |
| Amazon Cognito | User pools, users, app clients, identity pools; auth flows return mock tokens |

For per-operation coverage, executor modes, fidelity notes, and full-mode persistence details see the [Service Reference](#service-reference) section below.

---

## Quick Start

### Binary

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
./jaiscloud-aws start
```

### Docker

```bash
docker pull ghcr.io/jaisrajms/jaiscloud-aws:latest
docker run -p 4566:4566 ghcr.io/jaisrajms/jaiscloud-aws:latest
```

### Docker Compose (full mode with Postgres)

A `docker-compose.yml` is included at the repo root. It starts Postgres (port 5433) and JaisCloud (port 4566) with the Docker executor enabled:

```bash
# Build image and start services
make up-docker

# Run with a specific executor mode
JAISCLOUD_EXECUTOR_MODE=mock make up-docker

# Stop
make down-docker
```

Or use Docker Compose directly:

```bash
docker-compose up -d
docker-compose down
```

### Kubernetes (full mode with Postgres)

```yaml
# Persistent volume for S3 blob storage
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: jaiscloud-blobs
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
---
# Postgres StatefulSet
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          env:
            - name: POSTGRES_USER
              value: jaiscloud
            - name: POSTGRES_PASSWORD
              value: jaiscloud
            - name: POSTGRES_DB
              value: jaiscloud
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: pg-data
              mountPath: /var/lib/postgresql/data
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "jaiscloud"]
            initialDelaySeconds: 5
            periodSeconds: 5
  volumeClaimTemplates:
    - metadata:
        name: pg-data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 20Gi
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  clusterIP: None
  selector:
    app: postgres
  ports:
    - port: 5432
---
# JaisCloud Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaiscloud
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
          image: ghcr.io/jaisrajms/jaiscloud-aws:latest
          args: ["start", "--mode", "full", "--blob-dir", "/blobs", "--metrics"]
          ports:
            - containerPort: 4566
          env:
            - name: JAISCLOUD_REGION
              value: us-east-1
            - name: JAISCLOUD_DSN
              value: postgres://jaiscloud:jaiscloud@postgres:5432/jaiscloud
          volumeMounts:
            - name: blobs
              mountPath: /blobs
          readinessProbe:
            httpGet:
              path: /_jaiscloud/health
              port: 4566
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /_jaiscloud/health
              port: 4566
            initialDelaySeconds: 10
            periodSeconds: 15
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 512Mi
      volumes:
        - name: blobs
          persistentVolumeClaim:
            claimName: jaiscloud-blobs
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
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |
| `--dsn` | `JAISCLOUD_DSN` | _(empty)_ | PostgreSQL DSN (required when `--mode full`) |
| `--blob-dir` | `JAISCLOUD_BLOB_DIR` | _(empty)_ | Directory for S3 blob bytes (full mode, optional) |
| `--executor-mode` | `JAISCLOUD_EXECUTOR_MODE` | _(empty)_ | Container orchestrator: `mock` / `docker` / `k8s` |

### Spark K8s cluster-mode (pod templates)

When `JAISCLOUD_EXECUTOR_MODE=k8s`, the following variables control whether `StartJobRun` / `AddJobFlowSteps` runs Spark in K8s cluster deploy-mode (driver runs as a real K8s Pod) or local-mode inside the spark-submit container.

| Env var | Default | Description |
|---|---|---|
| `JAISCLOUD_SPARK_K8S_CLUSTER_MODE` | `auto` | `auto` — enable when a pod template is provided; `always` — always use cluster deploy-mode; `never` — always use local mode |
| `JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN` | `leave` | What `Close()` does to running cluster-mode Jobs: `leave` — leave running; `delete` — delete immediately |
| `JAISCLOUD_SPARK_K8S_CLUSTER_RESTART_POLICY` | `adopt` | On restart: `adopt` — re-track running cluster-mode Jobs; `reap` — delete them and dispatch FAILED |
| `JAISCLOUD_SPARK_K8S_RECONCILE_TIMEOUT` | `10m` | How long a Job may be missing from the K8s API before it is marked FAILED |
| `JAISCLOUD_INSTANCE_ID` | _(auto UUID)_ | Override instance identity used to label managed K8s resources; useful for CI isolation |

> **Required for cluster mode:** `JAISCLOUD_K8S_SA` (service account) must be set, and the Spark image must be pre-loaded in the cluster when `ImagePullPolicy=Never`. Missing either will produce a startup `WARN` and the Job will fail inside Kubernetes. See the [DEVELOPER_GUIDE](DEVELOPER_GUIDE.md#spark-k8s-cluster-mode-pod-templates) for a full checklist.

> **Pod templates:** JaisCloud passes pod templates verbatim to Spark — no merging or rewriting. Callers must supply templates sized for the target cluster. See [Devbox-compatible pod template requirements](DEVELOPER_GUIDE.md#devbox-compatible-pod-template-requirements).

### Platform Runtime Layer

The Platform Runtime Layer injects TLS trust, extra volumes, and environment variables uniformly into every JaisCloud-managed container or pod — without coupling the configuration to any specific executor.

| Env var | Default | Description |
|---|---|---|
| `JAISCLOUD_PLATFORM_TLS_ENABLED` | `true` | Enable TLS CA injection into managed containers |
| `JAISCLOUD_PLATFORM_TLS_CA_SOURCES` | JaisCloud ConfigMap | JSON/YAML array of CA sources (`configMap`, `secret`, or `file`) |
| `JAISCLOUD_PLATFORM_TLS_CA_SOURCES_FILE` | _(empty)_ | File path variant of the above |
| `JAISCLOUD_PLATFORM_TLS_PASSWORD` | `changeit` | JVM truststore password |
| `JAISCLOUD_PLATFORM_VOLUMES` | _(empty)_ | JSON/YAML array of extra volume specs to mount into every pod/container |
| `JAISCLOUD_PLATFORM_VOLUMES_FILE` | _(empty)_ | File path variant of the above |
| `JAISCLOUD_PLATFORM_ENV` | _(empty)_ | JSON/YAML map of extra environment variables injected into every pod/container |
| `JAISCLOUD_PLATFORM_ENV_FILE` | _(empty)_ | File path variant of the above |
| `JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST` | _(empty)_ | Comma-separated list of allowed `hostPath` prefixes (Docker bind-mounts) |

**`lite` (default)** — all state in memory. Zero external dependencies. State is lost on restart. Ideal for unit tests and CI.

**`full`** — resource metadata persisted in PostgreSQL; S3 object bytes optionally persisted to `--blob-dir`. Ideal for integration environments.

---

## CLI Commands

```bash
jaiscloud-aws start          # start the emulator
jaiscloud-aws version        # print version
jaiscloud-aws env            # print effective config as env vars
jaiscloud-aws doctor         # verify the emulator is reachable
jaiscloud-aws reset          # wipe all state
jaiscloud-aws export -o snapshot.json   # save state to file
jaiscloud-aws import -i snapshot.json   # restore state from file
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
aws --endpoint-url http://localhost:4566 emr list-clusters
```

---

## Running Tests

```bash
# Unit tests (no server required)
go test -race ./internal/...

# Integration tests (server must be running on :4566)
./jaiscloud-aws start &
go test -race -count=1 ./tests/integration/

# Target a specific service
go test -race -run TestS3             ./tests/integration/
go test -race -run TestSQS            ./tests/integration/
go test -race -run TestDynamo         ./tests/integration/
go test -race -run TestLambda         ./tests/integration/
go test -race -run TestKMS            ./tests/integration/
go test -race -run TestSecretsManager ./tests/integration/
go test -race -run TestSSM            ./tests/integration/
go test -race -run TestCF             ./tests/integration/

# Full-mode e2e via Makefile (handles server + postgres via docker-compose)
make test-e2e-lambda-docker        # Lambda Docker warm-pool (tag: lambda_e2e)
make test-e2e-lambda-k8s           # Lambda K8s warm-pod (tag: lambda_e2e)
make test-e2e-emr-docker           # EMR Spark via Docker (tag: spark_e2e)
make test-e2e-emrcontainers-k8s    # EMR Containers K8s (tag: spark_e2e)
make test-e2e-cloudformation       # CloudFormation (tag: cfn_fullmode)
make test-e2e-kms                  # KMS/SecretsManager/SSM (tag: kms_fullmode)
make test-e2e-iceberg              # Iceberg Glue (tag: iceberg_e2e)

# Or run a specific test
make test-e2e-lambda-docker TEST_RUN=TestLambda_DeleteAndReCreate
```

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for the full test matrix and how to set up the Spark and Lambda environments.

---

## Service Reference

### Persistence (lite vs full mode)

| Service | Lite mode | Full mode (`--mode full`) |
|---|---|---|
| SQS | In-memory | PostgreSQL |
| SNS | In-memory | PostgreSQL |
| DynamoDB + Streams | In-memory | PostgreSQL |
| S3 (metadata) | In-memory | PostgreSQL |
| S3 (object bytes) | In-memory | LocalFS (`--blob-dir`) |
| Lambda | In-memory config | PostgreSQL config |
| IAM / STS | In-memory | PostgreSQL |
| KMS | In-memory | PostgreSQL |
| Secrets Manager | In-memory | PostgreSQL |
| SSM Parameter Store | In-memory | PostgreSQL |
| API Gateway | In-memory | PostgreSQL |
| CloudFormation | In-memory | PostgreSQL |
| EventBridge | In-memory | PostgreSQL |
| Glue Data Catalog | In-memory | PostgreSQL |
| Kinesis | In-memory | In-memory |
| CloudWatch metrics | In-memory ring | In-memory ring + PostgreSQL alarms |
| CloudWatch Logs | In-memory | In-memory |
| Step Functions | In-memory | In-memory |
| EC2 / Route53 / RDS / ElastiCache / ECS / EKS | In-memory | In-memory |
| ELBv2 / ECR / ACM / Config / Resource Groups | In-memory | In-memory |
| Firehose / Redshift / Athena / SES / Cognito | In-memory | In-memory |

### Lambda execution modes

| Mode (`JAISCLOUD_EXECUTOR_MODE`) | Behavior |
|---|---|
| _(empty)_ / `mock` | Echo function — returns the invocation payload unchanged |
| `docker` | Warm Docker container pool per function; reuses containers across invocations |
| `k8s` | Warm K8s Pod + ClusterIP Service per function; survives JaisCloud restarts |

### EMR / Spark execution modes

| Mode | EMR on EC2 | EMR on EKS |
|---|---|---|
| _(empty)_ / `mock` | Steps complete instantly as `COMPLETED` | Job runs complete instantly as `COMPLETED` |
| `docker` | Each step runs as a Docker container | Each job run runs as a Docker container |
| `k8s` | Each step runs as a K8s `batch/v1 Job` | Each job run runs as a K8s `batch/v1 Job`; cluster-mode available |

---

## Architecture

```
HTTP request
  → gateway (Chi router + middleware)
      → CloudAdapter.DetectAndDecode     (hardcoded to the binary's cloud at compile time)
          (X-Amz-Target → SQS/DynamoDB/Glue/EMR JSON)
          (Authorization SigV4 scope → all services)
          (Action param → SQS/IAM/STS/SNS Query protocol)
      → inject: Clock, Region, AccountID, Cloud, ResourceID
      → Registry.Dispatch("Service.Action", NormalizedRequest)
          → Provider (business logic, pure Go)
              → ResourceStore          (metadata)
              → ServiceStore           (messages / items / objects)
              → CloudSparkTransform    (AWS | Azure | GCP — URI rewrite, confs, pod env)
              → Executor               (mock | docker | k8s)
                  → PlatformConfig     (TLS CA injection, extra volumes, extra env)
      → Codec.Encode (XML / JSON / raw bytes)
  → HTTP response
```

Each JaisCloud binary emulates exactly one cloud. There is no per-request cloud detection and no `--cloud` flag — the adapter and all providers are compiled into the binary at build time.

---

---

## Azure

`jaiscloud-azure` is the Azure binary. It accepts Azure wire-protocol requests and routes them through the same provider registry as the AWS binary, but using Azure-native resource IDs and authentication.

**Current status:** stub implementation — all Azure endpoints return HTTP 501 Not Implemented. Full implementation is in progress.

```bash
go build -o jaiscloud-azure ./cmd/jaiscloud-azure/
./jaiscloud-azure start
```

When the implementation is complete, you will point any Azure SDK at `http://localhost:4566` with no code changes, the same way you would point an AWS SDK at the AWS binary.

---

## GCP

`jaiscloud-gcp` is the GCP binary. It accepts GCP wire-protocol requests and routes them through the same provider registry, using GCP-native resource names and service account authentication.

**Current status:** stub implementation — all GCP endpoints return HTTP 501 Not Implemented. Full implementation is in progress.

```bash
go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/
./jaiscloud-gcp start
```

---

## Contributing

Contributions welcome. Please open an issue before starting large changes.

```bash
git clone https://github.com/jaisraj/jaiscloud
cd jaiscloud
go test -race ./...   # must pass
go vet ./...          # must pass
```

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for full development setup.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
