# JaisCloud

<p align="center">
  <img src="docs/images/jaiscloud-hero.png" alt="JaisCloud — AI Agent to multi-cloud emulation" width="100%"/>
</p>

> **Early Development Notice**
> JaisCloud is under active development. While core services are functional and tested, some operations may have incomplete implementations, behavioural differences from AWS, or known bugs. If you encounter an issue, please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) with a minimal reproduction — your report directly shapes what gets fixed next.

**⚡ JaisCloud — Fast, Local, Realistic Cloud for AI-Driven Development that runs anywhere — laptop, CI, or Kubernetes**

JaisCloud is a free and open-source, lightweight multi-cloud emulator designed to enable AI agents and developers to rapidly deploy and validate code changes for enterprise applications that depend on cloud resources—without interacting with real cloud environments. It goes beyond basic emulation by providing high-fidelity, scaled-down implementations of cloud services (across AWS, Azure, and GCP), including real execution backends for systems like EMR (Spark), Lambda, and container-based workloads. This allows workloads to behave much closer to actual cloud environments while still running locally or in isolated infrastructure.

By combining protocol-level compatibility with real execution semantics, JaisCloud enables end-to-end validation of cloud-dependent workflows in complete isolation, eliminating the latency, cost, and risk associated with real deployments. **This dramatically shortens the feedback loop: AI agents can provision resources, execute workflows, and verify behavior in seconds rather than minutes. At the same time, it significantly reduces cloud costs by removing the need to provision real cloud infrastructure for development and testing workflows.**

As an open-source project, JaisCloud offers transparency, extensibility, and community-driven innovation—allowing teams to customize, audit, and evolve the platform to fit their specific needs. It plays a critical role in accelerating AI-driven Software Development Lifecycle (AI-SDLC) by enabling fast, reliable, and repeatable validation of code changes. By empowering AI agents with tight feedback cycles and realistic execution environments, JaisCloud becomes a foundational tool for building, testing, and evolving cloud-native systems with high velocity.

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

> **Fidelity over features.** JaisCloud implements fewer services than LocalStack, but the ones it does implement pass the full AWS SDK integration test suite with no patching.

---

## Supported Services

| Service | Status | Coverage |
|---|---|---|
| Amazon S3 | ✅ Full | Buckets, objects, multipart, batch delete, chunked encoding, Iceberg/S3A compatible |
| Amazon SQS | ✅ Full | All 17 operations, FIFO, DLQ, JSON + Query/XML protocols |
| Amazon DynamoDB | ✅ Full | CRUD, expressions, batch ops, streams, composite keys, cursor pagination |
| Amazon SNS | ✅ Full | Topics, subscriptions, SQS fan-out with MessageAttributes |
| Amazon EventBridge | ✅ Full | Rules, targets, event pattern matching, SQS delivery |
| AWS IAM + STS | ✅ Full | Roles, policies, users, access keys, AssumeRole, GetCallerIdentity |
| AWS Lambda | ✅ Full | Echo / Docker warm pool / K8s warm pod (Pod + ClusterIP Service per function) |
| AWS Glue Data Catalog | ✅ Full | Databases, tables, partitions, Iceberg metadata CAS |
| Amazon EMR (on EC2) | ✅ Full | Clusters, steps, instance fleets/groups; bootstrap actions as K8s init containers; mock or real K8s Spark |
| Amazon EMR on EKS | ✅ Full | Virtual clusters, job runs, managed endpoints; mock or real K8s |
| AWS KMS | ✅ Full | Keys, aliases, grants, envelope crypto (AES-256-GCM), rotation |
| AWS Secrets Manager | ✅ Full | Secrets, versions, rotation, KMS-encrypted at rest |
| AWS SSM Parameter Store | ✅ Full | String / StringList / SecureString, history, path queries |
| AWS API Gateway (REST) | ✅ Full | Management plane + MOCK / AWS_PROXY / HTTP_PROXY invoke |
| AWS CloudFormation | ✅ Full | Intrinsics, topo sort, real resource dispatch (9 types) |
| Amazon EC2 | ⚙️ Stub | Wire protocol only |
| Amazon Route 53 | ⚙️ Stub | Wire protocol only |
| Amazon RDS | ⚙️ Stub | Wire protocol only |
| Amazon ElastiCache | ⚙️ Stub | Wire protocol only |
| Amazon ECS | ⚙️ Stub | Wire protocol only |

For per-operation coverage, executor modes, fidelity notes, and full-mode persistence details see **[docs/SERVICES.md](docs/SERVICES.md)**.

---

## Quick Start

### Binary

```bash
go build -o jaiscloud ./cmd/jaiscloud/
./jaiscloud start
```

### Docker

```bash
docker pull ghcr.io/jaisrajms/jaiscloud:latest
docker run -p 4566:4566 ghcr.io/jaisrajms/jaiscloud:latest
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
          image: ghcr.io/jaisrajms/jaiscloud:latest
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
| `--cloud` | `JAISCLOUD_CLOUD` | `aws` | Cloud to emulate: `aws`, `azure`, `gcp` |
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
| `JAISCLOUD_SPARK_K8S_STRIP_SCHEDULING` | `true` | Strip `nodeSelector`, `tolerations`, `affinity`, and `topologySpreadConstraints` from merged pod templates (prevents node-assignment conflicts) |
| `JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN` | `leave` | What `Close()` does to running cluster-mode Jobs: `leave` — leave running; `delete` — delete immediately |
| `JAISCLOUD_SPARK_K8S_POD_TEMPLATE_MAX_BYTES` | `262144` | Maximum allowed size per pod-template YAML (256 KiB default) |
| `JAISCLOUD_SPARK_K8S_TEMPLATE_BUCKET` | `jaiscloud-spark-templates` | S3 bucket used to store merged executor pod templates; auto-created on first use |

> **Required for cluster mode:** `JAISCLOUD_K8S_SA` (service account) must be set, and the Spark image must be pre-loaded in the cluster when `ImagePullPolicy=Never`. Missing either will produce a startup `WARN` and the Job will fail inside Kubernetes. See the [DEVELOPER_GUIDE](DEVELOPER_GUIDE.md#spark-k8s-cluster-mode-pod-templates) for a full checklist.

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
jaiscloud start          # start the emulator
jaiscloud version        # print version
jaiscloud env            # print effective config as env vars
jaiscloud doctor         # verify the emulator is reachable
jaiscloud reset          # wipe all state
jaiscloud export -o snapshot.json   # save state to file
jaiscloud import -i snapshot.json   # restore state from file
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
./jaiscloud start &
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

## Architecture

```
HTTP request
  → gateway (Chi router + middleware)
      → CloudAdapter.DetectAndDecode     (selected once at startup from --cloud)
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

Each JaisCloud instance emulates exactly one cloud (`--cloud`). There is no per-request cloud detection — the adapter is selected once at startup.

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
