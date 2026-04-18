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
| **License** | Apache-2.0 | Apache-2.0 | Apache-2.0 |

> **Fidelity over features.** JaisCloud implements fewer services than LocalStack, but the ones it does implement pass the full AWS SDK integration test suite with no patching.

---

## Supported Services

| Service | Status | Coverage |
|---|---|---|
| Amazon S3 | ✅ Full | Buckets, objects, multipart, batch delete, chunked encoding, Iceberg/S3A compatible |
| Amazon SQS | ✅ Full | All 17 operations, FIFO, DLQ, JSON + Query/XML protocols |
| Amazon DynamoDB | ✅ Full | CRUD, expressions, batch ops, streams, composite keys |
| Amazon SNS | ✅ Full | Topics, subscriptions, SQS fan-out with MessageAttributes |
| Amazon EventBridge | ✅ Full | Rules, targets, event pattern matching, SQS delivery |
| AWS IAM + STS | ✅ Full | Roles, policies, users, access keys, AssumeRole, GetCallerIdentity |
| AWS Lambda | ✅ Full | Echo / Docker warm pool / K8s one-shot job |
| AWS Glue Data Catalog | ✅ Full | Databases, tables, partitions, Iceberg metadata CAS |
| Amazon EMR (on EC2) | ✅ Full | Clusters, steps, instance fleets/groups; mock or real K8s Spark |
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
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U jaiscloud"]
      interval: 5s
      timeout: 5s
      retries: 10

  jaiscloud:
    image: ghcr.io/jaisrajms/jaiscloud:latest
    ports:
      - "4566:4566"
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      JAISCLOUD_MODE: full
      JAISCLOUD_DSN: postgres://jaiscloud:jaiscloud@postgres:5432/jaiscloud
      JAISCLOUD_REGION: us-east-1
    volumes:
      - blob_data:/var/lib/jaiscloud/blobs
    command: ["start", "--blob-dir", "/var/lib/jaiscloud/blobs", "--metrics"]

volumes:
  pg_data:
  blob_data:
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

# Full mode e2e (EMR K8s executor)
SPARK_E2E_SPARK_IMAGE=apache/spark:3.5.0 \
JAISCLOUD_EXECUTOR_MODE=k8s \
./jaiscloud start --mode full --dsn "<DB_CONNECTION_STRING>" &
go test -tags spark_e2e -timeout 30m ./tests/full_mode/emr/

# Iceberg e2e (requires Docker + Spark image + Postgres)
SPARK_E2E_ICEBERG_IMAGE=spark-iceberg-test \
./jaiscloud start --mode full --dsn "<DB_CONNECTION_STRING>" --blob-dir /tmp/blobs &
go test -tags iceberg_e2e -timeout 60m ./tests/full_mode/iceberg/
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
              → ResourceStore  (metadata)
              → ServiceStore   (messages / items / objects)
              → Executor       (mock | docker | k8s)
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
