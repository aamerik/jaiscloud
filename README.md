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

### ✅ Amazon SNS
| Operation | Supported |
|---|---|
| CreateTopic / DeleteTopic / ListTopics | ✅ |
| GetTopicAttributes / SetTopicAttributes | ✅ |
| Subscribe / Unsubscribe / ListSubscriptions | ✅ |
| Publish (fan-out to SQS subscriptions) | ✅ |
| DeleteTopic removes all subscriptions | ✅ |

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

### ✅ AWS Lambda (mock)
| Operation | Supported |
|---|---|
| CreateFunction / GetFunction / DeleteFunction / ListFunctions | ✅ |
| UpdateFunctionConfiguration / UpdateFunctionCode | ✅ |
| Invoke (echo mode — returns payload unchanged) | ✅ |

> Lambda Invoke uses **echo mode**: the payload you send is returned as the response. No subprocess is spawned. This is intentional — it lets you test fan-out pipelines, event routing, and infrastructure wiring without needing real function code.

---

## Fidelity Notes

JaisCloud prioritises **protocol correctness** over breadth:

- All responses use the exact XML/JSON envelope the AWS SDK expects.
- Error codes match AWS (`NoSuchBucket`, `ResourceNotFoundException`, etc.).
- `Last-Modified` headers use RFC 1123 GMT format, not UTC — the SDK parses these strictly.
- SQS supports both **JSON** (`X-Amz-Target`) and **Query/XML** protocols in the same server.
- DynamoDB key hash is computed from key attributes only (in schema order), matching AWS semantics.

**Known limitations:**
- No IAM policy evaluation — all requests are accepted regardless of attached policies.
- Lambda runs in echo mode only; no actual function execution.
- S3 versioning and object locking are stubbed (no error, no actual versioning).
- No cross-region or cross-account semantics.

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
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |

### Modes

**`lite` (default)** — all state in memory. Zero external dependencies. State is lost on restart. Ideal for unit tests and CI.

**`full`** — state persisted in PostgreSQL. Survives restarts. Requires `JAISCLOUD_DSN` or `--dsn`. Ideal for integration environments and long-running dev clusters.

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
go test -race -run TestS3 ./tests/integration/
go test -race -run TestSQS ./tests/integration/
go test -race -run TestDynamo ./tests/integration/
go test -race -run TestLambda ./tests/integration/
```

---

## Architecture

```
HTTP request
  → gateway (Chi router + middleware)
      → AWSAdapter.DetectAndDecode
          (X-Amz-Target → SQS/DynamoDB JSON)
          (Authorization SigV4 scope → all services)
          (Action param → SQS/IAM/STS/SNS Query protocol)
      → Registry.Dispatch("Service.Action", NormalizedRequest)
          → Provider (business logic, pure Go)
              → ResourceStore  (queue/table/function metadata)
              → ServiceStore   (messages/items/objects)
      → Codec.Encode (XML / JSON / raw bytes)
  → HTTP response
```

The adapter, provider, and store layers have no circular imports. The `model` package carries the shared `NormalizedRequest` / `ProviderResponse` types between them.

---

## Contributing

Contributions welcome. Please open an issue before starting large changes.

```bash
git clone https://github.com/jaisraj/jaiscloud
cd jaiscloud
go test -race ./...          # must pass
go vet ./...                 # must pass
```

Planned for Phase 2: EventBridge, Kinesis, SES, Secrets Manager, Parameter Store, OIDC auth.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
