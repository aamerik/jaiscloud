# JaisCloud

<p align="center">
  <img src="docs/images/jaiscloud-hero.png" alt="JaisCloud — AI Agent to multi-cloud emulation" width="100%"/>
</p>

> **Early Development Notice**
> JaisCloud is under active development. While core services are functional and tested, some operations may have incomplete implementations, behavioural differences from AWS, or known bugs. If you encounter an issue, please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) with a minimal reproduction — your report directly shapes what gets fixed next.

**⚡ JaisCloud — Fast, local, realistic cloud emulation for AI-driven development. Runs anywhere: laptop, CI, or Kubernetes.**

JaisCloud is a free, open-source multi-cloud emulator that lets AI agents and developers run, test, and validate cloud-dependent workloads without touching real cloud infrastructure. It implements the exact wire protocols each cloud uses — no SDK shims, no proxy rewrites — so your existing code works against JaisCloud unmodified.

**One binary per cloud.** Each binary is self-contained and speaks that cloud's exact protocol. There is no `--cloud` flag.

| Cloud | Binary | Status |
|---|---|---|
| AWS | `jaiscloud-aws` | Full implementation |
| Azure | `jaiscloud-azure` | In pipeline |
| GCP | `jaiscloud-gcp` | In pipeline |

---

## Why JaisCloud?

| | JaisCloud | LocalStack (Community) | Moto |
|---|---|---|---|
| **Single static binary** | ✅ | ❌ (Python + Docker) | ❌ (Python library) |
| **Zero runtime deps (memory mode)** | ✅ | ❌ | ❌ |
| **Postgres persistence (persistent mode)** | ✅ | 💰 Pro | ❌ |
| **Exact AWS wire protocol** | ✅ | ✅ | Partial |
| **Kubernetes-native** | ✅ | Partial | ❌ |
| **State export / import** | ✅ | ❌ | ❌ |
| **Prometheus metrics** | ✅ | 💰 Pro | ❌ |
| **Spark / EMR real execution** | ✅ | ❌ | ❌ |
| **Apache Iceberg (Glue Catalog)** | ✅ | ❌ | ❌ |
| **Written in Go** | ✅ | ❌ | ❌ |
| **Multi-account isolation** | ✅ | Partial | ❌ |
| **Multi Cloud** | &#x231B; | Partial | ❌ |
| **License** | Apache-2.0 | Apache-2.0 | Apache-2.0 |

---

## Supported AWS Services

### Full implementations

These services implement real business logic and pass the AWS SDK integration test suite.

| Service |
|---------|
| Amazon S3 |
| Amazon SQS |
| Amazon DynamoDB + Streams |
| Amazon SNS |
| Amazon EventBridge |
| AWS IAM + STS |
| AWS Lambda |
| AWS Glue Data Catalog |
| Amazon Kinesis |
| Amazon EMR (on EC2) |
| Amazon EMR on EKS |
| AWS KMS |
| AWS Secrets Manager |
| AWS SSM Parameter Store |
| AWS API Gateway (REST) |
| AWS CloudFormation |
| Amazon CloudWatch + Logs |
| AWS Step Functions |

### Metadata-only

These services implement the full wire protocol and resource CRUD but have no execution engine.

EC2 · Route 53 · RDS · ElastiCache · ECS · EKS · ELBv2 · ECR · ACM · Kinesis Firehose · AWS Config · Resource Groups · Redshift · Athena

### Stub

SES · Cognito (User Pools + Identity Pools)

For per-operation coverage, persistence details, and execution modes see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## Architecture

JaisCloud follows a **one binary per cloud** model. Each binary speaks that cloud's exact wire protocol and contains all adapter, provider, and store logic for that cloud. Nothing is shared across clouds except infrastructure utilities.

```
HTTP request
  → gateway.Server          (Chi router, middleware)
      → CloudAdapter         (detects service + action, decodes wire format)
          → Registry.Dispatch ("Service.Action", NormalizedRequest)
              → Provider     (business logic, in-memory or PostgreSQL store)
          → Codec.Encode     (serialises response to wire format)
  → HTTP response
```

### Identity and multi-account

Every incoming request carries a SigV4 `Authorization` header. JaisCloud parses the **access key** out of that header and derives the calling account from it — no config change needed.

```
Access key  →  account ID  →  per-account store scope
──────────────────────────────────────────────────────
AKIAIOSFODNN7EXAMPLE  →  (default account, e.g. 000000000000)
ASIA<base32(acct_int)>  →  12-digit account ID embedded in the key
000000000000            →  account ID taken literally
```

This is the **LSIA encoding** (LocalStack-compatible). `EncodeLSIA("123456789012")` produces the `ASIA…` key; `DecodeLSIA(key)` recovers the account ID. Every provider stores resources under `(account, region, type, id)` — two requests with different access keys see completely separate state.

### Request flow with multi-account

```
Authorization: AWS4-HMAC-SHA256 Credential=ASIA<acct_encoded>/…
  → identity.FromRequest  → nr.AccountID = "123456789012"
  → Registry.Dispatch
      → provider.resources.Get(ctx, "123456789012", "us-east-1", ...)
```

### Per-cloud binary layout

```
jaiscloud-aws  =  internal/aws/adapter  +  internal/aws/provider/*  +  shared infra
jaiscloud-azure  =  internal/azure/adapter (stub)  +  shared infra
jaiscloud-gcp    =  internal/gcp/adapter (stub)    +  shared infra
```

Shared infrastructure (`store`, `gateway`, `admin`, `blobfs`, `events`, `executor`) is cloud-neutral and never imports cloud-specific code.

---

## Multi-Account Support

JaisCloud emulates real AWS multi-account behaviour. Each account gets fully isolated state — queues, tables, buckets, keys, secrets — with no cross-contamination.

### How it works

Account identity is derived from the **access key** you pass to the SDK, using the same LSIA encoding as LocalStack. No server restart or config change is needed to use multiple accounts simultaneously.

| Access key format | Resolved account |
|---|---|
| `ASIA<base32-encoded-account>` | Decoded 12-digit account ID |
| Any 12-digit numeric string | Taken as account ID literally |
| Anything else (`test`, `AKIA…`) | Server default (`JAISCLOUD_ACCOUNT_ID`) |

### Quick start — two accounts

```go
import (
    "github.com/aws/aws-sdk-go-v2/credentials"
    "jaiscloud/internal/aws/identity"
)

// Mint LSIA-encoded access keys (or use the 12-digit literal shortcut).
keyA, _ := identity.EncodeLSIA("111111111111")
keyB, _ := identity.EncodeLSIA("222222222222")

// Build two SDK configs pointing at the same emulator.
cfgA, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(keyA, "test", "")),
)
cfgB, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(keyB, "test", "")),
)

sqsA := sqs.NewFromConfig(cfgA, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://localhost:4566") })
sqsB := sqs.NewFromConfig(cfgB, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://localhost:4566") })

// Account A and B each get isolated state.
sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("my-queue")})
sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("my-queue")})
// Each account sees only its own queue.
```

### Cross-account dispatch

Resources that reference other resources by ARN route correctly across accounts:

- **SNS → SQS**: subscribe a queue in account B to a topic in account A — messages land in B's queue.
- **EventBridge → Lambda / SQS**: rule targets in a different account are dispatched to that account's store.
- **STS AssumeRole**: returns LSIA-encoded credentials for the target account; subsequent calls resolve to that account.
- **Lambda cross-account invoke**: `InvokeFunction` with a full ARN routes to the function owner's account.

### KMS cross-account protection

Ciphertext produced by account A embeds A's account ID in the blob (v2 format). A `Decrypt` call from account B returns `IncorrectKeyException` — exactly matching real AWS behaviour.

### Scoped state reset

```bash
# Wipe all state (original behaviour)
curl -X POST http://localhost:4566/_jaiscloud/reset

# Wipe one account across all regions
curl -X POST "http://localhost:4566/_jaiscloud/reset?account=111111111111"

# Wipe one (account, region) pair
curl -X POST "http://localhost:4566/_jaiscloud/reset?account=111111111111&region=us-east-1"
```

---

## Install

### Homebrew (macOS)

```bash
brew tap rjaiswal/tap
brew install jaiscloud-aws
jaiscloud-aws start
```

### Scoop (Windows)

```powershell
scoop bucket add jaiscloud https://github.com/jaisrajms/scoop-jaiscloud
scoop install jaiscloud-aws
jaiscloud-aws start
```

### Download binary (all platforms — no Go required)

Pre-built binaries are published on the [GitHub Releases page](https://github.com/jaisrajms/jaiscloud/releases).

| Platform | Download |
|----------|----------|
| macOS arm64 (Apple Silicon) | `jaiscloud-aws_<version>_darwin_arm64.tar.gz` |
| macOS amd64 (Intel) | `jaiscloud-aws_<version>_darwin_amd64.tar.gz` |
| Linux amd64 | `jaiscloud-aws_<version>_linux_amd64.tar.gz` · `.deb` · `.rpm` |
| Linux arm64 | `jaiscloud-aws_<version>_linux_arm64.tar.gz` · `.deb` · `.rpm` |
| Windows amd64 | `jaiscloud-aws_<version>_windows_amd64.zip` |
| Windows arm64 | `jaiscloud-aws_<version>_windows_arm64.zip` |

```bash
# macOS arm64
curl -LO https://github.com/jaisrajms/jaiscloud/releases/latest/download/jaiscloud-aws_darwin_arm64.tar.gz
tar -xzf jaiscloud-aws_darwin_arm64.tar.gz
sudo mv jaiscloud-aws /usr/local/bin/
jaiscloud-aws start

# Linux amd64 (Debian/Ubuntu)
curl -LO https://github.com/jaisrajms/jaiscloud/releases/latest/download/jaiscloud-aws_linux_amd64.deb
sudo dpkg -i jaiscloud-aws_linux_amd64.deb
jaiscloud-aws start
```

A `checksums.txt` is published alongside every release for verification.

### Docker

```bash
docker run -p 4566:4566 ghcr.io/jaisrajms/jaiscloud-aws:latest
```

### Docker Compose (with Postgres persistence)

```bash
make up-docker    # starts Postgres + JaisCloud
make down-docker
```

### Build from source (requires Go 1.26+)

```bash
go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
./jaiscloud-aws start
```

---

## Connect your AWS SDK

Point any SDK at `http://localhost:4566` with dummy credentials — no code changes required.

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
    "s3",
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

## Configuration

The most common flags — all have an equivalent `JAISCLOUD_*` env var.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `JAISCLOUD_PORT` | `4566` | Listen port |
| `--mode` | `JAISCLOUD_MODE` | `memory` | `memory` (in-memory) or `persistence` (Memory/PostgreSQL) |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN (optional with `--mode persistent`) |
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |
| `--executor-mode` | `JAISCLOUD_EXECUTOR_MODE` | — | `mock` / `docker` / `k8s` for Lambda and EMR |

**Memory mode** (default) — all state in memory, zero external dependencies, lost on restart. Ideal for unit tests and CI.

**Persistent mode** — resource metadata persisted in PostgreSQL if postgres configured else in a file. Ideal for integration environments and state that must survive restarts.

For the full configuration reference including Kubernetes, Spark, and platform options see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## CLI Reference

```bash
jaiscloud-aws start                      # start the emulator
jaiscloud-aws version                    # print version, commit, build date
jaiscloud-aws env                        # print effective config as env vars
jaiscloud-aws doctor                     # verify the emulator is reachable
jaiscloud-aws reset                      # wipe all state
jaiscloud-aws export -o snapshot.json    # save full state snapshot to file
jaiscloud-aws import -i snapshot.json    # restore state from snapshot file
```

---

## Admin API

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | `{"status":"ok"}` liveness check |
| `/_jaiscloud/reset` | POST | Wipe all state |
| `/_jaiscloud/reset?account=X` | POST | Wipe all regions for account X |
| `/_jaiscloud/reset?account=X&region=Y` | POST | Wipe one (account, region) scope |
| `/_jaiscloud/export` | GET | JSON snapshot of all state (schema v3) |
| `/_jaiscloud/import` | POST | Restore state from JSON snapshot |
| `/metrics` | GET | Prometheus metrics (requires `--metrics`) |

---

## Contributing

Contributions welcome. Please open an issue before starting large changes.

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for build setup, test matrix, and architecture details.

---

## Author

**Raj Jaiswal** — [jaisraj@gmail.com](mailto:jaisraj@gmail.com)

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
