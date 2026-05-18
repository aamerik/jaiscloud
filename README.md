# JaisCloud

<p align="center">
  <img src="docs/images/jaiscloud-hero.png" alt="JaisCloud — AI Agent to multi-cloud emulation" width="100%"/>
</p>

> **Early Development Notice**
> JaisCloud is under active development. While core services are functional and tested, some operations may have incomplete implementations, behavioural differences from AWS, or known bugs. If you encounter an issue, please [open a GitHub issue](https://github.com/raj-jaiswal/jaiscloud/issues) with a minimal reproduction — your report directly shapes what gets fixed next.

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

## Install

### Homebrew (macOS)

```bash
brew tap raj-jaiswal/tap
brew install jaiscloud-aws
jaiscloud-aws start
```

### Scoop (Windows)

```powershell
scoop bucket add jaiscloud https://github.com/raj-jaiswal/scoop-jaiscloud
scoop install jaiscloud-aws
jaiscloud-aws start
```

### Download binary (all platforms — no Go required)

Pre-built binaries are published on the [GitHub Releases page](https://github.com/raj-jaiswal/jaiscloud/releases).

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
curl -LO https://github.com/raj-jaiswal/jaiscloud/releases/latest/download/jaiscloud-aws_darwin_arm64.tar.gz
tar -xzf jaiscloud-aws_darwin_arm64.tar.gz
sudo mv jaiscloud-aws /usr/local/bin/
jaiscloud-aws start

# Linux amd64 (Debian/Ubuntu)
curl -LO https://github.com/raj-jaiswal/jaiscloud/releases/latest/download/jaiscloud-aws_linux_amd64.deb
sudo dpkg -i jaiscloud-aws_linux_amd64.deb
jaiscloud-aws start
```

A `checksums.txt` is published alongside every release for verification.

### Docker

```bash
docker run -p 4566:4566 ghcr.io/raj-jaiswal/jaiscloud-aws:latest
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
| `--mode` | `JAISCLOUD_MODE` | `lite` | `lite` (in-memory) or `full` (PostgreSQL) |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN (required when `--mode full`) |
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |
| `--executor-mode` | `JAISCLOUD_EXECUTOR_MODE` | — | `mock` / `docker` / `k8s` for Lambda and EMR |

**Lite mode** (default) — all state in memory, zero external dependencies, lost on restart. Ideal for unit tests and CI.

**Full mode** — resource metadata persisted in PostgreSQL. Ideal for integration environments and state that must survive restarts.

For the full configuration reference including Kubernetes, Spark, and platform options see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## CLI Reference

```bash
jaiscloud-aws start                      # start the emulator
jaiscloud-aws version                    # print version, commit, build date
jaiscloud-aws env                        # print effective config as env vars
jaiscloud-aws doctor                     # verify the emulator is reachable
jaiscloud-aws reset                      # wipe all state
jaiscloud-aws export -o snapshot.json    # save state to file
jaiscloud-aws import -i snapshot.json    # restore state from file
```

---

## Admin API

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | `{"status":"ok"}` liveness check |
| `/_jaiscloud/reset` | POST | Wipe all state |
| `/_jaiscloud/export` | GET | JSON snapshot of all state |
| `/_jaiscloud/import` | POST | Restore state from JSON snapshot |
| `/metrics` | GET | Prometheus metrics (requires `--metrics`) |

---

## Contributing

Contributions welcome. Please open an issue before starting large changes.

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for build setup, test matrix, and architecture details.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
