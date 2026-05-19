# JaisCloud

<p align="center">
  <img src="docs/images/jaiscloud-hero.png" alt="JaisCloud — AI Agent to multi-cloud emulation" width="100%"/>
</p>

> **Early Development Notice**
> JaisCloud is under active development. While core services are functional and tested, some operations may have incomplete implementations, behavioural differences from AWS, or known bugs. If you encounter an issue, please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) with a minimal reproduction — your report directly shapes what gets fixed next.

**JaisCloud — Fast, local cloud emulation for developers and CI. Runs anywhere: laptop, CI, or Kubernetes.**

JaisCloud is a free, open-source cloud emulator that lets developers test cloud-dependent applications without touching real cloud infrastructure. It implements the exact wire protocols each cloud uses — no SDK shims, no proxy rewrites — so your existing code works against JaisCloud unmodified.

**One binary per cloud.** Each binary is fully self-contained. There is no `--cloud` flag.

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

For per-operation coverage details see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## Quick Start

Three steps: install, start, connect.

### 1. Install

```bash
# macOS
brew tap rjaiswal/tap && brew install jaiscloud-aws

# Docker (any platform)
docker pull ghcr.io/jaisrajms/jaiscloud-aws:latest

# Or download a pre-built binary from the Releases page (no Go required)
```

Full installation options are in the [Install](#install) section below.

### 2. Start

```bash
jaiscloud-aws start
# Listening on http://localhost:4566
```

### 3. Connect

```bash
# Set once — every AWS CLI call routes to JaisCloud automatically
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws s3 mb s3://my-bucket
aws sqs create-queue --queue-name my-queue
aws dynamodb list-tables
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

## Connect your SDK

Point any SDK at `http://localhost:4566` with dummy credentials — no code changes required.

### AWS CLI

The simplest approach is to set `AWS_ENDPOINT_URL` in your environment. Every AWS CLI and SDK call then routes to JaisCloud automatically with no per-call flags.

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws s3 mb s3://my-bucket
aws sqs create-queue --queue-name my-queue
aws dynamodb list-tables
```

Or pass `--endpoint-url` per call:

```bash
aws --endpoint-url http://localhost:4566 s3 mb s3://my-bucket
```

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

---

## Running in CI/CD

### GitHub Actions

Use the JaisCloud Docker image as a service container. The health check endpoint (`/_jaiscloud/health`) lets you wait for the emulator to be ready before running tests.

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      jaiscloud:
        image: ghcr.io/jaisrajms/jaiscloud-aws:latest
        ports:
          - 4566:4566

    steps:
      - uses: actions/checkout@v4

      - name: Wait for JaisCloud
        run: |
          until curl -sf http://localhost:4566/_jaiscloud/health; do
            echo "waiting for JaisCloud..."; sleep 1
          done

      - name: Run tests
        env:
          AWS_ENDPOINT_URL: http://localhost:4566
          AWS_REGION: us-east-1
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
        run: go test ./...
```

### Docker Compose

Add JaisCloud as a dependency in your `docker-compose.yml`:

```yaml
services:
  jaiscloud:
    image: ghcr.io/jaisrajms/jaiscloud-aws:latest
    ports:
      - "4566:4566"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:4566/_jaiscloud/health"]
      interval: 2s
      timeout: 5s
      retries: 15

  your-app:
    build: .
    depends_on:
      jaiscloud:
        condition: service_healthy
    environment:
      AWS_ENDPOINT_URL: http://jaiscloud:4566
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
```

### Reset state between tests

Call the reset endpoint between test runs to start with a clean slate:

```bash
curl -X POST http://localhost:4566/_jaiscloud/reset
```

In Go:
```go
func resetState(t *testing.T) {
    t.Helper()
    resp, err := http.Post("http://localhost:4566/_jaiscloud/reset", "", nil)
    require.NoError(t, err)
    require.Equal(t, http.StatusOK, resp.StatusCode)
}
```

---

## Configuration

The most common flags — all have an equivalent `JAISCLOUD_*` env var.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `JAISCLOUD_PORT` | `4566` | Listen port |
| `--mode` | `JAISCLOUD_MODE` | `memory` | `memory` (in-memory, no external deps) or `persistent` (PostgreSQL) |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN (optional; enables Postgres-backed stores when `--mode persistent`) |
| `--data-dir` | `JAISCLOUD_DATA_DIR` | — | Directory for snapshots and periodic state saves |
| `--region` | `JAISCLOUD_REGION` | `us-east-1` | AWS region reported in responses |
| `--account-id` | `JAISCLOUD_ACCOUNT_ID` | `000000000000` | Default AWS account ID in ARNs |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | `JAISCLOUD_METRICS` | `false` | Expose Prometheus metrics at `/metrics` |
| `--executor-mode` | `JAISCLOUD_EXECUTOR_MODE` | — | `mock` / `docker` / `k8s` for Lambda and EMR |

**Memory mode** (default) — all state lives in memory. Zero external dependencies. State is lost on restart. Ideal for unit tests and CI.

**Persistent mode** — state survives restarts. Two sub-modes depending on whether `--dsn` is provided:
- **File-backed** (no `--dsn`): stores are in-memory, state is saved periodically to `state.json` in `--data-dir`.
- **Postgres-backed** (`--dsn` set): stores are backed by PostgreSQL.

```bash
# Memory mode (default — state lost on restart)
jaiscloud-aws start

# Persistent mode, file-backed (state saved to ~/.jaiscloud/jaiscloud-aws/state.json)
jaiscloud-aws start --mode persistent

# Persistent mode, file-backed with explicit data directory
jaiscloud-aws start --mode persistent --data-dir ~/.jaiscloud

# Persistent mode, Postgres-backed
jaiscloud-aws start --mode persistent --dsn "postgres://user:pass@localhost:5433/jaiscloud"
```

For the full configuration reference including Kubernetes, Spark, and platform options see the [Developer Guide](DEVELOPER_GUIDE.md).

---

## CLI Reference

```bash
jaiscloud-aws start                             # start the emulator
jaiscloud-aws version                           # print version, commit, build date
jaiscloud-aws env                               # print effective config as env vars
jaiscloud-aws doctor                            # verify the emulator is reachable
jaiscloud-aws reset                             # wipe all state
jaiscloud-aws services                          # list service implementation levels
jaiscloud-aws export -o snapshot.tar.gz        # save full state to a snapshot tarball
jaiscloud-aws import -i snapshot.tar.gz        # restore state from a snapshot tarball
jaiscloud-aws snapshot create --name <name>    # create a named on-disk snapshot
jaiscloud-aws snapshot list                    # list all named snapshots
jaiscloud-aws snapshot revert <name>           # revert to a named snapshot
jaiscloud-aws snapshot delete <name> --yes     # delete a named snapshot
jaiscloud-aws snapshot inspect <name>          # show snapshot metadata
```

---

## Admin API

All endpoints are available at the emulator's base URL (default `http://localhost:4566`).

| Endpoint | Method | Description |
|---|---|---|
| `/_jaiscloud/health` | GET | `{"status":"ok"}` — liveness check |
| `/_jaiscloud/doctor` | GET | Emulator diagnostics (version, mode, instance ID, uptime) |
| `/_jaiscloud/reset` | POST | Wipe all state |
| `/_jaiscloud/reset?account=X` | POST | Wipe all regions for account X |
| `/_jaiscloud/reset?account=X&region=Y` | POST | Wipe one (account, region) scope |
| `/_jaiscloud/export` | GET | Export full state as a gzip tarball (`Content-Type: application/gzip`) |
| `/_jaiscloud/import` | POST | Restore from a gzip tarball (`Content-Type: application/gzip`); `?dry_run=true` validates only |
| `/_jaiscloud/snapshot` | POST | Create a named snapshot (`{"name":"<n>","description":"<d>"}`) |
| `/_jaiscloud/snapshots` | GET | List all named snapshots |
| `/_jaiscloud/snapshot/{name}` | GET | Inspect snapshot metadata |
| `/_jaiscloud/snapshot/{name}/revert` | POST | Revert to a named snapshot |
| `/_jaiscloud/snapshot/{name}` | DELETE | Delete a named snapshot (`?yes=true` required) |
| `/metrics` | GET | Prometheus metrics (requires `--metrics`) |

---

## UI Console

> **Coming Soon** — The JaisCloud UI Console is currently under active development and will be released in an upcoming version.

The UI Console will provide a browser-based interface for interacting with the emulator without the CLI or SDK. Planned features include:

- **Resource browser** — view and manage all emulated resources (queues, tables, buckets, functions, etc.) across accounts and regions
- **State management** — trigger reset, export, import, and named snapshot operations from the UI
- **Multi-account switcher** — toggle between emulated accounts and see per-account resource scopes
- **Metrics dashboard** — visualise request throughput and error rates without a separate Prometheus setup

Watch the [GitHub releases page](https://github.com/jaisrajms/jaiscloud/releases) for announcements.

---

## Multi-Account Support

JaisCloud supports isolated state per AWS account. Each account gets its own queues, tables, buckets, keys, and secrets — no cross-contamination.

Account identity is derived from the **access key** you pass to the SDK. To use multiple accounts simultaneously, pass a 12-digit account ID as the access key:

```go
// Account A — access key is the literal account ID
cfgA, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("111111111111", "test", "")),
)

// Account B
cfgB, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("222222222222", "test", "")),
)

sqsA := sqs.NewFromConfig(cfgA, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://localhost:4566") })
sqsB := sqs.NewFromConfig(cfgB, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://localhost:4566") })

// Each account sees only its own resources
sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("my-queue")})
sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("my-queue")})
```

Any other access key (e.g. `"test"`, `"AKIAIOSFODNN7EXAMPLE"`) resolves to the server default account (`JAISCLOUD_ACCOUNT_ID`, default `000000000000`).

For cross-account ARN routing, STS AssumeRole, and LSIA encoding details see the [Developer Guide](DEVELOPER_GUIDE.md).

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
