# Developer Guide

This guide covers everything you need to build, run, and extend JaisCloud locally.

**Contents**
- [Prerequisites](#prerequisites)
- [Docker Images](#docker-images)
  - [jaiscloud-lite](#jaiscloud-lite-image)
  - [jaiscloud (full)](#jaiscloud-full-image)
  - [jaiscloud-sdk](#jaiscloud-sdk-image)
- [Running in Lite Mode](#running-in-lite-mode)
- [Running in Full Mode (PostgreSQL)](#running-in-full-mode-postgresql)
- [Running on Kubernetes](#running-on-local-kubernetes)
- [EMR Spark Cluster — Docker Mode](#emr-spark-cluster--docker-mode)
- [EMR Spark Cluster — Kubernetes Mode](#emr-spark-cluster--kubernetes-mode)
- [Writing a Custom Plugin](#writing-a-custom-plugin)
- [Running Tests](#running-tests)
  - [Unit tests](#unit-tests-no-server-needed)
  - [Integration tests (lite mode)](#integration-tests)
  - [Full mode integration tests](#full-mode-integration-tests)
  - [Spark e2e tests](#spark-e2e-tests-spark_e2e-build-tag)
  - [Apache Iceberg e2e tests](#apache-iceberg-e2e-tests-iceberg_e2e-build-tag)
- [Platform Setup](#platform-setup)

---

## Prerequisites

- **Go 1.26+** — `go version` should print `go1.26` or higher
- **Docker** — for full mode and Spark cluster setup
- **kubectl** — for Kubernetes sections
- **AWS CLI** — for smoke-testing via the command line

Quick install check:
```bash
go version
docker version
kubectl version --client
aws --version
```

---

## Docker Images

JaisCloud ships three Docker images, each with a distinct purpose. Use the root `Makefile` to build them:

```bash
# Build a specific image
make docker-lite    # jaiscloud-lite:<version>
make docker-full    # jaiscloud-full:<version>
make docker-sdk     # jaiscloud-sdk:<version>

# Build all three at once
make docker-all

# Override the version tag
make docker-all VERSION=1.2.3

# Push to a registry
make docker-all REGISTRY=ghcr.io/myorg
```

The `VERSION` is inferred from `git describe --tags` automatically. You can override it with any string.

---

### `jaiscloud-lite` image

**Dockerfile:** [Dockerfile](Dockerfile)  
**Use case:** CI pipelines, unit testing, local development — anywhere you only need lite mode (in-memory state, no PostgreSQL, no plugins).

| Property | Value |
|---|---|
| Base image | `scratch` |
| Binary | CGO_ENABLED=0, fully static |
| Plugin support | None |
| Persistence | In-memory only |
| Image size | ~10 MB |

```bash
make docker-lite
```

Run it:

```bash
docker run -p 4566:4566 jaiscloud-lite:latest

# With options
docker run -p 4566:4566 \
  -e JAISCLOUD_LOG_LEVEL=debug \
  -e JAISCLOUD_METRICS=true \
  jaiscloud-lite:latest start --region eu-west-1
```

> This image is built from the existing `Dockerfile`. It cannot load `.so` plugins (CGO is disabled and the runtime is `scratch`).

---

### `jaiscloud` full image

**Dockerfile:** [Dockerfile.full](Dockerfile.full)  
**Use case:** Shared dev environments, staging, Kubernetes deployments — full mode with PostgreSQL persistence and the `aws-emr-spark` plugin pre-bundled.

| Property | Value |
|---|---|
| Base image | `debian:bookworm-slim` |
| Binary | CGO_ENABLED=1 (required for plugin loading) |
| Plugin support | Yes — `aws-emr-spark` plugin pre-installed at `/plugins/` |
| Persistence | PostgreSQL (requires `JAISCLOUD_DSN`) |
| Image size | ~60 MB |

```bash
make docker-full
```

Run it (full mode, plugin auto-loaded):

```bash
docker run -p 4566:4566 \
  -e JAISCLOUD_MODE=full \
  -e JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@host.docker.internal:5432/jaiscloud \
  jaiscloud-full:latest
```

The default `CMD` is `start --plugin-dir /plugins`, so the EMR Spark plugin is loaded automatically. Override with your own arguments if needed:

```bash
# Run with extra flags
docker run -p 4566:4566 \
  -e JAISCLOUD_DSN=postgres://... \
  jaiscloud-full:latest start --plugin-dir /plugins --metrics --log-level debug
```

With Docker Compose (PostgreSQL + JaisCloud):

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: jaiscloud
      POSTGRES_PASSWORD: jaiscloud
      POSTGRES_DB: jaiscloud
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U jaiscloud"]
      interval: 5s
      retries: 10

  jaiscloud:
    image: jaiscloud-full:latest
    ports:
      - "4566:4566"
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      JAISCLOUD_MODE: full
      JAISCLOUD_DSN: postgres://jaiscloud:jaiscloud@postgres:5432/jaiscloud
      JAISCLOUD_REGION: us-east-1
      JAISCLOUD_METRICS: "true"
```

Adding a custom plugin at runtime (without rebuilding the image):

```bash
# Build your plugin .so (see jaiscloud-sdk below)
# then mount it alongside the built-in plugins
docker run -p 4566:4566 \
  -v $(pwd)/my-plugin.so:/plugins/my-plugin.so \
  -e JAISCLOUD_DSN=postgres://... \
  jaiscloud-full:latest
```

> The full image uses `debian:bookworm-slim` (glibc) instead of `scratch` so that the CGO-linked binary can load `.so` files at runtime via `plugin.Open()`.

---

### `jaiscloud-sdk` image

**Dockerfile:** [Dockerfile.sdk](Dockerfile.sdk)  
**Use case:** Building custom plugin `.so` files without needing the full JaisCloud source tree. Designed to be used as a build environment or as a `FROM` base in your plugin's `Dockerfile`.

| Property | Value |
|---|---|
| Base image | `golang:1.26` |
| SDK location | `/jaiscloud/sdk` |
| `JAISCLOUD_SDK_PATH` env | `/jaiscloud/sdk` |

```bash
make docker-sdk
```

#### Build a plugin with it (one-liner)

```bash
# From your plugin directory — mounts source at /workspace, outputs .so there too
docker run --rm \
  -v $(pwd):/workspace \
  jaiscloud-sdk:latest \
  go build -buildmode=plugin -o /workspace/my-plugin.so .
```

Your plugin's `go.mod` must point the SDK replace directive at the in-image path:

```
# go.mod
module github.com/myorg/jaiscloud-plugin-myservice

go 1.26.2

require github.com/jaiscloud/plugin-sdk v0.0.0-00010101000000-000000000000

replace github.com/jaiscloud/plugin-sdk => /jaiscloud/sdk
```

#### Use it as a base in your plugin's Dockerfile

```dockerfile
# Stage 1: build the plugin .so
FROM jaiscloud-sdk:<version> AS builder
COPY . /workspace
RUN go build -buildmode=plugin -o /my-plugin.so .

# Stage 2: bundle into the full JaisCloud image
FROM jaiscloud-full:<version>
COPY --from=builder /my-plugin.so /plugins/
```

> Both the `jaiscloud-full` image and `jaiscloud-sdk` image use the same Go version and glibc toolchain, so plugins built with the SDK image are guaranteed to be compatible with the full runtime image.

---

## Running in Lite Mode

Lite mode keeps everything in memory. No external dependencies. State is lost when the server stops. This is the right choice for unit tests and CI pipelines.

### 1. Build the binary

```bash
# From the repo root
go build -o jaiscloud ./cmd/jaiscloud/
```

### 2. Start the server

```bash
./jaiscloud start
```

You should see:
```
INFO jaiscloud started port=4566 mode=lite
```

### 3. Verify it is running

```bash
./jaiscloud doctor
# OK: jaiscloud is running at http://localhost:4566

curl http://localhost:4566/_jaiscloud/health
# {"status":"ok"}
```

### 4. Try a quick smoke test

```bash
# Create an SQS queue
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    sqs create-queue --queue-name test-queue

# Send a message
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    sqs send-message \
    --queue-url http://localhost:4566/000000000000/test-queue \
    --message-body "hello"

# Receive the message
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    sqs receive-message \
    --queue-url http://localhost:4566/000000000000/test-queue
```

### 5. Useful flags

| Flag | Description |
|---|---|
| `--port 9000` | Change the listen port |
| `--region eu-west-1` | Change the region reported in responses |
| `--log-level debug` | Verbose request/response logging |
| `--metrics` | Enable Prometheus metrics at `/metrics` |
| `--deterministic --seed 42` | Reproducible random IDs (useful for golden-file tests) |

### 6. Wipe state between tests

```bash
curl -X POST http://localhost:4566/_jaiscloud/reset
```

This is what integration tests call automatically via `resetState(t)`. You can call it from any script to get a clean slate without restarting the server.

---

## Running in Full Mode (PostgreSQL)

Full mode persists all state — queues, topics, tables, S3 objects, IAM resources, SQS messages — in a PostgreSQL database. State survives server restarts. Use this for shared dev environments or long-running integration setups.

### 1. Start PostgreSQL

The quickest way is Docker:

```bash
docker run -d \
  --name jaiscloud-pg \
  -e POSTGRES_USER=jaiscloud \
  -e POSTGRES_PASSWORD=jaiscloud \
  -e POSTGRES_DB=jaiscloud \
  -p 5432:5432 \
  postgres:16-alpine
```

Or if PostgreSQL is already installed locally:
```bash
createdb jaiscloud
```

Wait a few seconds for the container to be ready, then test the connection:
```bash
docker exec jaiscloud-pg pg_isready -U jaiscloud
# /var/run/postgresql:5432 - accepting connections
```

### 2. Start JaisCloud in full mode

```bash
./jaiscloud start \
  --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
```

The server runs SQL migrations automatically on every startup — no manual schema setup needed. You will see:
```
INFO starting in full mode dsn=postgres://...
INFO jaiscloud started port=4566 mode=full
```

### 3. Verify

```bash
./jaiscloud doctor
./jaiscloud env     # shows JAISCLOUD_MODE=full and JAISCLOUD_DSN=...
```

### 4. Using Docker Compose (recommended for local dev)

Save this as `docker-compose.dev.yml` in the repo root:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: jaiscloud
      POSTGRES_PASSWORD: jaiscloud
      POSTGRES_DB: jaiscloud
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U jaiscloud"]
      interval: 5s
      retries: 10

  jaiscloud:
    build: .
    ports:
      - "4566:4566"
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      JAISCLOUD_MODE: full
      JAISCLOUD_DSN: postgres://jaiscloud:jaiscloud@postgres:5432/jaiscloud
      JAISCLOUD_REGION: us-east-1
      JAISCLOUD_LOG_LEVEL: info
      JAISCLOUD_METRICS: "true"

volumes:
  pg_data:
```

Start everything:
```bash
docker compose -f docker-compose.dev.yml up -d
docker compose -f docker-compose.dev.yml logs -f jaiscloud
```

Stop (data preserved):
```bash
docker compose -f docker-compose.dev.yml down
```

Wipe data completely:
```bash
docker compose -f docker-compose.dev.yml down -v
```

### 5. Connection string reference

| Component | Example | Notes |
|---|---|---|
| User | `jaiscloud` | postgres role |
| Password | `jaiscloud` | postgres password |
| Host | `localhost` | hostname or IP |
| Port | `5432` | default postgres port |
| Database | `jaiscloud` | must already exist |

Full DSN: `postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud`

Via environment variable: `JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud`

---

## Running on Local Kubernetes

`deploy/deploy.sh` is a one-click script that builds the Docker image, deploys JaisCloud and PostgreSQL to a local Kubernetes cluster, and runs a smoke test.

### Prerequisites

- Docker Desktop with Kubernetes enabled (Settings → Kubernetes → Enable Kubernetes)
- `kubectl` pointing at the `docker-desktop` context

Verify:
```bash
kubectl config current-context   # should print: docker-desktop
```

### One-click deploy

```bash
./deploy/deploy.sh
```

The script:
1. Builds the `jaiscloud-lite:latest` Docker image from the repo root `Dockerfile`
2. Creates the `jaiscloud` namespace
3. Deploys PostgreSQL with a 1 Gi PersistentVolumeClaim (`postgres-pvc`)
4. Deploys JaisCloud in full mode with a 5 Gi PersistentVolumeClaim for S3 blob bytes (`jaiscloud-blobs-pvc`)
5. Wires JaisCloud to the postgres pod via cluster-internal DNS
6. Waits for both rollouts to complete
7. Smoke-tests `/_jaiscloud/health`

When complete the server is reachable at:

| URL | Description |
|---|---|
| `http://localhost:4566` | AWS-compatible endpoint |
| `http://localhost:4566/_jaiscloud/health` | Liveness check |
| `http://localhost:4566/metrics` | Prometheus metrics |

### Command reference

| Command | Workloads | PVCs (data) |
|---|---|---|
| `./deploy/deploy.sh` | Created / updated | Created if absent, existing data kept |
| `./deploy/deploy.sh --delete` | Removed | **Kept** — data survives |
| `./deploy/deploy.sh --reset` | Removed | **Deleted** — all data wiped |

### Port forwarding (non-Docker-Desktop clusters)

On minikube or kind the external IP stays `<pending>`. Use port-forward instead:
```bash
kubectl port-forward -n jaiscloud svc/jaiscloud 4566:4566
```

### Viewing logs

```bash
kubectl logs -n jaiscloud deployment/jaiscloud -f
kubectl logs -n jaiscloud deployment/postgres -f
```

---

## EMR Spark Cluster — Docker Mode

The `aws-emr-spark` plugin ships with a `MockExecutor` and a `K8sExecutor`. In **Docker mode** (i.e., `JAISCLOUD_SPARK_MODE=mock`, which is the default), Spark jobs complete immediately without any actual Spark cluster. This is what you want for most local development and all unit tests.

To run in mock Spark mode with the plugin loaded:

### 1. Build the plugin

```bash
cd plugins/aws-emr-spark
make build
# Produces: ../../aws-emr-spark.so (in the repo root)
cd ../..
```

### 2. Start JaisCloud with the plugin

```bash
./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud" \
  --plugin-dir .
```

The `--plugin-dir .` tells JaisCloud to scan the repo root for `.so` files. You should see:
```
INFO plugin loaded name=aws-emr-spark version=1.0.0 services=[emr emrcontainers]
INFO jaiscloud started port=4566 mode=full
```

### 3. Submit an EMR job

```bash
# Create an EMR cluster
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    emr run-job-flow \
    --name "my-cluster" \
    --release-label emr-6.10.0 \
    --instance-groups '[{"InstanceRole":"MASTER","InstanceType":"m5.xlarge","InstanceCount":1}]' \
    --service-role EMR_DefaultRole

# Note the JobFlowId from the output, e.g. j-ABC123

# Add a step (Spark job)
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    emr add-steps \
    --cluster-id j-ABC123 \
    --steps '[{
      "Name": "my-spark-job",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "s3://my-bucket/my-app.jar",
        "Args": ["--input", "s3://my-bucket/data"]
      }
    }]'

# Check step status
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    emr describe-step \
    --cluster-id j-ABC123 \
    --step-id s-XXXX
```

In mock mode, all steps complete with `COMPLETED` state immediately.

### 4. Submit an EMR on EKS (virtual cluster) job

```bash
# Create a virtual cluster
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    emr-containers create-virtual-cluster \
    --name my-vc \
    --container-provider '{"id":"my-eks-cluster","type":"EKS","info":{"eksInfo":{"namespace":"spark-jobs"}}}'

# Note the id from the output, e.g. vc-ABC123

# Start a job run
aws --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-cli-pager \
    emr-containers start-job-run \
    --virtual-cluster-id vc-ABC123 \
    --name my-job \
    --execution-role-arn arn:aws:iam::000000000000:role/SparkRole \
    --release-label emr-6.10.0-latest \
    --job-driver '{"sparkSubmitJobDriver":{"entryPoint":"s3://my-bucket/app.jar","sparkSubmitParameters":"--class com.example.App"}}'
```

### 5. Controlling mock behaviour

Set `JAISCLOUD_SPARK_MODE=mock` (already the default) to use immediate job completion.

```bash
JAISCLOUD_SPARK_MODE=mock ./jaiscloud start --plugin-dir .
```

For delayed completion (useful for testing polling behaviour), set the delay in the plugin's `Init` — see `plugins/aws-emr-spark/internal/executor/spark/mock.go:NewMockExecutorWithDelay`.

---

## EMR Spark Cluster — Kubernetes Mode

In **Kubernetes mode** (`JAISCLOUD_SPARK_MODE=k8s`), the plugin uses the `K8sExecutor` to submit real `batch/v1 Jobs` to a Kubernetes cluster. Each `RunJobFlow` or `StartJobRun` call creates a K8s Job that runs `spark-submit --deploy-mode cluster` inside the configured Spark image. No `client-go` dependency is needed — the executor uses stdlib HTTP only, so the plugin image size is unchanged.

### How it works

1. `K8sExecutor.Submit` creates a `batch/v1 Job` named `spark-<sanitized-job-id>` (max 63 chars).
2. Spark creates the driver Pod; the driver spawns executor Pods.
3. `StatusPoller` polls the Job every 5 s and maps K8s conditions → `SparkState` (`RUNNING` / `COMPLETED` / `FAILED`).
4. `TerminateJobFlows` / `CancelJobRun` deletes the Job with `propagationPolicy=Background`, cascading to all Pods.
5. Completed Jobs self-delete after 1 hour (`ttlSecondsAfterFinished: 3600`).

### Auth — in-cluster (JaisCloud running inside a pod)

Only one env var is needed. The service account token, CA cert, and API server URL are auto-detected from the standard pod mount:

```bash
export JAISCLOUD_SPARK_MODE=k8s
# JAISCLOUD_K8S_NAMESPACE and JAISCLOUD_K8S_SA are optional
```

### Auth — out-of-cluster (local dev, CI)

```bash
export JAISCLOUD_SPARK_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443        # kubectl cluster-info
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa --duration=24h)
export JAISCLOUD_K8S_CA_FILE=$HOME/.kube/ca.crt              # or unset for system roots
export JAISCLOUD_K8S_NAMESPACE=spark-jobs
export JAISCLOUD_K8S_SA=spark-sa
```

### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `JAISCLOUD_SPARK_MODE` | `mock` | Set to `k8s` to enable real cluster submission |
| `JAISCLOUD_K8S_APISERVER` | `https://kubernetes.default.svc` | K8s API server URL |
| `JAISCLOUD_K8S_TOKEN` | in-cluster token file | Bearer token: literal string or path to a file (re-read per request for rotation) |
| `JAISCLOUD_K8S_CA_FILE` | in-cluster CA path | PEM CA cert. Unset = system roots. |
| `JAISCLOUD_K8S_NAMESPACE` | `default` | Namespace for Jobs and Pods |
| `JAISCLOUD_K8S_SA` | _(none)_ | Service account for the spark-submit Pod |

### Prerequisites

- A Kubernetes cluster (local: kind, minikube, or Docker Desktop)
- A namespace for Spark jobs
- A Spark Docker image accessible from the cluster (default: `apache/spark:3.5.0`)

### 1. Prepare the Spark namespace

```bash
kubectl create namespace spark-jobs

# Service account JaisCloud uses to create Jobs
kubectl create serviceaccount jaiscloud-sa -n spark-jobs

# Service account the spark-submit Pod runs as
kubectl create serviceaccount spark-sa -n spark-jobs
```

Apply RBAC — JaisCloud needs to manage Jobs; the spark-submit Pod needs to manage Pods:

```yaml
# jaiscloud-rbac.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: jaiscloud-spark
  namespace: spark-jobs
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: jaiscloud-spark
  namespace: spark-jobs
subjects:
- kind: ServiceAccount
  name: jaiscloud-sa
  namespace: spark-jobs
roleRef:
  kind: Role
  name: jaiscloud-spark
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-driver
  namespace: spark-jobs
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log", "services", "configmaps"]
  verbs: ["create", "get", "list", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-driver
  namespace: spark-jobs
subjects:
- kind: ServiceAccount
  name: spark-sa
  namespace: spark-jobs
roleRef:
  kind: Role
  name: spark-driver
  apiGroup: rbac.authorization.k8s.io
```

```bash
kubectl apply -f jaiscloud-rbac.yaml
```

### 2. Start JaisCloud with the plugin

```bash
export JAISCLOUD_SPARK_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n spark-jobs --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=spark-jobs
export JAISCLOUD_K8S_SA=spark-sa

./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud" \
  --plugin-dir .
```

### 3. Submit a job

Same AWS CLI commands as in mock mode:

```bash
# Create an EMR cluster (maps to a logical Spark job group)
CLUSTER_ID=$(aws --endpoint-url http://localhost:4566 emr create-cluster \
  --name "my-spark-cluster" \
  --release-label emr-6.15.0 \
  --instance-type m5.xlarge \
  --instance-count 2 \
  --service-role EMR_DefaultRole \
  --query 'JobFlowId' --output text)

# Add a step — this triggers K8sExecutor.Submit → batch/v1 Job creation
aws --endpoint-url http://localhost:4566 emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="MyJob",\
ActionOnFailure=CONTINUE,\
Args=[--class,com.example.App,s3://my-bucket/app.jar,arg1,arg2]
```

Watch the Job appear in the cluster:

```bash
kubectl get jobs -n spark-jobs -l app.kubernetes.io/managed-by=jaiscloud
kubectl describe job spark-j-<id> -n spark-jobs
kubectl logs -n spark-jobs -l spark-role=driver --tail=100
```

### 4. Understanding the generated spark-submit command

The plugin constructs the following `spark-submit` arguments for k8s mode:

```
spark-submit \
  --master k8s://https://<JAISCLOUD_K8S_APISERVER> \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
  --conf spark.driver.cores=500m \
  --conf spark.driver.memory=1Gi \
  --conf spark.executor.cores=500m \
  --conf spark.executor.memory=1Gi \
  --conf spark.executor.instances=1 \
  --conf spark.eventLog.enabled=true \         # only if JAISCLOUD_SPARK_S3_LOG_URI is set
  --conf spark.eventLog.dir=s3://my-bucket/spark-logs \
  --class com.example.App \
  s3://my-bucket/app.jar arg1 arg2
```

Cluster size controls resource allocation:

| Size | Executors | Driver CPU / Mem | Executor CPU / Mem |
|---|---|---|---|
| `small` (default) | 1 | 500m / 1Gi | 500m / 1Gi |
| `medium` | 2 | 1 / 2Gi | 1 / 2Gi |
| `large` | 4 | 2 / 4Gi | 2 / 4Gi |

---

## Writing a Custom Plugin

This section walks through building a complete plugin from scratch. By the end you will have a working plugin that handles a fake `myservice` API and can be loaded into JaisCloud at runtime.

### What is a Plugin?

A plugin is a regular Go package compiled as a shared library (`.so` file). JaisCloud loads it at startup, calls `Init` once to wire it up, and then routes all requests for the plugin's services to its `Handle` method.

The plugin and the host **never import each other's code**. They communicate only through the `github.com/jaiscloud/plugin-sdk` module, which has zero external dependencies (stdlib only).

```
JaisCloud host binary          Your plugin .so
─────────────────────          ─────────────────
internal/plugin/manager.go ──→ var Plugin sdk.SparkPlugin
                          Init(ctx, rm, store)
                          Manifest() → services: ["myservice"]
  request arrives ────────────→ Handle(ctx, req) → HandleResponse
  server shutdown ────────────→ Shutdown(ctx)
  POST /_jaiscloud/reset ─────→ Reset()
```

### Step 1: Create the plugin module

Create a directory outside the main repo (or inside `plugins/`):

```bash
mkdir -p plugins/my-service
cd plugins/my-service
go mod init github.com/myorg/jaiscloud-plugin-myservice
```

Add the SDK dependency:

```
# go.mod — add these two lines
require github.com/jaiscloud/plugin-sdk v0.0.0-00010101000000-000000000000

replace github.com/jaiscloud/plugin-sdk => ../../sdk
```

Your `go.mod` should look like:
```
module github.com/myorg/jaiscloud-plugin-myservice

go 1.26.2

require github.com/jaiscloud/plugin-sdk v0.0.0-00010101000000-000000000000

replace github.com/jaiscloud/plugin-sdk => ../../sdk
```

### Step 2: Implement the SparkPlugin interface

The SDK defines this interface (every method is required):

```go
type SparkPlugin interface {
    Init(ctx context.Context, rm ResourceManager, store ResourceStore) error
    Manifest() ManifestInfo
    Handle(ctx context.Context, req HandleRequest) HandleResponse
    Shutdown(ctx context.Context) error
    Reset()
}
```

Create `internal/plugin/myplugin.go`:

```go
package plugin

import (
    "context"
    "fmt"
    "sync"

    sdk "github.com/jaiscloud/plugin-sdk"
)

// MyPlugin implements sdk.SparkPlugin.
type MyPlugin struct {
    store    sdk.ResourceStore
    mu       sync.Mutex
    counters map[string]int // in-memory state: tracks how many times each key was called
}

// New creates a new MyPlugin. Called by main.go.
func New() *MyPlugin {
    return &MyPlugin{counters: make(map[string]int)}
}

// Init is called once after the plugin is loaded.
// Use it to save references to the store and resource manager,
// and to register any deletion guard rules you need.
func (p *MyPlugin) Init(_ context.Context, _ sdk.ResourceManager, store sdk.ResourceStore) error {
    p.store = store
    return nil
}

// Manifest tells the host which services this plugin handles.
// The host uses the Services list to route requests to this plugin.
func (p *MyPlugin) Manifest() sdk.ManifestInfo {
    return sdk.ManifestInfo{
        Name:     "my-service-plugin",
        Version:  "1.0.0",
        Services: []string{"myservice"},
    }
}

// Handle is called for every request routed to this plugin.
// req.Service will always be "myservice" (from the Manifest).
// req.Action is the API action name, e.g. "GetCounter".
// req.Params contains all decoded request parameters.
func (p *MyPlugin) Handle(ctx context.Context, req sdk.HandleRequest) sdk.HandleResponse {
    switch req.Action {
    case "IncrementCounter":
        return p.incrementCounter(req)
    case "GetCounter":
        return p.getCounter(req)
    default:
        return sdk.HandleResponse{
            Err: &sdk.PluginError{
                Code:       "UnsupportedOperation",
                Message:    fmt.Sprintf("action %q not supported by my-service-plugin", req.Action),
                HTTPStatus: 400,
            },
        }
    }
}

// Shutdown is called on graceful server stop.
// Stop any background goroutines here.
func (p *MyPlugin) Shutdown(_ context.Context) error {
    return nil
}

// Reset wipes all in-memory state.
// Called from POST /_jaiscloud/reset during integration tests.
func (p *MyPlugin) Reset() {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.counters = make(map[string]int)
}

// ─── Action handlers ──────────────────────────────────────────────────────────

func (p *MyPlugin) incrementCounter(req sdk.HandleRequest) sdk.HandleResponse {
    key, _ := req.Params["Key"].(string)
    if key == "" {
        return sdk.HandleResponse{
            Err: &sdk.PluginError{
                Code:       "ValidationException",
                Message:    "Key is required",
                HTTPStatus: 400,
            },
        }
    }

    p.mu.Lock()
    p.counters[key]++
    val := p.counters[key]
    p.mu.Unlock()

    return sdk.HandleResponse{
        Data: map[string]any{"Key": key, "Value": val},
    }
}

func (p *MyPlugin) getCounter(req sdk.HandleRequest) sdk.HandleResponse {
    key, _ := req.Params["Key"].(string)
    if key == "" {
        return sdk.HandleResponse{
            Err: &sdk.PluginError{
                Code:       "ValidationException",
                Message:    "Key is required",
                HTTPStatus: 400,
            },
        }
    }

    p.mu.Lock()
    val := p.counters[key]
    p.mu.Unlock()

    return sdk.HandleResponse{
        Data: map[string]any{"Key": key, "Value": val},
    }
}
```

### Step 3: Create main.go

The plugin entry point **must** be in `package main` and must export a variable named `Plugin`:

```go
// main.go
package main

import (
    sdk "github.com/jaiscloud/plugin-sdk"
    "github.com/myorg/jaiscloud-plugin-myservice/internal/plugin"
)

// Plugin is the symbol the host looks up with plugin.Lookup("Plugin").
// It must be a *sdk.SparkPlugin (pointer to interface).
var Plugin sdk.SparkPlugin = plugin.New()
```

> **Why is it in package main?** Go's plugin system requires the entry point to be in `package main`. By keeping only the one-liner in `main.go` and putting all real logic in `internal/plugin/`, the code remains fully unit-testable without needing `-buildmode=plugin`.

### Step 4: Write tests (without building .so)

Create `internal/plugin/myplugin_test.go`. Tests use the internal package directly — no `.so` needed:

```go
package plugin_test

import (
    "context"
    "testing"

    "github.com/myorg/jaiscloud-plugin-myservice/internal/plugin"
)

func TestMyPlugin_Manifest(t *testing.T) {
    p := plugin.New()
    m := p.Manifest()
    if m.Name != "my-service-plugin" {
        t.Errorf("unexpected name: %s", m.Name)
    }
    if len(m.Services) == 0 || m.Services[0] != "myservice" {
        t.Errorf("expected myservice in manifest services")
    }
}

func TestMyPlugin_IncrementCounter(t *testing.T) {
    p := plugin.New()
    p.Init(context.Background(), nil, nil)

    resp := p.Handle(context.Background(), sdk.HandleRequest{
        Service: "myservice",
        Action:  "IncrementCounter",
        Params:  map[string]any{"Key": "hits"},
    })
    if resp.Err != nil {
        t.Fatalf("unexpected error: %v", resp.Err)
    }
    if resp.Data["Value"].(int) != 1 {
        t.Errorf("expected Value=1, got %v", resp.Data["Value"])
    }
}

func TestMyPlugin_Reset_ClearsCounters(t *testing.T) {
    p := plugin.New()
    p.Init(context.Background(), nil, nil)

    p.Handle(context.Background(), sdk.HandleRequest{
        Service: "myservice", Action: "IncrementCounter",
        Params: map[string]any{"Key": "x"},
    })
    p.Reset()

    resp := p.Handle(context.Background(), sdk.HandleRequest{
        Service: "myservice", Action: "GetCounter",
        Params: map[string]any{"Key": "x"},
    })
    if resp.Data["Value"].(int) != 0 {
        t.Errorf("expected 0 after reset, got %v", resp.Data["Value"])
    }
}

// Interface compliance check — fails to compile if MyPlugin doesn't satisfy the interface.
var _ sdk.SparkPlugin = (*plugin.MyPlugin)(nil)
```

Run the tests:
```bash
cd plugins/my-service
go test -race ./internal/...
```

### Step 5: Build the .so

Plugin `.so` files must be built with the exact same Go toolchain and module dependency versions as the host binary. Build from the plugin directory:

```bash
cd plugins/my-service
go build -buildmode=plugin -o ../../my-service.so .
```

> **Important:** If you see `plugin was built with a different version of package X`, it means the host and plugin were compiled with different versions of a shared dependency. Fix it by making sure both use the same Go version and identical `sdk` module path (the `replace` directive ensures this).

### Step 6: Load the plugin at runtime

```bash
# From the repo root
./jaiscloud start --plugin-dir .
```

You should see:
```
INFO plugin loaded name=my-service-plugin version=1.0.0 services=[myservice]
INFO jaiscloud started port=4566 mode=lite
```

### Step 7: Test the plugin via the API

JaisCloud routes requests based on the service name. Your plugin registered `"myservice"`, so any request with `Service: myservice` is routed to it. The routing key in the registry is `"MyService.IncrementCounter"` (the service prefix is capitalised by `serviceToProviderPrefix` in `internal/plugin/routes.go`).

In practice, you call the plugin via a codec (SQS, DynamoDB, REST, etc.) that your plugin declares. For a quick sanity check, use the built-in EMR codec pattern as a reference and call it via the AWS CLI with the correct `X-Amz-Target` header if you add a JSON codec — or build a simple `curl` test:

```bash
# Example: direct JSON call (if you wire a JSON codec for myservice)
curl -s -X POST http://localhost:4566 \
  -H "X-Amz-Target: MyService.IncrementCounter" \
  -H "Content-Type: application/x-amz-json-1.1" \
  -d '{"Key":"hits"}' | jq .
```

### Step 8: Add a Makefile

Add a `Makefile` to your plugin directory for convenience:

```makefile
.PHONY: build test clean

build:
	go build -buildmode=plugin -o ../../my-service.so .

test:
	go test -race ./internal/...

clean:
	rm -f ../../my-service.so
```

### Step 9: Using the ResourceStore

The `store` passed to `Init` lets your plugin persist and retrieve resources across requests so state survives beyond a single `Handle` call.

```go
import sdk "github.com/jaiscloud/plugin-sdk"

// Store a resource
err := p.store.Create(ctx, sdk.ResourceEntry{
    Type:       "my_counter",
    ID:         key,
    Attributes: map[string]string{"value": "1", "name": key},
})

// Retrieve it later
entry, err := p.store.Get(ctx, "my_counter", key)
if err != nil {
    // handle not-found, etc.
}
val := entry.Attributes["value"]

// List all counters
entries, err := p.store.List(ctx, "my_counter", "")

// Update
entry.Attributes["value"] = "42"
p.store.Update(ctx, entry)

// Delete
p.store.Delete(ctx, "my_counter", key)
```

> In lite mode, the store is backed by `MemoryResourceStore`. In full mode it is backed by `PostgresResourceStore`. Your plugin does not need to care — the interface is identical.

### Step 10: Using the ResourceManager (deletion guards)

When your plugin has a parent→child relationship (e.g. "cluster" has "jobs"), use the `ResourceManager` to prevent deleting a parent while children still exist.

Register a guard rule during `Init`:

```go
func (p *MyPlugin) Init(ctx context.Context, rm sdk.ResourceManager, store sdk.ResourceStore) error {
    p.store = store
    p.rm = rm

    rm.RegisterRules([]sdk.DeleteGuardRule{
        {
            // When someone tries to delete a "my_cluster", find its jobs first.
            ParentType: "my_cluster",
            FindChildren: func(ctx context.Context, s sdk.ResourceStore, parentID string) ([]sdk.ChildRef, error) {
                jobs, err := s.List(ctx, "my_job", parentID)
                if err != nil {
                    return nil, err
                }
                refs := make([]sdk.ChildRef, len(jobs))
                for i, j := range jobs {
                    refs[i] = sdk.ChildRef{Type: "my_job", ID: j.ID}
                }
                return refs, nil
            },
            Policy:     sdk.PolicyFail,  // block the delete if any jobs exist
            FailCode:   "ValidationException",
            FailStatus: 400,
            // Optional: custom message
            FailMessage: func(parentID string, children []sdk.ChildRef) string {
                return fmt.Sprintf("cluster %s has %d active jobs; terminate them first", parentID, len(children))
            },
        },
    })
    return nil
}
```

Then use `AcquireDelete` before actually deleting:

```go
func (p *MyPlugin) deleteCluster(ctx context.Context, clusterID string) sdk.HandleResponse {
    // This checks the rules registered above.
    // If any jobs exist, it returns an error without acquiring the lock.
    handle, err := p.rm.AcquireDelete(ctx, "my_cluster", clusterID)
    if err != nil {
        if opErr, ok := err.(*sdk.PluginError); ok {
            return sdk.HandleResponse{Err: opErr}
        }
        return sdk.HandleResponse{Err: &sdk.PluginError{Code: "InternalError", Message: err.Error(), HTTPStatus: 500}}
    }
    defer handle.Release()

    // Safe to delete now.
    p.store.Delete(ctx, "my_cluster", clusterID)
    return sdk.HandleResponse{Data: map[string]any{"ClusterId": clusterID}}
}
```

Available deletion policies:

| Policy | Behaviour |
|---|---|
| `sdk.PolicyFail` | Block the delete and return an error if any children exist |
| `sdk.PolicyCascade` | Automatically delete all children first, then proceed |
| `sdk.PolicyForceTerminate` | Call your `ForceTerminate` callback on each child (e.g. stop a running process), then proceed |

### Common mistakes

**The plugin panics with "interface conversion failed"**  
Check that `main.go` declares `var Plugin sdk.SparkPlugin = ...` (not `var Plugin = ...`). The host does `sym.(*sdk.SparkPlugin)` — it must be a pointer to the interface.

**`plugin was built with a different version of package`**  
Both the host and plugin must use the exact same Go version and the same `sdk` module source. The `replace` directive in `go.mod` pointing to `../../sdk` ensures this for the SDK. For any other shared dependencies, make sure versions match in both `go.mod` files.

**Actions are not being routed to the plugin**  
The registry key is `"ProviderPrefix.Action"`. The gateway builds it as `cloudAdapter.ServiceToProvider(nr.Service) + "." + nr.Action`. For the AWS adapter, `ServiceToProvider` looks up `serviceProviderMap` which is derived from `awsServices` in `internal/adapter/aws/services.go`. If your service is not in that map, the service name is used as-is (lowercase).

To route properly, add a `ServiceDescriptor` entry for your service in `awsServices` with the correct `ProviderPrefix`. Also check `serviceToProviderPrefix` in `internal/plugin/routes.go` — the plugin manager uses this to register the wildcard handler; it must produce the same prefix.

**State is not persisted after restart**  
In lite mode, `MemoryResourceStore` is wiped on restart by design. Use `--mode full` with a DSN for persistence.

---

## Running Tests

### Unit tests (no server needed)

```bash
# Host module
go test -race ./internal/...

# Plugin SDK
cd sdk && go test -race ./...

# EMR Spark plugin
cd plugins/aws-emr-spark && go test -race ./internal/...
```

### Integration tests

Start the server first, then run:

```bash
./jaiscloud start &
go test -race -count=1 ./tests/integration/

# Run a specific service
go test -race -run TestSQS ./tests/integration/
go test -race -run TestEMR ./tests/integration/
go test -race -run TestEventBridge ./tests/integration/
```

Integration tests automatically call `POST /_jaiscloud/reset` between each test case via `resetState(t)`. You do not need to restart the server between runs.

Current integration test coverage: SQS, IAM/STS, SNS, DynamoDB, S3, Lambda, EC2, Route53, RDS, ElastiCache, ECS, Glue, CloudFormation, DynamoDB Streams, EMR, EMR Containers, EventBridge.

---

### Full mode integration tests

Full mode tests require a running PostgreSQL instance and a JaisCloud server started with `--mode full`. They share the same test files as lite mode integration tests (`tests/integration/`) but run against a persistent store.

#### Prerequisites

| Tool | Purpose |
|---|---|
| PostgreSQL 14+ | Persistent store backend |
| `go` 1.26+ | Test runner |

#### 1. Start PostgreSQL

```bash
docker run -d \
  --name jaiscloud-pg \
  -e POSTGRES_USER=jaiscloud \
  -e POSTGRES_PASSWORD=jaiscloud \
  -e POSTGRES_DB=jaiscloud \
  -p 5432:5432 \
  postgres:16-alpine

# Wait for it to be ready
docker exec jaiscloud-pg pg_isready -U jaiscloud
```

#### 2. Build and start JaisCloud in full mode

```bash
go build -o jaiscloud ./cmd/jaiscloud/
./jaiscloud start \
  --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud" &
```

Migrations run automatically on startup. You should see:
```
INFO jaiscloud started port=4566 mode=full
```

#### 3. Run the integration tests

```bash
go test -race -count=1 ./tests/integration/
```

The tests call `POST /_jaiscloud/reset` between cases, which truncates all PostgreSQL tables. No manual wipe is needed between runs.

#### 4. Verify persistence across restarts

```bash
# Create a resource
aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs create-queue --queue-name persist-test

# Kill and restart the server
kill %1
./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud" &

# Queue should still be there
aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs get-queue-url --queue-name persist-test
```

#### Environment variable override

```bash
# Point at a different host (e.g. a remote JaisCloud instance)
JAISCLOUD_HOST=http://my-remote-host:4566 go test -race -count=1 ./tests/integration/
```

---

### Spark e2e tests (`spark_e2e` build tag)

The Spark end-to-end tests cover the full EMR + EMR-on-EKS + EventBridge notification lifecycle. They live under `tests/full_mode/plugin/` and are **excluded from normal CI runs** by the `//go:build spark_e2e` tag. Two execution modes are supported: **Docker** (runs Spark in a local container) and **Kubernetes** (submits to a real cluster).

#### Prerequisites (both modes)

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.26 | Test runner |
| Docker | 20+ | Required for Docker mode; used by K8s mode to build images |
| PostgreSQL | 14+ | JaisCloud full-mode store |
| JaisCloud server | latest | Running in `--mode full` with the EMR Spark plugin loaded |

Build and start JaisCloud (same as the full mode section above, but with the plugin):

```bash
# Build the EMR Spark plugin
cd plugins/aws-emr-spark && make build && cd ../..
# aws-emr-spark.so is now in the repo root

# Start JaisCloud with the plugin
./jaiscloud start \
  --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud" \
  --plugin-dir .
```

#### Docker mode — EMR steps running SparkPi in a container

Docker mode uses `JAISCLOUD_SPARK_MODE=mock` (the default). The plugin's `MockExecutor` completes jobs immediately without a real Spark cluster. Set `SPARK_E2E_DOCKER_IMAGE` to any Spark image that has the examples JAR at `/opt/spark/examples/jars/`.

```bash
# Pull or build a Spark image (official Apache image works)
docker pull apache/spark:3.5.0

# Set the required env var
export SPARK_E2E_DOCKER_IMAGE=apache/spark:3.5.0

# Run all Docker-mode Spark e2e tests
go test -v -tags spark_e2e \
  -run TestSparkJob_Docker \
  -timeout 10m \
  ./tests/full_mode/plugin/
```

| Environment variable | Default | Description |
|---|---|---|
| `SPARK_E2E_DOCKER_IMAGE` | — | **(required)** Spark Docker image to run |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `3s` | How often to poll step/job state |
| `SPARK_E2E_JOB_TIMEOUT` | `5m` | Max time to wait for a job to reach terminal state |

#### Kubernetes mode — EMR Containers job runs on a real cluster

K8s mode uses `JAISCLOUD_SPARK_MODE=k8s`. The `K8sExecutor` logs the full `spark-submit` argument list it constructs, then delegates lifecycle to the `MockExecutor`. Real pod submission requires the Spark Operator installed in the target cluster.

```bash
# Set required env vars
export SPARK_E2E_SPARK_IMAGE=apache/spark:3.5.0
export SPARK_E2E_K8S_NAMESPACE=default        # optional, defaults to "default"

# Run all K8s-mode Spark e2e tests
go test -v -tags spark_e2e \
  -run TestSparkJob_K8s \
  -timeout 10m \
  ./tests/full_mode/plugin/
```

| Environment variable | Default | Description |
|---|---|---|
| `SPARK_E2E_SPARK_IMAGE` | — | **(required)** Spark image for K8s executor |
| `SPARK_E2E_K8S_NAMESPACE` | `default` | Kubernetes namespace for Spark pods |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `3s` | Polling interval |
| `SPARK_E2E_JOB_TIMEOUT` | `5m` | Job timeout |

#### EventBridge notification tests (no real Spark needed)

The EventBridge tests under `tests/full_mode/plugin/` use the `MockExecutor` and do not require any Docker image or Kubernetes cluster. They verify that EMR/EMR-on-EKS state transitions are published to EventBridge and delivered to SQS targets.

```bash
# No SPARK_E2E_DOCKER_IMAGE or SPARK_E2E_SPARK_IMAGE needed
go test -v -tags spark_e2e \
  -run TestSparkJob_EventBridge \
  -timeout 5m \
  ./tests/full_mode/plugin/
```

---

### Apache Iceberg e2e tests (`iceberg_e2e` build tag)

Iceberg tests run a real Spark SQL job via Docker, write Iceberg tables to JaisCloud's S3 (backed by Glue catalog and DynamoDB lock table), and verify the results via the AWS SDK. They are **excluded from normal CI** by the `//go:build iceberg_e2e` tag.

#### Prerequisites

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.26 | Test runner |
| Docker | 20+ | Runs Spark SQL container |
| curl | any | JAR download script |
| JaisCloud (full mode) | latest | S3, Glue, DynamoDB store |

JaisCloud must be running in full mode (lite mode works too — Glue, S3, and DynamoDB are all in-memory):

```bash
./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
```

#### 1. Download Iceberg JARs

The Spark container needs Iceberg and AWS connector JARs that are not included in the base image. Download them once from Maven Central:

```bash
cd tests/full_mode/iceberg/spark-iceberg
bash download-jars.sh
```

This creates a `jars/` directory containing:

| JAR | Version |
|---|---|
| `iceberg-spark-runtime-3.5_2.12` | 1.5.2 |
| `iceberg-aws-bundle` | 1.5.2 |
| `hadoop-aws` | 3.3.4 |
| `aws-java-sdk-bundle` | 1.12.262 |

#### 2. Build the Spark Iceberg Docker image

```bash
# Still inside tests/full_mode/iceberg/spark-iceberg/
docker build -t spark-iceberg-test .
cd ../../../..    # back to repo root
```

The Dockerfile extends `apache/spark:3.5.0` and copies the downloaded JARs into `/opt/spark/jars/`.

#### 3. Run the Iceberg tests

```bash
export SPARK_E2E_ICEBERG_IMAGE=spark-iceberg-test

go test -v -tags iceberg_e2e \
  -timeout 30m \
  ./tests/full_mode/iceberg/
```

`TestMain` runs first and creates the shared infrastructure once for the entire suite:

| Resource | Type | Purpose |
|---|---|---|
| `iceberg-warehouse` | S3 bucket | Iceberg data and metadata files |
| `iceberg_test_db` | Glue database | Table catalog |
| `iceberg_lock` | DynamoDB table | Iceberg `DynamoDbLockManager` commit lock |

After all tests complete, `TestMain` deletes the Glue database and DynamoDB table. S3 objects are left intact for debugging unless `ICEBERG_CLEAN_S3=true` is set.

#### 4. Run a single Iceberg test

```bash
go test -v -tags iceberg_e2e \
  -run TestIceberg_GlueCatalog_WriteAndRead \
  -timeout 15m \
  ./tests/full_mode/iceberg/
```

#### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `SPARK_E2E_ICEBERG_IMAGE` | — | **(required)** Docker image built in step 2 |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `5s` | Polling interval for Spark job completion |
| `SPARK_E2E_JOB_TIMEOUT` | `10m` | Max time to wait for a Spark SQL job |
| `ICEBERG_CLEAN_S3` | `false` | Set to `true` to delete S3 objects on teardown |

#### How it works

Each test writes and reads Iceberg tables through the Glue catalog:

```
Test (Go SDK) ─────── PutRule / CreateQueue (setup)
Spark SQL job ────── docker run spark-iceberg-test spark-sql
                         │
                         ├─ Glue catalog:  reads/writes metadata_location pointer
                         ├─ S3 (JaisCloud): reads/writes Parquet data and metadata files
                         └─ DynamoDB:       acquires commit locks via DynamoDbLockManager
Test (Go SDK) ─────── DescribeTable / GetObject (verify)
```

Spark connects to JaisCloud via `host.docker.internal` so it can reach the host's port 4566. The Docker `--add-host=host.docker.internal:host-gateway` flag is set automatically by the test runner.

#### Troubleshooting

**`SPARK_E2E_ICEBERG_IMAGE not set — skipping`**  
Set `export SPARK_E2E_ICEBERG_IMAGE=spark-iceberg-test` before running.

**Spark job times out**  
Increase `SPARK_E2E_JOB_TIMEOUT=20m` and check Docker resource limits (Spark needs at least 2 GB RAM).

**`Connection refused` from inside the Spark container**  
JaisCloud must be reachable at `host.docker.internal:4566` from the container. On Linux, ensure Docker is started with `--add-host=host.docker.internal:host-gateway` (the test runner adds this automatically).

**JAR download fails**  
Rerun `bash download-jars.sh` — it skips already-downloaded files. Check your internet connection and Maven Central reachability.

### Race detector

Always run with `-race`. JaisCloud is heavily concurrent and the race detector catches real bugs:

```bash
go test -race ./...
```

---

## Platform Setup

### macOS

```bash
chmod +x scripts/setup-mac.sh
./scripts/setup-mac.sh
```

This installs Homebrew, Go, Docker, AWS CLI and configures a `localcloud` AWS CLI profile with test credentials.

Manual AWS CLI profile setup:
```bash
aws configure set aws_access_key_id test --profile localcloud
aws configure set aws_secret_access_key test --profile localcloud
aws configure set region us-east-1 --profile localcloud

# Use the profile
aws --profile localcloud --endpoint-url http://localhost:4566 sqs list-queues
```

### Windows

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
.\scripts\setup-windows.ps1
```

Build and run:
```powershell
go build -o jaiscloud.exe ./cmd/jaiscloud/
.\jaiscloud.exe start
```

### AWS CLI shorthand

Add this to your shell profile to avoid repeating `--endpoint-url` and `--region`:

```bash
alias awslocal='aws --endpoint-url http://localhost:4566 --region us-east-1 --no-cli-pager'

# Then use:
awslocal sqs create-queue --queue-name my-queue
awslocal s3 ls
awslocal emr list-clusters
```
