# Developer Guide

This guide covers everything you need to build, run, and extend JaisCloud locally.

**Contents**
- [Prerequisites](#prerequisites)
- [Docker Image](#docker-image)
- [Running in Lite Mode](#running-in-lite-mode)
- [Running in Full Mode (PostgreSQL)](#running-in-full-mode-postgresql)
- [Running on Kubernetes](#running-on-local-kubernetes)
- [EMR Spark Cluster — Mock Mode](#emr-spark-cluster--mock-mode)
- [EMR Spark Cluster — Kubernetes Mode](#emr-spark-cluster--kubernetes-mode)
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

## Docker Image

JaisCloud ships a single Docker image built on `scratch` (CGO_ENABLED=0, fully static). It supports both lite and full mode at runtime via environment variables.

```bash
# Build
make docker

# Override the version tag
make docker VERSION=1.2.3

# Push to a registry
make docker REGISTRY=ghcr.io/myorg
```

| Property | Value |
|---|---|
| Base image | `scratch` |
| Binary | CGO_ENABLED=0, fully static |
| Persistence | In-memory (lite) or PostgreSQL (full) |
| Image size | ~10 MB |

Run in lite mode (default):

```bash
docker run -p 4566:4566 jaiscloud:latest
```

Run in full mode (PostgreSQL):

```bash
docker run -p 4566:4566 \
  -e JAISCLOUD_MODE=full \
  -e JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@host.docker.internal:5432/jaiscloud \
  jaiscloud:latest
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
    image: jaiscloud:latest
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
1. Builds the `jaiscloud:latest` Docker image from the repo root `Dockerfile`
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

## EMR Spark Cluster — Mock Mode

The built-in EMR and EMR-on-EKS providers use a `MockExecutor` by default (`JAISCLOUD_SPARK_MODE=off` or unset). In this mode, Spark jobs complete immediately without any actual Spark cluster. This is what you want for most local development and all unit tests.

### 1. Start JaisCloud

```bash
./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
```

You should see:
```
INFO jaiscloud started port=4566 mode=full
```

### 2. Submit an EMR job

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

The default (no `JAISCLOUD_SPARK_MODE` set) uses the mock executor, which completes jobs immediately.

Set `JAISCLOUD_SPARK_MODE=mock` explicitly if you want to be explicit:
```bash
JAISCLOUD_SPARK_MODE=mock ./jaiscloud start --mode full --dsn "postgres://..."
```

For real K8s submission, set `JAISCLOUD_SPARK_MODE=k8s` — see the next section.

---

## EMR Spark Cluster — Kubernetes Mode

In **Kubernetes mode** (`JAISCLOUD_SPARK_MODE=k8s`), the `K8sExecutor` submits real `batch/v1 Jobs` to a Kubernetes cluster. Each `RunJobFlow` or `StartJobRun` call creates a K8s Job that runs `spark-submit --deploy-mode cluster` inside the configured Spark image. No `client-go` dependency is needed — the executor uses stdlib HTTP only.

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

### 2. Start JaisCloud in K8s mode

```bash
export JAISCLOUD_SPARK_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n spark-jobs --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=spark-jobs
export JAISCLOUD_K8S_SA=spark-sa

./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
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

## Running Tests

### Unit tests (no server needed)

```bash
go test -race ./internal/...
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
| JaisCloud server | latest | Running in `--mode full` |

Build and start JaisCloud:

```bash
go build -o jaiscloud ./cmd/jaiscloud/
./jaiscloud start \
  --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
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
