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
  - [Lambda e2e tests](#lambda-e2e-tests-lambda_e2e-build-tag)
    - [Lambda Docker mode](#lambda-docker-mode)
    - [Lambda Kubernetes mode](#lambda-kubernetes-mode)
  - [KMS · SecretsManager · SSM e2e tests](#kms--secretsmanager--ssm-e2e-tests-kms_fullmode-build-tag)
  - [CloudFormation e2e tests](#cloudformation-e2e-tests-cfn_fullmode-build-tag)
  - [Apache Iceberg e2e tests](#apache-iceberg-e2e-tests-iceberg_e2e-build-tag)
- [Platform Runtime Layer](#platform-runtime-layer)
  - [TLS CA injection](#tls-ca-injection)
  - [Extra volumes](#extra-volumes)
  - [Extra environment variables](#extra-environment-variables)
  - [Docker mode behaviour](#docker-mode-behaviour)
  - [Kubernetes mode behaviour](#kubernetes-mode-behaviour)
- [Multi-Cloud Spark Transforms](#multi-cloud-spark-transforms)
  - [Azure](#azure-spark-transform)
  - [GCP](#gcp-spark-transform)
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

With Docker Compose — use the included `docker-compose.yml` at the repo root:

```bash
make up-docker       # builds image, starts postgres (port 5433) + jaiscloud (port 4566)
make down-docker     # stops and removes services
```

Or directly:

```bash
docker-compose up -d
docker-compose down
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

The quickest way is via docker-compose (port 5433 on the host):

```bash
make up-docker
```

Or start postgres manually with Docker:

```bash
docker run -d \
  --name jaiscloud-pg \
  -e POSTGRES_USER=jaiscloud \
  -e POSTGRES_PASSWORD=jaiscloud \
  -e POSTGRES_DB=jaiscloud \
  -p 5433:5432 \
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
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
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

The repo includes a `docker-compose.yml` at the root. Start with:

```bash
make up-docker            # builds jaiscloud image, starts postgres + server
make down-docker          # stop and remove services
docker-compose down -v    # wipe all data (postgres volume)
```

Postgres is exposed on host port **5433** (to avoid conflicts with local Postgres). JaisCloud is on port **4566**.

To view logs:
```bash
docker-compose logs -f jaiscloud
```

### 5. Connection string reference

| Component | Example | Notes |
|---|---|---|
| User | `jaiscloud` | postgres role |
| Password | `jaiscloud` | postgres password |
| Host | `localhost` | hostname or IP |
| Port | `5432` | default postgres port |
| Database | `jaiscloud` | must already exist |

Full DSN: `postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud`

Via environment variable: `JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud`

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

### Deploy with Makefile (recommended)

```bash
make up-k8s     # builds image, applies all manifests, waits for ready
make down-k8s   # removes deployment and cleans up Lambda/Spark resources
```

This applies manifests in order: `namespace.yaml` → `rbac.yaml` → `postgres.yaml` → `jaiscloud.yaml`.

### One-click deploy (legacy script)

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
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
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

Set `JAISCLOUD_EXECUTOR_MODE=mock` explicitly if you want to be explicit:
```bash
JAISCLOUD_EXECUTOR_MODE=mock ./jaiscloud start --mode full --dsn "postgres://..."
```

For real K8s submission, set `JAISCLOUD_EXECUTOR_MODE=k8s` — see the next section.

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
export JAISCLOUD_EXECUTOR_MODE=k8s
# JAISCLOUD_K8S_NAMESPACE and JAISCLOUD_K8S_SA are optional
```

### Auth — out-of-cluster (local dev, CI)

```bash
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443        # kubectl cluster-info
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa --duration=24h)
export JAISCLOUD_K8S_CA_FILE=$HOME/.kube/ca.crt              # or unset for system roots
export JAISCLOUD_K8S_NAMESPACE=spark-jobs
export JAISCLOUD_K8S_SA=spark-sa
```

### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `JAISCLOUD_EXECUTOR_MODE` | `mock` | Set to `k8s` to enable real cluster submission |
| `JAISCLOUD_K8S_APISERVER` | `https://kubernetes.default.svc` | K8s API server URL |
| `JAISCLOUD_K8S_TOKEN` | in-cluster token file | Bearer token: literal string or path to a file (re-read per request for rotation) |
| `JAISCLOUD_K8S_CA_FILE` | in-cluster CA path | PEM CA cert. Unset = system roots. |
| `JAISCLOUD_K8S_NAMESPACE` | `jaiscloud` | Namespace for Jobs and Pods |
| `JAISCLOUD_K8S_SA` | _(none)_ | Service account for the spark-submit Pod |

### Prerequisites

- A Kubernetes cluster (local: kind, minikube, or Docker Desktop)
- A namespace for Spark jobs
- A Spark Docker image accessible from the cluster (default: `apache/spark:3.5.0`)

### 1. Prepare the namespace and RBAC

Use the included manifests which create the `jaiscloud` namespace and grant full executor permissions:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/rbac.yaml
```

Or create manually and apply custom RBAC — JaisCloud needs to manage Jobs; the spark-submit Pod needs to manage Pods:

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
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n spark-jobs --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=spark-jobs
export JAISCLOUD_K8S_SA=spark-sa

./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
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
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud" &
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
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud" &

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

The Spark end-to-end tests cover the full EMR + EMR-on-EKS + EventBridge notification lifecycle. They live under `tests/full_mode/aws/emr/`, `tests/full_mode/aws/emrcontainers/`, and `tests/full_mode/aws/eventbridge/`, and are **excluded from normal CI runs** by the `//go:build spark_e2e` tag. Use the Makefile targets which handle server lifecycle via docker-compose or K8s.

#### Prerequisites (both modes)

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.26 | Test runner |
| Docker | 20+ | Required for Docker mode; used by K8s mode to build images |
| PostgreSQL | 14+ | JaisCloud full-mode store |
| JaisCloud server | latest | Running in `--mode full` |

Use the Makefile to start the server and run tests:

```bash
# EMR Docker tests (server starts via docker-compose)
make test-e2e-emr-docker SPARK_IMAGE=apache/spark:3.5.0

# EMR Containers K8s tests (server deployed to K8s)
make test-e2e-emrcontainers-k8s SPARK_IMAGE=apache/spark:3.5.0

# EventBridge tests
make test-e2e-eventbridge

# Narrow to a specific test
make test-e2e-emrcontainers-k8s TEST_RUN=TestSparkJob_K8s_CancelJobRun
```

Or run manually:

#### Docker mode — EMR steps running SparkPi in a container

```bash
# Start server via docker-compose
make up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=apache/spark:3.5.0

# Run tests
SPARK_E2E_DOCKER_IMAGE=apache/spark:3.5.0 JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags spark_e2e -timeout 10m ./tests/full_mode/aws/emr/

make down-docker
```

| Environment variable | Default | Description |
|---|---|---|
| `SPARK_E2E_DOCKER_IMAGE` | — | **(required)** Spark Docker image to run |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `3s` | How often to poll step/job state |
| `SPARK_E2E_JOB_TIMEOUT` | `5m` | Max time to wait for a job to reach terminal state |

#### Kubernetes mode — EMR Containers job runs on a real cluster

```bash
# Deploy server to K8s
make up-k8s

# Run EMR Containers tests
SPARK_E2E_SPARK_IMAGE=apache/spark:3.5.0 SPARK_E2E_K8S_NAMESPACE=jaiscloud \
  JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags spark_e2e -timeout 15m ./tests/full_mode/aws/emrcontainers/

make down-k8s
```

| Environment variable | Default | Description |
|---|---|---|
| `SPARK_E2E_SPARK_IMAGE` | — | **(required)** Spark image for K8s executor |
| `SPARK_E2E_K8S_NAMESPACE` | `jaiscloud` | Kubernetes namespace for Spark pods |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `SPARK_E2E_POLL_INTERVAL` | `3s` | Polling interval |
| `SPARK_E2E_JOB_TIMEOUT` | `5m` | Job timeout |

#### EventBridge notification tests (no real Spark needed)

```bash
make test-e2e-eventbridge
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=mock
JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags spark_e2e -timeout 10m ./tests/full_mode/aws/eventbridge/
make down-docker
```

---

---

### Lambda e2e tests (`lambda_e2e` build tag)

Lambda e2e tests live under `tests/full_mode/aws/lambda/` and are **excluded from normal CI** by the `//go:build lambda_e2e` tag. Use the Makefile targets which start the server via docker-compose or K8s:

```bash
make test-e2e-lambda-docker   # Docker warm-pool executor
make test-e2e-lambda-k8s      # K8s warm-pod executor
```

All tests follow these conventions:
- `resetState(t)` calls `POST /_jaiscloud/reset` before each test.
- Skip guards (`requireLambdaDockerEnv`, `requireLambdaK8sEnv`) skip when the executor is not configured.
- Timeouts controlled via environment variables.

---

#### Lambda Docker mode

In Docker mode the Lambda executor starts a real Docker container per function (warm pool). Each invocation posts to the container's Lambda Runtime Interface Emulator (RIE) endpoint on port 8080. The container is reused across invocations until deleted or idle.

##### Prerequisites

| Requirement | Notes |
|---|---|
| Docker running | `docker info` must succeed |
| `JAISCLOUD_EXECUTOR_MODE=docker` | Controls executor selection at startup |
| A Lambda container image | Must implement the Lambda RIC. Use the included echo image or build your own. |

##### Using the included echo image

The repo includes a ready-to-use echo handler at `deploy/images/lambda-echo/`:

```bash
docker build -t jaiscloud-lambda-echo deploy/images/lambda-echo/
```

The handler simply echoes the event back: `def handler(event, context): return event`. Tests pass `Handler: "handler.handler"` when creating functions.

##### Running Docker mode tests

```bash
make test-e2e-lambda-docker LAMBDA_IMAGE=jaiscloud-lambda-echo
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_LAMBDA_IMAGE=jaiscloud-lambda-echo
LAMBDA_E2E_DOCKER_IMAGE=jaiscloud-lambda-echo JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags lambda_e2e -timeout 10m ./tests/full_mode/aws/lambda/
make down-docker
```

| Test | What it verifies |
|---|---|
| `TestLambda_Docker_*` | Docker warm-pool: cold start, warm reuse, concurrent invocations, delete cleanup |
| `TestLambda_ColdStartAfterReset` | Container re-created after `/_jaiscloud/reset` |
| `TestLambda_DeleteAndReCreate` | Delete + immediate re-create without collision in warm pool |
| `TestLambda_HealthAfterOrphanCleanup` | Server healthy after startup orphan cleanup |

##### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `LAMBDA_E2E_DOCKER_IMAGE` | — | **(required)** Lambda container image URI. Tests are skipped if unset. |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `LAMBDA_E2E_INVOKE_TIMEOUT` | `2m` | Max time to wait for a single invocation |
| `LAMBDA_E2E_POLL_INTERVAL` | `3s` | Polling interval for async checks |

##### Troubleshooting Docker mode

**Tests skip with "LAMBDA_E2E_DOCKER_IMAGE not set"**  
Export the variable: `export LAMBDA_E2E_DOCKER_IMAGE=jaiscloud-lambda-test`

**Cold start times out**  
The container image pull can take 30–60 s on first run. Pre-pull the image: `docker pull jaiscloud-lambda-test`. Increase `LAMBDA_E2E_INVOKE_TIMEOUT=5m`.

**"connection refused" during warm invocation**  
JaisCloud's Docker executor exposes the container on a random host port. Make sure Docker's host networking is working: `docker run --rm alpine ping host.docker.internal`.

**Container not cleaned up after `DeleteFunction`**  
Container teardown is asynchronous. Run `docker ps | grep jc-lambda` to check. Orphaned containers are named `jc-lambda-<functionName>-<id>` and can be removed with `docker rm -f`.

---

#### Lambda Kubernetes mode

In K8s mode the Lambda executor creates **one warm Pod + ClusterIP Service per function** (matching the Docker executor pattern). Invocations POST to the RIE endpoint on the Service; the Pod is reused across calls and garbage-collected when idle.

##### Prerequisites

| Requirement | Notes |
|---|---|
| Kubernetes cluster | Docker Desktop, kind, minikube, or a remote cluster |
| `JAISCLOUD_EXECUTOR_MODE=k8s` | Controls executor selection at startup |
| In-cluster RBAC | Apply `deploy/k8s/rbac.yaml` — grants Jobs, Pods, Services permissions in the `jaiscloud` namespace |
| A Lambda container image pullable from the cluster | Use the echo image or your own |

##### RBAC

Apply the included manifest (grants all needed permissions in the `jaiscloud` namespace):

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/rbac.yaml
```

##### Running K8s mode tests

```bash
make test-e2e-lambda-k8s LAMBDA_IMAGE=jaiscloud-lambda-echo
# or manually:
make up-k8s
LAMBDA_E2E_K8S_IMAGE=jaiscloud-lambda-echo JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags lambda_e2e -timeout 15m ./tests/full_mode/aws/lambda/
make down-k8s
```

| Test | What it verifies |
|---|---|
| `TestLambda_K8s_*` | K8s warm-pod: cold start, warm reuse, concurrent invocations |
| `TestLambda_ColdStartAfterReset` | Pod re-created after `/_jaiscloud/reset` |
| `TestLambda_DeleteAndReCreate` | Delete + immediate re-create without K8s pod collision |
| `TestLambda_HealthAfterOrphanCleanup` | Server healthy after startup orphan pod cleanup |

##### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `LAMBDA_E2E_K8S_IMAGE` | — | **(required)** Lambda image URI pullable from the cluster. Tests skip if unset. |
| `JAISCLOUD_EXECUTOR_MODE` | `mock` | Must be `k8s` when the server starts |
| `JAISCLOUD_K8S_APISERVER` | `https://kubernetes.default.svc` | K8s API server URL |
| `JAISCLOUD_K8S_CA_FILE` | in-cluster CA | PEM CA cert; unset = system roots |
| `JAISCLOUD_K8S_NAMESPACE` | `jaiscloud` | Namespace for Lambda Pods + Services |
| `JAISCLOUD_HOST` | `http://localhost:4566` | JaisCloud endpoint |
| `LAMBDA_E2E_INVOKE_TIMEOUT` | `2m` | Max time to wait for cold start + invocation |
| `LAMBDA_E2E_POLL_INTERVAL` | `3s` | Polling interval |

##### Watching warm pods during tests

```bash
# Watch pods in real time
kubectl get pods -n jaiscloud -l app=jaiscloud-lambda -w

# Inspect a warm pod
kubectl describe pod -n jaiscloud -l app=jaiscloud-lambda

# Read logs
kubectl logs -n jaiscloud -l app=jaiscloud-lambda --tail=50
```

##### Troubleshooting K8s mode

**Tests skip with "LAMBDA_E2E_K8S_IMAGE not set"**  
Export `LAMBDA_E2E_K8S_IMAGE` before running.

**Pod stuck in `Pending`**  
Image may not be pullable. Check `kubectl describe pod -n jaiscloud` for `ImagePullBackOff`. For kind/minikube: `kind load docker-image jaiscloud-lambda-echo`.

**"Forbidden" creating Pods/Services**  
Apply `deploy/k8s/rbac.yaml` and verify `kubectl auth can-i create pods -n jaiscloud --as=system:serviceaccount:jaiscloud:jaiscloud`.

**Invocation timeout**  
K8s cold start includes image pull + pod scheduling. Increase `LAMBDA_E2E_INVOKE_TIMEOUT=10m` on first run. Subsequent invocations reuse the warm pod and are fast.

---

### KMS · SecretsManager · SSM e2e tests (`kms_fullmode` build tag)

These tests live under `tests/full_mode/aws/kms/` and verify cross-service integration between KMS, SecretsManager, and SSM Parameter Store. They do **not** require Docker or Kubernetes.

```bash
make test-e2e-kms
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=mock
JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags kms_fullmode -timeout 10m ./tests/full_mode/aws/kms/
make down-docker
```

| Test | What it verifies |
|---|---|
| `TestKMS_SecretsManager_Integration` | Create KMS CMK → create secret with that key → `GetSecretValue` decrypts correctly |
| `TestKMS_SecretsManager_BinarySecret` | Binary secret stored and retrieved as `SecretBinary` (not `SecretString`) |
| `TestKMS_SecretsManager_RotateSecret` | `PutSecretValue` creates a new version; latest version is returned |
| `TestKMS_DisabledKey_BlocksSecretCreate` | Creating a secret with a disabled KMS key returns an error |
| `TestSSM_SecureString_KMSEncryption` | `PutParameter` with `SecureString` type encrypts via KMS; `GetParameter` with `WithDecryption=true` decrypts |
| `TestSSM_PathHierarchy_RecursiveVsNonRecursive` | Non-recursive listing returns only direct children; recursive returns all descendants |
| `TestSSM_PathPrefix_NoFalseMatch` | `/app` does not match `/appname/x` (trailing-slash normalization fix) |
| `TestSSM_ParameterHistory` | Overwriting a parameter increments version and records history entry |

No environment variables beyond `JAISCLOUD_HOST` are required for this group.

---

### CloudFormation e2e tests (`cfn_fullmode` build tag)

These tests live under `tests/full_mode/aws/cloudformation/` and verify that CloudFormation stacks provision, update, and delete real downstream resources (SQS queues, Lambda functions, KMS keys, SecretsManager secrets). They do **not** require Docker or Kubernetes.

```bash
make test-e2e-cloudformation
# or manually:
make up-docker JAISCLOUD_EXECUTOR_MODE=mock
JAISCLOUD_HOST=http://localhost:4566 \
  go test -v -tags cfn_fullmode -timeout 10m ./tests/full_mode/aws/cloudformation/
make down-docker
```

| Test | Template resources | What it verifies |
|---|---|---|
| `TestCFN_StackProvisionsSQSAndLambda` | `AWS::SQS::Queue` + `AWS::Lambda::Function` | Stack reaches `CREATE_COMPLETE`; both resources queryable via their service APIs; `Outputs` reference them |
| `TestCFN_StackWithKMSKey_SecretRef` | `AWS::KMS::Key` + `AWS::SecretsManager::Secret` (with `KmsKeyId: !Ref AppKey`) | KMS key is enabled; secret is decryptable using that key |
| `TestCFN_UpdateStack_ChangesResources` | `AWS::SQS::Queue` (parameterized name) | Stack reaches `UPDATE_COMPLETE`; parameter value persisted in stack metadata |
| `TestCFN_DeleteStack_CascadesChildren` | `AWS::SQS::Queue` + `AWS::Lambda::Function` | After `DeleteStack`, both child resources return not-found from their service APIs |
| `TestCFN_StackParameters_DefaultsApplied` | Same as above, no explicit parameters | Template default values are applied; Lambda function created under the default name |

No environment variables beyond `JAISCLOUD_HOST` are required for this group.

##### Template reference

The tests use inline templates. Key patterns covered:

| Pattern | Template syntax | Test |
|---|---|---|
| `Ref` to a parameter | `{"Ref": "FunctionName"}` | `TestCFN_StackParameters_DefaultsApplied` |
| `Fn::GetAtt` for output | `{"Fn::GetAtt": ["ProcessorFunction", "Arn"]}` | `TestCFN_StackProvisionsSQSAndLambda` |
| Cross-resource `Ref` | `"KmsKeyId": {"Ref": "AppKey"}` | `TestCFN_StackWithKMSKey_SecretRef` |

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
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
```

#### 1. Download Iceberg JARs

The Spark container needs Iceberg and AWS connector JARs that are not included in the base image. Download them once from Maven Central:

```bash
cd tests/full_mode/aws/iceberg/spark-iceberg
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
  ./tests/full_mode/aws/iceberg/
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
  ./tests/full_mode/aws/iceberg/
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

---

## Platform Runtime Layer

The Platform Runtime Layer (`internal/platform/`) injects TLS certificate trust, extra volumes, and environment variables uniformly into **every** JaisCloud-managed container or pod — Lambda Docker/K8s executors and Spark Docker/K8s executors alike. Configuration is loaded once at startup via `platform.LoadFromEnv()` and passed by pointer to each executor constructor. It never embeds into any workload-specific config.

### TLS CA injection

Two materializers run in order whenever `JAISCLOUD_PLATFORM_TLS_ENABLED=true`:

| Materializer | What it does |
|---|---|
| **JVM truststore** | Copies the default JVM `cacerts` into an `emptyDir` and imports all CA certs via `keytool`. Sets `JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=...` on the main container. |
| **PEM bundle** | Concatenates all CA files into a single PEM bundle and sets `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `NODE_EXTRA_CA_CERTS`, `AWS_CA_BUNDLE`, `GIT_SSL_CAINFO`, `CURL_CA_BUNDLE` on the main container. |

CA sources are specified as a JSON or YAML array via `JAISCLOUD_PLATFORM_TLS_CA_SOURCES`:

```bash
# Single ConfigMap source (K8s mode — default if unset)
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"jaiscloud","source":{"kind":"configMap","name":"jaiscloud-ca-cert","key":"ca.crt"}}
]'

# Multiple sources (ConfigMap + Secret)
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"corp-ca","source":{"kind":"configMap","name":"corp-ca","key":"ca.pem"}},
  {"name":"internal","source":{"kind":"secret","name":"internal-ca","key":"tls.crt"}}
]'

# Local file (Docker mode — only "kind":"file" sources can be read by the host process)
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"my-ca","source":{"kind":"file","key":"/etc/ssl/certs/my-ca.pem"}}
]'
```

> **Docker mode note:** Only `kind="file"` CA sources can be materialised on the Docker host. `configMap` and `secret` sources are skipped with a warning — they are K8s-only. For Docker deployments, use `kind="file"` sources pointing to host-local PEM files.

Disable TLS injection entirely:

```bash
export JAISCLOUD_PLATFORM_TLS_ENABLED=false
```

### Extra volumes

Inject additional volumes into every pod/container via `JAISCLOUD_PLATFORM_VOLUMES` (JSON/YAML array, or `_FILE` variant for a file path). Both a short-form and a long-form spec are supported.

**Short form** — convenience for the common cases:

```bash
export JAISCLOUD_PLATFORM_VOLUMES='[
  {"name":"my-configmap", "configMap":"my-cm-name",   "mountPath":"/etc/conf"},
  {"name":"my-secret",    "secret":"my-secret-name",  "mountPath":"/etc/secret",  "readOnly":true},
  {"name":"my-pvc",       "pvc":"my-pvc-claim",       "mountPath":"/data"},
  {"name":"scratch",      "emptyDir":true,             "mountPath":"/tmp/scratch"}
]'
```

**Long form** — for projected volumes, CSI, hostPath, and per-mount subPath:

```bash
export JAISCLOUD_PLATFORM_VOLUMES='[
  {
    "name": "azure-workload-id",
    "source": {
      "kind": "projected",
      "projected": {
        "sources": [
          {"serviceAccountToken": {"audience":"api://AzureADTokenExchange","expirationSeconds":3600,"path":"token"}}
        ]
      }
    },
    "mounts": [{"mountPath":"/var/run/secrets/azure/identity"}]
  }
]'
```

> **Docker mode note:** Only `kind="hostPath"` volumes produce `-v` bind-mount args. `configMap`, `secret`, `pvc`, and `projected` volumes are K8s-only and are silently skipped in Docker mode.

### Extra environment variables

Inject extra env vars into every pod/container:

```bash
export JAISCLOUD_PLATFORM_ENV='{"CUSTOM_VAR":"value","ANOTHER":"value2"}'

# Or via file
export JAISCLOUD_PLATFORM_ENV_FILE=/etc/jaiscloud/extra-env.json
```

### Environment variable reference

| Variable | Default | Description |
|---|---|---|
| `JAISCLOUD_PLATFORM_TLS_ENABLED` | `true` | Enable CA injection |
| `JAISCLOUD_PLATFORM_TLS_CA_SOURCES` | JaisCloud default ConfigMap | JSON/YAML CA source array |
| `JAISCLOUD_PLATFORM_TLS_CA_SOURCES_FILE` | _(empty)_ | File-based variant; takes precedence over the inline var |
| `JAISCLOUD_PLATFORM_TLS_CLIENT_CERT` | _(empty)_ | Optional mTLS client cert source (same shape as one CA source element) |
| `JAISCLOUD_PLATFORM_TLS_PASSWORD` | `changeit` | JVM truststore password |
| `JAISCLOUD_PLATFORM_VOLUMES` | _(empty)_ | JSON/YAML volume spec array |
| `JAISCLOUD_PLATFORM_VOLUMES_FILE` | _(empty)_ | File-based variant |
| `JAISCLOUD_PLATFORM_ENV` | _(empty)_ | JSON/YAML `{"KEY":"VALUE"}` map |
| `JAISCLOUD_PLATFORM_ENV_FILE` | _(empty)_ | File-based variant |
| `JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST` | _(empty)_ | Comma-separated allowed `hostPath` prefixes; empty = hostPath disabled |

### Docker mode behaviour

`platform.ApplyDocker` is called inside `DockerExecutor.Submit` / `startContainer` and returns `-v` and `-e` args appended to the `docker run` command:

- TLS PEM bundle materialised to a host temp file → `-v /tmp/jaiscloud-ca-bundle-*.pem:/etc/ssl/certs/jaiscloud-ca.pem:ro`
- All six non-JVM env vars set to the container path → `-e SSL_CERT_FILE=/etc/ssl/certs/jaiscloud-ca.pem ...`
- `hostPath` volumes → `-v host/path:container/path:ro`
- Extra env → `-e KEY=VALUE`

### Kubernetes mode behaviour

`platform.ApplyK8s` is called inside `K8sExecutor.buildJobManifest` / `createPod` and mutates the `k8stypes.PodSpec` in place **after** the cloud-specific transform has already contributed its volumes and env:

1. CA source volumes added to `spec.volumes`
2. JVM materializer: `emptyDir` output volume + init container (keytool import) + main container env
3. PEM materializer: `emptyDir` output volume + init container (concatenate) + main container env vars
4. Platform extra volumes appended (conflict-checked against existing names)
5. Platform extra volume mounts added to the main container
6. Platform extra env merged into the main container env (first-wins deduplication)

---

## Multi-Cloud Spark Transforms

The `CloudSparkTransform` registry (`internal/executor/spark/cloud_transform.go`) decouples cloud-specific Spark contributions from the executor. Each cloud registers itself via `init()` and is selected at manifest build time from `SparkConfig.Cloud` (derived from `--cloud` / `JAISCLOUD_CLOUD`).

| Cloud | Transform | URI rewrite | Auth mechanism |
|---|---|---|---|
| `aws` | `awsTransform` | `s3a://` → `s3a://` (identity) | S3 endpoint env + Hadoop S3A confs |
| `azure` | `azureTransform` | `s3a://` → `abfss://bucket@account.dfs.core.windows.net/` | SharedKey or OAuth/Workload Identity |
| `gcp` | `gcpTransform` | `s3a://` → `gs://` | Service account key file or K8s Secret |

Each transform contributes:
- `ResolveCommand` — binary path + rewritten `spark-submit` args
- `SparkConfs` — `--conf` flags injected before the JAR path
- `PodEnv` — environment variables for the spark-submit container
- `PodVolumes` — cloud-specific volumes and mounts (credentials, identity tokens)

### Azure Spark transform

Set `JAISCLOUD_CLOUD=azure` and configure one of two authentication modes:

**Shared key** (simpler, for dev):

```bash
export JAISCLOUD_CLOUD=azure
export JAISCLOUD_AZURE_STORAGE_ACCOUNT=mystorageacct
export JAISCLOUD_AZURE_STORAGE_KEY=base64encodedkey==
```

This injects `fs.azure.account.auth.type.*.dfs.core.windows.net=SharedKey` and the account key into Spark confs. URIs are rewritten `s3a://bucket/key` → `abfss://bucket@mystorageacct.dfs.core.windows.net/key`.

**OAuth / Workload Identity** (for production K8s):

```bash
export JAISCLOUD_CLOUD=azure
export JAISCLOUD_AZURE_STORAGE_ACCOUNT=mystorageacct
export JAISCLOUD_AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export JAISCLOUD_AZURE_CLIENT_SECRET=my-client-secret
export JAISCLOUD_AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

This injects OAuth confs and mounts a `projected` volume (`serviceAccountToken` + `downwardAPI`) at `/var/run/secrets/azure/identity` for Workload Identity federation.

Optionally override the ADLS endpoint:

```bash
export JAISCLOUD_AZURE_STORAGE_ENDPOINT=https://mystorageacct.dfs.core.windows.net
```

### GCP Spark transform

Set `JAISCLOUD_CLOUD=gcp` and configure one of two service-account modes:

**Key file path** (the key is already present on the container filesystem):

```bash
export JAISCLOUD_CLOUD=gcp
export JAISCLOUD_GCP_PROJECT_ID=my-gcp-project
export JAISCLOUD_GCP_SA_KEY_PATH=/etc/gcp/key.json
```

**K8s Secret** (the key is stored in a Kubernetes Secret and mounted by the executor):

```bash
export JAISCLOUD_CLOUD=gcp
export JAISCLOUD_GCP_PROJECT_ID=my-gcp-project
export JAISCLOUD_GCP_SA_SECRET=my-gcp-sa-key-secret   # K8s Secret name in the executor namespace
```

The executor mounts the Secret at `/etc/gcp` and sets `GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/key.json`. URIs are rewritten `s3a://bucket/key` → `gs://bucket/key`. GCS Spark confs (`spark.hadoop.google.cloud.auth.service.account.enable=true`, `spark.hadoop.fs.gs.project.id`) are injected automatically.

Optionally override the GCS endpoint (for emulators):

```bash
export JAISCLOUD_GCP_STORAGE_ENDPOINT=http://fake-gcs-server:4443
```

---

## Executor Mode Configuration Examples

Complete environment variable sets for each executor mode. Copy the block that matches your setup and adjust paths / names.

### Mock mode (default — no containers, instant completion)

No extra configuration needed. This is the default when `JAISCLOUD_EXECUTOR_MODE` is unset.

```bash
# Minimal — everything defaults to in-memory mock
./jaiscloud start

# Explicit mock with full persistence
./jaiscloud start \
  --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud" \
  --executor-mode mock
```

All Lambda invocations return an echo of the request payload. All EMR steps and job runs complete immediately with `COMPLETED` state. No Docker daemon or Kubernetes cluster needed.

---

### Docker mode

Requires a running Docker daemon. Each Lambda function gets a warm Docker container; each Spark job runs `spark-submit` inside a Docker container.

```bash
# ── Core ──────────────────────────────────────────────────────────────────────
export JAISCLOUD_EXECUTOR_MODE=docker
export JAISCLOUD_MODE=full
export JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud

# ── Lambda ────────────────────────────────────────────────────────────────────
export JAISCLOUD_LAMBDA_IMAGE=public.ecr.aws/lambda/python:3.12  # default per-runtime; override globally here
export JAISCLOUD_LAMBDA_NETWORK=jaiscloud-net                     # Docker network containers join
export JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300                        # idle container TTL

# ── Spark (EMR / EMR on EKS) ──────────────────────────────────────────────────
export JAISCLOUD_SPARK_IMAGE=apache/spark:3.5.0   # Spark Docker image

# Optional: S3 endpoint so Spark containers can reach JaisCloud's S3
export JAISCLOUD_SPARK_S3_ENDPOINT=http://host.docker.internal:4566
export JAISCLOUD_AWS_REGION=us-east-1
export JAISCLOUD_AWS_ACCESS_KEY_ID=test
export JAISCLOUD_AWS_SECRET_ACCESS_KEY=test

# ── Platform layer ─────────────────────────────────────────────────────────────
# TLS: disable if no custom CA is needed
export JAISCLOUD_PLATFORM_TLS_ENABLED=false

# TLS: inject a local CA PEM file into every container
# export JAISCLOUD_PLATFORM_TLS_ENABLED=true
# export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[{"name":"corp-ca","source":{"kind":"file","key":"/etc/ssl/certs/corp-ca.pem"}}]'

# Extra env passed to every managed container
export JAISCLOUD_PLATFORM_ENV='{"HTTP_PROXY":"http://proxy.corp:3128","NO_PROXY":"localhost"}'

# hostPath bind-mounts (must be in allowlist)
export JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST=/etc/ssl/certs,/run/secrets
export JAISCLOUD_PLATFORM_VOLUMES='[{"name":"corp-certs","mountPath":"/etc/corp/certs","hostPath":"/etc/ssl/certs","readOnly":true}]'

./jaiscloud start
```

**Verify Docker executor is active:**
```bash
# After starting, you should see in the logs:
# INFO  executor  lambda=docker  spark=docker
# INFO  platform tls  enabled=false  ...

# Lambda cold start creates a container visible via:
docker ps --filter name=jc-lambda-
# Spark jobs create containers visible via:
docker ps --filter name=jc-spark-
```

---

### Kubernetes mode

Requires a reachable Kubernetes cluster. Each Lambda function gets a warm Pod + ClusterIP Service; each Spark job submits a `batch/v1` Job.

```bash
# ── Core ──────────────────────────────────────────────────────────────────────
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_MODE=full
export JAISCLOUD_DSN=postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud

# ── Kubernetes API server ─────────────────────────────────────────────────────
# In-cluster (JaisCloud runs inside a pod): leave these unset — auto-detected
# Out-of-cluster (local dev / CI):
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443          # kubectl cluster-info
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n jaiscloud --duration=24h)
export JAISCLOUD_K8S_CA_FILE=$HOME/.kube/ca.crt                # unset = system roots
export JAISCLOUD_K8S_NAMESPACE=jaiscloud
export JAISCLOUD_K8S_SA=jaiscloud-sa

# ── Lambda ────────────────────────────────────────────────────────────────────
export JAISCLOUD_LAMBDA_KEEPALIVE_SECS=300    # idle pod TTL before garbage collection

# ── Spark (EMR / EMR on EKS) ──────────────────────────────────────────────────
export JAISCLOUD_SPARK_IMAGE=apache/spark:3.5.0
export JAISCLOUD_K8S_SPARK_NAMESPACE=jaiscloud       # namespace for Spark batch Jobs
export JAISCLOUD_K8S_SPARK_SA=spark-sa               # service account for the Spark driver

# Optional: S3 endpoint for Spark pods to reach JaisCloud's S3
export JAISCLOUD_SPARK_S3_ENDPOINT=http://jaiscloud.jaiscloud.svc.cluster.local:4566
export JAISCLOUD_AWS_REGION=us-east-1
export JAISCLOUD_AWS_ACCESS_KEY_ID=test
export JAISCLOUD_AWS_SECRET_ACCESS_KEY=test

# ── Platform layer ─────────────────────────────────────────────────────────────
# TLS: inject a CA from a ConfigMap that already exists in the cluster
export JAISCLOUD_PLATFORM_TLS_ENABLED=true
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"corp-ca","source":{"kind":"configMap","name":"corp-ca-bundle","key":"ca.crt"}}
]'
# Password for the JVM truststore init container
export JAISCLOUD_PLATFORM_TLS_PASSWORD=changeit

# Extra env injected into every pod's main container
export JAISCLOUD_PLATFORM_ENV='{"CUSTOM_ENDPOINT":"http://internal-service:8080"}'

# Extra volume from a Secret mounted at a specific path
export JAISCLOUD_PLATFORM_VOLUMES='[
  {"name":"corp-tls","secret":"corp-tls-secret","mountPath":"/etc/corp/tls","readOnly":true}
]'

./jaiscloud start
```

**Verify K8s executor is active:**
```bash
# After starting you should see in the logs:
# INFO  executor  lambda=k8s  spark=k8s
# INFO  platform tls  enabled=true  ca-sources=[corp-ca]  ...

# Lambda warm pods:
kubectl get pods -n jaiscloud -l app=jaiscloud-lambda

# Lambda ClusterIP services (one per function):
kubectl get svc -n jaiscloud -l app=jaiscloud-lambda

# Spark batch jobs:
kubectl get jobs -n jaiscloud -l app=jaiscloud-spark
```

---

### Multi-cloud (Azure / GCP) with K8s executor

Set `--cloud` to select the cloud adapter, then add the cloud-specific Spark transform env vars. The Platform layer applies identically across all clouds.

**Azure (K8s executor, OAuth auth):**
```bash
export JAISCLOUD_CLOUD=azure
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n jaiscloud --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=jaiscloud

export JAISCLOUD_AZURE_STORAGE_ACCOUNT=mystorageacct
export JAISCLOUD_AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export JAISCLOUD_AZURE_CLIENT_SECRET=my-client-secret
export JAISCLOUD_AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

export JAISCLOUD_PLATFORM_TLS_ENABLED=true
export JAISCLOUD_PLATFORM_TLS_CA_SOURCES='[
  {"name":"azure-ca","source":{"kind":"configMap","name":"azure-ca-bundle","key":"ca.crt"}}
]'

./jaiscloud start --mode full --dsn "postgres://..."
```

**GCP (K8s executor, Secret-based SA key):**
```bash
export JAISCLOUD_CLOUD=gcp
export JAISCLOUD_EXECUTOR_MODE=k8s
export JAISCLOUD_K8S_APISERVER=https://127.0.0.1:6443
export JAISCLOUD_K8S_TOKEN=$(kubectl create token jaiscloud-sa -n jaiscloud --duration=24h)
export JAISCLOUD_K8S_NAMESPACE=jaiscloud

export JAISCLOUD_GCP_PROJECT_ID=my-gcp-project
export JAISCLOUD_GCP_SA_SECRET=my-gcp-sa-key-secret   # K8s Secret in the executor namespace

export JAISCLOUD_PLATFORM_TLS_ENABLED=false   # GCP root CAs already trusted by default JVM

./jaiscloud start --mode full --dsn "postgres://..."
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
