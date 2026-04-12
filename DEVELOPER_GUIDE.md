# Developer Guide

This guide covers everything you need to build, run, and extend JaisCloud locally.

**Contents**
- [Prerequisites](#prerequisites)
- [Running in Lite Mode](#running-in-lite-mode)
- [Running in Full Mode (PostgreSQL)](#running-in-full-mode-postgresql)
- [Running on Kubernetes](#running-on-local-kubernetes)
- [EMR Spark Cluster — Docker Mode](#emr-spark-cluster--docker-mode)
- [EMR Spark Cluster — Kubernetes Mode](#emr-spark-cluster--kubernetes-mode)
- [Writing a Custom Plugin](#writing-a-custom-plugin)
- [Running Tests](#running-tests)
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

In **Kubernetes mode** (`JAISCLOUD_SPARK_MODE=k8s`), the plugin uses the `K8sExecutor`. Currently the executor logs the `spark-submit` arguments it *would* send and delegates job lifecycle to the same `MockExecutor`. This gives you a realistic view of the Spark submission parameters while keeping tests fast.

> **Real k8s submission is planned for a future phase.** The groundwork (SparkSubmitArgs, namespace/service-account config) is already in place.

### Prerequisites

A working Kubernetes cluster with the Spark Operator installed, or a plain cluster where you run `spark-submit` directly. The plugin needs:

- A namespace for Spark jobs (e.g. `spark-jobs`)
- A service account with `edit` permissions in that namespace
- A Spark Docker image (default: `apache/spark:3.5.0`)

### 1. Prepare the Spark namespace

```bash
kubectl create namespace spark-jobs

# Create the service account
kubectl create serviceaccount spark-sa -n spark-jobs

# Grant it edit permissions (needed for driver → executor pod creation)
kubectl create rolebinding spark-sa-edit \
  --clusterrole=edit \
  --serviceaccount=spark-jobs:spark-sa \
  -n spark-jobs
```

### 2. Set Spark config via environment variables

```bash
export JAISCLOUD_SPARK_MODE=k8s
export JAISCLOUD_SPARK_NAMESPACE=spark-jobs
export JAISCLOUD_SPARK_SERVICE_ACCOUNT=spark-sa
export JAISCLOUD_SPARK_IMAGE=apache/spark:3.5.0
# Optional: enable S3 event logging
export JAISCLOUD_SPARK_S3_LOG_URI=s3://my-bucket/spark-logs
```

### 3. Start JaisCloud with the plugin

```bash
./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud" \
  --plugin-dir .
```

### 4. Submit a job

Same AWS CLI commands as in Docker mode. The difference is that the plugin will log the full `spark-submit` command it constructs:

```
INFO k8s executor: submitting spark job jobID=j-ABC123 args=["--master","k8s://https://...","--deploy-mode","cluster","--class","com.example.App",...]
```

### 5. Understanding SparkSubmitArgs

The plugin builds the following `spark-submit` argument list for k8s mode:

```
spark-submit \
  --master k8s://https://<api-server> \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
  --conf spark.executor.instances=1 \          # SizeSmall
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=2g \
  --conf spark.eventLog.enabled=true \         # if S3LogURI is set
  --conf spark.eventLog.dir=s3://my-bucket/spark-logs \
  --class com.example.App \
  s3://my-bucket/app.jar
```

Cluster size controls executor count:

| Size | Executors | Driver memory | Executor memory |
|---|---|---|---|
| `SizeSmall` | 1 | 1g | 2g |
| `SizeMedium` | 2 | 2g | 4g |
| `SizeLarge` | 4 | 4g | 8g |

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
The registry key is `"ProviderPrefix.Action"`. The prefix is derived from your service name by `serviceToProviderPrefix` in `internal/plugin/routes.go`. Check the mapping there — if `"myservice"` is not in the switch, it falls back to the raw service name (`"myservice"`). The prefix capitalisation must match what `serviceToProvider` in `server.go` produces for your service name. Add a case to both functions if needed.

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
```

Integration tests automatically call `POST /_jaiscloud/reset` between each test case via `resetState(t)`. You do not need to restart the server between runs.

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
