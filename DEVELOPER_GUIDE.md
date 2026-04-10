# Developer Guide — Local development

Prerequisites
- macOS: run the script at [scripts/setup-mac.sh](scripts/setup-mac.sh#L1-L1) (it installs Homebrew, Go, Docker, AWS CLI, etc.)
- Windows: run `scripts/setup-windows.ps1` (requires PowerShell 7 / Administrator)

Mac quick steps
1. Make the mac setup script executable and run it:

```bash
chmod +x scripts/setup-mac.sh
./scripts/setup-mac.sh
```

2. Build and run the server (option A — build binary):

```bash
go build -o jaiscloud ./cmd/jaiscloud/
./jaiscloud start
```

Option B — run directly (dev):

```bash
go run ./cmd/jaiscloud/ start
```

Running in full mode (PostgreSQL persistence)

By default JaisCloud runs in **lite mode** — all state is in memory and lost on restart. **Full mode** persists all state (queues, topics, tables, objects, IAM resources, SQS messages) in a local PostgreSQL database.

### 1. Start a local PostgreSQL instance

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

Or if you already have PostgreSQL installed locally, create a database:

```bash
createdb jaiscloud
```

### 2. Start the server in full mode

Pass the DSN with `--dsn` (or set `JAISCLOUD_DSN`):

```bash
./jaiscloud start --mode full \
  --dsn "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
```

The server runs migrations automatically on startup — no manual schema setup needed.

### 3. Verify

```bash
./jaiscloud doctor          # checks the server is reachable
./jaiscloud env             # prints effective config including mode and DSN
```

### Connection string reference

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

## Running in full mode on local Kubernetes

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

### Persistent storage

Two PersistentVolumeClaims (PVCs) hold data that survives pod restarts:

| PVC | Size | Contents |
|---|---|---|
| `postgres-pvc` | 1 Gi | PostgreSQL data directory (all control-plane metadata, SQS messages, DynamoDB items, S3 object metadata) |
| `jaiscloud-blobs-pvc` | 5 Gi | S3 object bytes (mounted at `/data/blobs` inside the container) |

### Stopping and removing workloads

To stop JaisCloud and Postgres without losing any data:

```bash
./deploy/deploy.sh --delete
```

This removes the Deployments, Services, ConfigMap and Secret — **PVCs are preserved**. Re-running `./deploy/deploy.sh` brings everything back with all data intact.

### Wiping all persisted data

To permanently delete all stored data (postgres database + S3 blobs) and remove all Kubernetes resources:

```bash
./deploy/deploy.sh --reset
```

This scales both deployments to zero (to release PVC bindings), deletes both PVCs, then deletes the entire namespace. Use this when you want a completely clean slate.

> **Warning:** `--reset` is irreversible. All SQS queues, DynamoDB tables, S3 objects, SNS topics, IAM resources and their data will be permanently deleted.

### Command reference

| Command | Workloads | PVCs (data) |
|---|---|---|
| `./deploy/deploy.sh` | Created / updated | Created if absent, existing data kept |
| `./deploy/deploy.sh --delete` | Removed | **Kept** — data survives |
| `./deploy/deploy.sh --reset` | Removed | **Deleted** — all data wiped |

### Configuration

Default settings are in [deploy/k8s/jaiscloud.yaml](deploy/k8s/jaiscloud.yaml). Override via the `jaiscloud-config` ConfigMap (non-secret) or `postgres-secret` Secret (credentials and DSN). Edit those files before running `deploy.sh`, or patch them after:

```bash
kubectl set env deployment/jaiscloud -n jaiscloud JAISCLOUD_LOG_LEVEL=debug
```

### Port forwarding on non-Docker-Desktop clusters

The Service uses `type: LoadBalancer`. Docker Desktop maps this directly to `localhost:4566`. On minikube or kind, the external IP stays `<pending>` — use port-forward instead:

```bash
kubectl port-forward -n jaiscloud svc/jaiscloud 4566:4566
```

### Viewing logs

```bash
# JaisCloud logs
kubectl logs -n jaiscloud deployment/jaiscloud -f

# PostgreSQL logs
kubectl logs -n jaiscloud deployment/postgres -f
```

---

Run unit tests

Unit tests do not require the server to be running. From the repository root run:

```bash
go test ./internal/... -v -count=1
```

Run integration tests

Start the server (see above), then in a separate shell run:

```bash
go test ./tests/integration/ -v -count=1 -timeout 60s
# or use the Makefile target (if present)
make test-integration
```

AWS CLI for tests
- The mac setup script creates a `localcloud` profile with test credentials and an `awslocal` alias. If you need to run manually:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
aws configure set aws_access_key_id test --profile localcloud
aws configure set aws_secret_access_key test --profile localcloud
aws configure set region us-east-1 --profile localcloud
```

Windows quick steps
1. Open an elevated PowerShell and run:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
.\scripts\setup-windows.ps1
```

2. Build & run server (same as mac, using PowerShell):

```powershell
go build -o jaiscloud ./cmd/jaiscloud/
.\jaiscloud start
```

Files
- `scripts/setup-mac.sh` — macOS installer script
- `scripts/setup-windows.ps1` — Windows installer script (created)
- `DEVELOPER_GUIDE.md` — this file
