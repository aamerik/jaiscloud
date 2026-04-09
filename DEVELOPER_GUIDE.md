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
