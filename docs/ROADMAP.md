# JaisCloud — Roadmap

## Overview

JaisCloud is a local multi-cloud emulator that speaks native AWS, GCP, and Azure wire protocols so any SDK can point at it without modification. Development is structured in 7 phases — 0 through 6.

| Phase | Name | Status | Timeline |
|---|---|---|---|
| 0 | SQS Proof of Concept | ✅ Complete | Weeks 1–2 |
| 1 | AWS Core Services | ✅ Complete | Weeks 3–5 |
| 2 | AWS Extended Services | Planned | Weeks 7–10 |
| 3 | AWS Athena (DuckDB) | Planned | Weeks 11–12 |
| 4 | Full State Export / Import | Planned | Weeks 13–14 |
| 5 | GCP API Layer | Planned | Weeks 15–18 |
| 6 | Azure API Layer | Planned | Weeks 19–22 |
| 7 | Polish & Release | Planned | Weeks 23–24 |

---

## Phase 0: SQS Proof of Concept ✅

**Goal:** Validate the full architecture stack end-to-end with a single service before committing to all Phase 1 services.

### Deliverables

| Category | Deliverable |
|---|---|
| **Foundation** | Go module, CLI (`cobra start`), config loading (`viper`) |
| **Store** | `ResourceStore` interface + `MemoryResourceStore` (lite mode) |
| **HTTP** | Chi gateway with Recovery, RequestID, Logging middleware |
| **Admin** | `/_jaiscloud/health`, `/_jaiscloud/reset` endpoints |
| **Adapter** | `DetectService()` (SQS only) + SQSCodec (JSON + Query/XML dual protocol) |
| **Provider** | Full `QueueProvider` — all 17 SQS operations (FIFO, DLQ, batches, visibility timeout) |
| **Store** | `SQSMessageStore` interface + in-memory implementation |
| **Clock** | `Clock` interface — `RealClock`, `FixedClock`, `OffsetClock` + deterministic mode |
| **Events** | `EventBus` (in-process, for DLQ redrive) |
| **Tests** | 32 SQS integration tests (Go SDK) — all passing, `go test -race` clean |

**Exit criteria:** All 32 integration tests pass; dual protocol (JSON + Query/XML) verified.

---

## Phase 1: AWS Core Services ✅

**Goal:** Add remaining 6 AWS services plus full persistence, observability, and CLI infrastructure.

### Services

| Service | Control Plane Operations | Data Plane Operations |
|---|---|---|
| **S3** | CreateBucket, DeleteBucket, ListBuckets, GetBucketLocation, HeadBucket | PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjectsV1, ListObjectsV2, DeleteObjects, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, ListParts |
| **DynamoDB** | CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, TagResource, UntagResource, ListTagsOfResource | PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem, TransactWriteItems, TransactGetItems |
| **SNS** | CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes, Subscribe, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic, GetSubscriptionAttributes, SetSubscriptionAttributes, TagResource, UntagResource, ListTagsForResource | Publish (fan-out to SQS), PublishBatch |
| **Lambda** | CreateFunction, DeleteFunction, GetFunction, GetFunctionConfiguration, ListFunctions, UpdateFunctionCode, UpdateFunctionConfiguration | InvokeFunction (echo, sync) |
| **IAM** | CreateRole, DeleteRole, GetRole, ListRoles, UpdateAssumeRolePolicy, TagRole, UntagRole, ListRoleTags, CreatePolicy, GetPolicy, DeletePolicy, ListPolicies, AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies, PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies, CreateUser, GetUser, DeleteUser, ListUsers, CreateAccessKey, DeleteAccessKey, ListAccessKeys | — (inline evaluation, accept-all) |
| **STS** | AssumeRole, GetCallerIdentity, GetSessionToken | — |

### Infrastructure Deliverables

| Component | Deliverable |
|---|---|
| **Persistence** | `PostgresResourceStore`, `PostgresSQSMessageStore`, `PostgresDynamoDBItemStore`, `PostgresS3ObjectMetaStore` |
| **BlobFS** | `BlobStore` interface — `MemoryBlobStore`, `LocalFSBlobStore` |
| **Migrations** | Auto-applied SQL migrations on startup (`internal/store/migrations/`) |
| **AWS Codecs** | `S3Codec` (REST/XML), `DynamoDBCodec` (JSON/Target + CRC32), `SNSCodec` (Query/XML), `LambdaCodec` (REST/JSON), `IAMCodec` (Query/XML, handles STS) |
| **CLI** | `env`, `doctor`, `version`, `export [-o file]`, `import [-i file]`, `reset` commands |
| **Observability** | Prometheus metrics (`--metrics`), structured `slog` logging; `--tracing` flag is wired but OTel middleware not yet implemented |
| **Admin** | `/_jaiscloud/export`, `/_jaiscloud/import`, snapshot/restore for `MemoryResourceStore` (control-plane metadata only — SQS messages, DynamoDB items, and S3 objects are not snapshotted) |
| **Mode system** | `--mode lite` (in-memory) and `--mode full` (PostgreSQL-backed) |
| **Tests** | Integration tests for SQS, IAM/STS, SNS, DynamoDB, S3, Lambda |

### Out of Scope (explicitly deferred)

| Category | Deferred To |
|---|---|
| EC2, VPC, Route53, RDS, ElastiCache, ECS | Phase 2 |
| EMR on EC2 (classic EMR) | Phase 2 |
| Glue Data Catalog / Iceberg | Phase 2 |
| CloudFormation / IaC engine | Phase 2 |
| DynamoDB Streams | Phase 2 |
| IAM condition keys, resource-level permissions | Phase 2 |
| Athena / DuckDB | Phase 3 |
| GCP services | Phase 4 |
| Azure services | Phase 5 |
| Web console UI | Post-Phase 6 |

---

## Phase 2: AWS Extended Services

**Goal:** Complete AWS surface area — compute, networking, managed databases, containers, and analytics foundation.

### Planned Deliverables

| Service | Scope |
|---|---|
| **EC2** | Instances, AMIs, security groups, key pairs |
| **VPC** | VPCs, subnets, route tables, internet gateways, NAT gateways |
| **Route53** | Hosted zones, record sets, health checks |
| **RDS** | DB instances, DB clusters — full mode: real Postgres/MySQL containers |
| **ElastiCache** | Cache clusters — full mode: real Redis containers |
| **ECS** | Task definitions, services, clusters — full mode: real Docker containers |
| **EMR on EC2** | Clusters, job flows — full mode: Spark on K8s (reuses Phase 1 SparkExecutor) |
| **Glue Data Catalog** | Databases, tables, partitions — Iceberg-compatible atomic CAS on metadata pointer |
| **CloudFormation** | Template parsing and stack provisioning |
| **DynamoDB Streams** | CDC / stream processing |
| **IAM advanced** | Condition keys, resource-level permissions, STS federation |

#### Glue / Iceberg Detail

The Glue catalog stores the `metadata_location` pointer (`s3://bucket/iceberg/metadata/v3.metadata.json`). Iceberg clients (Spark, Trino, PyIceberg) read this pointer, then read/write metadata and data files via S3. On commit, Glue atomically swaps the pointer to the new metadata version.

- **Lite mode:** `sync.Mutex` on table entry for CAS safety
- **Full mode:** Postgres row-level CAS — `UPDATE ... WHERE data->>'metadata_location' = $expected`
- **Operations:** CreateDatabase, GetDatabase, GetDatabases, DeleteDatabase, CreateTable, GetTable, GetTables, UpdateTable, DeleteTable, CreatePartition, GetPartition, GetPartitions, BatchCreatePartition, BatchDeletePartition, BatchUpdatePartition

---

## Phase 3: AWS Athena (DuckDB)

**Goal:** Emulate Athena using DuckDB as the query engine. Depends on Glue (Phase 2) and S3/BlobFS (Phase 1).

### Planned Deliverables

| Component | Scope |
|---|---|
| **AthenaProvider** | StartQueryExecution, GetQueryExecution, GetQueryResults, StopQueryExecution |
| **SQL rewriter** | Resolves Glue table names → S3 paths → BlobFS filesystem paths; rewrites to DuckDB `read_parquet(...)` |
| **Subprocess mode** (default) | DuckDB CLI invoked as child process — pure Go binary, ~50ms overhead per query |
| **Embedded CGO mode** (optional) | DuckDB linked via CGO — ~1–5ms overhead, requires C compiler at build time |
| **Build targets** | `make build` (CGO_ENABLED=0, subprocess) and `make build-cgo` (CGO_ENABLED=1, embedded) |

#### Query Flow

```
StartQueryExecution(SQL)
  → Resolve table names via Glue catalog → get S3 locations
  → Map S3 paths to BlobFS filesystem paths
  → Rewrite SQL: "mydb.mytable" → "read_parquet('/blobfs/s3/bucket/path/*.parquet')"
  → Execute via DuckDB (subprocess or embedded)
  → Store results in S3 (as CSV) → return QueryExecutionId
  → GetQueryResults reads from S3
```

#### Performance Comparison

| Metric | Subprocess | Embedded CGO | Real AWS Athena |
|---|---|---|---|
| Simple SELECT (100 rows) | ~80ms | ~5ms | ~2–5s |
| Scan 1M rows Parquet | ~200ms | ~150ms | ~5–10s |
| Scan 10M rows Parquet | ~1.2s | ~1.0s | ~15–30s |
| Build complexity | None | Needs C compiler + DuckDB headers | N/A |
| Docker image size | ~35MB | ~50MB | N/A |

---

## Phase 4: Full State Export / Import

**Goal:** Allow a developer to export the complete state of a full-mode JaisCloud instance (all control-plane metadata + all data-plane data + blob files) into a portable `tar.gz` archive, and import it on another machine to recreate the exact same environment. Lite mode returns `409 Conflict` for export/import — it has no persistent state.

### Use Cases

| Scenario | Command |
|---|---|
| **Share seeded dev environment with teammate** | `jaiscloud export -o my-env.tar.gz` → send file → `jaiscloud import --file my-env.tar.gz` |
| **Snapshot before destructive testing** | `jaiscloud export -o snapshot-before-migration.tar.gz` |
| **CI/CD: seed reproducible test data** | Commit tar.gz to repo → `jaiscloud import --file seed-data.tar.gz --yes` in CI setup |
| **Quick metadata backup (skip large blobs)** | `jaiscloud export --metadata-only` |
| **Preview import without applying changes** | `jaiscloud import --file backup.tar.gz --dry-run` |
| **Add resources without wiping existing state** | `jaiscloud import --file extra-resources.tar.gz --merge` |

### Archive Format

```
jaiscloud-export-aws-20260407T143022.tar.gz
  ├── manifest.json          # cloud, version, timestamp, resource counts, SHA-256 checksums
  ├── store-dump.sql         # pg_dump of all JaisCloud Postgres tables (all services)
  └── blobs/                 # S3 object bodies from BlobFS (LocalFSBlobStore)
        ├── my-bucket/
        │     ├── data/file1.parquet
        │     └── config.json
        └── logs-bucket/
              └── 2024/03/app.log
```

> Phase 2 services (RDS, ElastiCache) add `containers.json` and `container-data/` (pg_dump per RDS instance, Redis RDB per ElastiCache cluster) to the archive. Not applicable until Phase 2.

**manifest.json**:

```json
{
  "version": "1.0",
  "jaiscloud_version": "0.3.0",
  "cloud": "aws",
  "region": "us-east-1",
  "mode": "full",
  "exported_at": "2026-04-07T14:30:22Z",
  "resources": {
    "s3_buckets": 3,
    "s3_objects": 120,
    "dynamodb_tables": 5,
    "dynamodb_items": 2300,
    "sqs_queues": 4,
    "sqs_messages": 12,
    "lambda_functions": 2,
    "iam_roles": 6,
    "sns_topics": 3
  },
  "blob_size_bytes": 8388608,
  "checksums": {
    "store-dump.sql": "sha256:a1b2c3...",
    "blobs": "sha256:d4e5f6..."
  }
}
```

### What Gets Exported Per Service

| Service | Control Plane | Data Plane | Blobs |
|---|---|---|---|
| **S3** | Bucket definitions | Object metadata | Object bodies (BlobFS) |
| **DynamoDB** | Table schemas | All items (JSONB) | — |
| **SQS** | Queue definitions | Pending messages | — |
| **SNS** | Topics, subscriptions | — | — |
| **Lambda** | Function configs | — | Code ZIPs (BlobFS) |
| **IAM** | Roles, policies, users, access keys | — | — |

All Postgres tables (`jc_resources`, `jc_sqs_messages`, `jc_dynamodb_items`, `jc_s3_objects`, `jc_sqs_dedup`) are included in `store-dump.sql` via `pg_dump`. Blob files are written under `blobs/` mirroring the BlobFS directory structure.

### Import Modes

| Mode | Flag | Behaviour |
|---|---|---|
| **Replace** (default) | — | Wipe current state first, then restore. Equivalent to `reset` + import. |
| **Merge** | `--merge` | Add imported resources alongside existing ones. Fails on key conflicts. |
| **Dry run** | `--dry-run` | Print what would be imported, make no changes. |

### Version Compatibility

The manifest `jaiscloud_version` field is checked on import:
- Same major version → full compatibility, proceed
- Different minor version → warn, proceed (migrations handle schema changes)
- Different major version → reject with error (breaking schema change)

### Deliverables

| Component | Scope |
|---|---|
| **`LocalFSBlobStore` wiring** | Wire `LocalFSBlobStore` in `main.go` for full mode so blob files are on disk and exportable (currently `MemoryBlobStore` is used even in full mode) |
| **`pg_dump` / `pg_restore`** | Run `pg_dump` of the JaisCloud Postgres database into `store-dump.sql`; `pg_restore` on import |
| **BlobFS export** | Walk `LocalFSBlobStore` directory and stream files into `blobs/` in the archive |
| **BlobFS restore** | Extract `blobs/` from archive back into the BlobFS directory |
| **`manifest.json`** | Generate on export with resource counts and SHA-256 checksums; validate on import |
| **Admin HTTP upgrade** | Replace current JSON `GET /_jaiscloud/export` with `tar.gz` streaming response; replace JSON `POST /_jaiscloud/import` with multipart `tar.gz` upload. **Full mode only — 409 in lite mode.** |
| **`GET /_jaiscloud/state/summary`** | New endpoint returning resource counts per service (works in both modes) |
| **CLI `export` upgrade** | `jaiscloud export [-o file.tar.gz] [--metadata-only]` |
| **CLI `import` upgrade** | `jaiscloud import --file file.tar.gz [--merge] [--dry-run] [--yes]` |
| **Integration test** | Export → reset → import round-trip: all resources and data restored correctly |

---

## Phase 5: GCP API Layer

**Goal:** Map GCP APIs to the existing provider/store layer so GCP SDKs work without modification.

### Planned Deliverables

| Component | Scope |
|---|---|
| **GCP adapter** | REST path routing, OAuth token parsing, URL path version detection |
| **GCS codec** | `storage.googleapis.com` v1 — maps to existing `ObjectProvider` |
| **Compute codec** | `compute.googleapis.com` v1 — maps to new `ComputeProvider` |
| **Pub/Sub codec** | `pubsub.googleapis.com` v1 — maps to existing `QueueProvider` + `NotificationProvider` |
| **BigQuery codec** | `bigquery.googleapis.com` v2 — maps to existing `TableProvider` |
| **Cloud SQL codec** | `sqladmin.googleapis.com` v1beta4 — maps to new `RelationalProvider` |
| **Integration tests** | Google Cloud Go SDK test suite |

---

## Phase 6: Azure API Layer

**Goal:** Map Azure ARM and data-plane APIs to the existing provider/store layer.

### Planned Deliverables

| Component | Scope |
|---|---|
| **Azure adapter** | ARM path routing, Bearer/SAS token parsing, `api-version` query param detection |
| **ARM control plane** | Resource groups, subscriptions, ARM template deployment (basic) |
| **Blob Storage codec** | Azure Blob REST API — maps to existing `ObjectProvider` |
| **Service Bus codec** | Azure Service Bus REST — maps to existing `QueueProvider` |
| **Cosmos DB codec** | SQL API — maps to existing `TableProvider` |
| **Azure Functions codec** | HTTP trigger invoke — maps to existing `FunctionProvider` |
| **AKS codec** | Cluster CRUD — maps to new `ContainerProvider` |
| **Integration tests** | Azure SDK for Go test suite |

---

## Phase 7: Polish & Release

**Goal:** Production-ready packaging, compatibility testing, and public release.

### Planned Deliverables

| Component | Scope |
|---|---|
| **Terraform compatibility** | End-to-end testing with AWS, GCP, and Azure Terraform providers pointing at JaisCloud |
| **Helm chart** | K8s manifests and Helm chart for deploying JaisCloud in CI clusters |
| **kind auto-provisioning** | Auto-provision a local kind cluster for full mode (no manual K8s setup) |
| **Benchmarks** | Performance benchmarking suite — latency, throughput, memory per service |
| **Documentation** | Full API coverage docs, migration guides, SDK compatibility matrix |
| **CI/CD** | GitHub Actions pipeline — unit tests, integration tests, race detector, release binaries |
| **Binary releases** | Multi-platform releases (linux/amd64, linux/arm64, darwin/arm64, windows/amd64) |

---

## Post-Phase 7 (Future)

| Feature | Notes |
|---|---|
| **Web console UI** | `/_console/` SPA for browsing resources, viewing queues, triggering resets |
| **Plugin system** | Third-party resource providers via gRPC sidecars (not planned unless community demand) |
| **gRPC support** | GCP gRPC adapter for Pub/Sub and Spanner |
| **IAM enforcement** | Opt-in full IAM policy evaluation via `--iam-enforce` flag |
| **Lambda hot reload** | File watcher for Lambda code mounts in full mode |
| **Debugger attach** | Debug port mapping for Lambda functions |
| **Multi-cloud single instance** | Single instance serving AWS + GCP (currently requires separate instances per cloud) |
