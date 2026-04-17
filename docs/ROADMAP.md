# JaisCloud — Roadmap

## Overview

JaisCloud is a local multi-cloud emulator that speaks native AWS, GCP, and Azure wire protocols so any SDK can point at it without modification. Development is structured in phases — 0 through 7.

> **Multi-cloud note:** GCP (`--cloud gcp`) and Azure (`--cloud azure`) adapters are stubs that return 501 for all requests until Phases 5 and 6 respectively. The `--cloud` flags are intentionally kept in the binary as a forward-compatible API, but `jaiscloud doctor` and the startup log clearly report which services are active vs. stub. Do not expose GCP/Azure as supported in documentation until Phase 5/6 work lands.

| Phase | Name | Status | Timeline |
|---|---|---|---|
| 0 | SQS Proof of Concept | ✅ Complete | Weeks 1–2 |
| 1 | AWS Core Services | ✅ Complete | Weeks 3–5 |
| 2 | AWS Extended Services | ✅ Complete | Weeks 7–10 |
| 2.5 | P0 Gap Services (KMS, SecretsManager, SSM, API Gateway, Lambda real exec, CloudFormation) | ✅ Complete | Weeks 11–13 |
| 3 | AWS Athena (DuckDB) + Terraform compat + Docker image | Planned | Weeks 14–16 |
| 4 | Full State Export / Import | Planned | Weeks 17–18 |
| 5 | GCP API Layer | Planned | Weeks 19–22 |
| 6 | Azure API Layer | Planned | Weeks 23–26 |
| 7 | Polish & Release | Planned | Weeks 27–28 |

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
| EC2, VPC, Route53, RDS, ElastiCache, ECS | ✅ Delivered in Phase 2 |
| EMR on EC2 (classic EMR) | ✅ Delivered in Phase 2 |
| Glue Data Catalog / Iceberg | ✅ Delivered in Phase 2 |
| CloudFormation / IaC engine | ✅ Delivered in Phase 2 |
| DynamoDB Streams | ✅ Delivered in Phase 2 |
| IAM condition keys, resource-level permissions | ✅ Delivered in Phase 2 |
| Athena / DuckDB | Phase 3 |
| GCP services | Phase 5 |
| Azure services | Phase 6 |
| Web console UI | Post-Phase 7 |

---

## Phase 2: AWS Extended Services ✅

**Goal:** Complete AWS surface area — compute, networking, managed databases, containers, and analytics foundation.

### Services

| Service | Wire Protocol | Operations |
|---|---|---|
| **EC2 + VPC** | Query/XML | RunInstances, DescribeInstances, StartInstances, StopInstances, TerminateInstances, DescribeImages, CreateSecurityGroup, DescribeSecurityGroups, AuthorizeSecurityGroupIngress, AuthorizeSecurityGroupEgress, RevokeSecurityGroupIngress, DeleteSecurityGroup, CreateKeyPair, DescribeKeyPairs, DeleteKeyPair, ImportKeyPair, CreateVpc, DescribeVpcs, DeleteVpc, CreateSubnet, DescribeSubnets, DeleteSubnet, CreateRouteTable, DescribeRouteTables, CreateRoute, AssociateRouteTable, CreateInternetGateway, DescribeInternetGateways, AttachInternetGateway, CreateNatGateway, DescribeNatGateways, AllocateAddress, DescribeAddresses |
| **Route53** | REST/XML | CreateHostedZone, GetHostedZone, ListHostedZones, DeleteHostedZone, ChangeResourceRecordSets, ListResourceRecordSets, CreateHealthCheck, GetHealthCheck, ListHealthChecks, DeleteHealthCheck |
| **RDS** | Query/XML | CreateDBInstance, DescribeDBInstances, ModifyDBInstance, DeleteDBInstance, CreateDBCluster, DescribeDBClusters, ModifyDBCluster, DeleteDBCluster, CreateDBSubnetGroup, DescribeDBSubnetGroups, DeleteDBSubnetGroup |
| **ElastiCache** | Query/XML | CreateCacheCluster, DescribeCacheClusters, ModifyCacheCluster, DeleteCacheCluster, CreateReplicationGroup, DescribeReplicationGroups, ModifyReplicationGroup, DeleteReplicationGroup |
| **ECS** | JSON/Target | CreateCluster, DescribeClusters, ListClusters, DeleteCluster, RegisterTaskDefinition, DescribeTaskDefinition, ListTaskDefinitions, DeregisterTaskDefinition, CreateService, UpdateService, DescribeServices, ListServices, DeleteService, RunTask, DescribeTasks, ListTasks, StopTask |
| **EMR on EC2** | JSON/Target | RunJobFlow, DescribeCluster, ListClusters, TerminateJobFlows, ModifyCluster, SetTerminationProtection, SetVisibleToAllUsers, AddJobFlowSteps, DescribeStep, ListSteps, CancelSteps, AddInstanceFleet, ListInstanceFleets, ModifyInstanceFleet, AddInstanceGroups, ListInstanceGroups, ModifyInstanceGroups, ListBootstrapActions, AddTags, RemoveTags, GetBlockPublicAccessConfiguration, PutBlockPublicAccessConfiguration, PutManagedScalingPolicy, GetManagedScalingPolicy, RemoveManagedScalingPolicy |
| **EMR on EKS** | REST/JSON | CreateVirtualCluster, DescribeVirtualCluster, DeleteVirtualCluster, ListVirtualClusters, StartJobRun, DescribeJobRun, CancelJobRun, ListJobRuns, CreateManagedEndpoint, DescribeManagedEndpoint, DeleteManagedEndpoint, ListManagedEndpoints, TagResource, UntagResource, ListTagsForResource |
| **Glue Data Catalog** | JSON/Target | CreateDatabase, GetDatabase, GetDatabases, DeleteDatabase, CreateTable, GetTable, GetTables, UpdateTable, DeleteTable, CreatePartition, GetPartition, GetPartitions, BatchCreatePartition, BatchDeletePartition, BatchUpdatePartition |
| **CloudFormation** | Query/XML | CreateStack, UpdateStack, DeleteStack, DescribeStacks, ListStacks, DescribeStackResources, ValidateTemplate |
| **DynamoDB Streams** | JSON/Target | ListStreams, DescribeStream, GetShardIterator, GetRecords |
| **EventBridge** | JSON/Target | PutRule, DeleteRule, DescribeRule, ListRules, EnableRule, DisableRule, PutTargets, RemoveTargets, ListTargetsByRule, PutEvents (delivers matched events to SQS targets; integrates with EMR/EMR-on-EKS state-change events) |
| **IAM Advanced** | Query/XML | GetFederationToken (STS), CreateGroup, GetGroup, DeleteGroup, ListGroups, AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser, AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies, PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies, TagUser, UntagUser, ListUserTags, UpdateUser, UpdateAccessKey, CreateInstanceProfile, GetInstanceProfile, DeleteInstanceProfile, AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile, ListInstanceProfiles, SimulatePrincipalPolicy, SimulateCustomPolicy |

### Infrastructure Deliverables

| Component | Deliverable |
|---|---|
| **Stream store** | `MemoryStreamStore` — per-table DynamoDB Streams ring buffer with shard iterator and sequence number tracking |
| **EC2 Codec** | Query/XML with multi-value filter flattening (`Filter.N.Name` / `Filter.N.Value.M`) |
| **Route53 Codec** | REST/XML with path-based action detection, XML body parsing, and change-set encoding |
| **RDS/ElastiCache/CF Codecs** | Generic `flattenQueryValues` Query/XML extractor (replaces IAM-specific allowlist) |
| **IAM Codec fix** | Replaced hardcoded param allowlist with generic extractor; fixed all list XML encoders to output fields directly inside `<member>` (no spurious wrapper elements) |
| **Glue/ECS/EMR/Streams Codecs** | JSON/Target codecs registered under their respective `X-Amz-Target` prefixes |
| **Service registry** | `internal/adapter/aws/services.go` — `ServiceDescriptor` + `awsServices` as single source of truth for all service metadata. Eliminates hardcoded X-Amz-Target strings, SigV4 allow-list, Action validators in `router.go`, and `serviceToProvider` switch in `server.go`. Adding a service requires one entry in `awsServices`. |
| **ARN formatter map** | `awsARNFormatters` map in `config.go` replaces the ARN `switch` statement; `AWSResourceID` is a three-line function that never needs changing. |
| **`CloudAdapter.ServiceToProvider`** | New method on the `CloudAdapter` interface. AWS delegates to `serviceProviderMap` (derived from `awsServices`); Azure/GCP return service name unchanged. Gateway calls the adapter instead of its own switch. |
| **Tests** | Integration tests for all Phase 2 services/feature areas — all passing, `go test -race` clean |

### Glue / Iceberg Implementation

The Glue catalog stores the `metadata_location` pointer (`s3://bucket/iceberg/metadata/v3.metadata.json`). Iceberg clients (Spark, Trino, PyIceberg) read this pointer, then read/write metadata and data files via S3. On commit, Glue atomically swaps the pointer to the new metadata version.

- **Lite mode:** `sync.Mutex`-protected CAS on `metadata_location` in the table entry
- **Operations:** CreateDatabase, GetDatabase, GetDatabases, DeleteDatabase, CreateTable, GetTable, GetTables, UpdateTable (Iceberg atomic CAS), DeleteTable, CreatePartition, GetPartition, GetPartitions, BatchCreatePartition, BatchDeletePartition, BatchUpdatePartition

### DynamoDB Streams Implementation

Streams are integrated into `TableProvider` rather than a standalone provider — they share table state directly.

- `MemoryStreamStore` holds a per-table ring buffer of change records (INSERT / MODIFY / REMOVE)
- `PutItem`, `UpdateItem`, `DeleteItem` capture old/new images and call `appendStreamRecord`
- `UpdateTable` enables/disables a stream via `StreamSpecification`
- Shard iterators are base64-encoded `"tableName:sequenceNumber"` tokens

**Exit criteria:** All integration tests for all Phase 2 services pass; full suite (`go test -race ./tests/integration/`) clean.

---

## Phase 2.5: P0 Gap Services ✅

**Goal:** Add the services that block real-world AWS application testing but have low implementation cost. These must ship before Athena because they are cross-cutting dependencies for Lambda, ECS, and CloudFormation workflows.

**Why before Phase 3:** KMS/SecretsManager/SSM are required by most Terraform modules that provision Lambda + RDS. Without them, `terraform apply` against JaisCloud fails on resource creation, making Terraform compat testing (Phase 3) impossible to validate.

### Services Delivered

| Service | Key Operations | Status |
|---|---|---|
| **KMS** | CreateKey, DescribeKey, ListKeys, DescribeKey, Encrypt, Decrypt, ReEncrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext, CreateAlias, DeleteAlias, ListAliases, EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion, EnableKeyRotation, DisableKeyRotation, GetKeyRotationStatus, CreateGrant, RetireGrant, RevokeGrant, ListGrants, TagResource, UntagResource, ListResourceTags | ✅ |
| **Secrets Manager** | CreateSecret, DescribeSecret, GetSecretValue, PutSecretValue, UpdateSecret, DeleteSecret, RestoreSecret, ListSecrets, RotateSecret, TagResource, UntagResource, ListSecretVersionIds | ✅ |
| **SSM Parameter Store** | PutParameter, GetParameter, GetParameters, GetParametersByPath, GetParameterHistory, DeleteParameter, DeleteParameters, LabelParameterVersion, AddTagsToResource, ListTagsForResource | ✅ |
| **API Gateway (REST)** | CreateRestApi, GetRestApi, GetRestApis, UpdateRestApi, DeleteRestApi, GetResources, GetResource, CreateResource, DeleteResource, PutMethod, GetMethod, DeleteMethod, PutIntegration, GetIntegration, DeleteIntegration, PutMethodResponse, PutIntegrationResponse, CreateDeployment, GetDeployments, DeleteDeployment, CreateStage, GetStage, GetStages, UpdateStage, DeleteStage | ✅ |
| **API Gateway execute-api** | InvokeApi (AWS_PROXY→Lambda, MOCK, HTTP_PROXY integrations) | ✅ |
| **Lambda real execution** | DockerExecutor (warm pool per function), K8sExecutor (batch/v1 Job per invocation); `JAISCLOUD_LAMBDA_MODE=docker\|k8s` | ✅ |
| **CloudFormation** | CreateStack, UpdateStack, DeleteStack, DescribeStacks, ListStacks, DescribeStackResources, ValidateTemplate, GetTemplate; intrinsics (Ref, Fn::GetAtt, Fn::Sub, Fn::Join, Fn::If, Fn::Select, Fn::Split, Fn::FindInMap, Fn::Base64, Fn::Not, Fn::And, Fn::Or, Fn::Equals, Fn::Length); topological sort (DependsOn + implicit Ref/GetAtt); real resource dispatch to 9 provider types | ✅ |

### Infrastructure Delivered

| Component | Scope |
|---|---|
| **`jc_kms_keys`, `jc_kms_aliases`, `jc_kms_master_key`** | KMS key store with DEK/KEK envelope encryption |
| **`jc_secrets`, `jc_secret_versions`** | SecretsManager with AES-GCM encryption at rest via KMS |
| **`jc_parameters`, `jc_parameter_history`** | SSM Parameter Store with SecureString encryption via KMS |
| **API Gateway provider** | Full management plane (`internal/provider/apigw/`) + execute-api invoke plane |
| **Lambda Docker executor** | Warm container pool, runtime image mapping, GC goroutine, `JAISCLOUD_LAMBDA_MODE=docker` |
| **Lambda K8s executor** | One-shot `batch/v1 Job` per invocation, TTL cleanup, `JAISCLOUD_LAMBDA_MODE=k8s` |
| **CloudFormation intrinsics engine** | `internal/provider/stack/intrinsics.go` — full CloudFormation intrinsic function resolver |
| **Topological sort** | `internal/provider/stack/topsort.go` — Kahn's algorithm with DependsOn + implicit Ref/GetAtt dependency extraction and cycle detection |
| **CFN resource dispatch** | `internal/provider/stack/dispatch.go` + `registerCFNHandlers` in `main.go` — wires 9 resource types (SQS Queue, SNS Topic, S3 Bucket, DynamoDB Table, IAM Role, Lambda Function, SSM Parameter, SecretsManager Secret, KMS Key) to real providers |
| **Integration tests** | Full test coverage for KMS, SecretsManager, SSM, APIGateway, CloudFormation |

### Exit Criteria

All exit criteria met:

- ✅ KMS, SecretsManager, SSM, API Gateway, CloudFormation integration tests pass.
- ✅ Lambda Docker and K8s executor modes operational.
- ✅ CloudFormation stacks with intrinsics (Ref, Fn::GetAtt, Fn::Sub, DependsOn) provision real resources via underlying providers.
- ✅ Full integration test suite (`go test -race ./tests/integration/`) green.

---

## Phase 3: AWS Athena (DuckDB) + Terraform Compat + Docker Image

**Goal:** Emulate Athena using DuckDB as the query engine. Depends on Glue (Phase 2) and S3/BlobFS (Phase 1). This phase also ships the two highest-leverage adoption unlockers: Terraform provider compatibility testing and a published Docker image.

### Planned Deliverables

| Component | Scope |
|---|---|
| **AthenaProvider** | StartQueryExecution, GetQueryExecution, GetQueryResults, StopQueryExecution |
| **SQL rewriter** | Resolves Glue table names → S3 paths → BlobFS filesystem paths; rewrites to DuckDB `read_parquet(...)` |
| **Subprocess mode** (default) | DuckDB CLI invoked as child process — pure Go binary, ~50ms overhead per query |
| **Embedded CGO mode** (optional) | DuckDB linked via CGO — ~1–5ms overhead, requires C compiler at build time |
| **Build targets** | `make build` (CGO_ENABLED=0, subprocess) and `make build-cgo` (CGO_ENABLED=1, embedded) |
| **Docker image** | `docker run jaiscloud/jaiscloud` one-liner; published to Docker Hub on each release; includes `lite` and `full` variants |
| **Terraform compat suite** | Run AWS Terraform provider against JaisCloud; target: S3, DynamoDB, IAM, Lambda, SQS, SNS, KMS, SecretsManager, SSM; fix any wire-protocol edge cases surfaced (presigned URLs, error shapes, pagination token formats) |
| **SDK compatibility matrix** | Document tested SDK versions: `aws-sdk-go-v2`, `boto3`, `aws-sdk-js-v3`; publish as `docs/SDK_COMPAT.md` |

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
| **`LocalFSBlobStore` wiring** | ⚠ Already implemented — must be wired in Phase 2.5. Listed here for full-mode export completeness. |
| **`pg_dump` / `pg_restore`** | Run `pg_dump` of the JaisCloud Postgres database into `store-dump.sql`; `pg_restore` on import |
| **BlobFS export** | Walk `LocalFSBlobStore` directory and stream files into `blobs/` in the archive |
| **BlobFS restore** | Extract `blobs/` from archive back into the BlobFS directory |
| **`manifest.json`** | Generate on export with resource counts, SHA-256 checksums, and `streams_excluded` flag if DynamoDB Streams state was omitted |
| **`--strip-kek` export** | Decrypt DEK before archiving so import does not require KEK; patched row uses VERSION=0x00 plaintext layout |
| **`--export-key <hex>` flag** | AES-GCM wrap the `jc_kms_master_key` row in the archive; safe to store in S3 or git; requires `--export-key` on import |
| **Admin HTTP upgrade** | Replace current JSON `GET /_jaiscloud/export` with `tar.gz` streaming response; replace JSON `POST /_jaiscloud/import` with multipart `tar.gz` upload |
| **Lite-mode JSON snapshot export** | `jaiscloud export` in lite mode produces a JSON snapshot of all in-memory stores via the existing `Snapshotter` interface (no blobs); returns 200, not 409 |
| **`GET /_jaiscloud/state/summary`** | New endpoint returning resource counts per service (works in both modes) |
| **`MemoryStreamStore` snapshot** | Implement `Snapshot`/`Restore` on `MemoryStreamStore`; register with `adminHandler`; or emit `streams_excluded=true` in manifest if deferred |
| **CLI `export` upgrade** | `jaiscloud export [-o file.tar.gz] [--metadata-only] [--strip-kek] [--export-key <hex>]` |
| **CLI `import` upgrade** | `jaiscloud import --file file.tar.gz [--merge] [--dry-run] [--yes] [--export-key <hex>]` |
| **Integration test** | Full-mode: export → reset → import round-trip. Lite-mode: JSON snapshot export → reset → import. Cross-instance: strip-kek export → import on fresh instance → rotate-master-key. |

---

## Phase 5: GCP API Layer

**Goal:** Map GCP APIs to the existing provider/store layer so GCP SDKs work without modification.

**Pre-requisite (must complete in Phase 2.5):** Move `awsARNFormatters` out of `config.go` into `aws/adapter.go` and add `FormatResourceID` to the `CloudAdapter` interface. GCP adapter will implement its own resource path formatters. Without this, providers calling `nr.ResourceID(...)` with `cloud=gcp` silently produce bare IDs. See ARCHITECTURE_P0_EXPANSION.md section 12.1.

### Planned Deliverables

| Component | Scope |
|---|---|
| **GCP adapter** | REST path routing, OAuth token parsing, URL path version detection; implements `FormatResourceID` with GCP resource path format |
| **GCS codec** | `storage.googleapis.com` v1 — maps to existing `ObjectProvider` |
| **Compute codec** | `compute.googleapis.com` v1 — maps to new `ComputeProvider` |
| **Pub/Sub codec** | `pubsub.googleapis.com` v1 — maps to existing `QueueProvider` + `NotificationProvider` |
| **BigQuery codec** | `bigquery.googleapis.com` v2 — maps to existing `TableProvider` |
| **Cloud SQL codec** | `sqladmin.googleapis.com` v1beta4 — maps to new `RelationalProvider` |
| **Two-instance compose example** | `docker-compose.yml` running AWS instance (:4566) + GCP instance (:4567) for multi-cloud app testing |
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
