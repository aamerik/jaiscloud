# JaisCloud for GCP

> **Early Development Notice**
> `jaiscloud-gcp` is under active development on the `gcp` branch and has not yet been released as a packaged binary (see the [main README](README.md), which currently lists GCP as "In pipeline"). Build it from source. Some operations may have incomplete implementations, behavioural differences from real GCP, or known bugs — see [Known Limitations](#known-limitations) below, and please [open a GitHub issue](https://github.com/jaisrajms/jaiscloud/issues) for anything not already listed there.

**JaisCloud — a free GCP emulator for developers and CI.** It implements real GCP wire protocols (both the REST/JSON APIs and the native gRPC APIs Firestore, Pub/Sub, Datastore, KMS, Secret Manager, Cloud Logging, and Cloud Monitoring actually use) — no SDK shims, no proxy rewrites. Point an official Google client library at it and it works.

**One binary per cloud.** `jaiscloud-gcp` is fully self-contained — no `--cloud` flag, no shared runtime with `jaiscloud-aws`. See the [main README](README.md) for the project-wide picture (AWS is the reference implementation; this document covers the GCP binary specifically).

---

## Supported GCP Services

| Service | Transport | Notes |
|---|---|---|
| Cloud Storage (GCS) | REST + gRPC v2 | Buckets, objects, resumable/multipart uploads, CMEK, CSEK |
| Cloud Pub/Sub | REST + gRPC | Topics, subscriptions, push/pull delivery, ordering keys, DLQ |
| Secret Manager | REST + gRPC | Secrets, versions, rotation, CMEK envelope encryption |
| Cloud KMS | REST + gRPC | Key rings, crypto keys/versions, symmetric + asymmetric, rotation |
| Cloud IAM | REST | Service accounts, service account keys |
| Cloud Firestore (Native mode) | REST + gRPC | Documents, transactions, queries, composite indexes, `Listen` streaming |
| Cloud Datastore mode | gRPC | Entities, queries, ID allocation — see [Known Limitations](#known-limitations) for transaction support |
| Cloud Functions (v1) | REST | Deploy, invoke (mock echo by default, Docker/K8s execution modes) |
| Cloud Workflows | REST | Workflow definitions + executions, real YAML expression engine |
| Cloud Dataproc | REST | Clusters + jobs, **real Spark execution** in Docker/K8s executor mode (same model as AWS EMR) |
| Dataproc Metastore | REST | Control-plane CRUD (services/backups/metadata-imports) — no Hive Thrift / Iceberg table-metadata plane, see [Known Limitations](#known-limitations) |
| BigLake Iceberg REST Catalog | REST | `org.apache.iceberg.rest.RESTCatalog` surface mounted at `/iceberg/` — namespaces, tables, atomic `CommitTableRequest` requirements/updates, see [Known Limitations](#known-limitations) |
| Managed Kafka | REST | Metadata-only clusters/topics — see [Known Limitations](#known-limitations) |
| BigQuery | REST | Metadata + stored rows — no SQL engine, see [Known Limitations](#known-limitations) |
| Cloud Monitoring | gRPC | Metrics, alert policies (evaluated), notification channels + incidents — see [Known Limitations](#known-limitations) |
| Cloud Logging | gRPC | Log entries, filtering, log-based routing |
| Eventarc | REST | Metadata-only triggers/channels + provider discovery — no event-delivery engine, see [Known Limitations](#known-limitations) |
| Cloud DNS | REST | Metadata-only managed zones + record sets/changes — no authoritative DNS server, see [Known Limitations](#known-limitations) |
| Memorystore for Redis | REST | Metadata-only instances + location discovery — no Redis data plane, see [Known Limitations](#known-limitations) |
| Cloud SQL Admin | REST | Metadata-only instances/databases/users — no SQL engine or data plane, see [Known Limitations](#known-limitations) |
| Compute Engine | REST | Metadata-only instances/disks/networks/firewalls/subnetworks — no VM, disk, or network data plane, see [Known Limitations](#known-limitations) |

---

## Quick Start

### 1. Build

```bash
git clone https://github.com/jaisrajms/jaiscloud.git && cd jaiscloud
go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/
# or: make build-gcp
```

### 2. Start

```bash
./jaiscloud-gcp start
# Listening on http://localhost:8080  (gRPC on :8081)
```

### 3. Connect

Point any official Google client library at the emulator. Most services use plain endpoint overrides; the gRPC-native services (Firestore, Pub/Sub, Datastore, Monitoring) use the real Google client libraries' own emulator-host environment variables where those exist.

```bash
export GCP_EMULATOR_ENDPOINT=http://localhost:8080/    # REST services (GCS, BigQuery, Dataproc, Workflows, ...)
export FIRESTORE_EMULATOR_HOST=localhost:8081           # Firestore (gRPC)
export STORAGE_EMULATOR_HOST=http://localhost:8080      # GCS REST client
```

---

## Connect your SDK

### Go — REST-based services (GCS, BigQuery, Dataproc, Workflows, Secret Manager REST, ...)

```go
import "google.golang.org/api/option"

opts := []option.ClientOption{
    option.WithEndpoint("http://localhost:8080/"),
    option.WithoutAuthentication(),
}
```

### Go — Firestore (gRPC, real emulator-host support)

```go
import (
    "cloud.google.com/go/firestore"
    "google.golang.org/api/option"
)

// export FIRESTORE_EMULATOR_HOST=localhost:8081
client, err := firestore.NewClient(ctx, projectID, option.WithoutAuthentication())
```

### Go — Cloud Datastore (gRPC, real emulator-host support)

```go
import "cloud.google.com/go/datastore"

// export DATASTORE_EMULATOR_HOST=localhost:8081
client, err := datastore.NewClient(ctx, projectID)
```

### Go — Cloud Monitoring (gRPC, manual endpoint — no official emulator-host var for this client)

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

conn, err := grpc.NewClient("localhost:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
```

---

## Configuration

The most common flags — all have an equivalent `JAISCLOUD_*` env var.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `JAISCLOUD_PORT` | `8080` | REST listen port |
| `--grpc-port` | — | `8081` | gRPC (h2c, plaintext) listen port |
| `--dsn` | `JAISCLOUD_DSN` | — | PostgreSQL DSN; when set all state is stored in PostgreSQL |
| `--ephemeral` | `JAISCLOUD_EPHEMERAL` | `false` | Disable all persistence — state is lost on exit (CI / unit tests) |
| `--data-dir` | `JAISCLOUD_DATA_DIR` | `~/.jaiscloud/jaiscloud-gcp` | Directory for state.json saves and named snapshots |
| — | `JAISCLOUD_GCP_PROJECT_ID` | — | Default GCP project when a request carries none |
| — | `JAISCLOUD_GCP_SERVICE_ACCOUNT` | — | Default service-account identity returned by the metadata emulator |
| `--gcp-metadata` | `JAISCLOUD_GCP_METADATA_ENABLED` | `false` | Enable the GCP metadata-server emulator |
| `--kms-master-key` | `JAISCLOUD_KMS_MASTER_KEY` | — | 32-byte hex KEK wrapping the KMS DEK at rest |
| `--log-level` | `JAISCLOUD_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--metrics` | — | `false` | Expose Prometheus metrics at `/metrics` |

Storage model (identical semantics to the AWS binary — see the [main README](README.md#configuration)): default is memory + periodic `state.json` saves; `--dsn` stores everything in PostgreSQL; `--ephemeral` is purely in-memory with no disk writes.

```bash
./jaiscloud-gcp start                                              # default: memory + periodic saves
./jaiscloud-gcp start --dsn "postgres://user:pass@localhost:5433/jaiscloud"
./jaiscloud-gcp start --ephemeral                                  # CI / unit tests
```

---

## CLI Reference

```bash
jaiscloud-gcp start                             # start the emulator
jaiscloud-gcp version                           # print version
jaiscloud-gcp env                               # print effective config as env vars
jaiscloud-gcp doctor                            # verify the emulator is reachable
jaiscloud-gcp reset                             # wipe all state
jaiscloud-gcp export -o snapshot.tar.gz         # save full state to a snapshot tarball
jaiscloud-gcp import -i snapshot.tar.gz         # restore state from a snapshot tarball
jaiscloud-gcp snapshot create --name <name>     # create a named on-disk snapshot
jaiscloud-gcp snapshot list                     # list all named snapshots
jaiscloud-gcp snapshot revert <name>            # revert to a named snapshot
jaiscloud-gcp snapshot delete <name> --yes      # delete a named snapshot
jaiscloud-gcp snapshot inspect <name>           # show snapshot metadata
```

## Admin API

Identical shared endpoints to the AWS binary (see the [main README](README.md#admin-api)) — `/_jaiscloud/health`, `/_jaiscloud/reset`, `/_jaiscloud/export`, `/_jaiscloud/import`, `/_jaiscloud/snapshot*`, `/_jaiscloud/clock`, `/metrics` — all served on the REST port (`8080` by default).

```bash
curl -X POST http://localhost:8080/_jaiscloud/reset
```

---

## Known Limitations

This section documents deliberate simplifications and known correctness edge cases — distinct from ordinary bugs, these are behaviours a developer relying on this emulator should know about up front.

### Firestore: optimistic-concurrency conflict detection can miss a race under a frozen clock

Firestore's `documents.patch` and transform-carrying `commit`/`batchWrite` writes are protected against concurrent lost updates via an optimistic-concurrency check: the document's `UpdateTime` at read time is captured and re-validated, atomically with applying the write, against the live document's current `UpdateTime` — a conflicting concurrent write causes the loser to receive `409 ABORTED` instead of silently overwriting the winner. This is correct and race-free under the real (or offset) clock, where `UpdateTime` values are effectively unique to nanosecond resolution.

**Edge case:** if the emulator's clock is *frozen* (`POST /_jaiscloud/clock` with `{"mode":"fixed",...}`, used for deterministic tests), two writes to the same document landing while the clock is frozen can be stamped with the *identical* `UpdateTime`. In that specific case, the conflict-detection check can fail to notice that a write happened in between — the version token didn't move, even though the document did. This is a testing-mode-only concern (frozen clocks aren't a realistic production condition — the real GCP backend's own `update_time` is generated by Spanner/TrueTime, not an application clock, so it has no equivalent failure mode). If your test suite freezes the clock *and* exercises concurrent writes to the same document in the same test, be aware the emulator's lost-update protection is not guaranteed in that combination.

We evaluated an alternative (a checksum of document content as the version token) and rejected it: a content hash reintroduces the classic ABA problem — a document that changes and then reverts to its original bytes produces the same hash as if nothing had happened, which would make conflict detection *less* reliable, not more, since it would miss real intervening writes whenever content happened to return to a prior state. We also considered a purely internal monotonic version counter decoupled from `UpdateTime`, but real GCP's `Precondition` proto only supports `exists`/`update_time` — no opaque version token — and client libraries read and pass forward the real `updateTime` value for their own `currentDocument.updateTime` preconditions. Introducing a second, different notion of "version" internally, even without changing the wire-facing `UpdateTime` field itself, was judged higher-risk than documenting the known edge case for the (real-world-rare) frozen-clock scenario.

### Datastore: no transaction support

The Datastore gRPC service is intentionally non-transactional. `BeginTransaction` returns an opaque id (so SDK init paths that poll the transaction surface don't error) and `Rollback` is a no-op, but a `Commit` carrying a transaction selector is rejected with `Unimplemented` rather than being silently applied as a non-transactional write. Non-transactional `Commit`, `Lookup`, and `RunQuery` are fully supported.

### BigQuery: metadata only, no SQL engine

`jobs.query` never evaluates SQL — it stores the query and reports `jobComplete: true` with empty results. `tabledata.insertAll` streams rows without validating them against the table schema and does not honor `insertId`-based deduplication, `skipInvalidRows`, `ignoreUnknownValues`, or `templateSuffix`. `tabledata.list` ignores `startIndex`. `projects.getServiceAccount` returns a synthetic `bq-{project}@gcp-sa-bigquery.iam.gserviceaccount.com` rather than a real service account.

### Managed Kafka: metadata only, no real broker

A cluster is a logical record only — the emulator never stands up a real Kafka broker. Consumer groups are not tracked: `ListConsumerGroups` always returns an empty list, and get/update/delete operations on a consumer group return `Unimplemented`.

### Cloud Monitoring: alert policies are evaluated for `condition_threshold`

Alert policies are evaluated by a background worker (30s tick, matching the AWS CloudWatch alarm evaluator). Only `condition_threshold` conditions are evaluated; `condition_absent`, `condition_matched_log`, `condition_monitoring_query_language`, `condition_prometheus_query_language`, and `condition_sql` are stored but never evaluated (and never fire). The evaluated filter subset is equality clauses joined by `AND` on `metric.type`, `resource.type`, `metric.label.{key}`, and `resource.label.{key}`; unknown clauses are ignored. The matching series' latest point in each alignment window is reduced across series (`REDUCE_SUM`/`MEAN`/`MAX`/`MIN`/`COUNT`, default mean), then compared to `threshold_value`; `duration` requires the comparison to hold across every alignment window it spans, and `trigger` count/percent is honored when no reducer is set. A firing policy opens an incident and delivers notifications; a below-threshold evaluation or no matching data closes its open incident and delivers a resolution notification. Incidents have no public API, so they are observable via the snapshot/export surface and logs. `pubsub` channels receive a CloudEvents-style JSON message on `labels.topic`; `email`/`webhook`/`sms` notifications are recorded on the incident and logged but not sent.

`GetMonitoredResourceDescriptor` and `CreateServiceTimeSeries` are `Unimplemented`. `ListMetricDescriptors`/`ListMonitoredResourceDescriptors` ignore the `filter` field. `DISTRIBUTION`-typed point values are rejected. `ListTimeSeries` supports only the `metric.type` / `resource.type` equality filter subset — the full Monitoring Query Language is not implemented. `NotificationChannelService` supports CRUD for `pubsub`/`email`/`webhook`/`sms` channels; `ListNotificationChannelDescriptors`, `GetNotificationChannelDescriptor`, and the verification-code RPCs are `Unimplemented`.

### Dataproc: `Reset` does not drain in-flight Spark job goroutines

`POST /_jaiscloud/reset` wipes the Dataproc store but does not cancel or wait for jobs currently executing (Docker/K8s executor mode). This matches AWS EMR's own `Reset` behaviour in this codebase, which is a no-op for the same reason — not a GCP-specific gap. If you reset while a job is mid-execution and then resubmit a job with the *same* `(project, region, jobId)` before the stale run finishes, the stale run's completion could overwrite the new job's state. Avoid reusing job IDs across a reset boundary while a prior run may still be in flight.

### Dataproc Metastore: control plane only, no table-metadata plane

Only the management plane is implemented (`Service` / `Backup` / `MetadataImport` CRUD with long-running operations). The actual table-metadata plane — the Hive Metastore **Thrift** server that Spark's Hive/Iceberg clients talk to on `endpoint_uri:9083` — is not implemented; `endpointUri` is a synthesized placeholder. `ExportMetadata`, `RestoreService`, `QueryMetadata`, `MoveTableToDatabase`, and `AlterMetadataResourceLocation` return `Unimplemented`. On the single host, `locations/{l}/operations/{id}` is path-identical to Cloud Workflows' LRO surface and therefore routes to Workflows — Metastore's own operations are returned inline (`done: true`), so no client needs to poll them.

### BigLake Iceberg REST Catalog: DB-backed, standard REST spec

The BigLake Iceberg REST Catalog is mounted at `/iceberg/` (Spark configures `uri=http://host:port/iceberg/`) and speaks the standard `org.apache.iceberg.rest.RESTCatalog` protocol. It models GCP's real BigLake Metastore managed-Iceberg product, whose catalog is the standard Apache Iceberg REST spec — Spark, Trino, and Flink attach over that spec. (Dataproc Metastore's own table plane is the Hive Metastore Thrift server, a separate product.) The catalog is database-backed (Polaris-style): it stores the `TableMetadata` JSON and a synthesized `metadata-location` pointer (`{location}/metadata/{version:05d}-{uuid}.metadata.json`) but never writes `metadata.json`/`version-hint.text` to object storage itself — the client's `FileIO` does that. `CommitTableRequest` requirements (`assert-table-uuid`, `assert-ref-snapshot-id`, schema/spec/sort-order assertions, …) and updates (`assign-uuid`, `add-schema`, `add-snapshot`, `set-properties`, …) are applied atomically, so concurrent commits cannot lose updates. `set-statistics`/`remove-statistics`/`remove-partition-statistics` are accepted as no-ops, and `GET /tables/{table}/metrics` returns `501 Not Implemented`.

### Eventarc: metadata only, no event-delivery engine

Eventarc is implemented as trigger-based metadata CRUD, **not** the AWS EventBridge "bus + rule + event-pattern + target" fan-out model (the two are different models despite the shared "event routing" purpose). A `Trigger` is a stored record with a `destination` (Cloud Run service, Workflows, HTTP endpoint, or GKE — accepted as metadata references since Cloud Run does not exist in the emulator), a `transport.pubsub.topic` source, and `eventFilters` (at least one of which must have `attribute: "type"`, as real Eventarc requires); a `Channel` is a 3rd-party-source registration record referencing a `provider`; and `Provider` resources are read-only discovery (a small catalogue of real providers — `pubsub.googleapis.com`, `storage.googleapis.com` — not invented ones). `destination.cloudFunction` is **output-only/read-only and rejected on create** (Cloud Functions v2 triggers are created through the Cloud Functions API, not by an Eventarc create call), so it is not offered as an accepted destination. No events are ever delivered: there is no Pub/Sub subscription, Cloud Run deployment, or Workflow execution created, and no event-matching engine. Source/destination references are validated structurally — a `transport.pubsub.topic` must name an existing Pub/Sub topic and a `destination.workflow` must name an existing Workflow (`NotFound` otherwise) — and a channel's `pubsubTopic`/`activationToken` are synthesized placeholders.

Trigger and Channel writes honor `updateMask` (masked paths take the incoming value, unmasked paths retain the stored value, merged inside the store's atomic mutate closure), support `validateOnly=true` (validate then skip the write), and enforce `etag` optimistic concurrency control: a deterministic content-checksum etag is recomputed on every create/update, and a stale etag on patch/delete is rejected with **409 `ABORTED`**. A trigger/channel `uid` is a UUID4, and a newly created unconnected Channel reports `state: PENDING` (it only becomes `ACTIVE` once a provider connects). Trigger/Channel IAM (`getIamPolicy`/`setIamPolicy`/`testIamPermissions`) is implemented via the shared policy store (etag OCC included), not `Unimplemented`.

Known debt for Eventarc: list `filter`/`orderBy` are **not** honored (`ListTriggers`/`ListChannels` ignore them and return the full page); output-only fields (`name`/`uid`/`etag`/times/`state`/`activationToken`/`pubsubTopic`) are overlaid on read but a client-supplied output-only field in a create/patch body is not rejected, it is echoed into the stored config; and unknown Eventarc v1 resources/custom methods (e.g. `ChannelConnection`, `GoogleApiSource`, `MessageBus`, `Pipeline`, `Enrollment`) are not routed, so they fall through to the adapter's generic 404 envelope rather than an Eventarc-specific `NOT_FOUND`.

### Cloud DNS: metadata only, no authoritative DNS server

Cloud DNS is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the AWS Route53 provider. `ManagedZone`s, `ResourceRecordSet`s, and `Change`s are stored records — the emulator never stands up an authoritative DNS server, so `nameServers` are synthesized `ns-cloud-*.googledomains.com.` placeholders and zones never resolve queries. `managedZones.{create,get,list,patch,update,delete}` and `resourceRecordSets.{create,get,list,patch,delete}` are supported, and `changes.create` applies its `additions`/`deletions` to the stored record sets synchronously and returns `status: "done"`. `projects.get` returns a synthesized `dns#project` with a `quota` block. The `id` of a managed zone and the project `number` are stable numeric strings derived from the resource name.

Known debt for Cloud DNS: DNSSEC (`dnsKeys`), `policies`/`responsePolicies`, and the managed-zone IAM custom methods (`:getIamPolicy`/`:setIamPolicy`/`:testIamPermissions`) are **not** implemented and fail loud with `501 UNIMPLEMENTED`. List pagination honors `maxResults`/`pageToken`; `managedZones.patch` and `managedZones.update` share merge semantics (description/visibility/labels are overlaid, unmasked fields retained, and `dnsName` is immutable/ignored). Only `name`, `dnsName`, `description`, `visibility`, and `labels` are persisted — the other managed-zone config blocks (`dnssecConfig`, `forwardingConfig`, `peeringConfig`, `privateVisibilityConfig`) are accepted but dropped, so no DNSSEC, forwarding, peering, or private-zone visibility behavior is applied.

### Memorystore for Redis: metadata only, no Redis data plane

Memorystore for Redis is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the Cloud DNS provider and keyed by project + location (region). The emulator never stands up a Redis server, so an instance is born in `state: READY` with a synthesized `host` (a stable `10.x.y.z` placeholder) and `port: 6379` that nothing listens on — there is no data plane, no `AUTH`, no persistence, and no failover. `instances.create` takes `?instanceId=`, defaults `tier` to `BASIC` (rejecting anything but `BASIC`/`STANDARD_HA` with `400`), defaults `memorySizeGb` to `1` and `redisVersion` to `REDIS_7_0`, and stores `displayName`, `labels`, `redisConfigs`, and the location. `instances.get/list/delete` and `instances.patch` (which applies `updateMask`; unsupported mask paths fail loud with `400`) are supported, and `instances.upgrade` applies the requested `redisVersion`. List pagination honors `pageSize`/`pageToken`, and `locations.get`/`locations.list` return synthesized `projects/{p}/locations/{l}` records.

Create/Update/Delete return the `google.longrunning.Operation` **inline** with `done: true` and the resulting instance in `response` (the Dataproc/Metastore convention): the shared `locations/{location}/operations/{id}` path is path-identical to Cloud Workflows' LRO surface on the single emulator host, so Memorystore does not claim it and operations are never persisted or pollable. Location discovery is the one bare `/v1/projects/{p}/locations[/{l}]` path claimed by Memorystore; no other emulator service serves it. Deferred surfaces — `instances.import`/`export`/`failover`/`rescheduleMaintenance`/`getAuthString`, backup collections, and the Redis Cluster surface — are not routed and fall through to the generic `404` rather than silently succeeding.

### Cloud SQL Admin: metadata only, no SQL engine

Cloud SQL Admin (`sqladmin.googleapis.com/sql/v1beta4`) is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the Cloud DNS/Memorystore providers and the AWS RDS provider, keyed by the project at `store.GlobalRegion`. The emulator never stands up a database server, so an instance is born in `state: RUNNABLE` with a synthesized `connectionName` (`{project}:{region}:{instance}`), `selfLink`, `serviceAccountEmailAddress`, `settings` defaults (`tier` `db-f1-micro`, `dataDiskSizeGb` `"10"`, `availabilityType` `ZONAL`, ...), and a cosmetic `PRIMARY` IP that nothing listens on — there is no SQL engine, no data plane, and no AuthN/AuthZ. `instances.{insert,get,list,update,patch,delete,restart}`, `databases.{insert,get,list,update,patch,delete}`, and `users.{insert,get,list,update,delete}` are supported, plus the synthesized `flags.list`, `tiers.list`, and `connectSettings.get` discovery surfaces.

Every mutation returns Cloud SQL's own `Operation` envelope (`kind: "sql#operation"`, with `operationType`, `targetId`, `targetLink`, `selfLink`, and `insertTime`) inline with `status: "DONE"` — **not** a `google.longrunning.Operation` — and the same operation is persisted so `operations.get`/`operations.list` read it back. List responses use Cloud SQL's envelopes (`sql#instancesList`, `sql#databasesList`, `sql#usersList`, `sql#operationsList`), and pagination honors `maxResults`/`pageToken`.

Known debt for Cloud SQL: the engine-less design means `Instances.Update` and `Instances.Patch` share merge semantics (body fields overlay the stored instance, `settings` sub-fields are deep-merged, and the server-owned `region`/`state`/`selfLink`/`ipAddresses` are immutable/ignored); a user's `password` is accepted but never stored or echoed; `settings` blocks other than the fields the emulator defaults are preserved verbatim without any behavioral effect; and deferred surfaces — `sslCerts`, `backupRuns`, `clone`, `failover`, `promoteReplica`, `import`/`export`, `demote`, `resetSslConfig`, `operations.cancel`, and the instance certificate custom methods (`instances:generateEphemeralCert`, ...) — fail loud with `501 UNIMPLEMENTED` rather than silently succeeding. The `v1beta4` path shape is recognized by the shared GCP identity extractor (`/v{N}beta{M}/projects/{project}`).

### Compute Engine: metadata only, no VM/disk/network data plane

Compute Engine (`compute.googleapis.com/compute/v1`) is implemented as metadata CRUD over the shared `ResourceStore`, mirroring the Cloud DNS/Memorystore/Cloud SQL providers and the AWS EC2 provider. Records are keyed by the project (AccountID) and the request scope — the zone for zonal resources, the region for regional resources, and `store.GlobalRegion` for global resources — so instances and disks with the same name in different zones never collide. The emulator never boots a VM or attaches a real disk: an instance is born in `status: RUNNING` with synthesized ids, `selfLink`s, `creationTimestamp`s, a cosmetic `nic0` interface that nothing listens on, and a stable `cpuPlatform`; `start`/`stop`/`reset` only flip the stored status (`RUNNING`/`TERMINATED`). There is no data plane and no AuthN/AuthZ.

Supported surface: zonal `instances.{insert,get,list,delete,start,stop,reset}` plus `instances.aggregatedList`; zonal `disks.{insert,get,list,delete}`; global `networks.{insert,get,list,delete}` and `firewalls.{insert,get,list,delete}`; regional `subnetworks.{insert,get,list,delete}`; synthesized `machineTypes.get/list` (zonal), `zones.get/list`, and `regions.get/list`; and zonal/regional/global `operations.get/list`. Every mutation returns Compute Engine's own `Operation` envelope (`kind: "compute#operation"`, with `operationType`, `targetId`, `targetLink`, `selfLink`, `insertTime`, and `progress: 100`) inline with `status: "DONE"` — **not** a `google.longrunning.Operation` — and the same operation is persisted at its scope so `operations.get`/`operations.list` read it back. List responses use the `compute#*List` envelopes, and pagination honors `maxResults`/`pageToken`.

Known debt for Compute Engine: the data-plane fields are accepted and preserved but have no effect (attached disks are stored verbatim, no boot disk is provisioned, `metadata`/`tags`/`labels` are cosmetic); `id`/`targetId` are stable numeric strings derived from the resource name (the real API assigns server-side integers); the default network is only a synthesized name, not a seeded resource. Everything outside the supported surface — addresses, images, snapshots, instance templates and groups, routers, backend services, `instances.setMetadata`/`attachDisk` and the other custom methods, `operations.wait`, and `aggregatedList` for resource types other than instances — resolves to `501 UNIMPLEMENTED` rather than a `404`. The `compute/v1` path shape is recognized by the shared GCP service detector by its `/compute/v1/` prefix, and the client `BasePath` must include that prefix.

---

## Contributing

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) and [CLAUDE.md](CLAUDE.md) for build setup, architecture conventions, and the AWS-vs-GCP isolation model. Please open an issue before starting large changes.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
