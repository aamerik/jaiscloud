# GCP local-testability contract

This is the **local-testability contract** for `jaiscloud-gcp`. It states, per GCP
service, **what a local run may be trusted to prove** and **what must be verified
against real GCP**. An **AWS → GCP mapping** is included for reference.

It is a companion to, not a replacement for:

- [`docs/GA.md`](GA.md) — the GA contract: what `ga`/`limited`/`preview`/`unsupported`
  mean, stability, CI gates, client compatibility.
- [`README-GCP.md`](../README-GCP.md) — the service overview and the authoritative
  [Known Limitations](../README-GCP.md#known-limitations) list.
- [`docs/fidelity/fidelity-matrix.md`](fidelity/fidelity-matrix.md) — the **source of
  truth** for per-operation state and reasons. This document derives every number from
  it; where they disagree, the matrix wins.

---

## 1. Purpose & scope

**Scope:** what `jaiscloud-gcp` can and cannot prove locally when used as a dev/CI
target.

**What the emulator can prove locally**

- An official Google client (REST service client or native gRPC client) pointed at
  `jaiscloud-gcp` sees the same request/response *shapes*, status codes, error
  envelopes, field names, and long-running-operation envelopes as the real API, within
  the surface declared by the fidelity matrix.
- Control-plane / IaC / metadata workflows round-trip and persist.
- For a **Green, data-plane** service, the data-plane operations and most semantics are
  exercised locally and gated in CI against captured real-GCP responses.

**What it cannot prove locally**

- That a client behaves correctly against the *real backend*. The emulator is not
  real GCP: several services are metadata-only, some long-running operations finish
  synchronously, authorization is not enforced, and there is no quota/throttling plane.
- Anything behind a `preview` service (no engine ships locally), or the data plane of a
  metadata-only service.

A local green run is evidence about the **wire contract**, not about real GCP
behaviour. Treat this document as the checklist for the gap.

---

## 2. Tier legend

Tiers classify each service from its fidelity-matrix cells, with two documented
overrides (`functions` → Yellow, `kms` → Green) so the tier reflects local **trust**
rather than a raw `ga` ratio.

| Tier | Matrix basis | Locally trustworthy? | What it means for local testing |
| --- | --- | --- | --- |
| 🟢 **Green** | `ga` cells, no `preview` | **Yes** — for data-plane services; shape only for metadata services | Build the GCP adapter and unit/CI-test it against the emulator. |
| 🟡 **Yellow** | only `limited` cells (or a large `limited` share) | **Shape only** | Control-plane/IaC metadata works; data-plane and authorization behaviour is not modelled. Gate on a real-GCP smoke test. |
| 🔴 **Red** | any `preview` cell | **No** | No engine/logic behind it (e.g. BigQuery SQL). Local green would be meaningless. |

> `ga` means *supported and wire-conformant*, not *behaves like real GCP*. Read §5
> (behavioural depth) alongside the colour — a Green service can still be metadata-only.

---

## 3. Coverage snapshot

Read from [`docs/fidelity/fidelity-matrix.md`](fidelity/fidelity-matrix.md) at the time
of writing:

| Layer | Cells | `ga` | `limited` | `preview` | `unsupported` | `ga` share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Overall** | 716 | 498 | 100 | 37 | 81 | 70% |
| **gRPC** (official clients) | 328 | 245 | 11 | 0 | 72 | 75% |
| **REST** (Discovery-backed) | 388 | 253 | 89 | 37 | 9 | 65% |

- gRPC-only services (no REST transport): **Firestore Admin, Operations (long-running)**.
- gRPC split = 245 `ga` + 11 `limited` + 72 `unsupported` = 328. REST split = 253 + 89 + 37 + 9 = 388.
  Overall = 328 + 388 = 716.

**How to refresh.** The matrix is generated, not hand-edited. Run
`make gen-gcp-fidelity-matrix`, then re-read
[`docs/fidelity/fidelity-matrix.md`](fidelity/fidelity-matrix.md) (or the canonical
[`fidelity-matrix.json`](fidelity/fidelity-matrix.json)) and update the numbers here.
`make check-gcp-fidelity-matrix` fails CI if the committed matrix has drifted.

---

## 4. Per-service contract

`ga`/total is taken directly from the matrix. "Depth" is the behavioural-depth class
from §5. "Locally trustworthy?" answers the local-trust question, not the matrix state.

| Service | Transport(s) | `ga`/total | Tier | Depth | Locally trustworthy? | Note |
| --- | --- | ---: | --- | --- | --- | --- |
| `pubsub` | grpc, rest | 45/45 | 🟢 | Full | Yes | Topics, subscriptions, snapshots, seek, ordering, DLQ; subscription `retryPolicy` (`minimumBackoff`/`maximumBackoff`, 0–600s) is validated and round-tripped on `subscriptions.create`/`patch` over REST + gRPC, but has no effect on the emulator's fixed delivery backoff. |
| `storage` | grpc, rest | 52/52 | 🟢 | Full | Yes | Bucket/object/IAM/resumable; server-streaming `ReadObject` + bidirectional `BidiReadObject`. |
| `kms` | grpc, rest | 56/60 | 🟢 | Full | Yes | Symmetric/asym/MAC/raw + delete/import-job; 4 hard crypto leftovers. |
| `secretmanager` | grpc, rest | 30/32 | 🟢 | Full | Yes | Rotation schedule tracked; managed rotation needs Cloud SQL. |
| `firestore` | grpc, rest | 33/33 | 🟢 | Full | Yes | Full incl. `Listen`/`Write`; pipeline is a read-only subset. |
| `firestoreadmin` | grpc | 4/32 | 🟢 | Shape only | Shape only | Composite-index CRUD only; Databases/Backups/UserCreds/Schedules/Fields/Export/Import are `unsupported` stubs. |
| `datastore` | grpc, rest | 16/16 | 🟢 | Full | Yes | Transactions are single entity-group with a read-set. |
| `monitoring` | grpc, rest | 48/48 | 🟢 | Full | Yes | Metrics/alerts/channels; `condition_threshold` + `condition_absent` are evaluated. |
| `logging` | grpc, rest | 10/11 | 🟢 | Full | Yes | Write/List/Delete; `TailLogEntries` is a bounded poll. |
| `iam` | grpc, rest | 16/16 | 🟢 | Shape only | Shape only | Service accounts + policy: authz is **not enforced**. |
| `eventarc` | grpc, rest | 30/57 | 🟢 | Shape only | Shape only | Trigger/channel/provider CRUD over one core (gRPC + REST); a Pub/Sub-sourced trigger whose `destination.cloudFunction` names an existing function delivers matching events to it and provisions a backing Pub/Sub subscription (`transport.pubsub.subscription`, output-only) as its dead-letter surface (the other destinations are metadata only). The other 27 Eventarc RPCs are `unsupported` stubs. |
| `managedkafka` | grpc, rest | 34/44 | 🟢 | Shape only | Shape only | Metadata only; no real broker. Consumer-group `list` returns an empty set and `get`/`update`/`delete` report `NOT_FOUND`. |
| `metastore` | grpc, rest | 26/38 | 🟢 | Shape only | Shape only | Control plane only; the Hive Thrift serving plane (:9083) is a separate surface (a single global catalog, not per-service) that serves databases/tables/partitions/locks, the client-connect identity RPC (`set_ugi`), and the request-struct table reads (`get_table_req`/`get_table_objects_by_name_req`) Hive 2.3+/3.x clients use. An off-the-shelf Go HMS client drives it end-to-end (with Postgres restart) in `tests/persistent_mode/gcp/hms/`. The 5 deferred RPCs (export/restore/query/move/alter) are `unsupported` stubs by decision (admin/DR operations off the Spark job path, with no AWS Glue analogue); the shared `operations` path routes to workflows, so `operations.get`/`list` are `limited`. |
| `dataproc` | grpc, rest | 28/30 | 🟢 | Shape only | Shape only | Cluster/job metadata + real Spark on Docker/K8s executors; `DiagnoseCluster` is an unsupported stub. |
| `operations` | grpc | 5/5 | 🟢 | Shape only | Shape only | Synchronous operation stub. |
| `serviceusage` | grpc, rest | 10/11 | 🟢 | Shape only | Shape only | Accept-and-succeed enable/disable; no real API gating. `BatchGetServices` is an unsupported stub. |
| `resourcemanager` | grpc, rest | 8/15 | 🟢 | Shape only | Shape only | v1 REST + v3 gRPC project surfaces over one core: project lookup + project IAM (etag OCC); the 7 project lifecycle/lookup gRPC RPCs are unsupported stubs; authz not enforced. |
| `workflows` | grpc, rest | 11/12 | 🟢 | Shape only | Shape only | Workflow definitions + executions; LROs complete synchronously. `ListWorkflowRevisions` is an unsupported stub. |
| `workflowexecutions` | grpc, rest | 8/8 | 🟢 | Shape only | Shape only | Executions are synchronous. |
| `functions` | grpc, rest | 38/38 | 🟡 | Shape only | Shape only | Metadata CRUD + mock/docker call over REST and gRPC; v2 runtime catalog (`/v2/.../runtimes`, gRPC `ListRuntimes`) served; common-API `GetLocation`/`CancelOperation`/`DeleteOperation`/`WaitOperation` served; function mutations persist a pollable `google.longrunning` operation store (memory + Postgres + snapshot), so `operations.get`/`list` and REST `:wait` return the typed response; GCS-referenced source archives (`sourceArchiveUrl` / `storageSource`) are fetched, persisted with a revision hash, and executed under Docker/K8s; v2 `generateUploadUrl` provisions a GCS-backed upload target so `gcloud functions deploy --gen2` runs end-to-end, and each deploy bumps a persisted revision counter so a deployed function renders `serviceConfig.revision` (the backing Cloud Run service revision) + `allTrafficOnLatestRevision` (no real container build); the v2 1st→2nd gen upgrade/traffic control plane (`setupFunctionUpgradeConfig`, `redirect`/`rollbackFunctionUpgradeTraffic`, `commitFunctionUpgrade`/`commitFunctionUpgradeAsGen2`, `abortFunctionUpgrade`, `detachFunction`) is served over REST with a persisted `upgradeInfo` state machine — REST-only in practice (documented as `FunctionService` RPCs, but absent from the public `googleapis` proto and all generated clients); the synthesized HTTPS trigger URL (`{location}-{project}.cloudfunctions.net/{id}`) is served (Host-scoped raw-body invocation, HTTP 500 on an executor error/timeout, 404 for an unknown or event-only function); event triggers deliver Pub/Sub publishes, GCS object finalize/delete, and matching Eventarc `cloudFunction` routes through the same executor, retry per `failurePolicy.retry`/`retryPolicy`, and persist a delivery record (`delivered`/`failed`/`dead_letter`); v2 `serviceConfig` instance/concurrency settings (`minInstanceCount`, `maxInstanceCount`, `maxInstanceRequestConcurrency`, `availableCpu`) are stored, range-validated against the real API (including the Cloud Run sub-1-vCPU → concurrency-1 rule), and surfaced; a configured `maxInstanceCount` (× `maxInstanceRequestConcurrency`) and a project-wide account cap are enforced by an invocation admission gate that returns HTTP 429 `RESOURCE_EXHAUSTED` when the capacity is exceeded, while no separate project quota/account-settings API is modelled (FD11). A Pub/Sub or Cloud Storage event trigger materializes a backing Eventarc trigger (`eventTrigger.trigger`, output-only) whose platform-provisioned `transport.pubsub.subscription` is user-configurable with `deadLetterPolicy` over `subscriptions.patch` (a Cloud Storage trigger's transport topic is auto-provisioned by the platform, mirroring real Eventarc); exhausted deliveries are republished to the dead-letter topic with the `CloudPubSubDeadLetterSource*` attributes. The v2 `ListRuntimes` filter evaluates the AIP-160 subset `=`/`!=`/`:`(contains)/`<`/`<=`/`>`/`>=` with `AND`/`OR`/`NOT` and parentheses over `name`/`displayName`/`stage`/`environment` (AIP-160 function calls are rejected with `InvalidArgument`). |
| `compute` | rest | 0/33 | 🟡 | Metadata only | Metadata only | No VM/disk/network data plane. |
| `cloudsql` | rest | 0/24 | 🟡 | Metadata only | Metadata only | No SQL engine or data plane. |
| `clouddns` | rest | 0/16 | 🟡 | Metadata only | Metadata only | No authoritative DNS server. |
| `memorystore` | rest | 0/8 | 🟡 | Metadata only | Metadata only | No Redis data plane. |
| `bigquery` | rest | 0/23 | 🔴 | None | No | No SQL engine; `jobs.query` evaluates nothing. |
| `iceberg` | rest | 0/14 | 🔴 | None | No | BigLake Iceberg REST catalog; `preview`. |

Tier groups: **Green (19)** `dataproc`, `datastore`, `eventarc`, `firestore`,
`firestoreadmin`, `iam`, `kms`, `logging`, `managedkafka`, `metastore`, `monitoring`,
`operations`, `pubsub`, `resourcemanager`, `secretmanager`, `serviceusage`, `storage`,
`workflowexecutions`, `workflows`. **Yellow (5)** `clouddns`,
`cloudsql`, `compute`, `functions`, `memorystore`. **Red (2)** `bigquery`, `iceberg`.

---

## 5. Behavioural depth — the honesty check

`ga` ≠ "behaves like real GCP". These four classes tell you how much real behaviour sits
behind a wire-conformant API.

| Depth | Services | What you can actually rely on locally |
| --- | --- | --- |
| **Full** | `pubsub`, `storage`, `kms`, `secretmanager`, `firestore`, `datastore`, `monitoring`, `logging` | Data-plane operations and most semantics, gated against captured real-GCP responses. |
| **Shape only** (wire-conformant, thin behaviour) | `iam` (authz not enforced), `resourcemanager` (v1 REST + v3 gRPC over one core; projects synthesized; authz not enforced; IAM policy is metadata), `serviceusage` (no real API gating), `eventarc` (Cloud Functions-only delivery), `firestoreadmin` (composite-index CRUD only), `managedkafka` (no broker), `metastore` (control plane shape only; the Hive Thrift plane serves databases/tables/partitions/locks plus Hive-3.x `get_table_meta`/`alter_table_with_cascade`, the identity RPC `set_ugi`, and the request-struct reads `get_table_req`/`get_table_objects_by_name_req`; niche partition methods unsupported), `operations` (LROs synchronous), `workflows` (LROs synchronous), `workflowexecutions` (LROs synchronous), `dataproc` (no real cluster locally unless an executor is wired), `functions` (no real container build; single revision) | Control-plane shape and metadata. Real behaviour must be tested on real GCP. |
| **Metadata only** | `compute`, `cloudsql`, `clouddns`, `memorystore` | Resource records + `get`/`list`; nothing actually runs. |
| **None (preview)** | `bigquery` (no SQL engine), `iceberg` | Nothing local counts as evidence. |

> **Rule of thumb:** build against **Full** services locally; treat **Shape only**,
> **Metadata only**, and **None** as "the API shape is right, the behaviour is not
> proven" and gate those on a real-GCP smoke test.

---

## 6. AWS → GCP mapping

| AWS (today) | GCP target | Emulator tier | Local trust |
| --- | --- | --- | --- |
| SQS | **Pub/Sub** | 🟢 `ga` (45/45) | High — full surface. |
| S3 | **Cloud Storage** | 🟢 `ga` (52/52) | High — full read surface (`ReadObject` + `BidiReadObject`). |
| DynamoDB | **Firestore** / Datastore | 🟢 `ga` (Firestore 33/33, Firestore Admin 4/32, Datastore 16/16) | High — watch transaction/OCC caveats ([Known Limitations](../README-GCP.md#known-limitations)). |
| Lambda | **Cloud Functions** | 🟡 `limited` (38/38) | Control plane + GCS-referenced source execution (Docker/K8s); v2 `gcloud functions deploy --gen2` end-to-end (upload → create → poll); per-deploy revisions (Cloud Run shape) and the v2 1st→2nd gen upgrade/traffic control plane over REST; event triggers deliver Pub/Sub / GCS / Eventarc events with retry + dead-letter records (a Pub/Sub or GCS trigger's platform-provisioned backing subscription carries a user-configurable `deadLetterPolicy`). |
| KMS | **Cloud KMS** | 🟢 `ga` (56/60) | High; 4 hard crypto leftovers (`ImportCryptoKeyVersion`, trusted-key wraps, `Decapsulate`). |
| Secrets Manager | **Secret Manager** | 🟢 `ga` (30/32) | High; managed rotation needs Cloud SQL. |
| IAM | **Cloud IAM** | 🟢 `ga` (16/16) | Shape only — authz not enforced. |
| CloudWatch Logs / Metrics | **Cloud Logging / Monitoring** | 🟢 `ga` (Logging 31/54, Monitoring 48/48) | High; `TailLogEntries` is a bounded poll, sink routing is evaluated but not delivered, and the logging bucket/view/link/CMEK gRPC RPCs are `unsupported` stubs; `condition_threshold` + `condition_absent` evaluated. |
| EventBridge | **Eventarc** | 🟢 `ga` (30/57) | Trigger CRUD; Pub/Sub-sourced `cloudFunction` triggers deliver to their function with a backing subscription dead-letter surface. |
| Step Functions | **Workflows / Workflow Executions** | 🟢 `ga` (Workflows 11/12, Executions 8/8) | LROs complete synchronously. |
| Athena / Redshift | **BigQuery** | 🔴 `preview` (0/23) | **None** — real GCP required. |
| RDS | **Cloud SQL** | 🟡 `limited` (0/24) | Metadata only. |
| EC2 | **Compute Engine** | 🟡 `limited` (0/33) | Metadata only. |
| ElastiCache | **Memorystore** | 🟡 `limited` (0/8) | Metadata only. |
| Route 53 | **Cloud DNS** | 🟡 `limited` (0/16) | Metadata only. |

*(The last five rows — BigQuery, Cloud SQL, Compute, Memorystore, Cloud DNS — are the
ones where AWS parity cannot be validated locally at all; budget real-GCP testing for
them up front.)*

---

## 7. Must test on real GCP

The local emulator deliberately does not model the following. A local green run is **not**
evidence for any of them.

- [ ] **Authz / IAM enforcement.** Permissions are not checked; Cloud IAM is shape-only
      across all services. Any permission-sensitive path must be smoke-tested on real GCP.
- [ ] **Async long-running-operation (LRO) timing.** The emulator completes operations
      synchronously (`operations`, `workflows`, `workflowexecutions`, `functions`,
      `kms`, `cloudsql`, `compute`). Code that assumes immediate readiness will pass
      locally and may fail against real, eventually-consistent GCP.
- [ ] **Metadata-only services.** `compute`, `cloudsql`, `clouddns`, `memorystore` have
      no control/data plane locally — only resource records. Test the real data plane.
- [ ] **BigQuery.** No SQL engine ships locally; `jobs.query` evaluates nothing. All
      query behaviour must be tested against real BigQuery.
- [ ] **Cloud Functions v2 build.** v2 request/response *shapes* are served, runtime resolution
      works (`/v2/.../runtimes`), `gcloud functions deploy --gen2` runs end-to-end
      (`generateUploadUrl` → GCS-backed upload → create → poll), and source referenced by a GCS
      object (`sourceArchiveUrl` / `storageSource`) is fetched and executed in Docker/K8s mode.
      There is **no real container build** (the archive is only checked non-empty); each deploy
      bumps a persisted revision counter so revision names change, and the v2 1st→2nd gen
      upgrade/traffic control plane (setup/redirect/rollback/commit/abort/detach) is modelled over
      REST, but build failures and a real Gen2 build pipeline must be exercised on real GCP.
- [ ] **Quotas, throttling, and retry/backoff.** No general rate-limit or quota plane is
      modelled, so most backoff and quota-exhaustion paths are never exercised locally. Cloud
      Functions instance/concurrency configuration (`minInstanceCount`/`maxInstanceCount`/
      `maxInstanceRequestConcurrency`/`availableCpu`) is stored, range-validated, and surfaced,
      and a configured `maxInstanceCount` (× `maxInstanceRequestConcurrency`) plus a
      project-wide account cap is enforced by an invocation admission gate that returns HTTP
      429 `RESOURCE_EXHAUSTED` with no available instance. The project quota/account-settings
      API itself is still not modelled (FD11): Cloud Functions declares no account/quota method
      (real GCP exposes quotas only through the separate Cloud Quotas / Service Usage APIs), so
      Lambda's `GetAccountSettings` has no Cloud Functions analogue and is deliberately not faked.
- [ ] **Frozen-clock OCC / TTL.** With the clock frozen (`POST /_jaiscloud/clock`),
      Datastore optimistic-concurrency conflict detection and DynamoDB-style TTL edge cases can
      behave differently. (Firestore's `UpdateTime` OCC token is kept strictly monotonic per
      document even under a frozen clock.) Do not rely on frozen-clock results as production
      evidence.
- [ ] **Per-language SDK wire paths.** Different official SDKs exercise different wire
      paths. Add each client SDK/language in use to the conformance matrix rather
      than assuming one client's pass transfers.

Additional service-specific caveats live in
[`README-GCP.md` → Known Limitations](../README-GCP.md#known-limitations); read the
section for every service you touch.

---

## 8. Do not depend on emulator-only behaviour

Production code paths must never rely on affordances that exist only in the emulator.
These are fixed by policy, not by the fidelity matrix:

- **`/_jaiscloud/*` admin endpoints** — `health`, `doctor`, `reset`, `export`, `import`,
  `snapshot*`, `clock`, `ttl-sweep`, `eb-tick`, and `/metrics` are local control surfaces,
  not GCP APIs. Never call them from application code.
- **Reset / state wipe** — `POST /_jaiscloud/reset` and the `reset` CLI command exist for
  test isolation only; production code must not assume resettable state.
- **Lenient validation** — the emulator accepts and ignores more than real GCP (shallow
  create validation, dropped unmasked/unknown fields, synthesized placeholders). Do not
  encode emulator acceptance as a correctness assumption.
- **Absent authz** — requests succeed without credentials/permissions locally. Treat every
  IAM decision as unverified until tested on real GCP.
- **Frozen clock** — deterministic-time mode is a test affordance. Business logic must not
  depend on a frozen/offset clock.

A useful guard is a lint/test that rejects references to `/_jaiscloud/` and clock control
in production packages.
