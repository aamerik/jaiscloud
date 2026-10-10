# SDK tour — official-client compliance for jaiscloud-gcp

One realistic workflow, implemented with the **official Google Cloud client
SDKs in Go, Python, Java and Node**, all run against a local `jaiscloud-gcp`,
so the emulator is exercised through the same code paths a real application
uses. It is a demo / compliance artifact — not a parity-ledger item and not a
conformance gate — and it deliberately surfaces SDK-native behavior that the
REST/IaC tests do not: resumable uploads, streaming pull/listen/tail, publish
batching, pagination iterators, gax long-running-operation pollers, retries and
ADC/emulator-host wiring.

`run.sh` runs all four languages and prints a cross-language PASS/FAIL matrix;
`make demo-sdk-tour` is the same thing.

## Run

```bash
# From the repo root:
make demo-sdk-tour                 # or: demo/sdk-tour/run.sh
```

`run.sh` ensures the toolchains, builds the emulator, starts a **local
ephemeral** `jaiscloud-gcp` (`--port 8080 --grpc-port 8081 --ephemeral`), runs
each language, aggregates `results/<run-id>/` and exits non-zero on any FAIL or
cross-language divergence. The ephemeral emulator needs no k3d/Docker and is
CI-reproducible.

To run against an existing emulator (e.g. a k3d deployment via a port-forward):

```bash
SDK_TOUR_EMULATOR=k3d EMULATOR_REST=http://localhost:8080 EMULATOR_GRPC=localhost:8081 \
  demo/sdk-tour/run.sh
```

Toolchains/caches install under `/tmp/opencode` (Go, Node LTS tarball, JDK 21,
Maven repo); per-language build output (`node_modules/`, `.venv/`, `target/`,
`results/`) is gitignored. Client versions are pinned: Go `go.mod`/`go.sum`,
Python `python/requirements.txt`, Java `pom.xml` (single `libraries-bom`), Node
`package-lock.json`.

## Wiring: pointing each official client at the emulator

The emulator is plaintext (REST :8080, gRPC :8081). Only some libraries have an
emulator hook, so each language uses the recipe below (all verified against this
emulator):

| Service | Go | Python | Java | Node |
| --- | --- | --- | --- | --- |
| Storage | `STORAGE_EMULATOR_HOST` env (REST) | `STORAGE_EMULATOR_HOST` env | `StorageOptions.setHost(rest)` + `NoCredentials` | `new Storage({apiEndpoint})` (see note) |
| Pub/Sub | `PUBSUB_EMULATOR_HOST` env | `PUBSUB_EMULATOR_HOST` env | explicit plaintext channel + `NoCredentials` | `PUBSUB_EMULATOR_HOST` env |
| Firestore | `FIRESTORE_EMULATOR_HOST` env | `FIRESTORE_EMULATOR_HOST` env | `setHost(grpc)` + plaintext channel + no creds | `FIRESTORE_EMULATOR_HOST` env |
| Logging | gRPC client + endpoint/insecure/no-auth | generated transport over `grpc.insecure_channel` | generated `LoggingClient` + plaintext channel | generated client + `apiEndpoint` host + `port` + insecure `sslCreds` |
| Secret Manager | gRPC client + endpoint/insecure/no-auth | generated transport over insecure channel | generated client + plaintext channel | generated client + host/port + insecure `sslCreds` |
| KMS | gRPC client + endpoint/insecure/no-auth | generated transport over insecure channel | generated client + plaintext channel | generated client + host/port + insecure `sslCreds` |
| BigQuery | REST apiary `WithEndpoint` + no auth | `api_endpoint` + `AnonymousCredentials` | `BigQueryOptions.setHost(rest)` + `NoCredentials` | `BIGQUERY_EMULATOR_HOST` env |
| IAM | Pub/Sub topic `IAM()` handle | `PublisherClient.get/set_iam_policy` | `TopicAdminClient.get/setIamPolicy` | `topic.iam.get/setPolicy` |
| Dataproc (LRO) | gRPC client + insecure + gax `op.Wait` | generated transport + `op.result()` | plaintext channel + `OperationFuture.get()` | host/port + insecure `sslCreds` + `op.promise()` |

**Node Storage note.** `@google-cloud/storage` uses `STORAGE_EMULATOR_HOST` as
the *whole* JSON base URL (`baseUrl = EMULATOR_HOST`) but appends
`/upload/storage/v1` to `apiEndpoint` for resumable uploads, so no single env
value satisfies both against a server that serves only `/storage/v1/...`.
Passing `apiEndpoint` instead makes the JSON base `apiEndpoint + "/storage/v1"`
and the resumable URL `apiEndpoint + "/upload/storage/v1"` — both of which
jaiscloud serves.

## Scenarios

Same 18 scenarios in every language; each prints `LANG service.op OK|FAIL|SKIP`.

| Scenario | SDK-native behavior exercised |
| --- | --- |
| `storage.create_bucket` | bucket create + attrs |
| `storage.resumable_upload` | 4 MiB object over the **resumable/chunked** protocol (256 KiB chunks) |
| `storage.stream_download_checksum` | chunked `Read`/read stream, sha256 |
| `storage.list_pagination` | page-size-2 iterator over 7 objects |
| `pubsub.batch_publish` | publisher **batching** (32 messages) |
| `pubsub.streaming_pull_ack` | high-level **streaming pull** subscriber + ack |
| `firestore.listen_write` | gRPC server-streaming **Listen** receives a write |
| `firestore.transaction` | read-modify-write **transaction** |
| `firestore.query_pagination` | `orderBy`+`limit` cursor pagination |
| `logging.write_entry` | `WriteLogEntries` + `ListLogEntries` |
| `logging.tail` | bidirectional **`TailLogEntries`** sees a new entry |
| `secretmanager.add_access_list` | add version, access, list versions |
| `kms.encrypt_decrypt` | symmetric encrypt/decrypt round-trip |
| `kms.asymmetric_sign` | `AsymmetricSign` verified with `GetPublicKey` |
| `bigquery.insertall_query` | dataset/table, `insertAll`, query |
| `bigquery.load_job` | `gs://` load job to `DONE` |
| `lro.dataproc_cluster` | Dataproc create polled by the SDK's gax **LRO poller** |
| `iam.policy_read_modify_write` | get/set IAM policy on a topic |

## Status model and agreement

* `OK` — expected to work and did.
* `SKIP` — a pre-declared unsupported surface; a documented gap, not a failure.
* `FAIL` — an expected-OK scenario that failed; the run goes red.

`aggregate.py` also asserts **cross-language agreement**: the same scenario must
produce the same normalized observable (`sha256=…`, `size=…`, `total=…`, …) in
every language. Any divergence, any FAIL, or a language that did not run turns
the matrix red.

## Findings

While building this, the official clients exposed two genuine Firestore gaps
(fixed in `internal/gcp`; see the PR):

1. **`BatchGetDocuments` ignored `new_transaction`.** Firestore returns the new
   transaction id on the first streamed response; the Node SDK's
   `runTransaction` begins transactions lazily through a read and fails with
   "Transaction ID was missing from server response". Fixed for gRPC and the
   REST `documents.batchGet` provider.
2. **`RunQuery`'s terminal `done` frame omitted `read_time`.** Real Firestore
   always sets `read_time`; without it the Node SDK throws "No QuerySnapshot
   result" on an empty result page (e.g. the last pagination page). Fixed for
   the gRPC `RunQuery`.

No other SDK/emulator surface in the 18 scenarios needed a fix; any future
failure is captured with its request/response and classified as emulator bug,
wiring gap, or unimplemented surface rather than papered over.

## Error / retry / idempotency tour (`SDK_TOUR_MODE=errors`)

The happy-path tour above asserts the clients can drive every surface; it cannot
see how they behave when a request *fails*. The errors mode runs the same four
official clients through the failure surfaces — error-code mapping, retry
classification, backoff/`Retry-After` honoring, idempotency and resumable-upload
rewind — and prints a second cross-language matrix.

```bash
make demo-sdk-tour-errors          # or: demo/sdk-tour/run-errors.sh
```

`run-errors.sh` starts a local ephemeral emulator with `--metrics` and runs two
phases:

* **Phase A (env).** The emulator is started with the throttle configured
  through the environment (`JAISCLOUD_GCP_THROTTLE=fault`,
  `JAISCLOUD_GCP_THROTTLE_SERVICES=storage`,
  `JAISCLOUD_GCP_THROTTLE_FAIL_FIRST=1`,
  `JAISCLOUD_GCP_THROTTLE_STATUS=429`,
  `JAISCLOUD_GCP_THROTTLE_RETRY_DELAY=1s`) and a raw probe asserts the first
  matching request is refused with `429` + `Retry-After: 1` +
  `google.rpc.RetryInfo{retryDelay:"1s"}`.
* **Phase B (runtime).** The emulator restarts clean; each leg arms the injector
  per scenario through `POST /_jaiscloud/throttle` and clears it afterwards, so
  one process covers every throttle configuration without further restarts. The
  runtime control plane reaches the same injector the env path does.

Attempt counts are **observed, not assumed**: every leg scrapes the emulator's
Prometheus counter (`jaiscloud_requests_total`, hence `--metrics`) before and
after the operation and reports the delta. The matrix asserts those deltas and
the final observable agree across the four SDKs.

### Scenarios (`errors.*`)

| Scenario | Injected / expected | Observable |
| --- | --- | --- |
| `storage_already_exists` | duplicate bucket create → 409 | `already_exists=yes` |
| `storage_not_found` | missing object → 404 | `not_found=yes` |
| `invalid_argument` | malformed Standard SQL → 400 | `invalid_argument=yes` |
| `iam_failed_precondition` | stale bucket IAM etag → 409 | `failed_precondition=yes` |
| `no_retry_on_4xx` | 404 is not retried | `no_retry_attempts=1` |
| `retry_429_storage_get` | first storage request refused 429 | `retry_429_attempts=2` |
| `retry_503_storage_get` | first storage request refused 503 | `retry_503_attempts=2` |
| `retry_info_shape` | raw 429 envelope | `RetryInfo:1s:Retry-After=1` |
| `pagination_stability` | 429 on the first list request | `pagination_total=5` |
| `idempotent_insertall` | replayed `insertId` | `idempotent_rows=1` |
| `resumable_rewind` | 429 on the first chunk PUT | `resumable_sha256=…` |
| `pubsub_topic_already_exists` | duplicate topic create (gRPC) | `grpc_already_exists=yes` |

Each scenario asserts client-observable facts (attempt count, final code,
`Retry-After`/`RetryInfo`, checksum, row count); a scenario that cannot be wired
in a language is reported `MISSING`/`FAIL` with its raw error and a
classification, never silently skipped.

### Findings

* **Retry classification matches.** All four SDKs treat the injected `429` and
  `503` as retryable and the `404`/`400` surfaces as terminal — `errors.*` shows
  exactly two attempts for the transient cases and one for the 4xx case.
  Backoff honors the advertised `Retry-After`/`RetryInfo`.
* **Error mapping matches.** Storage 409/404, BigQuery 400 INVALID_ARGUMENT and
  Pub/Sub IAM 409 ABORTED surface with the same status and machine-readable
  code in every language; only human-readable prose differs.
* **Resumable rewind works.** A `429` scoped to the chunk PUT
  (`services=["storage/objectsinsertresumable"]`, so the session start is
  untouched) is re-sent by every SDK and the object still verifies by sha256.
* **Idempotency.** Replaying a BigQuery row with the same `insertId` leaves one
  stored row in every language. The recorded real-GCP golden
  (`tests/gcpdifferential/testdata/golden-tour-errors`) shows real GCP dedups
  silently while the emulator also reports an `insertErrors[].duplicate`; the
  scenario therefore asserts the *stored row count*, not the duplicate error.
* **Real-GCP golden diff.** `make test-gcp-differential-errors` replays the
  recorded error bodies and reports the divergences; see the differential
  README for the open (minor) items.

## Layout

```
demo/sdk-tour/
  run.sh          # toolchains -> ephemeral emulator -> 4 languages -> matrix
  aggregate.py    # cross-language matrix + agreement check
  run-errors.sh   # error/retry phases -> 4 languages -> error matrix
  aggregate_errors.py # cross-language error matrix + agreement check
  go/             # official cloud.google.com/go/* clients (separate module)
  python/         # official google-cloud-* clients (requirements.txt)
  java/           # official com.google.cloud clients (Maven, libraries-bom)
  node/           # official @google-cloud/* clients (package-lock.json)
  results/        # per-run JSONL (gitignored)
  results-errors/ # per-run error JSONL (gitignored)
```
