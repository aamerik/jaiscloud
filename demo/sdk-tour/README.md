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

## Layout

```
demo/sdk-tour/
  run.sh          # toolchains -> ephemeral emulator -> 4 languages -> matrix
  aggregate.py    # cross-language matrix + agreement check
  go/             # official cloud.google.com/go/* clients (separate module)
  python/         # official google-cloud-* clients (requirements.txt)
  java/           # official com.google.cloud clients (Maven, libraries-bom)
  node/           # official @google-cloud/* clients (package-lock.json)
  results/        # per-run JSONL (gitignored)
```
