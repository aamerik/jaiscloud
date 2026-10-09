# GCP differential harness (record → replay → diff)

An offline-first differential (record/replay) conformance harness for the
jaiscloud GCP emulator. It records the responses of a curated set of requests
from **real GCP** into committed goldens, then replays the *same* requests
against the emulator and diffs the normalized responses. It is not the spec
(AGENTS.md source-of-truth order applies); it is a regression gate and a
divergence finder.

Everything here is behind the `gcp_differential` build tag and uses the
`gcpdifferential` package.

## Layout

| Directory | Set | Record target | Replay target |
| --- | --- | --- | --- |
| `testdata/golden/` | curated REST (`Scenarios`) | real GCP REST | emulator REST `:8080` |
| `testdata/golden-grpc/` | curated gRPC (`GRPCScenarios`) | real GCP gRPC | emulator gRPC `:8081` |
| `testdata/golden-tour/` | SDK-tour REST (`TourScenarios`) | real GCP REST | emulator REST `:8080` |
| `testdata/golden-tour-grpc/` | SDK-tour gRPC (`TourGRPCScenarios`) | real GCP gRPC | emulator gRPC `:8081` |

Each golden is one `Exchange` (`{index, service, op, method, path, transport,
status, request, response}`); `manifest.json` records a content hash per file.
Matching a golden to a scenario is by `(Service, Op)`, never by position, so the
scenario list may grow ahead of the next recording ("pending recording" is
logged and skipped; a golden with no scenario is a fatal orphan).

Reports are written per set under `testdata/report*/report.{json,md}`: one row
per divergence with the divergent field, the real-GCP value, the emulator value
and (for accepted divergences) the triage rule's reason.

## Make targets

```bash
make test-gcp-differential        # ephemeral emulator; replays every set (+tour)
make test-gcp-differential-tour   # ephemeral emulator; tour sets only
make record-gcp-differential      # curated REST   from real GCP (needs ADC)
make record-gcp-differential-grpc # curated gRPC   from real GCP (needs ADC)
make record-gcp-differential-tour # SDK-tour (REST + gRPC) from real GCP (needs ADC)
```

Record mode needs Application Default Credentials (`gcloud auth
application-default login`) and a project with the relevant APIs enabled
(`parity-diff-jaiscloud` by default; override with `GCP_DIFFERENTIAL_PROJECT` /
`GCP_DIFFERENTIAL_PROJECT_NUMBER`). No token is ever printed or written by this
package. Recordings clean up every created resource (KMS keys excepted: GCP
cannot delete them, so fixed reusable names are used).

## SDK-tour differential (demo/sdk-tour)

The SDK tour (`demo/sdk-tour`) runs one workflow through the official
Go/Python/Java/Node clients and asserts the emulator answers them. This set
turns that "does it work" into "does it match real GCP": it records the exact
operations the **official Go tour issues** and replays them, so the wire behavior
the SDKs depend on (resumable uploads, streaming/pagination, SDK-native request
shapes) is diffed field by field.

### Recorded REST operations (`testdata/golden-tour/`)

| Op | What it covers |
| --- | --- |
| `tour_bucket_create` | bucket create |
| `tour_resumable_start` / `tour_resumable_chunk` / `tour_resumable_finalize` | resumable (chunked) upload: session start → non-final 256 KiB chunk (308) → finalizing chunk (200 + object) |
| `tour_object_metadata` | created object's crc32c/md5/size |
| `tour_media_upload` / `tour_media_download` | small `alt=media` download round-trip |
| `tour_page_upload_00..06` / `tour_list_page_1..4` | `objects.list` pagination (`maxResults=2` over 7 objects → 2+2+2+1) |
| `tour_bq_dataset_create` / `tour_bq_table_create` / `tour_bq_insertall` / `tour_bq_query` | BigQuery `insertAll` + Standard SQL query |
| `tour_bq_load_object_upload` / `tour_bq_load_table_create` / `tour_bq_load_insert` / `tour_bq_load_poll` / `tour_bq_load_tabledata` | `gs://` load job to `DONE` + row read |

Chunk request bodies are 256 KiB binary and are deliberately **not** captured
(`NoRequestCapture`); the chunk status and the finalize response carry the
parity. The resumable session URI arrives in the `Location` response header and
is captured path-only (`SaveHeader`), so record (real GCP) and replay (emulator)
address the same session through their own origin.

### Recorded gRPC operations (`testdata/golden-tour-grpc/`)

| Op | What it covers |
| --- | --- |
| `tour_kms_encrypt` / `tour_kms_decrypt` | symmetric KMS round-trip |
| `tour_kms_asym_get_public_key` / `tour_kms_asym_sign` | asymmetric sign + public key (fixed reusable sign key) |
| `tour_secret_create` / `tour_secret_add_version` / `tour_secret_access` / `tour_secret_list_versions` | Secret Manager add/access/list |
| `tour_fs_tx_seed` / `tour_fs_tx_commit` | Firestore read-modify-write transaction (BeginTransaction → GetDocument → Commit) |
| `tour_fs_page_seed` / `tour_fs_query_page_1..3` | Firestore `RunQuery` cursor pagination (`StartAfter`) |
| `tour_log_write` / `tour_log_list` | `WriteLogEntries` + `ListLogEntries` |
| `tour_pubsub_topic_create` / `tour_pubsub_publish` | Pub/Sub topic + one batched `Publish` |
| `tour_pubsub_iam_get` / `tour_pubsub_iam_set` | topic IAM policy read-modify-write (`google.iam.v1.IAMPolicy` over gRPC) |

Firestore `RunQuery` is a server stream, so the scenario drains the stream and
captures the document frames as `{"frames":[...]}`.

### Normalization

Before diffing, both sides are normalized so goldens are stable across runs and
contain no project/run-specific strings or secrets. The tour reuses the shared
normalizer (`normalize.go`) and adds:

* run-suffixed resource names → `TourBucket`, `TourBQDataset`, `TourBQTable`,
  `TourBQJob`, `TourTopic`, `TourSecret`, `TourFSCounter`, `TourFSPage`,
  `TourDataproc` fold to the same `<bucket>/<dataset>/…` placeholders the
  curated sets use (so list-scoping rules apply);
* session/continuation query tokens in a path (`pageToken`, `upload_id`,
  `resumeToken`, `sessionToken`) → `<token>`;
* KMS asymmetric material (`pem`, `signature` and their CRCs) → placeholders,
  since the emulator and real GCP hold different keys;
* Firestore transaction ids → `<transaction>` (server-generated, echoed in the
  read/commit request, so folded on both sides like a cursor);
* the project **number** is folded to `<project>` in the gRPC half too (Secret
  Manager canonicalizes names to the number).

### Deferrals (documented, not silently skipped)

* **Pub/Sub streaming pull, Firestore Listen, Logging TailLogEntries** — these
  are long-lived streams whose frame timing is not deterministic across real GCP
  and the emulator; a stable golden needs a bounded-capture contract that does
  not yet exist. They remain exercised by the live SDK tour (`demo/sdk-tour`).
* **Dataproc LRO create/poll** — creating a real Dataproc cluster is
  heavyweight (minutes; cost) and the emulator completes LROs synchronously.
  The curated set deliberately excludes a real Dataproc create for the same
  reason; the SDK tour still covers the gax LRO poller against the emulator.
* **Resumable multi-chunk** — the golden covers session start, one non-final
  chunk and the finalizing chunk (512 KiB total); larger transfers differ only
  in chunk count.

## Divergence classification

Every divergence is either **open** (a real bug to investigate) or **accepted**
(by design, with a rule and a reason in `triage.go`). Accepted rules cover
output-only fields the emulator does not synthesize, cosmetic/vanity metadata,
documented default-enum equivalence, and superset fields. A regression in the
executed result itself (status, rows, schema, resource identity) stays open.
Run `make test-gcp-differential` (optionally with `GCP_DIFFERENTIAL_STRICT=1`) to
gate on the open set.
