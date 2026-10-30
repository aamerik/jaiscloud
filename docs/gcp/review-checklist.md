# GCP review checklist

How a GCP change is reviewed in jaiscloud: two lenses, grounded in the service's
own API definition, with a calibrated triage. The repo's source-of-truth order is
in [`AGENTS.md`](../../AGENTS.md) — this file is the operational checklist.

Use it for a new operation, a new service surface, a parity fix, or a console
change that exposes wire behaviour. Two worked examples of the review brief that
goes with it: [`review-briefs/phase1-gcs.md`](review-briefs/phase1-gcs.md) and
[`review-briefs/eventarc.md`](review-briefs/eventarc.md).

## 1. Ground the review in the API definition, not model memory

Fetch the service's machine-readable definition once, condense the parts under
review, and paste that into every reviewer prompt. Reviewers must not browse from
memory, and neither the emulator's tests nor floci-gcp/localgcp expectations are
authority.

```
https://storage.googleapis.com/$discovery/rest?version=v1
https://pubsub.googleapis.com/$discovery/rest?version=v1
https://secretmanager.googleapis.com/$discovery/rest?version=v1
https://cloudkms.googleapis.com/$discovery/rest?version=v1
https://iam.googleapis.com/$discovery/rest?version=v1
https://firestore.googleapis.com/$discovery/rest?version=v1
https://cloudfunctions.googleapis.com/$discovery/rest?version=v1
https://cloudresourcemanager.googleapis.com/$discovery/rest?version=v1
https://bigquery.googleapis.com/discovery/v1/apis/bigquery/v2/rest
https://workflowexecutions.googleapis.com/$discovery/rest?version=v1
```

Canonical source: `github.com/googleapis/googleapis`, `google/<service>/<version>/<service>.json`.
For gRPC-only behaviour, use the `.proto` (and the generated client defaults).

The brief should list, from the document: the `methods` under review (verb +
path, including custom `:verb` methods), the `schemas` fields the response must
carry, the pagination parameters, and the error/status expectations. Keep the
fetch date in the brief — it is a snapshot, not a permanent claim.

## 2. Lens A — GCP architect (wire parity)

- **Resource names**: every response `name`/`selfLink` is the full
  `projects/{p}/...`/documented path; no invented or emulator-only fields.
  Extra emulator-only response fields are findings, not conveniences.
- **Routing**: each custom method (`:publish`, `:setIamPolicy`, `:access`,
  `:cancel`, …) matches the documented verb and path; unknown combos return a
  clean `404`/`400`, never a panic or a silent success.
- **Pagination**: `pageSize`/`pageToken` → `nextPageToken` where documented (GCS
  uses `maxResults`); pagination must compose with `delimiter`/filters rather than
  paginating one half of the result set.
- **Bodies**: field names, required fields, enum values, nested
  `Any`/`@type` envelopes, CRC32C echoes, base64 payloads.
- **Errors**: `{"error":{"code":<HTTP>,"message","status":"<GOOGLE_RPC_STATUS>"}}`
  with the documented status string; gRPC via the mapped code.
- **IAM**: `getIamPolicy`/`setIamPolicy`/`testIamPermissions` at *every* level
  (resource, parent, project), `setIamPolicy` honours `etag` OCC (409 on
  mismatch, fresh etag per set).
- **Transports agree**: REST and gRPC over the same core must return the same
  semantics; where real GCP is REST-only, only REST is exposed (and the docs say
  so).
- **Fail loud**: a real operation that is not implemented returns
  `501 Unimplemented`, never a fabricated success.

## 3. Lens B — senior developer (conventions + code quality)

- `clock.Now()` for business time, `clock.RealNow()` for infra time — never
  `time.Now()`.
- `nr.ResourceID("type", name)` for response resource-name fields; **store keys
  stay raw** (`store.ResourceEntry.ID` is not a resource name).
- Store scoping `store.GlobalRegion` + `nr.AccountID`; `errors.Is` for sentinels,
  never `==`.
- In-memory state has a `Reset(ctx)` registered with the admin handler;
  snapshot-relevant state has a `Snapshotter`.
- `internal/gcp/` imports only shared infra — never `internal/aws/`.
- Memory and Postgres (`--dsn`) behave identically; a schema change ships a
  migration.
- Code soundness: unsafe `.(string)`/`.(map[string]any)` assertions, dead code,
  duplicated logic between transports, naming, and missing tests (negative paths,
  concurrency, codec routing table).

## 4. Protocol: two reviewers, cross-audit, tie-breaker

1. **Lens A reviewer** (architect) — findings only, no code, as
   `file:line | finding | severity | Discovery reference`.
2. **Lens B reviewer** (senior dev) — same shape, conventions + quality.
3. **Cross-audit** — swap roles: each reviewer audits the other's list and marks
   every finding *grounded* or *false positive* with one line of evidence.
4. **Tie-breaker** — only where they disagree: the Discovery Doc/proto decides.
   Do not average opinions, and do not accept a model's memory.
5. **Triage** — verify each surviving finding against the reference and the AWS
   counterpart before fixing; fix blockers/majors with a test each; defer minors
   with a plan-doc row and a verdict.

Optional, when findings touch a CLI-reachable service: cross-check with the
official `gcloud` CLI (the floci `sdk-test-gcloud` suite) — it enforces
client-side checks the SDKs skip. It cannot reach gRPC-only services (Pub/Sub,
Firestore, Datastore, Logging, Monitoring).

## 5. Calibration — recurring false positives and real bugs

Treat these as settled; re-litigating them wastes a review round.

- **An emulator's tests are not the spec.** Known non-compliant expectations:
  Firestore delete-of-missing is idempotent (not `NotFound`); Firestore `IN []`
  is rejected (not "empty result"); Listen sends `TargetChange(ADD)` then
  `CURRENT`/`NO_CHANGE` before the first `DocumentChange`; a resumable `PUT` with
  no `Content-Range` returns `308` (not `200`); `/_localgcp/*` endpoints are
  localgcp-only; Pub/Sub fan-out copies, and topic delete sets
  `_deleted-topic_`. Classify REAL vs TEST-NON-COMPLIANT before fixing.
- **"Store key must use nr.ResourceID" is a false positive** (see §3).
- **GCS `objects.update` (PUT) replaces** (omitted writable fields cleared);
  **`objects.patch` (PATCH) merges** (omitted fields preserved). Route them
  separately in the codec.
- **Pagination composes with `delimiter`**; the cursor spans items *and* prefixes.
- **Full IAM surface per resource** — missing object-level IAM was a real Phase 0
  bug, not a scope decision.
- **`setIamPolicy` must enforce etag OCC** — 409 on mismatch, fresh etag per set.
- **One binary per cloud** — the GCP binary must not gain AWS imports, and the
  AWS binary must keep building (`go build ./...`, `go vet ./cmd/jaiscloud-aws/`).

## 6. Gate before the PR

```bash
export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH
go build ./... && go vet ./internal/gcp/... ./cmd/jaiscloud-gcp/
gofmt -l internal/gcp/ cmd/jaiscloud-gcp/          # must be empty
go test ./internal/gcp/...
make ga-check                                       # fidelity + REST/gRPC/gcloud conformance
```

Behavioural work also runs `make gcp-status-behavioral-gate` /
`make gcp-status-depth-gate`; deferrals are recorded in the plan doc before the
PR is opened (see `AGENTS.md`).
