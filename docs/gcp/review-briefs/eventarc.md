> **Historical example — not a live instruction.** This is the review brief used
> during the GCP build-out (when work landed on the `gcp` branch). Keep it as a
> format reference for condensing a Discovery Document into reviewer prompts;
> the current base branch is `main` and the workflow is in
> [`../review-checklist.md`](../review-checklist.md) and `AGENTS.md`.

# Eventarc Review Brief — PR #59 (feat/gcp-eventarc)

Target: https://github.com/jaisrajms/jaiscloud/pull/59
Base: `jaisrajms/jaiscloud:gcp` (`c0128927`) | Head: `aamerik:feat/gcp-eventarc` (`38208edd`)
Reviewer lenses: **A = GCP Architect (wire-protocol parity)**, **B = Senior Developer (AWS parity + conventions)**.

Files under review:
- `internal/gcp/provider/eventarc/eventarc.go` (+test) — triggers/channels/providers CRUD
- `internal/gcp/store/eventarc/{store,memory,postgres,snapshot}.go` + `internal/gcp/store/migrations/032_eventarc.sql`
- `internal/gcp/adapter/{services,router,jsoncodec}.go` — descriptor + v1 detection + action derivation
- `internal/gcp/resource/resource.go` — eventarc formatters
- `cmd/jaiscloud-gcp/main.go` — wiring
- `tests/integration/gcp/sdk-eventarc/`

## Repo conventions (calibration — verify before flagging)
- `clock.Now()` business / `clock.RealNow()` infra; never `time.Now()`.
- `nr.ResourceID` for response resource-name FIELDS; store keys (`ResourceEntry.ID`) stay raw — flagging store keys for not using `nr.ResourceID` is a FALSE POSITIVE.
- `errors.Is` for sentinels, never `==`.
- GCP error envelope `{"error":{"code":<HTTP>,"message":"…","status":"<GOOGLE_RPC_STATUS>"}}`.
- `internal/gcp/` must NEVER import `internal/aws/`.
- Memory + Postgres parity; `admin.Snapshotter`/`Resetter` registered in `main.go`.
- Emulator discipline: real GCP protocol only; unimplemented real methods → `501 Unimplemented` (fail loud), not silent success.
- Discovery Doc is the SOLE wire authority — not model memory.

---

## GROUND TRUTH — Eventarc v1 Discovery Document (fetched 2026-09-10, revision 20260826)

baseUrl `https://eventarc.googleapis.com/`, servicePath `""`.

### Triggers
| op | path | method | params | body | response |
|---|---|---|---|---|---|
| create | `v1/{+parent}/triggers` | POST | `triggerId`, `validateOnly`, `parent` | `Trigger` | `GoogleLongrunningOperation` |
| get | `v1/{+name}` | GET | `name` | — | `Trigger` |
| list | `v1/{+parent}/triggers` | GET | `pageSize`, `pageToken`, `filter`, `orderBy`, `parent` | — | `ListTriggersResponse` |
| patch | `v1/{+name}` | PATCH | `updateMask`, `allowMissing`, `validateOnly` | `Trigger` | `GoogleLongrunningOperation` |
| delete | `v1/{+name}` | DELETE | `etag`, `allowMissing`, `validateOnly` | — | `GoogleLongrunningOperation` |
| IAM | `…/triggers/{t}:getIamPolicy` GET, `:setIamPolicy` POST, `:testIamPermissions` POST | | | | |

**The trigger ID is the `triggerId` QUERY PARAM on create — the path has NO id segment.** Same for `channelId` on channels.

### Channels
create `POST v1/{+parent}/channels` (`channelId`); get/list/patch/delete analogous; IAM trio present.

### Providers (read-only discovery)
`GET v1/{+name}` → `Provider`; `GET v1/{+parent}/providers` (pageSize/pageToken/filter/orderBy) → `ListProvidersResponse`.

### Schemas (authoritative fields — camelCase)
- `Trigger` { name, uid, createTime, updateTime, etag, destination(Destination), transport(Transport), eventFilters(array of EventFilter), serviceAccount, channel, labels, conditions, eventDataContentType, retryPolicy(RetryPolicy), satisfiesPzs }
- `Destination` { workflow(string), cloudFunction(string), gke(GKE), httpEndpoint(HttpEndpoint), cloudRun(CloudRun), networkConfig(NetworkConfig) }  ← oneof; **`cloudFunction` IS a documented field**
- `Transport` { pubsub(Pubsub) } ; `Pubsub` { topic, subscription }
- `EventFilter` { attribute, operator, value }
- `CloudRun` { region, path, service } ; `GKE` { cluster, location, namespace, path, service } ; `HttpEndpoint` { uri } ; `RetryPolicy` { maxAttempts }
- `Channel` { name, uid, createTime, updateTime, state, provider, pubsubTopic, activationToken, cryptoKeyName, labels, satisfiesPzs }
- `Provider` { name, displayName, eventTypes(array) }
- `ListTriggersResponse` { triggers, nextPageToken, unreachable } ; `ListChannelsResponse` { channels, nextPageToken, unreachable } ; `ListProvidersResponse` { providers, nextPageToken, unreachable }
- Operation type is `GoogleLongrunningOperation` (create/patch/delete).

### Real Eventarc resources NOT in the implemented subset (must fail loud, not silently absent)
`pipelines`, `messageBuses`, `googleApiSources`, `enrollments`, `channelConnections`, `googleChannelConfig`, `locations`, `operations`.

---

## Known high-risk probes for this PR
1. Create path: ID from `triggerId`/`channelId` query param (not body, not path) — verify.
2. `cloudFunction` destination: documented field — verify it is not incorrectly rejected.
3. IAM trio on triggers/channels: real Eventarc serves it — `501` is a fidelity gap (repo has an IAM router).
4. `unreachable` field + `pageSize/pageToken` pagination + `filter`/`orderBy` params.
5. Unknown/unimplemented resources → `501`/`404` fail-loud (not a panic / silent 200).
6. Operation envelope shape (`GoogleLongrunningOperation`: name, done, response/error, metadata).
