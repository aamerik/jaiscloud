> **Historical example — not a live instruction.** This is the review brief used
> during the GCP build-out (when work landed on the `gcp` branch). Keep it as a
> format reference for condensing a Discovery Document into reviewer prompts;
> the current base branch is `main` and the workflow is in
> [`../review-checklist.md`](../review-checklist.md) and `AGENTS.md`.

# Phase 1 Review Brief — PR #10 (Pub/Sub, Secret Manager, KMS, IAM)

Target: https://github.com/jaisrajms/jaiscloud/pull/10
Base: `jaisrajms/jaiscloud:gcp` | Head: `aamerik:feat/gcp-phase1`
Diff: 13 files, +1584/−8 (Phase 1 only — no Phase 0 leakage, verified after rebase).

Files under review:
- `internal/gcp/adapter/jsoncodec.go` (+test) — generic `/v1/projects/{project}/...` codec
- `internal/gcp/adapter/services.go`, `gcp.go` — service table + codec derivation
- `internal/gcp/provider/pubsub/`, `secret/`, `kms/`, `iam/` (+tests)
- `cmd/jaiscloud-gcp/main.go` — provider registrations

Repo conventions (from CLAUDE.md + Phase 0 calibration):
- `clock.Now()` for business time; `clock.RealNow()` for infra wall time. Never `time.Now()`.
- `nr.ResourceID("type", name)` for resource-name FIELDS in responses; store keys
  (`ResourceEntry.ID`) stay raw names — a finding demanding formatters on store
  keys is a false positive (verified against `internal/aws/provider/...`).
- `store.GlobalRegion` + `nr.AccountID` scoping; `errors.Is` for sentinels.
- Error envelope for these services: `{"error":{"code":<HTTP>,"message":"...","status":"<GOOGLE_RPC_STATUS>"}}`
  — the generic GCP envelope WITH `status` (differs from the GCS codec, which has none).
- Providers expose `Routes() map[string]provider.HandlerFunc`; in-memory state
  needs `Reset()` + admin `RegisterResetter`.
- Pagination here uses `pageSize`/`pageToken` (NOT GCS's `maxResults`).

---

## GROUND TRUTH — Discovery Document wire facts (v1, fetched 2026-09-02)

### Pub/Sub (`pubsub.googleapis.com/$discovery/rest?version=v1`)
Methods (implemented subset):
- Topic: create `PUT v1/{+name}` (name = `projects/{p}/topics/{t}`); get `GET v1/{+topic}`;
  list `GET v1/{+project}/topics` (params: `pageSize`, `pageToken`); delete `DELETE v1/{+topic}`;
  publish `POST v1/{+topic}:publish`.
- Subscription: create `PUT v1/{+name}`; get/list/delete; pull `POST v1/{+subscription}:pull`;
  acknowledge `POST v1/{+subscription}:acknowledge`; modifyAckDeadline `POST v1/{+subscription}:modifyAckDeadline`.
Schemas:
- `Topic` { name, labels, messageStoragePolicy, kmsKeyName, messageRetentionDuration (google-duration), state, ... }
- `Subscription` { name, topic, ackDeadlineSeconds (int32), pushConfig, retainAckedMessages, expirationPolicy, state, ... }
- `PublishRequest` { messages: array of `PubsubMessage` } → `PublishResponse` { messageIds: array }
- `PubsubMessage` { data (byte, base64), attributes (object), messageId, publishTime, orderingKey }
- `PullRequest` { maxMessages (int32), returnImmediately } → `PullResponse` { receivedMessages: array of `ReceivedMessage` }
- `ReceivedMessage` { ackId, message }
- `AcknowledgeRequest` { ackIds: array }; `ModifyAckDeadlineRequest` { ackIds, ackDeadlineSeconds (int32) }
- `ListTopicsResponse` { topics, nextPageToken }; `ListSubscriptionsResponse` { subscriptions, nextPageToken }
- `Policy` { version (int32), etag (byte), bindings }
- IAM surface exists on topics/subscriptions: getIamPolicy `GET v1/{+resource}:getIamPolicy`,
  setIamPolicy `POST v1/{+resource}:setIamPolicy`, testIamPermissions.

### Secret Manager (`secretmanager.googleapis.com/$discovery/rest?version=v1`)
- Secret create: `POST v1/{+parent}/secrets` (parent = project); list `GET v1/{+parent}/secrets`
  (`pageSize`/`pageToken`); get `GET v1/{+name}`; update `PATCH v1/{+name}`; delete `DELETE v1/{+name}`.
- addVersion: `POST v1/{+parent}:addVersion` — custom method ON THE SECRET (parent = secret name),
  body `AddSecretVersionRequest` { payload: `SecretPayload` { data (byte), dataCrc32c (int64) } }.
- access: `GET v1/{+name}:access` — **GET, not POST** — on the VERSION name.
  Response `AccessSecretVersionResponse` { name, payload }.
- destroy/disable/enable: `POST v1/{+name}:destroy|:disable|:enable` on the version.
- `Secret` { name, replication (automatic|userManaged), createTime, etag, labels, rotation, ttl, expireTime, topics }
- `SecretVersion` { name, createTime, destroyTime, state (ENABLED|DISABLED|DESTROYED), etag, replicationStatus }
- `ListSecretsResponse` { secrets, nextPageToken, totalSize (int32) };
  `ListSecretVersionsResponse` { versions, nextPageToken, totalSize }.
- IAM surface: getIamPolicy/setIamPolicy/testIamPermissions on secrets.

### KMS (`cloudkms.googleapis.com/$discovery/rest?version=v1`)
- KeyRing create: `POST v1/{+parent}/keyRings` (parent = `projects/{p}/locations/{l}`);
  list `GET v1/{+parent}/keyRings`; get `GET v1/{+name}`.
- CryptoKey create: `POST v1/{+parent}/cryptoKeys` (parent = keyRing); list/get.
- encrypt: `POST v1/{+name}:encrypt` — `EncryptRequest` { plaintext (byte), plaintextCrc32c (int64),
  additionalAuthenticatedData } → `EncryptResponse` { name, ciphertext (byte), protectionLevel,
  verifiedPlaintextCrc32c (bool), ciphertextCrc32c }.
- decrypt: `POST v1/{+name}:decrypt` — `DecryptRequest` { ciphertext (byte), ciphertextCrc32c } →
  `DecryptResponse` { plaintext (byte), plaintextCrc32c, protectionLevel, usedPrimary }.
- `KeyRing` { name, createTime }; `CryptoKey` { name, purpose, primary (CryptoKeyVersion), createTime, rotationPeriod };
  `CryptoKeyVersion` { name, state, algorithm, createTime, generateTime }.
- `ListKeyRingsResponse` { keyRings, nextPageToken, totalSize }; `ListCryptoKeysResponse` { cryptoKeys, nextPageToken, totalSize }.

### IAM (`iam.googleapis.com/$discovery/rest?version=v1`)
- ServiceAccount create: `POST v1/{+name}/serviceAccounts` (name = project) — NOTE the path shape
  (`/v1/projects/{p}/serviceAccounts`, NOT `:create`).
- list `GET v1/{+name}/serviceAccounts` (`pageSize`/`pageToken`); get `GET v1/{+name}`
  (name = `projects/{p}/serviceAccounts/{uniqueId|id}`); update `PUT v1/{+name}`; delete.
- getIamPolicy `GET v1/{+resource}:getIamPolicy`; setIamPolicy `POST v1/{+resource}:setIamPolicy`.
- `ServiceAccount` { name (`projects/{p}/serviceAccounts/{id}`), projectId, uniqueId, email,
  displayName, description, disabled, etag (byte), oauth2ClientId }.
- `ListServiceAccountsResponse` { accounts, nextPageToken }.
- `Policy` { version (int32), etag (byte), bindings: array of `Binding` { role, members, condition } }.

---

## STEP 1 — GEMINI (GCP ARCHITECT LENS)

Prompt to paste into Gemini:

> You are a **GCP Principal Architect** reviewing PR #10 ("feat(gcp): Phase 1 —
> Pub/Sub, Secret Manager, KMS, IAM") of the jaiscloud emulator
> (https://github.com/jaisrajms/jaiscloud/pull/10). The diff is 13 files under
> `internal/gcp/` — a generic `/v1/projects/{project}/...` JSON codec
> (`internal/gcp/adapter/jsoncodec.go`, `services.go`) plus four providers
> (`internal/gcp/provider/{pubsub,secret,kms,iam}/`).
>
> Your lens is **wire-protocol parity with the real Google APIs**. Ground every
> finding in the official Discovery Documents (fetch if you can; the key facts
> are attached below — do NOT rely on memory):
> - https://pubsub.googleapis.com/$discovery/rest?version=v1
> - https://secretmanager.googleapis.com/$discovery/rest?version=v1
> - https://cloudkms.googleapis.com/$discovery/rest?version=v1
> - https://iam.googleapis.com/$discovery/rest?version=v1
>
> Check specifically:
> 1. **Resource names** — every response `name` must be the full
>    `projects/{p}/...` path; response fields must match the Discovery Doc
>    schemas (attached) — missing/renamed/extra fields are findings.
> 2. **Custom-method routing** — the codec strips `:suffix` from the last path
>    segment. Verify each implemented custom method against the documented
>    paths: `:publish` (POST), `:pull`, `:acknowledge`, `:modifyAckDeadline`
>    (all POST); Secret Manager `:addVersion` (POST on the SECRET name),
>    `:access` (**GET**, on the VERSION name), `:destroy/:disable/:enable`
>    (POST); KMS `:encrypt`/`:decrypt` (POST); IAM `:getIamPolicy` (GET) /
>    `:setIamPolicy` (POST).
> 3. **Pagination** — list methods use `pageSize`/`pageToken` (NOT GCS's
>    `maxResults`); responses carry `nextPageToken` (+ `totalSize` for Secret
>    Manager and KMS).
> 4. **Request/response bodies** — PublishRequest→PublishResponse, PullResponse
>    `receivedMessages[]`, SecretPayload `data` base64, KMS encrypt/decrypt
>    byte fields + CRC32C echo fields, Policy `{version, etag, bindings}`.
> 5. **Error envelope** — `{"error":{"code":<HTTP>,"message","status":"<GOOGLE_RPC_STATUS>"}}`
>    (with `status`; NOT the GCS envelope).
> 6. **IAM OCC** — every `setIamPolicy` must validate `etag` (409 on mismatch),
>    per the Phase 0 precedent.
>
> Attached wire facts: <paste the GROUND TRUTH section above>.
>
> Output: a markdown table — `file:line | finding | severity (blocker/major/minor)
> | Discovery Doc reference (method or schema)`. Prioritize blockers/majors.
> Do not propose code; identify defects only.

## STEP 2 — DEEPSEEK (SENIOR DEVELOPER LENS)

Prompt to paste into DeepSeek:

> You are a **Senior Go Engineer** on the jaiscloud emulator reviewing PR #10
> ("feat(gcp): Phase 1 — Pub/Sub, Secret Manager, KMS, IAM",
> https://github.com/jaisrajms/jaiscloud/pull/10). Diff: 13 files under
> `internal/gcp/` (generic v1 JSON codec + four providers + tests).
>
> Your lens is **AWS parity + repo conventions + code quality**. Reference the
> AWS providers under `internal/aws/provider/...` and `CLAUDE.md`:
> 1. `nr.ResourceID("type", name)` for every resource-name FIELD in responses —
>    never `fmt.Sprintf("projects/%s/...")`. (Store keys stay raw — do NOT flag
>    `ResourceEntry.ID`.)
> 2. Time: `clock.Now()`/`clock.RealNow()` only — never `time.Now()`.
> 3. Store scoping `store.GlobalRegion` + `nr.AccountID`; `errors.Is` not `==`.
> 4. Provider `Routes()` shape; `Reset()` + admin `RegisterResetter` for any
>    in-memory state.
> 5. Code soundness: unsafe `.(string)`/`.(map[string]any)` assertions, dead
>    code, duplicated logic, naming, missing tests (especially negative paths,
>    concurrency, and the new codec's routing table).
> 6. Cross-check the codec: `detectResourceType`/`deriveAction` must be
>    exhaustive vs the services table in `services.go` — unmapped method/type
>    combos should return a clean 404/400, not panic.
>
> Output: markdown table — `file:line | finding | severity (blocker/major/minor)`.
> Do not propose code; identify defects only.

## STEP 3 — CROSS-AUDIT (roles swapped)

> You previously reviewed PR #10 as <ROLE A>. You are now the opposing reviewer
> (<ROLE B>). Audit each finding from the other reviewer's list and mark it
> **grounded** or **false positive**, with one line of evidence each. You may
> change a severity, but only with justification. For any API-semantics claim,
> cite the Discovery Doc method/schema — never memory.

Run this in BOTH directions: Gemini audits DeepSeek's list, DeepSeek audits Gemini's.

## STEP 4 — TIE-BREAKER (only when the two disagree)

> Resolve ONLY against the Discovery Document (attached ground truth) or the
> AWS reference implementation. If neither settles it, mark the finding
> "unverifiable — defer as minor/debt". Never accept a model's recollection
> of API behavior over the spec.

## STEP 5+ — Triage, fix, verify, PR hygiene

- Fix blockers + majors; one test per fix; re-run `go build ./...`,
  `go vet ./internal/gcp/...`, `gofmt -l`, `go test -race ./internal/gcp/...`.
- Deploy to k3d and smoke the four services with curl (`:publish`, `:access`,
  `:encrypt`/`:decrypt`, serviceAccounts create/list).
- Update PR #10 description with the review-round summary; resolve threads;
  request re-review via `@jaisrajms` comment.
