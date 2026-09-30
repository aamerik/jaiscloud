# BigQuery SQL engine — decision record (BQ0)

> **Status: DECIDED — Option A: pure-Go in-process SQLite (`modernc.org/sqlite`),
> one engine in both memory and `--dsn` modes. No engine shipped yet.**
> This record freezes the engine choice and the v1 dialect subset. Implementation
> is BQ1–BQ4 (`plan_docs/gcp-bigquery-ga-wave-plan.md`).

Date: 2026-09-29 · Backlog ID: **BQ0** · Branch: `spike/gcp-bigquery-sql-engine`

## 1. Problem

BigQuery is the only `preview` GCP service in jaiscloud: all 23 cells are
"metadata + stored rows only, **no SQL engine**" (`docs/GA.md` §7,
`docs/fidelity/fidelity-matrix.md`). `jobs.query` stores the query and returns
`jobComplete=true` with an empty result (`internal/gcp/provider/bigquery/bigquery.go`),
so any client that evaluates SQL locally gets a successful-looking wrong answer.
`docs/GCP-TESTABILITY.md` §4 grades `bigquery` 🔴 *"Nothing local counts as evidence."*

Nothing else in the emulator consumes BigQuery (it is a leaf: no Logging sink
delivery, no Dataflow, Spark wires only GCS), so this is a **client-fidelity
product decision**, not a cross-service or AWS-parity gap (the AWS analogues —
Athena/RDS/Redshift — are metadata-only too).

## 2. Decision

**Option A: one pure-Go, in-process SQLite engine (`modernc.org/sqlite`) as the
sole BigQuery query engine, in both memory and `--dsn` modes.**

- SQLite is **disposable query scratch**, never persisted: rows are hydrated from
  the existing store (`jc_bq_rows` JSONB under `--dsn`, memory store otherwise),
  the query runs, results are encoded, the scratch is discarded.
- The **store stays the single source of truth**; snapshots/export/import/reset/
  `--dsn` durability are untouched and need no new migration.
- **One engine** preserves the memory-vs-`--dsn` parity guarantee: only the
  storage backend differs, never the SQL dialect.

## 3. Spike evidence (BQ0 feasibility gate)

Throwaway harness: a real SQLite scratch over hydrated tables exercising the
subset, plus the encoding to the Discovery result shape. Results:

- **Translation + execution (all green):** `SELECT *` over a backticked
  `` `project.dataset.table` ``, `WHERE col = literal`, `COUNT(*)`,
  `GROUP BY`/`HAVING`/`AVG`, `INNER JOIN`, `LEFT JOIN` (NULL-preserving), `WITH`
  CTE, `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)`.
- **Mandatory semantic rewrite proven:** BigQuery `/` is FLOAT64 division, SQLite
  `/` is integer division (`SELECT 1/2` → `0` vs `0.5`); the translator must cast
  operands. Unknown tables fail loud at resolution.
- **Discovery encoding:** `{kind:"bigquery#queryResponse", jobComplete:true,
  jobReference, schema:{fields:[{name,type,mode}]}, rows:[{f:[{v}]}],
  totalRows:"N"}` — scalars as strings, `INT64`→`INTEGER`, `BOOL`→`BOOLEAN`.
- **Cross-compile + size (`CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`):**

| target | with SQLite | stdlib-only | delta |
|---|---:|---:|---:|
| linux/amd64 | 6.7 MB | 1.2 MB | +5.5 MB |
| linux/arm64 | 6.5 MB | 1.2 MB | +5.3 MB |
| darwin/amd64 | 6.6 MB | 1.2 MB | +5.5 MB |
| darwin/arm64 | 6.5 MB | 1.2 MB | +5.4 MB |
| windows/amd64 | 6.6 MB | 1.2 MB | +5.5 MB |
| windows/arm64 | 6.3 MB | 1.1 MB | +5.3 MB |

  All six goreleaser targets build cleanly with `CGO_ENABLED=0`; the dependency
  adds **~5.5 MB** and no libc/libstdc++ requirement, so
  `gcr.io/distroless/static` keeps working. (`modernc.org/sqlite v1.60.1`.)

## 4. Rejected options

| Option | Why not |
|---|---|
| **B — Postgres as the query engine** | Memory mode has no Postgres, so two dialects would diverge (type affinity, int/float division, collation, NULL ordering, date functions). Violates the backend-parity rule. Postgres stays **storage-only**. |
| **C — DuckDB executor sidecar** | Kept as the designated *fallback* if the subset proves insufficient, not the default. Adds a Docker/k8s sidecar topology for a gain (arrays/structs/`UNNEST`) the v1 subset deliberately defers, and still needs a BigQuery→DuckDB translator. |
| **DuckDB in-process** (`github.com/duckdb/duckdb-go/v2`) | Measured: `CGO_ENABLED=0` **fails** (`build constraints exclude all Go files`); with cgo a trivial program is **57 MB** vs 1.2 MB, dynamically linked against `libstdc++`/`libc`, so `distroless/static` cannot host it; prebuilt libs exist for only 5 targets (**no `windows/arm64`**); cross-compiling needs per-target C toolchains. Disproportionate for a leaf service. |
| **LocalBQ** (localgcp's approach) | A third-party BigQuery emulator owning its own catalog/data → two sources of truth, duplicated control plane, and jaiscloud cannot honestly grade or fix it. Localgcp's model has no `--dsn`/snapshot parity contract. |

## 5. Frozen v1 dialect subset

**Supported** (translate + execute):

- `SELECT` / `WHERE` / `GROUP BY` / `HAVING` / `ORDER BY` / `LIMIT` / `OFFSET` /
  `DISTINCT`; `INNER`/`LEFT` joins; subqueries; `WITH` CTEs; `UNION ALL`.
- Aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) and window functions.
- A bounded scalar/string/date function set mapped to SQLite equivalents.
- `COUNT(*)` and typed column references.

**Emulated** (translated, must not silently diverge):

- BigQuery `/` → `CAST(… AS REAL)/CAST(… AS REAL)` (FLOAT64 division).
- Backticked `` `project.dataset.table` `` / `` `dataset.table` `` → hydrated
  temp tables (`defaultDataset` supplies the project/dataset when unqualified).
- `QUALIFY` → subquery + `WHERE` (or fail loud if not implemented).
- `SAFE.` functions → NULL-on-error patterns where feasible.

**Fail loud (`InvalidArgument` / 400 `invalidQuery`)** — never return wrong rows:

- `UNNEST` / `ARRAY` / `STRUCT` (beyond storage round-trip), `GEOGRAPHY`.
- Wildcard tables / `TABLE_SUFFIX`, `INFORMATION_SCHEMA`.
- Scripting, stored procedures, `MERGE`, DDL/DML beyond the accepted set,
  legacy SQL.
- Any construct the translator cannot map.

## 6. Non-goals (recorded so they are not re-litigated)

- **gRPC CRUD for BigQuery** — real GCP's BigQuery control plane is REST-only
  (`plan_docs/gcp-dual-protocol-parity.md` §2); adding gRPC would invent a
  protocol no client speaks.
- **BigQuery Storage Read/Write API (gRPC)** — the Spark connector's data plane.
  A separate, larger scope (not in BQ1–BQ4); needed only if Spark/Storage-API
  support is prioritized.
- **Full GoogleSQL** — arrays/structs/geography/scripting are deferred by design.
- **`routines`/`models`/`rowAccessPolicies`** — remain documented `501`.
- **Real DB data planes for other services** (Cloud SQL/Memorystore) — a
  different problem (wire protocol + real server), not helped by this engine.
- **Authz, real LRO timing, quotas** — unchanged accepted risks (`docs/GA.md` §10).

## 7. Consequences / follow-ups

- **BQ1** implements the subset → SQLite translator + scratch hydration and
  SELECT execution. **BQ2** adds DDL/DML + catalog sync + result encoding.
  **BQ3** wires SDK/wire conformance and re-grades the cells `preview` →
  `limited` (not `ga`), updating `docs/GA.md` §1/§7, `docs/GCP-TESTABILITY.md` §4,
  README-GCP and the matrix overrides. **BQ4** proves memory/`--dsn` parity,
  snapshot/export safety and lakehouse scale.
- **`go.mod` gains `modernc.org/sqlite` in BQ1**, not here; BQ0 ships no
  production code.
- BQ3 must not chase floci-gcp's `invalidQuery` assertions for constructs the
  frozen subset *does* support (e.g. `GROUP BY`/`ORDER BY`) — those pin floci's
  narrower engine, not real GCP.
