# DynamoDB GSI/LSI Support — Design Document

## Overview

This document describes the design for proper Global Secondary Index (GSI) and Local Secondary Index
(LSI) support in JaisCloud's DynamoDB emulation. It covers the problem with the current
implementation, the PostgreSQL schema, write/read paths, memory optimisations, and implementation
phases. It is written so a developer new to the codebase can understand both the _what_ and the
_why_.

---

## Background: What Are GSI and LSI?

DynamoDB is a key-value / document store. Every table has a **primary key** that uniquely identifies
each item:

- **Partition key only** (simple primary key): e.g. `UserId`
- **Partition key + sort key** (composite primary key): e.g. `UserId` + `CreatedAt`

By default you can only query a table efficiently by its primary key. If you want to query by a
different attribute — say, find all orders by `CustomerId` when the table's PK is `OrderId` — you
need a secondary index.

### Global Secondary Index (GSI)

A GSI lets you define an **entirely different partition key** (and optional sort key) for query
purposes.

```
Table "Orders"
  Primary key:  OrderId (PK)
  GSI "byCustomer":  CustomerId (PK)  +  CreatedAt (SK)
```

With this GSI you can efficiently answer: _"Give me all orders for customer C, sorted by date."_

- Up to **20 GSIs** per table (AWS default quota).
- GSI can be added or removed via `UpdateTable` after the table is created.
- GSI is non-unique: multiple items can share the same GSI partition key + sort key pair.
- A GSI is **sparse**: items that don't have the GSI's partition key attribute are simply not
  included in the index.

### Local Secondary Index (LSI)

An LSI keeps the **same partition key** as the base table but uses a **different sort key**.

```
Table "Orders"
  Primary key:  CustomerId (PK)  +  OrderId (SK)
  LSI "byDate":  CustomerId (PK)  +  CreatedAt (SK)
```

This lets you answer: _"Give me all orders for customer C, sorted by creation date"_ — even though
the table's natural sort key is `OrderId`.

- Up to **5 LSIs** per table.
- LSIs **must be declared at `CreateTable` time**. You cannot add or remove them later.
- LSI partition key is always identical to the base table's partition key.
- LSI requires the base table to have a composite primary key (PK + SK).

---

## The Problem with the Current Implementation

### Current schema (one global table)

```sql
CREATE TABLE jc_dynamodb_items (
    table_name TEXT  NOT NULL,
    pk_hash    TEXT  NOT NULL,   -- "UserId=abc|OrderId=123"
    item       JSONB NOT NULL,
    PRIMARY KEY (table_name, pk_hash)
);
CREATE INDEX idx_dynamo_items_table ON jc_dynamodb_items (table_name);
-- ^ this index is redundant: table_name is already the leading column of the PK B-tree
```

Every DynamoDB item from every table lives here. `pk_hash` is a text concatenation of the item's
primary key values (e.g. `"UserId=abc|OrderId=123"`).

**Why this cannot support GSI/LSI:**

1. There is no sort key column. Range queries like `CreatedAt BETWEEN :start AND :end` require
   sorting by a specific attribute — but every item is just an opaque JSONB blob.

2. There are no index columns. To query a GSI you need a separate indexed structure keyed on the
   GSI's partition key. There is nowhere to store it.

3. `Query` ignores `IndexName` entirely. Both the memory and Postgres stores receive the `IndexName`
   parameter but scan the entire table and filter in memory.

4. Expression operators `>`, `<`, `>=`, `<=`, `BETWEEN` are not implemented. Only `=` and
   `begins_with` work.

5. **`pk_hash` is computed from the entire item**, not just the key attributes. The `hashKey`
   fallback in `postgres.go` hashes `table + ":" + json.Marshal(item)`, which includes non-key
   attributes. Two items with the same primary key but different non-key attributes hash differently,
   causing `PutItem` to insert a duplicate instead of replacing the existing item.

6. **`DeleteTable` calls `Reset()`** which wipes items for _all_ DynamoDB tables, not just the one
   being deleted.

7. **Memory footprint**: `filterRows` loads every row for a table into `[]map[string]any` before
   filtering. For a table with 50K items and `Limit=25`, all 50K items are decoded and held in heap
   simultaneously.

### What breaks in practice

```python
# This works (table primary key):
table.get_item(Key={"OrderId": "o-1"})

# This silently returns WRONG results (GSI is ignored, full scan happens):
table.query(
    IndexName="byCustomer",
    KeyConditionExpression="CustomerId = :c",
    ExpressionAttributeValues={":c": {"S": "cust-42"}}
)

# This returns nothing because BETWEEN is not implemented:
table.query(
    IndexName="byCustomer",
    KeyConditionExpression="CustomerId = :c AND CreatedAt BETWEEN :start AND :end",
    ...
)

# DeleteTable silently wipes ALL other tables too (Reset() bug):
table.delete()
```

---

## Design Goals

1. Correct GSI and LSI query routing — `IndexName` uses the right key schema and index table.
2. Range queries on sort keys pushed into PostgreSQL SQL, not in-memory filtering.
3. Low memory footprint — row streaming instead of full-table loads; copy-on-return not
   copy-on-scan.
4. Minimal PostgreSQL table and index count (3 tables, 5 indexes per DynamoDB table).
5. No data migration required (local emulator, not a production system).
6. Enforce AWS limits: 20 GSIs and 5 LSIs per table.
7. Memory store stays correct for unit tests without a database.

---

## Schema Design

### Key insight: one set of PostgreSQL tables per DynamoDB table

Instead of a single shared `jc_dynamodb_items` table, each DynamoDB table gets its own set of
three PostgreSQL tables created at `CreateTable` time and dropped at `DeleteTable` time. This
eliminates cross-table contamination, enables per-table indexes, and makes `DeleteTable` a clean
`DROP TABLE`.

A stable **16-character hex suffix** is derived from `sha256(dynamodb_table_name)[:16]` and stored
once. All PostgreSQL tables for that DynamoDB table share this suffix. This avoids SQL injection
risks and identifier length limits (DynamoDB table names can be up to 255 chars; PostgreSQL
identifiers are limited to 63 chars).

Example: DynamoDB table `"Orders"` → suffix `"a3f1c8d294e05b71"` → PostgreSQL tables
`jc_dt_a3f1c8d294e05b71`, `jc_dt_a3f1c8d294e05b71_gsi`, `jc_dt_a3f1c8d294e05b71_lsi`.

### Metadata table (static, one row per DynamoDB table)

Created once in migration `012_dynamodb_per_table.sql`:

```sql
CREATE TABLE jc_dynamo_tables (
    table_name  TEXT        PRIMARY KEY,
    pg_suffix   TEXT        NOT NULL UNIQUE,
    schema_json JSONB       NOT NULL,   -- full CreateTable definition
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`schema_json` stores the complete table definition including key schema, GSI definitions, LSI
definitions, and attribute types. It is the authoritative source for `DescribeTable` and for
knowing which attributes are index keys at query time.

### Table 1 — main items (`jc_dt_{suffix}`)

```sql
CREATE TABLE jc_dt_{suffix} (
    pk_val      TEXT        NOT NULL,              -- raw partition key value
    sk_val      TEXT        NOT NULL DEFAULT '',   -- raw sort key value ('' if no SK)
    sk_num      NUMERIC,                           -- populated iff SK type = 'N', NULL otherwise
    item        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pk_val, sk_val)
    -- PK B-tree satisfies: point lookup (pk_val, sk_val), partition scan (pk_val),
    -- string SK range (pk_val, sk_val >=/<=/BETWEEN). No extra index needed for these.
);

-- Numeric SK range scan: WHERE pk_val=$1 AND sk_num BETWEEN $2 AND $3
-- Partial index keeps it narrow — only rows where SK is actually a number.
CREATE INDEX ON jc_dt_{suffix} (pk_val, sk_num) WHERE sk_num IS NOT NULL;
```

Why `sk_num` alongside `sk_val`? DynamoDB numbers are transmitted as strings: `{"N": "42.5"}`.
Storing the parsed `NUMERIC` separately lets PostgreSQL use `ORDER BY sk_num` and
`WHERE sk_num BETWEEN $1 AND $2` without a per-row `CAST` — which would prevent index use.
String sort keys use `sk_val` directly; `sk_num` is NULL for them.

### Table 2 — GSI index (`jc_dt_{suffix}_gsi`)

All GSIs for this DynamoDB table share a single PostgreSQL table, distinguished by `index_name`.
This means `UpdateTable` adding a new GSI requires zero DDL — rows are simply inserted with the
new `index_name`.

```sql
CREATE TABLE jc_dt_{suffix}_gsi (
    index_name  TEXT        NOT NULL,              -- e.g. "byCustomer"
    gsi_pk_val  TEXT        NOT NULL,              -- GSI partition key value
    gsi_sk_val  TEXT        NOT NULL DEFAULT '',   -- GSI sort key value ('' if no GSI SK)
    gsi_sk_num  NUMERIC,                           -- populated iff GSI SK type = 'N'
    tbl_pk_val  TEXT        NOT NULL,              -- back-ref: main table PK value
    tbl_sk_val  TEXT        NOT NULL DEFAULT '',   -- back-ref: main table SK value
    -- No item column — full item is fetched via JOIN to main table at read time.
    PRIMARY KEY (index_name, gsi_pk_val, gsi_sk_val, tbl_pk_val, tbl_sk_val)
    -- Composite PK because multiple table items can share the same GSI key pair (non-unique).
    -- The PK B-tree satisfies (index_name, gsi_pk_val, gsi_sk_val) prefix scans directly —
    -- no separate query-path index needed.
);

-- Numeric GSI SK range queries
CREATE INDEX ON jc_dt_{suffix}_gsi (index_name, gsi_pk_val, gsi_sk_num)
    WHERE gsi_sk_num IS NOT NULL;

-- Write path: find all GSI rows for a given main-table item (DeleteItem / UpdateItem)
CREATE INDEX ON jc_dt_{suffix}_gsi (tbl_pk_val, tbl_sk_val);
```

Note: the earlier draft had an explicit `CREATE INDEX ON ... (index_name, gsi_pk_val, gsi_sk_val)`
but the PK B-tree `(index_name, gsi_pk_val, gsi_sk_val, tbl_pk_val, tbl_sk_val)` already covers
all prefix scans on those three leading columns. That index was **redundant** and has been removed.

### Table 3 — LSI index (`jc_dt_{suffix}_lsi`)

```sql
CREATE TABLE jc_dt_{suffix}_lsi (
    index_name  TEXT        NOT NULL,              -- e.g. "byDate"
    pk_val      TEXT        NOT NULL,              -- SAME as main table PK (LSI constraint)
    lsi_sk_val  TEXT        NOT NULL DEFAULT '',   -- LSI sort key value
    lsi_sk_num  NUMERIC,                           -- populated iff LSI SK type = 'N'
    tbl_sk_val  TEXT        NOT NULL DEFAULT '',   -- back-ref: main table SK value
    -- No item column — full item is fetched via JOIN.
    PRIMARY KEY (index_name, pk_val, lsi_sk_val, tbl_sk_val)
    -- PK B-tree covers (index_name, pk_val, lsi_sk_val) prefix scans.
    -- No separate query-path index needed.
);

-- Numeric LSI SK range queries
CREATE INDEX ON jc_dt_{suffix}_lsi (index_name, pk_val, lsi_sk_num)
    WHERE lsi_sk_num IS NOT NULL;

-- Write path: find LSI rows to delete/update when table SK changes
CREATE INDEX ON jc_dt_{suffix}_lsi (pk_val, tbl_sk_val);
```

Same redundancy note: the earlier draft's explicit `(index_name, pk_val, lsi_sk_val)` index
duplicated the PK B-tree prefix and has been removed.

### Final index count

| Table | Indexes | Notes |
|---|---|---|
| `jc_dt_{suffix}` | 1 (PK) + 1 (sk_num partial) = **2** | PK covers string SK scans |
| `jc_dt_{suffix}_gsi` | 1 (PK) + 1 (gsi_sk_num partial) + 1 (write-path reverse) = **3** | PK covers query path |
| `jc_dt_{suffix}_lsi` | 1 (PK) + 1 (lsi_sk_num partial) + 1 (write-path reverse) = **3** | PK covers query path |
| **Total** | **5 non-PK indexes** (2+3 implicit PK B-trees counted separately) | Constant regardless of GSI/LSI count |

The previous design created up to **26 tables and ~52 indexes** per DynamoDB table. This design is
constant at **3 tables and 5 non-PK indexes**.

---

## Read Query Design

### Why no item column in GSI/LSI tables

An earlier draft stored the full item JSONB in every GSI/LSI row. The DBA review rejected this:

- An `UpdateItem` changing a non-key attribute would need to update all index copies atomically.
  A partial transaction failure leaves stale item data in index tables.
- With 20 GSIs, every `PutItem` writes the item 21 times. For wide items this is significant.
- `ProjectionType=KEYS_ONLY` still had to send the full item over the wire then trim it in Go.

The revised design stores **only key columns** in GSI/LSI tables. The full item is fetched via a
JOIN to the main table at query time. `UpdateItem` only touches GSI/LSI rows when the **index key
attributes themselves change** — non-key updates only touch the main table.

### Table primary key query (no IndexName)

```sql
SELECT item
FROM   jc_dt_{suffix}
WHERE  pk_val = $1
  AND  sk_val >= $2          -- string SK range example
ORDER  BY sk_val ASC
LIMIT  $3;
```

For numeric SK: substitute `sk_num >= $2::NUMERIC ORDER BY sk_num ASC`.

The PK B-tree `(pk_val, sk_val)` satisfies both the WHERE and ORDER BY without a sort pass.

### GSI query — MATERIALIZED CTE pattern

A naive `JOIN ... ORDER BY g.gsi_sk_val` forces PostgreSQL to either sort the full join result or
pick a Nested Loop plan. For correctness and predictable memory use, use a **MATERIALIZED CTE** to
resolve the index rows first (bounded by LIMIT), then join only those rows to the main table:

```sql
WITH gsi_rows AS MATERIALIZED (
    SELECT tbl_pk_val, tbl_sk_val, gsi_sk_val
    FROM   jc_dt_{suffix}_gsi
    WHERE  index_name  = $1          -- "byCustomer"
      AND  gsi_pk_val  = $2          -- partition key equality (required by DynamoDB)
      AND  gsi_sk_val >= $3          -- optional sort key range
    ORDER  BY gsi_sk_val ASC
    LIMIT  $4                        -- DynamoDB Limit = max rows to examine
)
SELECT m.item
FROM   gsi_rows g
JOIN   jc_dt_{suffix} m ON m.pk_val = g.tbl_pk_val AND m.sk_val = g.tbl_sk_val;
```

The `MATERIALIZED` keyword forces PostgreSQL to execute the CTE independently — the index scan,
sort, and LIMIT happen before the JOIN. The JOIN then touches at most `Limit` rows of the main
table. This keeps both SQL-level memory and Go heap bounded.

For numeric GSI sort key: substitute `gsi_sk_num >= $3::NUMERIC ORDER BY gsi_sk_num ASC`.

### LSI query — same MATERIALIZED CTE pattern

```sql
WITH lsi_rows AS MATERIALIZED (
    SELECT pk_val, tbl_sk_val, lsi_sk_val
    FROM   jc_dt_{suffix}_lsi
    WHERE  index_name  = $1          -- "byDate"
      AND  pk_val      = $2          -- SAME partition key as main table
      AND  lsi_sk_val BETWEEN $3 AND $4   -- sort key range
    ORDER  BY lsi_sk_val ASC
    LIMIT  $5
)
SELECT m.item
FROM   lsi_rows l
JOIN   jc_dt_{suffix} m ON m.pk_val = l.pk_val AND m.sk_val = l.tbl_sk_val;
```

### Full table Scan

```sql
SELECT item
FROM   jc_dt_{suffix}
ORDER  BY pk_val, sk_val
LIMIT  $1;
```

Scan on a GSI or LSI (valid in DynamoDB) uses the same CTE pattern but omits the
`gsi_pk_val = $x` WHERE clause — the CTE scans all rows for that `index_name`.

---

## Projection Pushdown into SQL

Rather than fetching the full item JSONB and trimming it in Go, push projection into the SELECT
using `jsonb_build_object`. This reduces wire volume and Go heap for wide items.

### ProjectionType = KEYS_ONLY

Return only the table primary key attributes and the index key attributes:

```sql
-- GSI KEYS_ONLY: table PK is "CustomerId"+"OrderId", GSI keys are "Status"+"CreatedAt"
WITH gsi_rows AS MATERIALIZED (
    SELECT tbl_pk_val, tbl_sk_val
    FROM   jc_dt_{suffix}_gsi
    WHERE  index_name = $1 AND gsi_pk_val = $2
    ORDER  BY gsi_sk_val ASC
    LIMIT  $3
)
SELECT jsonb_build_object(
    'CustomerId', m.item->'CustomerId',
    'OrderId',    m.item->'OrderId',
    'Status',     m.item->'Status',
    'CreatedAt',  m.item->'CreatedAt'
) AS item
FROM   gsi_rows g
JOIN   jc_dt_{suffix} m ON m.pk_val = g.tbl_pk_val AND m.sk_val = g.tbl_sk_val;
```

### ProjectionType = INCLUDE

Build `jsonb_build_object` dynamically in Go from the key attribute list plus `NonKeyAttributes`:

```go
func buildProjectionSQL(keyAttrs, includeAttrs []string) string {
    all := append(keyAttrs, includeAttrs...)
    parts := make([]string, 0, len(all)*2)
    for _, a := range all {
        escaped := strings.ReplaceAll(a, "'", "''")
        parts = append(parts, fmt.Sprintf("'%s', m.item->'%s'", escaped, escaped))
    }
    return "jsonb_build_object(" + strings.Join(parts, ", ") + ")"
}
```

### ProjectionType = ALL (default)

No pushdown needed — `SELECT m.item` returns the full JSONB.

To wire this up, add `ProjectionAttrs []string` to `QuerySpec` and `ScanSpec`. The provider
populates this list from the GSI/LSI definition before calling the store.

---

## Row Streaming and Memory Footprint

### PostgreSQL store — stream rows, stop early

The current `filterRows` appends every row into `[]map[string]any` before returning. For a table
with 100K items and `Limit=25`, all 100K items are decoded and held on the Go heap simultaneously.

**Fix:** use pgx's cursor as a forward stream. Break as soon as `limit` items are accepted. pgx
handles early close safely via `defer rows.Close()`.

```go
func (s *PostgresDynamoDBItemStore) filterRows(
    rows pgx.Rows, keyExpr, filterExpr string,
    names map[string]string, values map[string]any,
    exclusiveStartKey string, limit int,
) ([]map[string]any, string, error) {
    defer rows.Close()

    // Decode pagination cursor once.
    var startHash string
    if exclusiveStartKey != "" {
        var startKey map[string]any
        if json.Unmarshal([]byte(exclusiveStartKey), &startKey) == nil {
            startHash = itemPKHash(startKey)
        }
    }

    pastCursor := startHash == ""
    var lastExamined map[string]any
    var result []map[string]any
    examined := 0

    for rows.Next() {
        var raw []byte
        if err := rows.Scan(&raw); err != nil {
            return nil, "", err
        }
        var item map[string]any
        if err := json.Unmarshal(raw, &item); err != nil {
            continue
        }

        // Advance past the exclusive start key cursor.
        if !pastCursor {
            if itemPKHash(item) == startHash {
                pastCursor = true
            }
            continue
        }

        // DynamoDB Limit counts examined items (before FilterExpression), not returned items.
        examined++
        lastExamined = item
        if limit > 0 && examined > limit {
            // Stopped before end of table — return cursor to last examined item.
            b, _ := json.Marshal(lastExamined)
            return result, string(b), rows.Err()
        }

        if !matchesKeyCondition(item, keyExpr, names, values) {
            continue
        }
        if filterExpr != "" && !matchesFilter(item, filterExpr, names, values) {
            continue
        }
        result = append(result, item)
    }
    if err := rows.Err(); err != nil {
        return nil, "", err
    }
    if result == nil {
        result = []map[string]any{}
    }
    return result, "", nil
}
```

**Why `examined` not `len(result)` for the limit?** DynamoDB's `Limit` parameter means _"examine
at most N items before stopping"_ — counting happens before `FilterExpression` is applied. A Scan
with `Limit=100` and a filter that rejects 90% of items returns 10 items with a
`LastEvaluatedKey` pointing at item 100. The old code used SQL `LIMIT` which counted returned (not
examined) rows — incorrect when a `FilterExpression` is present.

**Peak heap:** reduced from O(table size) to O(limit). For `Limit=25` on a 100K-item table, only
~25 items are ever alive on the Go heap at once.

### Memory store — filter first, copy last

The current `Query` calls `copyItem` for every item that passes the filter before sorting. For a
table with 50K items returning 25, 50K maps are copied then 49,975 are discarded.

**Fix:** collect pointers into the live map under `RLock` (safe — the lock is held throughout),
sort and paginate the pointer slice, then copy only the final page:

```go
func (s *MemoryDynamoDBItemStore) Query(_ context.Context, table string, q QuerySpec) ([]map[string]any, string, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    t := s.tables[table]
    if t == nil {
        return []map[string]any{}, "", nil
    }

    type entry struct {
        hash string
        item map[string]any // pointer into live map — safe under RLock
    }

    // Step 1: filter without copying.
    var matched []entry
    for hash, item := range t {
        if !matchesKeyCondition(item, q.KeyConditionExpression,
            q.ExpressionAttributeNames, q.ExpressionAttributeValues) {
            continue
        }
        if q.FilterExpression != "" && !matchesFilter(item, q.FilterExpression,
            q.ExpressionAttributeNames, q.ExpressionAttributeValues) {
            continue
        }
        matched = append(matched, entry{hash: hash, item: item})
    }

    // Step 2: sort for determinism.
    sort.Slice(matched, func(i, j int) bool { return matched[i].hash < matched[j].hash })

    // Step 3: apply pagination cursor.
    start := 0
    if q.ExclusiveStartKey != "" {
        var startKey map[string]any
        if json.Unmarshal([]byte(q.ExclusiveStartKey), &startKey) == nil {
            startHash := itemPKHash(startKey)
            for i, e := range matched {
                if e.hash == startHash {
                    start = i + 1
                    break
                }
            }
        }
    }
    matched = matched[start:]
    if q.Limit > 0 && len(matched) > q.Limit {
        matched = matched[:q.Limit]
    }

    // Step 4: copy only the page we are actually returning.
    result := make([]map[string]any, len(matched))
    for i, e := range matched {
        result[i] = copyItem(e.item)
    }

    var lastKey string
    if q.Limit > 0 && len(result) == q.Limit {
        b, _ := json.Marshal(result[len(result)-1])
        lastKey = string(b)
    }
    return result, lastKey, nil
}
```

**Allocations:** from O(table size) copies down to O(result page) copies. The intermediate
`[]entry` slice holds pointers, not copies — each entry is two words (pointer + string).

The same pattern applies to `Scan`.

---

## Data Flow

### CreateTable

```
Provider: CreateTable
  → Validate: ≤ 20 GSIs, ≤ 5 LSIs  (ValidationException if exceeded)
  → Validate: LSIs require a composite primary key (ValidationException if no SK)
  → Compute pg_suffix = hex16(sha256(tableName))
  → BEGIN transaction (PostgreSQL DDL is transactional — rollback is clean on failure)
      INSERT INTO jc_dynamo_tables (table_name, pg_suffix, schema_json)
      CREATE TABLE jc_dt_{suffix} (...)
      CREATE TABLE jc_dt_{suffix}_gsi (...)   -- always created, even if no GSIs
      CREATE TABLE jc_dt_{suffix}_lsi (...)   -- always created, even if no LSIs
      CREATE INDEX ... (x5 non-PK indexes)
  → COMMIT
  → Store schema in jc_resources (for DescribeTable / ListTables — unchanged)
```

### PutItem

```
Provider: PutItem
  → Load TableSchema from jc_resources (cached in provider)
  → Extract pk_val, sk_val, sk_num from item using schema's PKAttr / SKAttr
  → BEGIN transaction
      1. Upsert main table:
           INSERT INTO jc_dt_{suffix} (pk_val, sk_val, sk_num, item)
           ON CONFLICT (pk_val, sk_val) DO UPDATE SET item=$new, sk_num=$new, updated_at=now()

      2. For each GSI in schema:
           DELETE FROM jc_dt_{suffix}_gsi
           WHERE tbl_pk_val=$pk AND tbl_sk_val=$sk AND index_name=$gsi_name

           IF new item has a value for this GSI's partition key attribute:
               INSERT INTO jc_dt_{suffix}_gsi
                 (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, tbl_pk_val, tbl_sk_val)
           -- If item lacks the GSI pk attribute → sparse index → no row inserted

      3. Same delete+insert for each LSI
  → COMMIT
```

The write-path reverse index on `(tbl_pk_val, tbl_sk_val)` makes the DELETE an index scan (not a
sequential scan), keeping write latency proportional to the number of indexes, not the table size.

### DeleteItem

```
  → BEGIN transaction
      DELETE FROM jc_dt_{suffix}     WHERE pk_val=$pk AND sk_val=$sk
      DELETE FROM jc_dt_{suffix}_gsi WHERE tbl_pk_val=$pk AND tbl_sk_val=$sk
      DELETE FROM jc_dt_{suffix}_lsi WHERE pk_val=$pk AND tbl_sk_val=$sk
  → COMMIT
```

### UpdateItem

```
  → Read old item from main table
  → Apply UpdateExpression to get new item
  → BEGIN transaction
      1. Upsert new item to main table
      2. For each GSI/LSI: delete old index row, insert new if key attrs present
         (same as PutItem step 2/3)
  → COMMIT
```

For efficiency: if none of the updated attributes are index key attributes, skip the GSI/LSI
delete+insert. The write-path is still correct if this optimisation is skipped — it just does
unnecessary deletes and re-inserts.

### UpdateTable — add GSI

```
  → Validate: current GSI count < 20 (LimitExceededException if at cap)
  → BEGIN transaction
      UPDATE jc_dynamo_tables
         SET schema_json = jsonb_set(schema_json, '{GlobalSecondaryIndexes}', $updated_gsi_list)
       WHERE table_name = $t

      -- Backfill existing items into the new GSI. No DDL needed — shared table.
      INSERT INTO jc_dt_{suffix}_gsi (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, tbl_pk_val, tbl_sk_val)
      SELECT $new_gsi_name,
             item->>'$gsi_pk_attr',
             COALESCE(item->>'$gsi_sk_attr', ''),
             CASE WHEN '$gsi_sk_type' = 'N' THEN (item->>'$gsi_sk_attr')::NUMERIC END,
             pk_val, sk_val
      FROM   jc_dt_{suffix}
      WHERE  item ? '$gsi_pk_attr'    -- sparse: skip items without the GSI pk attribute
  → COMMIT
  → Return IndexStatus: ACTIVE  (emulator skips the CREATING transient state)
```

### UpdateTable — delete GSI

```
  → BEGIN transaction
      DELETE FROM jc_dt_{suffix}_gsi WHERE index_name = $gsi_name
      UPDATE jc_dynamo_tables SET schema_json = ... (remove GSI from list)
  → COMMIT
```

### UpdateTable — LSI changes (rejected)

```
  → Return ValidationException: "Local secondary indexes cannot be modified after table creation"
```

### DeleteTable

```
  → BEGIN transaction
      DROP TABLE jc_dt_{suffix}
      DROP TABLE IF EXISTS jc_dt_{suffix}_gsi
      DROP TABLE IF EXISTS jc_dt_{suffix}_lsi
      DELETE FROM jc_dynamo_tables WHERE table_name = $t
  → COMMIT
  → Remove from jc_resources (unchanged)
```

`DROP TABLE` in PostgreSQL is transactional — if anything fails the table survives.

---

## Pagination and LastEvaluatedKey

### Table primary key

`LastEvaluatedKey` is the JSON-encoded last item returned. Resume SQL:

```sql
WHERE pk_val = $pk AND sk_val > $last_sk_val
```

### GSI pagination

DynamoDB's `LastEvaluatedKey` for a GSI includes **four** fields — both the table primary key and
the GSI key — so the caller can resume from the exact position:

```json
{
  "OrderId":    {"S": "o-1234"},
  "CreatedAt":  {"S": "2024-01-15"},
  "CustomerId": {"S": "cust-42"}
}
```

Resume uses a multi-column cursor in the CTE:

```sql
WITH gsi_rows AS MATERIALIZED (
    SELECT tbl_pk_val, tbl_sk_val, gsi_sk_val
    FROM   jc_dt_{suffix}_gsi
    WHERE  index_name = $gsi_name
      AND  gsi_pk_val = $customer_id
      AND  (
               gsi_sk_val > $last_gsi_sk
            OR (gsi_sk_val = $last_gsi_sk AND tbl_pk_val > $last_tbl_pk)
            OR (gsi_sk_val = $last_gsi_sk AND tbl_pk_val = $last_tbl_pk
                AND tbl_sk_val > $last_tbl_sk)
           )
    ORDER  BY gsi_sk_val, tbl_pk_val, tbl_sk_val ASC
    LIMIT  $limit
)
SELECT m.item FROM gsi_rows g JOIN jc_dt_{suffix} m ON ...;
```

For numeric sort key, substitute `gsi_sk_num` with `NULLS LAST`.

---

## Projection Types

DynamoDB controls which attributes index queries return:

| ProjectionType | Returned attributes |
|---|---|
| `ALL` | Every attribute in the item |
| `KEYS_ONLY` | Table primary key + index key attributes only |
| `INCLUDE` | Key attributes + explicit `NonKeyAttributes` list |

Projection is applied at read time using `jsonb_build_object` in the SELECT clause (see
_Projection Pushdown_ above). No extra storage needed — the full item always lives in the main
table, and projection is a view transformation at query time.

---

## Range Operators

The expression evaluation layer must be extended to support the full DynamoDB sort key range
language. All operators must be **numeric-aware**: `"9" > "10"` lexicographically but `9 < 10`
numerically.

| Operator | DynamoDB syntax | In-memory (Go) | SQL (PostgreSQL) |
|---|---|---|---|
| Equal | `sk = :v` | `==` string/float64 | `sk_val = $v` or `sk_num = $v::NUMERIC` |
| Not equal | `sk <> :v` | `!=` | `sk_val <> $v` |
| Less than | `sk < :v` | float64 if type N | `sk_num < $v::NUMERIC` or `sk_val < $v` |
| Less or equal | `sk <= :v` | float64 if type N | `sk_num <= $v::NUMERIC` or `sk_val <= $v` |
| Greater than | `sk > :v` | float64 if type N | `sk_num > $v::NUMERIC` or `sk_val > $v` |
| Greater or equal | `sk >= :v` | float64 if type N | `sk_num >= $v::NUMERIC` or `sk_val >= $v` |
| Between | `sk BETWEEN :lo AND :hi` | float64 if type N | `sk_num BETWEEN $lo AND $hi` |
| Begins with | `begins_with(sk, :p)` | `strings.HasPrefix` | `sk_val LIKE $p || '%'` |

The key type (S/N/B) is known from `AttributeDefinitions` in the table schema, so the query
generator always knows which column and comparison to use.

---

## Bug Fixes Included in This Implementation

These bugs exist in the current codebase and must be fixed as part of this implementation:

### 1. `pk_hash` computed from entire item (not just key attributes)

`hashKey` in `postgres.go` hashes `table + ":" + json.Marshal(item)`, which includes non-key
attributes. Two items with the same primary key but different attribute values hash differently,
causing `PutItem` to insert a duplicate instead of replacing.

**Fix:** hash only the key attributes using the table's `KeySchema`:

```go
func pkHashFromKeyAttrs(item map[string]any, pkAttr, skAttr string) string {
    key := map[string]any{pkAttr: item[pkAttr]}
    if skAttr != "" {
        key[skAttr] = item[skAttr]
    }
    b, _ := json.Marshal(key)
    h := sha256.Sum256(b)
    return fmt.Sprintf("%x", h)
}
```

In the per-table schema this is moot because `pk_val` and `sk_val` are stored as raw values, not
hashes. But the fix is needed for the transition period while the old schema is still in use.

### 2. `DeleteTable` wipes all tables (calls `Reset()`)

`Reset()` does `DELETE FROM jc_dynamodb_items` — deletes items for every DynamoDB table.

**Fix (current schema):**
```sql
DELETE FROM jc_dynamodb_items WHERE table_name = $1
```

**Fix (per-table schema):** `DROP TABLE jc_dt_{suffix}` handles this cleanly.

### 3. Redundant index on `jc_dynamodb_items`

`idx_dynamo_items_table ON jc_dynamodb_items(table_name)` is a prefix of the PK B-tree
`(table_name, pk_hash)`. PostgreSQL maintains two identical B-tree pages for no benefit.

**Fix:** drop the index in migration `012_dynamodb_per_table.sql`.

---

## New Types (`internal/store/aws/dynamodb/schema.go`)

```go
// TableSchema is the parsed form of a CreateTable request.
type TableSchema struct {
    TableName string
    PKAttr    string      // partition key attribute name
    SKAttr    string      // sort key attribute name ("" if no sort key)
    PKType    string      // "S", "N", or "B"
    SKType    string      // "S", "N", or "B"
    GSIs      []IndexDef
    LSIs      []IndexDef
}

// IndexDef describes a single GSI or LSI.
type IndexDef struct {
    IndexName  string
    PKAttr     string
    SKAttr     string      // "" if index has no sort key
    PKType     string
    SKType     string
    Projection ProjectionDef
}

// ProjectionDef controls which attributes index queries return.
type ProjectionDef struct {
    Type        string   // "ALL", "KEYS_ONLY", or "INCLUDE"
    NonKeyAttrs []string // only used when Type = "INCLUDE"
}

// IndexKeyRef is passed in QuerySpec/ScanSpec so the store selects the right
// index table and columns. The provider resolves this from TableSchema before
// calling the store.
type IndexKeyRef struct {
    IndexName string
    PKAttr    string
    SKAttr    string
    PKType    string
    SKType    string
    IsLSI     bool
}
```

### Extended `QuerySpec` and `ScanSpec`

```go
type QuerySpec struct {
    IndexName                 string
    IndexSchema               *IndexKeyRef  // nil = query the table primary key
    ScanIndexForward          bool
    ProjectionAttrs           []string      // nil = ALL; populated by provider for KEYS_ONLY/INCLUDE
    KeyConditionExpression    string
    FilterExpression          string
    ExpressionAttributeNames  map[string]string
    ExpressionAttributeValues map[string]any
    ExclusiveStartKey         string
    Limit                     int
}
```

---

## File Change Map

| File | Change | Description |
|---|---|---|
| `internal/store/migrations/012_dynamodb_per_table.sql` | new | `jc_dynamo_tables` metadata table; drop redundant `idx_dynamo_items_table` |
| `internal/store/aws/dynamodb/schema.go` | new | `TableSchema`, `IndexDef`, `ProjectionDef`, `IndexKeyRef` types |
| `internal/store/aws/dynamodb/store.go` | modify | Add `CreateTableSchema`, `DropTableSchema`, `AddGSI`, `DeleteGSI` to interface; extend `QuerySpec`/`ScanSpec` with `IndexSchema` and `ProjectionAttrs` |
| `internal/store/aws/dynamodb/postgres.go` | modify | Per-table DDL; transactional write amplification; MATERIALIZED CTE queries; streaming row scan; projection pushdown; fix `pk_hash` bug; fix `DeleteTable` scope |
| `internal/store/aws/dynamodb/memory.go` | modify | Schema registry; GSI maps; index routing in Query/Scan; filter-first copy-last pattern |
| `internal/store/aws/dynamodb/expressions.go` | new | Extracted expression evaluator; add `>`, `<`, `>=`, `<=`, `BETWEEN` with numeric awareness |
| `internal/provider/table/table.go` | modify | Limit enforcement; pass `TableSchema` on writes; resolve `IndexKeyRef` on reads; projection list computation; `UpdateTable` GSI management |

---

## Implementation Phases

| Phase | Scope | Risk | Test signal |
|---|---|---|---|
| 1 | Schema types + `jc_dynamo_tables` migration + `CreateTableSchema`/`DropTableSchema` in both stores | Low — additive only | `CreateTable` creates per-table PG tables; `DeleteTable` drops them |
| 2 | Fix `pk_hash` bug + fix `DeleteTable` scope | Low | Existing integration tests pass; no cross-table corruption |
| 3 | Write amplification in Postgres store (transactional PutItem/DeleteItem/UpdateItem) | Medium — transaction correctness | GSI/LSI rows appear after PutItem |
| 4 | Write amplification in memory store | Low | Unit tests pass for write path |
| 5 | Query/Scan index routing + MATERIALIZED CTE + `LastEvaluatedKey` for indexes | Medium — query semantics change | GSI/LSI queries return correct results and paginate correctly |
| 6 | Streaming row scan (`filterRows` early-break) + memory store filter-first | Low | No functional change; memory usage drops |
| 7 | Range operators pushed to SQL; expression layer extended | Low–Medium | BETWEEN, >, < work on sort keys |
| 8 | Projection pushdown (`jsonb_build_object`) | Low | KEYS_ONLY/INCLUDE return correct attribute sets |
| 9 | Limit enforcement + `UpdateTable` GSI add/delete with backfill | Medium | Full `UpdateTable` support |

---

## Testing Plan

### Unit tests (memory store — no database required)

- `TestCreateTableSchema_RegistersSchema` — schema stored after `CreateTable`
- `TestPutItem_UpdatesGSIMap` — GSI map correct after `PutItem`
- `TestPutItem_SparseGSI_SkipsItemWithoutGSIPK` — item without GSI pk attr not in index
- `TestQuery_GSI_ReturnsCorrectItems` — query with `IndexName` uses GSI key schema
- `TestQuery_LSI_BETWEEN_RangeOperator` — BETWEEN on LSI sort key returns correct items
- `TestQuery_FilterFirst_NoCopyLeak` — verify only page-sized allocations (benchmark)
- `TestUpdateTable_AddGSI_RejectsOverLimit` — 21st GSI returns `LimitExceededException`
- `TestUpdateTable_RejectsLSIChanges` — `UpdateTable` with LSI returns `ValidationException`
- `TestDeleteTable_DoesNotWipeOtherTables` — targeted delete, not Reset()

### Integration tests (full mode — requires PostgreSQL)

- `TestGSI_PutQuery_StringPK` — put items, query by GSI string partition key
- `TestGSI_QuerySortKeyRange_BETWEEN` — numeric BETWEEN on GSI sort key
- `TestGSI_SparseIndex_MissingAttrExcluded` — items without GSI PK absent from index
- `TestGSI_Pagination_LastEvaluatedKey` — multi-page GSI query resumes correctly
- `TestGSI_UpdateItem_NonKeyChange_NoIndexRebuild` — non-key update does not change GSI row
- `TestGSI_UpdateItem_KeyChange_RebuildsIndexRow` — GSI pk attr change updates index
- `TestLSI_SortKeyOverride` — LSI query returns different order than table query
- `TestLSI_BETWEEN` — LSI sort key BETWEEN query
- `TestUpdateTable_AddGSI_BackfillsExistingItems` — existing items appear in new GSI
- `TestUpdateTable_DeleteGSI_PurgesRows` — deleted GSI rows removed
- `TestProjection_KeysOnly` — returns only key attributes
- `TestProjection_Include_CorrectAttrs` — returns specified non-key attributes
- `TestDeleteTable_Isolated` — delete one table does not affect items in other tables

---

## Worked Example

### Setup

```python
import boto3

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

table = dynamodb.create_table(
    TableName="Orders",
    KeySchema=[
        {"AttributeName": "CustomerId", "KeyType": "HASH"},
        {"AttributeName": "OrderId",    "KeyType": "RANGE"},
    ],
    AttributeDefinitions=[
        {"AttributeName": "CustomerId", "AttributeType": "S"},
        {"AttributeName": "OrderId",    "AttributeType": "S"},
        {"AttributeName": "Status",     "AttributeType": "S"},
        {"AttributeName": "CreatedAt",  "AttributeType": "S"},
    ],
    GlobalSecondaryIndexes=[{
        "IndexName": "byStatus",
        "KeySchema": [
            {"AttributeName": "Status",    "KeyType": "HASH"},
            {"AttributeName": "CreatedAt", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    }],
    LocalSecondaryIndexes=[{
        "IndexName": "byDate",
        "KeySchema": [
            {"AttributeName": "CustomerId", "KeyType": "HASH"},
            {"AttributeName": "CreatedAt",  "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "KEYS_ONLY"},
    }],
    BillingMode="PAY_PER_REQUEST",
)
```

**What happens in PostgreSQL (single transaction):**

```sql
INSERT INTO jc_dynamo_tables VALUES ('Orders', 'a3f1c8d294e05b71', '{...schema...}', now());
CREATE TABLE jc_dt_a3f1c8d294e05b71 (pk_val TEXT NOT NULL, sk_val TEXT NOT NULL DEFAULT '', ...);
CREATE TABLE jc_dt_a3f1c8d294e05b71_gsi (index_name TEXT NOT NULL, gsi_pk_val TEXT NOT NULL, ...);
CREATE TABLE jc_dt_a3f1c8d294e05b71_lsi (index_name TEXT NOT NULL, pk_val TEXT NOT NULL, ...);
-- + 5 non-PK indexes
```

### Put an item

```python
table.put_item(Item={
    "CustomerId": "cust-42",
    "OrderId":    "order-99",
    "Status":     "SHIPPED",
    "CreatedAt":  "2024-03-15",
    "Total":      "129.99",
})
```

**SQL (single transaction):**

```sql
-- 1. Main table
INSERT INTO jc_dt_a3f1c8d294e05b71 (pk_val, sk_val, sk_num, item)
VALUES ('cust-42', 'order-99', NULL, '{"CustomerId":...}')
ON CONFLICT (pk_val, sk_val) DO UPDATE SET item=EXCLUDED.item, updated_at=now();

-- 2. GSI "byStatus" — Status present, non-sparse
DELETE FROM jc_dt_a3f1c8d294e05b71_gsi
WHERE index_name='byStatus' AND tbl_pk_val='cust-42' AND tbl_sk_val='order-99';

INSERT INTO jc_dt_a3f1c8d294e05b71_gsi
  (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, tbl_pk_val, tbl_sk_val)
VALUES ('byStatus', 'SHIPPED', '2024-03-15', NULL, 'cust-42', 'order-99');

-- 3. LSI "byDate" — same PK, different SK
DELETE FROM jc_dt_a3f1c8d294e05b71_lsi
WHERE index_name='byDate' AND pk_val='cust-42' AND tbl_sk_val='order-99';

INSERT INTO jc_dt_a3f1c8d294e05b71_lsi
  (index_name, pk_val, lsi_sk_val, lsi_sk_num, tbl_sk_val)
VALUES ('byDate', 'cust-42', '2024-03-15', NULL, 'order-99');
```

### Query by GSI

```python
response = table.query(
    IndexName="byStatus",
    KeyConditionExpression="#s = :s AND CreatedAt >= :d",
    ExpressionAttributeNames={"#s": "Status"},
    ExpressionAttributeValues={":s": "SHIPPED", ":d": "2024-03-01"},
)
```

**SQL (MATERIALIZED CTE — index scan first, join second):**

```sql
WITH gsi_rows AS MATERIALIZED (
    SELECT tbl_pk_val, tbl_sk_val, gsi_sk_val
    FROM   jc_dt_a3f1c8d294e05b71_gsi
    WHERE  index_name = 'byStatus'
      AND  gsi_pk_val = 'SHIPPED'
      AND  gsi_sk_val >= '2024-03-01'
    ORDER  BY gsi_sk_val ASC
    LIMIT  100
)
SELECT m.item
FROM   gsi_rows g
JOIN   jc_dt_a3f1c8d294e05b71 m ON m.pk_val = g.tbl_pk_val AND m.sk_val = g.tbl_sk_val;
```

The CTE resolves the index with an index scan (no sort pass needed — the PK B-tree is already
sorted by `gsi_sk_val`). The JOIN touches at most `Limit` rows of the main table.

### Query by LSI with BETWEEN

```python
response = table.query(
    IndexName="byDate",
    KeyConditionExpression="CustomerId = :c AND CreatedAt BETWEEN :start AND :end",
    ExpressionAttributeValues={
        ":c":     "cust-42",
        ":start": "2024-03-01",
        ":end":   "2024-03-31",
    },
)
```

**SQL:**

```sql
WITH lsi_rows AS MATERIALIZED (
    SELECT pk_val, tbl_sk_val
    FROM   jc_dt_a3f1c8d294e05b71_lsi
    WHERE  index_name  = 'byDate'
      AND  pk_val      = 'cust-42'
      AND  lsi_sk_val BETWEEN '2024-03-01' AND '2024-03-31'
    ORDER  BY lsi_sk_val ASC
    LIMIT  100
)
SELECT jsonb_build_object(          -- ProjectionType=KEYS_ONLY: only key attrs returned
    'CustomerId', m.item->'CustomerId',
    'OrderId',    m.item->'OrderId',
    'CreatedAt',  m.item->'CreatedAt'
) AS item
FROM   lsi_rows l
JOIN   jc_dt_a3f1c8d294e05b71 m ON m.pk_val = l.pk_val AND m.sk_val = l.tbl_sk_val;
```

The `jsonb_build_object` projection runs inside PostgreSQL — only the three requested attributes
are sent over the wire and decoded into Go maps, not the full item.
