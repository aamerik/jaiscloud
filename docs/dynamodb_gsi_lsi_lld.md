# DynamoDB GSI/LSI — Low-Level Design and Implementation Plan

> **Prerequisite reading:** [dynamodb_gsi_lsi_design.md](dynamodb_gsi_lsi_design.md) explains the
> background, schema rationale, and architecture decisions. This document focuses on exact Go
> signatures, SQL statements, and a step-by-step task plan.

---

## 1. New File: `internal/store/aws/dynamodb/schema.go`

Create this file from scratch. It holds all types shared between the store and provider layers.

```go
package dynamodb

// TableSchema is the parsed, normalised form of a DynamoDB table definition.
// It is built by the provider at CreateTable time, stored in jc_resources as JSON,
// and loaded on every data-plane call so the store knows which attributes are key columns.
type TableSchema struct {
    TableName string     // e.g. "Orders"
    PKAttr    string     // partition key attribute name, e.g. "CustomerId"
    SKAttr    string     // sort key attribute name; "" if table has no sort key
    PKType    string     // "S", "N", or "B"
    SKType    string     // "S", "N", or "B"; "" if no sort key
    GSIs      []IndexDef // up to 20
    LSIs      []IndexDef // up to 5; immutable after CreateTable
}

// IndexDef describes a single GSI or LSI.
type IndexDef struct {
    IndexName  string
    PKAttr     string // index partition key attribute name
    SKAttr     string // index sort key attribute name; "" if index has no sort key
    PKType     string // "S", "N", or "B"
    SKType     string // "S", "N", or "B"; "" if no index sort key
    Projection ProjectionDef
    IsLSI      bool // true for LocalSecondaryIndex, false for GlobalSecondaryIndex
}

// ProjectionDef controls which attributes an index query returns.
type ProjectionDef struct {
    // Type is "ALL", "KEYS_ONLY", or "INCLUDE".
    // "ALL" returns every attribute; "KEYS_ONLY" returns only the table and index key
    // attributes; "INCLUDE" returns key attributes plus NonKeyAttrs.
    Type        string
    NonKeyAttrs []string // only meaningful when Type == "INCLUDE"
}

// IndexKeyRef is resolved by the provider from TableSchema and passed in QuerySpec / ScanSpec
// so the store can select the right index table and columns without re-reading jc_resources.
type IndexKeyRef struct {
    IndexName string // e.g. "byStatus"
    PKAttr    string // index partition key attribute name
    SKAttr    string // index sort key attribute name
    PKType    string // "S", "N", "B"
    SKType    string // "S", "N", "B"
    IsLSI     bool   // routes to jc_dt_{suffix}_lsi instead of _gsi
}

// AttrVal extracts the raw string value from a DynamoDB typed attribute map.
// Input: {"S": "hello"} → "hello"
// Input: {"N": "42"}    → "42"
// Input: {"B": "..."}   → base64-encoded bytes as string
// Returns ("", false) if the map does not match any known type.
func AttrVal(v any) (string, bool) {
    m, ok := v.(map[string]any)
    if !ok {
        return "", false
    }
    for _, t := range []string{"S", "N", "B"} {
        if s, ok := m[t].(string); ok {
            return s, true
        }
    }
    return "", false
}

// AttrType returns "S", "N", "B", or "" for an unknown/nil attribute.
func AttrType(v any) string {
    m, ok := v.(map[string]any)
    if !ok {
        return ""
    }
    for _, t := range []string{"S", "N", "B"} {
        if _, ok := m[t]; ok {
            return t
        }
    }
    return ""
}

// ParseNumeric converts a DynamoDB number string to float64.
// Returns (0, false) if v is not a {"N": "..."} attribute.
func ParseNumeric(v any) (float64, bool) {
    m, ok := v.(map[string]any)
    if !ok {
        return 0, false
    }
    s, ok := m["N"].(string)
    if !ok {
        return 0, false
    }
    var f float64
    _, err := fmt.Sscanf(s, "%f", &f)
    return f, err == nil
}
```

---

## 2. Modified File: `internal/store/aws/dynamodb/store.go`

### 2a. Extend `QuerySpec`

```go
// QuerySpec describes a DynamoDB Query operation.
type QuerySpec struct {
    // IndexSchema is nil when querying the table's primary key.
    // Set by the provider after resolving the IndexName from TableSchema.
    IndexSchema *IndexKeyRef

    // ProjectionAttrs is nil for ProjectionType=ALL.
    // For KEYS_ONLY or INCLUDE, the provider populates this list and the store
    // uses jsonb_build_object to return only these attributes.
    ProjectionAttrs []string

    IndexName                 string
    KeyConditionExpression    string
    FilterExpression          string
    ExpressionAttributeNames  map[string]string
    ExpressionAttributeValues map[string]any
    ScanIndexForward          bool
    Limit                     int
    ExclusiveStartKey         string // JSON-encoded map of key attributes
    Select                    string
}
```

### 2b. Extend `ScanSpec`

```go
// ScanSpec describes a DynamoDB Scan operation.
type ScanSpec struct {
    IndexSchema     *IndexKeyRef // nil = scan the main table
    ProjectionAttrs []string     // nil = return full item

    IndexName                 string
    FilterExpression          string
    ExpressionAttributeNames  map[string]string
    ExpressionAttributeValues map[string]any
    Limit                     int
    ExclusiveStartKey         string
    Select                    string
}
```

### 2c. Extend `BatchWriteRequest`

```go
// BatchWriteRequest is a single put or delete within a BatchWriteItem call.
type BatchWriteRequest struct {
    Table      string
    Schema     *TableSchema   // NEW: needed so the store can maintain index tables
    PutItem    map[string]any
    PutHash    string
    DeleteKey  map[string]any
    DeleteHash string
}
```

### 2d. Extend `DynamoDBItemStore` interface

Add the four lifecycle methods below to the existing interface. All existing method signatures
remain unchanged.

```go
type DynamoDBItemStore interface {
    // ── Existing data-plane methods (signatures unchanged) ───────────────────
    PutItem(ctx context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error)
    GetItem(ctx context.Context, table, pkHash string) (map[string]any, error)
    DeleteItem(ctx context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error)
    UpdateItem(ctx context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error)
    Query(ctx context.Context, table string, q QuerySpec) ([]map[string]any, string, error)
    Scan(ctx context.Context, table string, s ScanSpec) ([]map[string]any, string, error)
    BatchWriteItems(ctx context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error)
    BatchGetItems(ctx context.Context, reqs []BatchGetRequest) (map[string][]map[string]any, error)
    Reset()

    // ── New table-lifecycle methods ──────────────────────────────────────────

    // CreateTableSchema creates the per-table PostgreSQL tables and indexes.
    // For the memory store this registers the schema so index maps are initialised.
    // Called by TableProvider.CreateTable inside the same logical transaction.
    CreateTableSchema(ctx context.Context, schema TableSchema) error

    // DropTableSchema drops the per-table PostgreSQL tables.
    // For the memory store this removes the schema and all in-memory index data.
    // Called by TableProvider.DeleteTable.
    DropTableSchema(ctx context.Context, tableName string) error

    // AddGSI creates the index rows for a new GSI by backfilling from the main table.
    // No DDL is needed — rows are inserted into the shared _gsi table with the new index_name.
    // Called by TableProvider.UpdateTable when a GSI is being created.
    AddGSI(ctx context.Context, tableName string, schema TableSchema, idx IndexDef) error

    // DeleteGSI removes all rows for the named GSI from the shared _gsi table.
    // Called by TableProvider.UpdateTable when a GSI is being deleted.
    DeleteGSI(ctx context.Context, tableName string, schema TableSchema, indexName string) error
}
```

### 2e. Add `WriteContext` — passed on every data-plane write

To maintain index tables on every `PutItem`/`DeleteItem`/`UpdateItem`, the store needs the full
`TableSchema`. Rather than changing the existing method signatures (breaking change), pass it via
a new optional field on each write spec.

**Alternative approach (simpler, chosen):** Add `Schema *TableSchema` to `ConditionSpec` and
`UpdateSpec`, and add a new `ItemWriteContext` passed to `PutItem`/`DeleteItem`.

Actually, the cleanest approach without changing existing signatures is to add a single optional
field to `ConditionSpec` which is already passed to PutItem/DeleteItem, and a separate `WriteContext`
embedded in `UpdateSpec`. This avoids signature changes and stays backward compatible.

```go
// ConditionSpec carries optional condition checking fields shared by PutItem and DeleteItem.
type ConditionSpec struct {
    ConditionExpression       string
    ExpressionAttributeNames  map[string]string
    ExpressionAttributeValues map[string]any
    ReturnValues              string

    // Schema is required for index maintenance. When nil, index tables are not updated.
    // The provider must always set this field.
    Schema *TableSchema
}

// UpdateSpec describes a DynamoDB UpdateItem operation.
type UpdateSpec struct {
    UpdateExpression          string
    ConditionExpression       string
    ExpressionAttributeNames  map[string]string
    ExpressionAttributeValues map[string]any
    ReturnValues              string

    // Schema is required for index maintenance.
    Schema *TableSchema
}
```

---

## 3. New File: `internal/store/aws/dynamodb/expressions.go`

Extract the expression evaluation code from `memory.go` into this dedicated file and add the
missing range operators.

### 3a. Function signatures (public)

```go
// MatchesKeyCondition evaluates a DynamoDB KeyConditionExpression against a single item.
// It handles: =, <>, <, <=, >, >=, BETWEEN, begins_with, attribute_exists, attribute_not_exists.
// Comparisons are numeric-aware: if the attribute type is "N", values are compared as float64.
// attrTypes maps attribute name → "S"|"N"|"B" from the table's AttributeDefinitions.
func MatchesKeyCondition(
    item map[string]any,
    expr string,
    names map[string]string,
    values map[string]any,
    attrTypes map[string]string,
) bool

// MatchesFilter evaluates a DynamoDB FilterExpression against a single item.
// Uses the same engine as MatchesKeyCondition.
func MatchesFilter(
    item map[string]any,
    expr string,
    names map[string]string,
    values map[string]any,
    attrTypes map[string]string,
) bool

// ApplyUpdateExpression applies SET / REMOVE / ADD clauses to item in-place.
func ApplyUpdateExpression(
    item map[string]any,
    expr string,
    names map[string]string,
    values map[string]any,
)
```

### 3b. Internal `evalCondition` — new operators

The existing `evalCondition` handles only `=` and `begins_with`. Extend it to cover:

```go
func evalCondition(item map[string]any, cond string, names map[string]string, values map[string]any, attrTypes map[string]string) bool {
    // ... existing attribute_exists / attribute_not_exists / begins_with handlers ...

    // BETWEEN: "sk BETWEEN :lo AND :hi"
    if upper contains " BETWEEN " && upper contains " AND " {
        attr := resolveExprName(left, names)
        lo := resolveExprValue(loRef, values)
        hi := resolveExprValue(hiRef, values)
        itemVal := item[attr]
        if attrTypes[attr] == "N" {
            return toFloat(itemVal) >= toFloat(lo) && toFloat(itemVal) <= toFloat(hi)
        }
        ls, _ := AttrVal(lo)
        hs, _ := AttrVal(hi)
        is, _ := AttrVal(itemVal)
        return is >= ls && is <= hs
    }

    // Comparison operators: <>, <, <=, >, >=
    for _, op := range []string{" <> ", " <= ", " >= ", " < ", " > "} {
        if strings.Contains(cond, op) {
            parts := strings.SplitN(cond, op, 2)
            attr := resolveExprName(strings.TrimSpace(parts[0]), names)
            val  := resolveExprValue(strings.TrimSpace(parts[1]), values)
            itemVal := item[attr]
            if attrTypes[attr] == "N" {
                a, b := toFloat(itemVal), toFloat(val)
                switch strings.TrimSpace(op) {
                case "<>": return a != b
                case "<":  return a < b
                case "<=": return a <= b
                case ">":  return a > b
                case ">=": return a >= b
                }
            }
            as, _ := AttrVal(itemVal)
            bs, _ := AttrVal(val)
            switch strings.TrimSpace(op) {
            case "<>": return as != bs
            case "<":  return as < bs
            case "<=": return as <= bs
            case ">":  return as > bs
            case ">=": return as >= bs
            }
        }
    }

    // existing "=" handler ...
}
```

---

## 4. Modified File: `internal/store/aws/dynamodb/memory.go`

### 4a. New data structures

```go
// MemoryDynamoDBItemStore is an in-memory DynamoDBItemStore.
type MemoryDynamoDBItemStore struct {
    mu      sync.RWMutex
    schemas map[string]TableSchema                        // tableName → schema
    items   map[string]map[string]map[string]any          // tableName → pkHash → item (unchanged)
    gsiIdx  map[string]map[string]map[string][]string     // tableName → indexName → gsiPKVal → []pkHash
}

func NewMemoryDynamoDBItemStore() *MemoryDynamoDBItemStore {
    return &MemoryDynamoDBItemStore{
        schemas: make(map[string]TableSchema),
        items:   make(map[string]map[string]map[string]any),
        gsiIdx:  make(map[string]map[string]map[string][]string),
    }
}
```

`gsiIdx[table][indexName][gsiPKValue]` holds a slice of `pkHash` values in the main `items` map.
This lets `Query` with an `IndexSchema` skip items whose GSI partition key doesn't match, reducing
the scan from O(table) to O(matching partition).

LSIs share the table PK so no separate `lsiIdx` map is needed — the existing `items` map is scanned
filtered by partition key, then re-sorted by LSI sort key.

### 4b. `CreateTableSchema`

```go
func (s *MemoryDynamoDBItemStore) CreateTableSchema(_ context.Context, schema TableSchema) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.schemas[schema.TableName] = schema
    if s.items[schema.TableName] == nil {
        s.items[schema.TableName] = make(map[string]map[string]any)
    }
    s.gsiIdx[schema.TableName] = make(map[string]map[string][]string)
    for _, gsi := range schema.GSIs {
        s.gsiIdx[schema.TableName][gsi.IndexName] = make(map[string][]string)
    }
    return nil
}
```

### 4c. `DropTableSchema`

```go
func (s *MemoryDynamoDBItemStore) DropTableSchema(_ context.Context, tableName string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.schemas, tableName)
    delete(s.items, tableName)
    delete(s.gsiIdx, tableName)
    return nil
}
```

### 4d. `PutItem` — update GSI index maps

The provider passes `cond.Schema`. After writing the item, update `gsiIdx`:

```go
func (s *MemoryDynamoDBItemStore) PutItem(_ context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    t := s.tableMap(table)

    // ... existing condition check logic unchanged ...

    // Remove old GSI entries for this pkHash (if item already existed).
    if old, exists := t[pkHash]; exists && cond.Schema != nil {
        s.removeGSIEntries(table, pkHash, old, cond.Schema)
    }

    t[pkHash] = copyItem(item)

    // Add new GSI entries.
    if cond.Schema != nil {
        s.addGSIEntries(table, pkHash, item, cond.Schema)
    }

    // ... existing ReturnValues logic ...
}

// removeGSIEntries removes pkHash from all GSI index slices where the old item appeared.
func (s *MemoryDynamoDBItemStore) removeGSIEntries(table, pkHash string, old map[string]any, schema *TableSchema) {
    idx := s.gsiIdx[table]
    if idx == nil {
        return
    }
    for _, gsi := range schema.GSIs {
        gsiPKVal, ok := AttrVal(old[gsi.PKAttr])
        if !ok {
            continue
        }
        bucket := idx[gsi.IndexName][gsiPKVal]
        for i, h := range bucket {
            if h == pkHash {
                idx[gsi.IndexName][gsiPKVal] = append(bucket[:i], bucket[i+1:]...)
                break
            }
        }
    }
}

// addGSIEntries adds pkHash to the GSI index bucket for each GSI whose PK attribute is present.
func (s *MemoryDynamoDBItemStore) addGSIEntries(table, pkHash string, item map[string]any, schema *TableSchema) {
    if s.gsiIdx[table] == nil {
        s.gsiIdx[table] = make(map[string]map[string][]string)
    }
    for _, gsi := range schema.GSIs {
        gsiPKVal, ok := AttrVal(item[gsi.PKAttr])
        if !ok {
            continue // sparse index — item lacks this GSI's PK attribute
        }
        if s.gsiIdx[table][gsi.IndexName] == nil {
            s.gsiIdx[table][gsi.IndexName] = make(map[string][]string)
        }
        s.gsiIdx[table][gsi.IndexName][gsiPKVal] = append(
            s.gsiIdx[table][gsi.IndexName][gsiPKVal], pkHash,
        )
    }
}
```

### 4e. `DeleteItem` — remove from GSI index maps

```go
func (s *MemoryDynamoDBItemStore) DeleteItem(_ context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    t := s.tables[table]
    oldItem := t[pkHash]
    // ... existing condition check ...

    if cond.Schema != nil && oldItem != nil {
        s.removeGSIEntries(table, pkHash, oldItem, cond.Schema)
    }
    delete(t, pkHash)
    // ... existing ReturnValues logic ...
}
```

### 4f. `Query` — index routing

```go
func (s *MemoryDynamoDBItemStore) Query(_ context.Context, table string, q QuerySpec) ([]map[string]any, string, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    schema := s.schemas[table]
    t := s.items[table]
    if t == nil {
        return []map[string]any{}, "", nil
    }

    // Collect candidate pkHashes to visit.
    var candidates []string
    if q.IndexSchema != nil && !q.IndexSchema.IsLSI {
        // GSI: use index map to narrow candidates to items matching the GSI partition key.
        gsiPKVal := extractConditionValue(q.KeyConditionExpression, q.IndexSchema.PKAttr,
            q.ExpressionAttributeNames, q.ExpressionAttributeValues)
        if gsiPKVal != "" {
            if idx := s.gsiIdx[table][q.IndexSchema.IndexName]; idx != nil {
                candidates = idx[gsiPKVal]
            }
        }
    } else {
        // Table PK or LSI: visit all items (LSI filtered below by pk_val equality).
        for h := range t {
            candidates = append(candidates, h)
        }
    }

    attrTypes := buildAttrTypes(schema)

    type entry struct {
        hash string
        item map[string]any
    }
    var matched []entry
    for _, h := range candidates {
        item := t[h]
        if item == nil {
            continue
        }
        if !MatchesKeyCondition(item, q.KeyConditionExpression,
            q.ExpressionAttributeNames, q.ExpressionAttributeValues, attrTypes) {
            continue
        }
        if q.FilterExpression != "" && !MatchesFilter(item, q.FilterExpression,
            q.ExpressionAttributeNames, q.ExpressionAttributeValues, attrTypes) {
            continue
        }
        matched = append(matched, entry{hash: h, item: item})
    }

    // Sort: table PK uses pkHash order; indexes sort by index SK attribute.
    sortKey := func(e entry) string {
        if q.IndexSchema != nil && q.IndexSchema.SKAttr != "" {
            v, _ := AttrVal(e.item[q.IndexSchema.SKAttr])
            return v
        }
        return e.hash
    }
    sort.Slice(matched, func(i, j int) bool {
        si, sj := sortKey(matched[i]), sortKey(matched[j])
        if q.ScanIndexForward {
            return si < sj
        }
        return si > sj
    })

    // Pagination cursor.
    start := findCursorPosition(matched, q.ExclusiveStartKey, q.IndexSchema)
    matched = matched[start:]
    if q.Limit > 0 && len(matched) > q.Limit {
        matched = matched[:q.Limit]
    }

    // Copy only the returned page; apply projection.
    result := make([]map[string]any, len(matched))
    for i, e := range matched {
        result[i] = applyProjection(copyItem(e.item), q.ProjectionAttrs)
    }

    var lastKey string
    if q.Limit > 0 && len(result) == q.Limit {
        lastKey = encodeLastKey(result[len(result)-1], schema, q.IndexSchema)
    }
    return result, lastKey, nil
}
```

Helper `buildAttrTypes` builds `map[attrName]type` from `TableSchema.AttributeDefinitions`-equivalent
data stored in `schema`. It is used by the expression evaluator for numeric-aware comparisons.

### 4g. `AddGSI` / `DeleteGSI`

```go
func (s *MemoryDynamoDBItemStore) AddGSI(_ context.Context, tableName string, schema TableSchema, idx IndexDef) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.gsiIdx[tableName] == nil {
        s.gsiIdx[tableName] = make(map[string]map[string][]string)
    }
    bucket := make(map[string][]string)
    // Backfill from existing items.
    for pkHash, item := range s.items[tableName] {
        gsiPKVal, ok := AttrVal(item[idx.PKAttr])
        if !ok {
            continue
        }
        bucket[gsiPKVal] = append(bucket[gsiPKVal], pkHash)
    }
    s.gsiIdx[tableName][idx.IndexName] = bucket
    return nil
}

func (s *MemoryDynamoDBItemStore) DeleteGSI(_ context.Context, tableName string, _ TableSchema, indexName string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if idx := s.gsiIdx[tableName]; idx != nil {
        delete(idx, indexName)
    }
    return nil
}
```

### 4h. `Reset`

```go
func (s *MemoryDynamoDBItemStore) Reset() {
    s.mu.Lock()
    defer s.mu.Unlock()
    // Re-initialise everything; keep schema registrations (tables still exist).
    for table := range s.items {
        s.items[table] = make(map[string]map[string]any)
    }
    for table, schema := range s.schemas {
        s.gsiIdx[table] = make(map[string]map[string][]string)
        for _, gsi := range schema.GSIs {
            s.gsiIdx[table][gsi.IndexName] = make(map[string][]string)
        }
    }
}
```

---

## 5. Modified File: `internal/store/aws/dynamodb/postgres.go`

### 5a. Helper: `pgSuffix`

```go
import "crypto/sha256"
import "fmt"

// pgSuffix returns a 16-character lowercase hex string derived from the DynamoDB table name.
// Used to name per-table PostgreSQL tables: jc_dt_{suffix}, jc_dt_{suffix}_gsi, jc_dt_{suffix}_lsi.
func pgSuffix(tableName string) string {
    h := sha256.Sum256([]byte(tableName))
    return fmt.Sprintf("%x", h[:8]) // 8 bytes = 16 hex chars
}
```

### 5b. Helper: `pgTableName`, `pgGSITableName`, `pgLSITableName`

```go
func pgTableName(tableName string) string    { return "jc_dt_" + pgSuffix(tableName) }
func pgGSITableName(tableName string) string { return "jc_dt_" + pgSuffix(tableName) + "_gsi" }
func pgLSITableName(tableName string) string { return "jc_dt_" + pgSuffix(tableName) + "_lsi" }
```

### 5c. Helper: `skNumExpr`

```go
// skNumExpr parses a DynamoDB number string to *float64 (nil if skType != "N" or value absent).
func skNumExpr(item map[string]any, attrName, skType string) *float64 {
    if skType != "N" || attrName == "" {
        return nil
    }
    v, ok := item[attrName]
    if !ok {
        return nil
    }
    f, ok := ParseNumeric(v)
    if !ok {
        return nil
    }
    return &f
}
```

### 5d. `CreateTableSchema`

```go
func (s *PostgresDynamoDBItemStore) CreateTableSchema(ctx context.Context, schema TableSchema) error {
    main := pgTableName(schema.TableName)
    gsiT := pgGSITableName(schema.TableName)
    lsiT := pgLSITableName(schema.TableName)

    _, err := s.pool.Exec(ctx, fmt.Sprintf(`
        -- Record mapping
        INSERT INTO jc_dynamo_tables (table_name, pg_suffix, schema_json)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (table_name) DO NOTHING;

        -- Main items table
        CREATE TABLE IF NOT EXISTS %s (
            pk_val     TEXT        NOT NULL,
            sk_val     TEXT        NOT NULL DEFAULT '',
            sk_num     NUMERIC,
            item       JSONB       NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (pk_val, sk_val)
        );
        CREATE INDEX IF NOT EXISTS ON %s (pk_val, sk_num) WHERE sk_num IS NOT NULL;

        -- Shared GSI table
        CREATE TABLE IF NOT EXISTS %s (
            index_name TEXT NOT NULL,
            gsi_pk_val TEXT NOT NULL,
            gsi_sk_val TEXT NOT NULL DEFAULT '',
            gsi_sk_num NUMERIC,
            tbl_pk_val TEXT NOT NULL,
            tbl_sk_val TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (index_name, gsi_pk_val, gsi_sk_val, tbl_pk_val, tbl_sk_val)
        );
        CREATE INDEX IF NOT EXISTS ON %s (index_name, gsi_pk_val, gsi_sk_num) WHERE gsi_sk_num IS NOT NULL;
        CREATE INDEX IF NOT EXISTS ON %s (tbl_pk_val, tbl_sk_val);

        -- Shared LSI table
        CREATE TABLE IF NOT EXISTS %s (
            index_name TEXT NOT NULL,
            pk_val     TEXT NOT NULL,
            lsi_sk_val TEXT NOT NULL DEFAULT '',
            lsi_sk_num NUMERIC,
            tbl_sk_val TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (index_name, pk_val, lsi_sk_val, tbl_sk_val)
        );
        CREATE INDEX IF NOT EXISTS ON %s (index_name, pk_val, lsi_sk_num) WHERE lsi_sk_num IS NOT NULL;
        CREATE INDEX IF NOT EXISTS ON %s (pk_val, tbl_sk_val);
    `, main, main,
        gsiT, gsiT, gsiT,
        lsiT, lsiT, lsiT,
    ), schema.TableName, pgSuffix(schema.TableName), schemaJSON(schema))
    return err
}
```

Note: PostgreSQL DDL is transactional so if the caller wraps this in a transaction the entire
`CreateTable` operation (metadata + DDL) rolls back atomically on error.

### 5e. `DropTableSchema`

```go
func (s *PostgresDynamoDBItemStore) DropTableSchema(ctx context.Context, tableName string) error {
    main := pgTableName(tableName)
    gsiT := pgGSITableName(tableName)
    lsiT := pgLSITableName(tableName)
    _, err := s.pool.Exec(ctx, fmt.Sprintf(`
        DROP TABLE IF EXISTS %s;
        DROP TABLE IF EXISTS %s;
        DROP TABLE IF EXISTS %s;
        DELETE FROM jc_dynamo_tables WHERE table_name = $1;
    `, main, gsiT, lsiT), tableName)
    return err
}
```

### 5f. `PutItem` — transactional write to main + GSI + LSI

```go
func (s *PostgresDynamoDBItemStore) PutItem(ctx context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error) {
    schema := cond.Schema

    // Extract pk_val and sk_val from item using schema key attributes.
    pkVal, skVal, skNum := extractKeyVals(item, schema)

    raw, _ := json.Marshal(item)

    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback(ctx)

    main := pgTableName(table)

    // Evaluate condition against existing item if required.
    var oldItem map[string]any
    if cond.ConditionExpression != "" || cond.ReturnValues == "ALL_OLD" {
        oldItem = s.getItemTx(ctx, tx, main, pkVal, skVal)
        if cond.ConditionExpression != "" {
            check := oldItem
            if check == nil { check = map[string]any{} }
            attrTypes := attrTypesFromSchema(schema)
            if !MatchesFilter(check, cond.ConditionExpression,
                cond.ExpressionAttributeNames, cond.ExpressionAttributeValues, attrTypes) {
                return nil, &conditionFailedError{}
            }
        }
    }

    // Upsert main table.
    _, err = tx.Exec(ctx, fmt.Sprintf(`
        INSERT INTO %s (pk_val, sk_val, sk_num, item)
        VALUES ($1, $2, $3, $4::jsonb)
        ON CONFLICT (pk_val, sk_val) DO UPDATE
            SET item=$4::jsonb, sk_num=$3, updated_at=now()
    `, main), pkVal, skVal, skNum, json.RawMessage(raw))
    if err != nil {
        return nil, err
    }

    // Maintain GSI/LSI rows.
    if schema != nil {
        if err := s.upsertIndexRows(ctx, tx, table, pkVal, skVal, oldItem, item, schema); err != nil {
            return nil, err
        }
    }

    if err := tx.Commit(ctx); err != nil {
        return nil, err
    }
    if cond.ReturnValues == "ALL_OLD" {
        return oldItem, nil
    }
    return nil, nil
}
```

### 5g. `upsertIndexRows` — maintains GSI and LSI tables in-transaction

```go
func (s *PostgresDynamoDBItemStore) upsertIndexRows(
    ctx context.Context, tx pgx.Tx,
    tableName, pkVal, skVal string,
    oldItem, newItem map[string]any,
    schema *TableSchema,
) error {
    gsiT := pgGSITableName(tableName)
    lsiT := pgLSITableName(tableName)

    // Delete old index entries keyed by (tbl_pk_val, tbl_sk_val).
    if oldItem != nil {
        if _, err := tx.Exec(ctx, fmt.Sprintf(
            `DELETE FROM %s WHERE tbl_pk_val=$1 AND tbl_sk_val=$2`, gsiT,
        ), pkVal, skVal); err != nil {
            return err
        }
        if _, err := tx.Exec(ctx, fmt.Sprintf(
            `DELETE FROM %s WHERE pk_val=$1 AND tbl_sk_val=$2`, lsiT,
        ), pkVal, skVal); err != nil {
            return err
        }
    }

    // Insert new GSI rows.
    for _, gsi := range schema.GSIs {
        gsiPKVal, ok := AttrVal(newItem[gsi.PKAttr])
        if !ok {
            continue // sparse index
        }
        gsiSKVal := ""
        var gsiSKNum *float64
        if gsi.SKAttr != "" {
            gsiSKVal, _ = AttrVal(newItem[gsi.SKAttr])
            if gsi.SKType == "N" {
                if f, ok := ParseNumeric(newItem[gsi.SKAttr]); ok {
                    gsiSKNum = &f
                }
            }
        }
        if _, err := tx.Exec(ctx, fmt.Sprintf(`
            INSERT INTO %s (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, tbl_pk_val, tbl_sk_val)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (index_name, gsi_pk_val, gsi_sk_val, tbl_pk_val, tbl_sk_val) DO UPDATE
                SET gsi_sk_num=$4
        `, gsiT), gsi.IndexName, gsiPKVal, gsiSKVal, gsiSKNum, pkVal, skVal); err != nil {
            return err
        }
    }

    // Insert new LSI rows.
    for _, lsi := range schema.LSIs {
        lsiSKVal, ok := AttrVal(newItem[lsi.SKAttr])
        if !ok {
            lsiSKVal = ""
        }
        var lsiSKNum *float64
        if lsi.SKType == "N" {
            if f, ok := ParseNumeric(newItem[lsi.SKAttr]); ok {
                lsiSKNum = &f
            }
        }
        if _, err := tx.Exec(ctx, fmt.Sprintf(`
            INSERT INTO %s (index_name, pk_val, lsi_sk_val, lsi_sk_num, tbl_sk_val)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (index_name, pk_val, lsi_sk_val, tbl_sk_val) DO UPDATE
                SET lsi_sk_num=$4
        `, lsiT), lsi.IndexName, pkVal, lsiSKVal, lsiSKNum, skVal); err != nil {
            return err
        }
    }
    return nil
}
```

### 5h. `DeleteItem` — transactional delete from main + indexes

```go
func (s *PostgresDynamoDBItemStore) DeleteItem(ctx context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error) {
    schema := cond.Schema
    pkVal, skVal := pkHashToVals(pkHash, schema) // decode pkHash → (pk_val, sk_val)
    main := pgTableName(table)

    tx, err := s.pool.Begin(ctx)
    if err != nil { return nil, err }
    defer tx.Rollback(ctx)

    var oldItem map[string]any
    if cond.ConditionExpression != "" || cond.ReturnValues == "ALL_OLD" {
        oldItem = s.getItemTx(ctx, tx, main, pkVal, skVal)
        if cond.ConditionExpression != "" {
            check := oldItem
            if check == nil { check = map[string]any{} }
            if !MatchesFilter(check, cond.ConditionExpression,
                cond.ExpressionAttributeNames, cond.ExpressionAttributeValues,
                attrTypesFromSchema(schema)) {
                return nil, &conditionFailedError{}
            }
        }
    }

    if _, err := tx.Exec(ctx, fmt.Sprintf(
        `DELETE FROM %s WHERE pk_val=$1 AND sk_val=$2`, main,
    ), pkVal, skVal); err != nil {
        return nil, err
    }

    if schema != nil {
        gsiT := pgGSITableName(table)
        lsiT := pgLSITableName(table)
        tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE tbl_pk_val=$1 AND tbl_sk_val=$2`, gsiT), pkVal, skVal)
        tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE pk_val=$1 AND tbl_sk_val=$2`, lsiT), pkVal, skVal)
    }

    tx.Commit(ctx)
    if cond.ReturnValues == "ALL_OLD" { return oldItem, nil }
    return nil, nil
}
```

### 5i. `Query` — MATERIALIZED CTE pattern

```go
func (s *PostgresDynamoDBItemStore) Query(ctx context.Context, table string, q QuerySpec) ([]map[string]any, string, error) {
    if q.IndexSchema == nil {
        return s.queryMainTable(ctx, table, q)
    }
    if q.IndexSchema.IsLSI {
        return s.queryLSI(ctx, table, q)
    }
    return s.queryGSI(ctx, table, q)
}

func (s *PostgresDynamoDBItemStore) queryGSI(ctx context.Context, table string, q QuerySpec) ([]map[string]any, string, error) {
    gsiT := pgGSITableName(table)
    main := pgTableName(table)
    idx  := q.IndexSchema

    // Build WHERE clause for GSI partition key (required) and optional sort key range.
    pkVal := extractPKFromCondition(q.KeyConditionExpression, idx.PKAttr,
        q.ExpressionAttributeNames, q.ExpressionAttributeValues)
    skPred, skArgs := buildSKPredicate(q.KeyConditionExpression, idx.SKAttr, idx.SKType,
        q.ExpressionAttributeNames, q.ExpressionAttributeValues)

    orderDir := "ASC"
    if !q.ScanIndexForward { orderDir = "DESC" }
    skOrderCol := "gsi_sk_val"
    if idx.SKType == "N" { skOrderCol = "gsi_sk_num" }

    cursorClause, cursorArgs := buildGSICursor(q.ExclusiveStartKey, idx, skOrderCol, orderDir)

    projSQL := "m.item"
    if len(q.ProjectionAttrs) > 0 {
        projSQL = buildProjectionSQL(q.ProjectionAttrs)
    }

    args := []any{q.IndexSchema.IndexName, pkVal}
    args = append(args, skArgs...)
    args = append(args, cursorArgs...)
    limit := q.Limit
    if limit == 0 { limit = 1000 } // safety cap for emulator

    sql := fmt.Sprintf(`
        WITH gsi_rows AS MATERIALIZED (
            SELECT tbl_pk_val, tbl_sk_val, %s
            FROM   %s
            WHERE  index_name = $1
              AND  gsi_pk_val = $2
              %s
              %s
            ORDER BY %s %s
            LIMIT  %d
        )
        SELECT %s
        FROM   gsi_rows g
        JOIN   %s m ON m.pk_val = g.tbl_pk_val AND m.sk_val = g.tbl_sk_val
    `, skOrderCol, gsiT, skPred, cursorClause, skOrderCol, orderDir, limit, projSQL, main)

    return s.streamRows(ctx, sql, args, q.FilterExpression,
        q.ExpressionAttributeNames, q.ExpressionAttributeValues, q.Limit)
}
```

`buildSKPredicate` translates the sort key clause of `KeyConditionExpression` into a parameterised
SQL fragment. Examples:

| KeyConditionExpression (SK part) | SK type | SQL fragment |
|---|---|---|
| `sk = :v` | S | `AND gsi_sk_val = $N` |
| `sk = :v` | N | `AND gsi_sk_num = $N::NUMERIC` |
| `sk > :v` | N | `AND gsi_sk_num > $N::NUMERIC` |
| `sk BETWEEN :lo AND :hi` | S | `AND gsi_sk_val BETWEEN $N AND $M` |
| `begins_with(sk, :p)` | S | `AND gsi_sk_val LIKE $N \|\| '%'` |
| (no SK clause) | — | (empty string) |

### 5j. `streamRows` — streaming cursor, early break

```go
func (s *PostgresDynamoDBItemStore) streamRows(
    ctx context.Context, sql string, args []any,
    filterExpr string, names map[string]string, values map[string]any,
    limit int,
) ([]map[string]any, string, error) {
    rows, err := s.pool.Query(ctx, sql, args...)
    if err != nil {
        return nil, "", err
    }
    defer rows.Close()

    var result []map[string]any
    var lastExamined map[string]any
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

        // DynamoDB Limit = max items examined before FilterExpression.
        examined++
        lastExamined = item
        if limit > 0 && examined > limit {
            b, _ := json.Marshal(lastExamined)
            return result, string(b), rows.Err()
        }

        if filterExpr != "" && !MatchesFilter(item, filterExpr, names, values, nil) {
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

### 5k. `AddGSI` — backfill without DDL

```go
func (s *PostgresDynamoDBItemStore) AddGSI(ctx context.Context, tableName string, schema TableSchema, idx IndexDef) error {
    main := pgTableName(tableName)
    gsiT := pgGSITableName(tableName)

    skType := idx.SKType
    skAttr := idx.SKAttr

    // Backfill using a single INSERT ... SELECT.
    // The CASE expression handles optional numeric SK.
    _, err := s.pool.Exec(ctx, fmt.Sprintf(`
        INSERT INTO %s (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, tbl_pk_val, tbl_sk_val)
        SELECT $1,
               item->>'%s',
               COALESCE(item->>'%s', ''),
               CASE WHEN $2 = 'N' THEN (item->>'%s')::NUMERIC ELSE NULL END,
               pk_val,
               sk_val
        FROM   %s
        WHERE  item ? '%s'
        ON CONFLICT DO NOTHING
    `, gsiT, idx.PKAttr, skAttr, skAttr, main, idx.PKAttr),
        idx.IndexName, skType)
    return err
}
```

### 5l. `DeleteGSI`

```go
func (s *PostgresDynamoDBItemStore) DeleteGSI(ctx context.Context, tableName string, _ TableSchema, indexName string) error {
    gsiT := pgGSITableName(tableName)
    _, err := s.pool.Exec(ctx, fmt.Sprintf(
        `DELETE FROM %s WHERE index_name = $1`, gsiT,
    ), indexName)
    return err
}
```

### 5m. `Reset` — targeted per-table truncate

```go
func (s *PostgresDynamoDBItemStore) Reset() {
    ctx := context.Background()
    // List all known per-table tables from metadata.
    rows, err := s.pool.Query(ctx, `SELECT pg_suffix FROM jc_dynamo_tables`)
    if err != nil {
        return
    }
    defer rows.Close()
    for rows.Next() {
        var suffix string
        if rows.Scan(&suffix) != nil {
            continue
        }
        s.pool.Exec(ctx, fmt.Sprintf(`TRUNCATE jc_dt_%s, jc_dt_%s_gsi, jc_dt_%s_lsi`, suffix, suffix, suffix))
    }
    // Legacy table (kept for one release).
    s.pool.Exec(ctx, `DELETE FROM jc_dynamodb_items`)
}
```

---

## 6. Modified File: `internal/provider/table/table.go`

### 6a. Update `tableSchema` struct

Add `LocalSecondaryIndexes` (currently missing) and a helper to build `TableSchema`:

```go
type tableSchema struct {
    TableName              string              `json:"TableName"`
    TableArn               string              `json:"TableArn"`
    TableStatus            string              `json:"TableStatus"`
    KeySchema              []map[string]string `json:"KeySchema"`
    AttributeDefinitions   []map[string]string `json:"AttributeDefinitions"`
    GlobalSecondaryIndexes []map[string]any    `json:"GlobalSecondaryIndexes,omitempty"`
    LocalSecondaryIndexes  []map[string]any    `json:"LocalSecondaryIndexes,omitempty"`  // NEW
    BillingMode            string              `json:"BillingMode"`
    ItemCount              int                 `json:"ItemCount"`
    TableSizeBytes         int                 `json:"TableSizeBytes"`
    CreationDateTime       time.Time           `json:"CreationDateTime"`
    Tags                   map[string]string   `json:"Tags"`
    StreamEnabled          bool                `json:"StreamEnabled"`
    StreamViewType         string              `json:"StreamViewType"`
    LatestStreamArn        string              `json:"LatestStreamArn"`
}

// toStoreSchema converts the provider's tableSchema into a dynamostore.TableSchema
// used by the store for index maintenance.
func toStoreSchema(ts tableSchema) dynamostore.TableSchema {
    // ... parse KeySchema, AttributeDefinitions, GSIs, LSIs into typed structs ...
}
```

### 6b. `CreateTable` — limits + `CreateTableSchema`

```go
func (p *TableProvider) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    // ... existing name/arn/billing parsing ...

    gsis := parseGSIs(nr.Params["GlobalSecondaryIndexes"])
    lsis := parseLSIs(nr.Params["LocalSecondaryIndexes"])  // NEW

    // Enforce AWS limits.
    if len(gsis) > 20 {
        return nil, model.NewProviderError("ValidationException",
            fmt.Sprintf("Number of GSIs %d exceeds the per-table limit of 20", len(gsis)), 400)
    }
    if len(lsis) > 5 {
        return nil, model.NewProviderError("ValidationException",
            fmt.Sprintf("Number of LSIs %d exceeds the per-table limit of 5", len(lsis)), 400)
    }
    // LSIs require a sort key.
    if len(lsis) > 0 && !tableHasSortKey(keySchema) {
        return nil, model.NewProviderError("ValidationException",
            "Local secondary indexes require a table with a sort key", 400)
    }

    ts := tableSchema{ /* ... existing fields ... */
        LocalSecondaryIndexes: lsis,
    }

    // Save metadata to jc_resources (unchanged).
    raw, _ := json.Marshal(ts)
    if err := p.resources.Create(ctx, store.ResourceEntry{Type: "dynamodb_tables", ID: name, Data: raw}); err != nil {
        // ... existing error handling ...
    }

    // Create per-table PostgreSQL tables (new).
    schema := toStoreSchema(ts)
    if err := p.items.CreateTableSchema(ctx, schema); err != nil {
        // Roll back the resource entry.
        p.resources.Delete(ctx, "dynamodb_tables", name)
        return nil, fmt.Errorf("create table schema: %w", err)
    }

    return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}
```

### 6c. `DeleteTable` — scoped delete, not Reset()

```go
func (p *TableProvider) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    name := strParam(nr.Params, "TableName")
    ts, err := p.loadTable(ctx, name)
    if err != nil {
        return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
    }
    _ = p.resources.Delete(ctx, "dynamodb_tables", name)

    // Drop per-table PostgreSQL tables (new). Was: p.items.Reset() which wiped ALL tables.
    schema := toStoreSchema(ts)
    if err := p.items.DropTableSchema(ctx, schema); err != nil {
        return nil, fmt.Errorf("drop table schema: %w", err)
    }

    return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}
```

### 6d. `PutItem` — pass schema via ConditionSpec

```go
func (p *TableProvider) PutItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    // ... existing item extraction ...
    ts, _ := p.loadTable(ctx, name)
    pkHash := computePKHash(item, ts)
    existingForStream, _ := p.items.GetItem(ctx, name, pkHash)

    storeSchema := toStoreSchema(ts)
    cond := dynamostore.ConditionSpec{
        ConditionExpression:       strParam(nr.Params, "ConditionExpression"),
        ExpressionAttributeNames:  exprNames(nr.Params),
        ExpressionAttributeValues: exprValues(nr.Params),
        ReturnValues:              strParam(nr.Params, "ReturnValues"),
        Schema:                    &storeSchema,  // NEW
    }
    // ... rest unchanged ...
}
```

### 6e. `DeleteItem` — pass schema via ConditionSpec

```go
func (p *TableProvider) DeleteItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    // ... existing key extraction ...
    storeSchema := toStoreSchema(ts)
    cond := dynamostore.ConditionSpec{
        // ... existing fields ...
        Schema: &storeSchema,  // NEW
    }
    // ... rest unchanged ...
}
```

### 6f. `UpdateItem` — pass schema via UpdateSpec

```go
func (p *TableProvider) UpdateItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    // ... existing extraction ...
    storeSchema := toStoreSchema(ts)
    spec := dynamostore.UpdateSpec{
        // ... existing fields ...
        Schema: &storeSchema,  // NEW
    }
    // ... rest unchanged ...
}
```

### 6g. `Query` — resolve IndexKeyRef and ProjectionAttrs

```go
func (p *TableProvider) Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    name := strParam(nr.Params, "TableName")
    ts, err := p.loadTable(ctx, name)
    if err != nil {
        return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
    }
    storeSchema := toStoreSchema(ts)

    indexName := strParam(nr.Params, "IndexName")
    var indexRef *dynamostore.IndexKeyRef
    var projAttrs []string

    if indexName != "" {
        idx, found := findIndex(storeSchema, indexName)
        if !found {
            return nil, model.NewProviderError("ValidationException",
                fmt.Sprintf("The table does not have an index named %q", indexName), 400)
        }
        indexRef = &dynamostore.IndexKeyRef{
            IndexName: idx.IndexName,
            PKAttr:    idx.PKAttr,
            SKAttr:    idx.SKAttr,
            PKType:    idx.PKType,
            SKType:    idx.SKType,
            IsLSI:     idx.IsLSI,
        }
        projAttrs = resolveProjectionAttrs(idx, storeSchema)
    }

    q := dynamostore.QuerySpec{
        IndexSchema:               indexRef,
        ProjectionAttrs:           projAttrs,
        IndexName:                 indexName,
        KeyConditionExpression:    strParam(nr.Params, "KeyConditionExpression"),
        FilterExpression:          strParam(nr.Params, "FilterExpression"),
        ExpressionAttributeNames:  exprNames(nr.Params),
        ExpressionAttributeValues: exprValues(nr.Params),
        ScanIndexForward:          boolParam(nr.Params, "ScanIndexForward", true),
        Limit:                     intParam(nr.Params, "Limit", 0),
        ExclusiveStartKey:         exclusiveStartKey(nr.Params),
    }

    items, lastKey, err := p.items.Query(ctx, name, q)
    if err != nil {
        return nil, err
    }
    result := map[string]any{"Items": items, "Count": len(items)}
    if lastKey != "" {
        result["LastEvaluatedKey"] = json.RawMessage(lastKey)
    }
    return provider.OK(result), nil
}
```

`resolveProjectionAttrs` returns nil for `ProjectionType=ALL`, the key attribute list for
`KEYS_ONLY`, or key attributes + `NonKeyAttrs` for `INCLUDE`.

### 6h. `UpdateTable` — GSI add/delete, LSI rejection

```go
func (p *TableProvider) UpdateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
    name := strParam(nr.Params, "TableName")
    ts, err := p.loadTable(ctx, name)
    if err != nil {
        return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
    }

    // Reject any LSI changes.
    if _, hasLSI := nr.Params["LocalSecondaryIndexUpdates"]; hasLSI {
        return nil, model.NewProviderError("ValidationException",
            "Local secondary indexes cannot be modified after table creation", 400)
    }

    // Handle GSI updates.
    if gsiUpdates, ok := nr.Params["GlobalSecondaryIndexUpdates"].([]any); ok {
        storeSchema := toStoreSchema(ts)
        for _, u := range gsiUpdates {
            update, _ := u.(map[string]any)
            if create, ok := update["Create"].(map[string]any); ok {
                if len(ts.GlobalSecondaryIndexes) >= 20 {
                    return nil, model.NewProviderError("LimitExceededException",
                        "Cannot add more than 20 global secondary indexes", 400)
                }
                newGSI := parseOneGSI(create)
                ts.GlobalSecondaryIndexes = append(ts.GlobalSecondaryIndexes, newGSI)
                idxDef := gsiToIndexDef(newGSI, ts)
                if err := p.items.AddGSI(ctx, name, storeSchema, idxDef); err != nil {
                    return nil, fmt.Errorf("backfill GSI: %w", err)
                }
            } else if del, ok := update["Delete"].(map[string]any); ok {
                idxName, _ := del["IndexName"].(string)
                ts.GlobalSecondaryIndexes = removeGSI(ts.GlobalSecondaryIndexes, idxName)
                if err := p.items.DeleteGSI(ctx, name, storeSchema, idxName); err != nil {
                    return nil, fmt.Errorf("delete GSI: %w", err)
                }
            }
        }
    }

    // ... existing BillingMode / StreamSpecification handling ...

    _ = p.saveTable(ctx, ts)
    return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}
```

---

## 7. New Migration: `internal/store/migrations/012_dynamodb_per_table.sql`

```sql
-- Metadata table: one row per DynamoDB table, maps table name to PostgreSQL suffix.
CREATE TABLE IF NOT EXISTS jc_dynamo_tables (
    table_name  TEXT        PRIMARY KEY,
    pg_suffix   TEXT        NOT NULL UNIQUE,
    schema_json JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drop the redundant index on the legacy shared table.
-- table_name is already the leading column of the PRIMARY KEY (table_name, pk_hash).
DROP INDEX IF EXISTS idx_dynamo_items_table;
```

---

## 8. Helper Functions Reference

These small helpers are referenced above and should be implemented alongside the main changes.

| Function | Location | Description |
|---|---|---|
| `pgSuffix(tableName) string` | `postgres.go` | hex16 of sha256(tableName)[:8] |
| `pgTableName(tableName) string` | `postgres.go` | `"jc_dt_" + pgSuffix(tableName)` |
| `pgGSITableName(tableName) string` | `postgres.go` | `pgTableName + "_gsi"` |
| `pgLSITableName(tableName) string` | `postgres.go` | `pgTableName + "_lsi"` |
| `extractKeyVals(item, schema) (pk, sk string, skNum *float64)` | `postgres.go` | pulls pk_val/sk_val/sk_num from item using schema key attrs |
| `pkHashToVals(pkHash, schema) (pk, sk string)` | `postgres.go` | decodes the "PK=x\|SK=y" hash back to raw values |
| `getItemTx(ctx, tx, table, pk, sk) map[string]any` | `postgres.go` | SELECT item within an existing transaction |
| `attrTypesFromSchema(schema *TableSchema) map[string]string` | `schema.go` | builds attrName→"S"\|"N"\|"B" map |
| `buildSKPredicate(expr, skAttr, skType, names, values) (sql, args)` | `postgres.go` | translates SK clause of KeyConditionExpression to SQL |
| `buildGSICursor(lastKey, idx, skCol, dir) (sql, args)` | `postgres.go` | builds multi-column WHERE clause for pagination resume |
| `buildProjectionSQL(attrs []string) string` | `postgres.go` | returns `jsonb_build_object('a', m.item->'a', ...)` |
| `encodeLastKey(item, schema, idx) string` | `schema.go` | JSON-encodes the LastEvaluatedKey map |
| `resolveProjectionAttrs(idx, schema) []string` | `table.go` | returns nil\|key attrs\|key+include attrs |
| `toStoreSchema(ts tableSchema) TableSchema` | `table.go` | converts provider struct → store struct |
| `findIndex(schema, indexName) (IndexDef, bool)` | `table.go` | finds GSI or LSI by name in schema |
| `parseLSIs(v any) []map[string]any` | `table.go` | parses LocalSecondaryIndexes request param |
| `applyProjection(item, attrs []string) map[string]any` | `schema.go` | returns copy of item with only listed attrs |
| `buildAttrTypes(schema TableSchema) map[string]string` | `schema.go` | attrName→type from schema |

---

## 9. Implementation Plan

Each phase is self-contained. All existing tests must pass at the end of every phase before moving
to the next.

---

### Phase 1 — Schema types and migration
**Estimated effort:** 0.5 day

**Tasks:**
1. Create `internal/store/aws/dynamodb/schema.go` with `TableSchema`, `IndexDef`, `ProjectionDef`,
   `IndexKeyRef`, `AttrVal`, `AttrType`, `ParseNumeric`, `applyProjection`, `buildAttrTypes`,
   `encodeLastKey`.
2. Create `internal/store/migrations/012_dynamodb_per_table.sql` with `jc_dynamo_tables` table and
   the `DROP INDEX IF EXISTS idx_dynamo_items_table` statement.
3. Add `CreateTableSchema`, `DropTableSchema`, `AddGSI`, `DeleteGSI` to the
   `DynamoDBItemStore` interface in `store.go`. Provide **stub implementations** in both
   `memory.go` and `postgres.go` that return `nil` (no-op). This keeps the build green.
4. Add `IndexSchema *IndexKeyRef`, `ProjectionAttrs []string`, `Schema *TableSchema` fields to
   `QuerySpec`, `ScanSpec`, `ConditionSpec`, `UpdateSpec`, `BatchWriteRequest` in `store.go`.
   Existing code that doesn't set these fields continues to work (nil = old behaviour).

**Acceptance criteria:**
- `go build ./...` passes.
- `go test -race ./internal/...` passes (all existing tests green).
- `jc_dynamo_tables` table created when running in full mode.

---

### Phase 2 — Bug fixes (no index work yet)
**Estimated effort:** 0.5 day

**Tasks:**
1. Fix `hashKey` in `postgres.go`: hash only key attributes, not the full item. The provider
   already passes a correct `pkHash` via `computePKHash` — the `hashKey` fallback is only reached
   when `pkHash == ""`. Change the fallback to `return ""` and make the caller (provider) assert
   that `pkHash` is never empty before calling the store.
2. Fix `DeleteTable` in `table.go`: replace `p.items.Reset()` with a targeted delete.
   - Memory store: `delete(s.tables, tableName)`.
   - Postgres store: `DELETE FROM jc_dynamodb_items WHERE table_name = $1` (legacy table) and call
     `DropTableSchema` (which is still a no-op at this phase — that's fine).
3. Add `parseLSIs` to `table.go` (mirrors existing `parseGSIs`).
4. Add `LocalSecondaryIndexes` field to `tableSchema` struct.

**Acceptance criteria:**
- Integration test: create two tables A and B, put items in both, delete table A, verify table B
  still has its items.
- Integration test: `PutItem` with the same primary key twice results in one item (idempotent
  replace, not insert of a duplicate).

---

### Phase 3 — Per-table DDL (Postgres) + schema registration (memory)
**Estimated effort:** 1 day

**Tasks:**
1. Implement `CreateTableSchema` in `postgres.go` (section 5d): runs the DDL to create the three
   per-table tables and five indexes inside a PostgreSQL transaction that also inserts into
   `jc_dynamo_tables`.
2. Implement `DropTableSchema` in `postgres.go` (section 5e): `DROP TABLE` + delete from
   `jc_dynamo_tables`.
3. Implement `CreateTableSchema` in `memory.go` (section 4b): registers schema in `s.schemas`,
   initialises `s.gsiIdx`.
4. Implement `DropTableSchema` in `memory.go` (section 4c): removes from `s.schemas`, `s.items`,
   `s.gsiIdx`.
5. Wire up calls in `table.go` `CreateTable` and `DeleteTable` (sections 6b and 6c).
6. Implement `toStoreSchema` helper in `table.go`.

**Acceptance criteria:**
- After `CreateTable` in full mode: `SELECT * FROM jc_dynamo_tables` returns one row; tables
  `jc_dt_{suffix}`, `jc_dt_{suffix}_gsi`, `jc_dt_{suffix}_lsi` exist in PostgreSQL.
- After `DeleteTable`: all three tables are gone; `jc_dynamo_tables` row is deleted.
- Existing SQS/DynamoDB integration tests unaffected.

---

### Phase 4 — Write amplification (Postgres)
**Estimated effort:** 1.5 days

**Tasks:**
1. Implement `pgSuffix`, `pgTableName`, `pgGSITableName`, `pgLSITableName` helpers.
2. Implement `extractKeyVals` and `pkHashToVals` helpers.
3. Implement `upsertIndexRows` (section 5g).
4. Rewrite `PutItem` in `postgres.go` to use the per-table main table and call `upsertIndexRows`
   (section 5f).
5. Rewrite `DeleteItem` in `postgres.go` (section 5h).
6. Update `UpdateItem` in `postgres.go`: read old item → apply update → call new `PutItem`.
7. Update `BatchWriteItems` in `postgres.go` to pass `Schema` from each `BatchWriteRequest`.
8. Wire `Schema: &storeSchema` in `table.go` `PutItem`, `DeleteItem`, `UpdateItem` (sections
   6d–6f).
9. Implement `AddGSI` and `DeleteGSI` in `postgres.go` (sections 5k–5l).

**Acceptance criteria:**
- Integration test: create table with one GSI, put three items, query `SELECT * FROM
  jc_dt_{suffix}_gsi` directly in psql — rows appear.
- Integration test: delete an item — its GSI row is also gone.
- Existing DynamoDB integration tests (table PK queries) continue to pass.

---

### Phase 5 — Write amplification (memory store)
**Estimated effort:** 0.5 day

**Tasks:**
1. Implement `removeGSIEntries` and `addGSIEntries` helpers in `memory.go` (section 4d).
2. Update `PutItem` in `memory.go` to call them (section 4d).
3. Update `DeleteItem` in `memory.go` (section 4e).
4. Update `UpdateItem` in `memory.go`: reads old item before overwriting, calls both helpers.
5. Implement `AddGSI` and `DeleteGSI` in `memory.go` (section 4g).

**Acceptance criteria:**
- Unit test: create table with GSI via `CreateTableSchema`, put items, inspect `s.gsiIdx` directly
  — correct GSI pk buckets.
- Unit test: delete item — GSI bucket no longer contains its hash.

---

### Phase 6 — Expression engine
**Estimated effort:** 1 day

**Tasks:**
1. Create `internal/store/aws/dynamodb/expressions.go`.
2. Move `matchesKeyCondition`, `matchesFilter`, `evalCondition`, `splitAND`, `applyUpdateExpression`
   and all helpers from `memory.go` into `expressions.go`, renaming them to exported
   `MatchesKeyCondition`, `MatchesFilter`, `ApplyUpdateExpression`.
3. Add `attrTypes map[string]string` parameter to `evalCondition` and `MatchesKeyCondition` /
   `MatchesFilter`.
4. Implement `BETWEEN`, `<>`, `<`, `<=`, `>`, `>=` in `evalCondition` (section 3b).
5. Update all callers in `memory.go` and `postgres.go` to pass `attrTypesFromSchema(schema)`.

**Acceptance criteria:**
- Unit tests for each new operator: `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`.
- Test with both string and numeric attribute types.
- All existing expression tests still pass.

---

### Phase 7 — Query/Scan index routing (Postgres)
**Estimated effort:** 1.5 days

**Tasks:**
1. Implement `buildSKPredicate` and `buildGSICursor` helpers in `postgres.go`.
2. Implement `streamRows` (section 5j) — replaces the existing `filterRows`.
3. Implement `queryMainTable` in `postgres.go`: uses the new per-table main table; passes
   `attrTypes` to `MatchesFilter` for in-Go filter evaluation after SQL key condition.
4. Implement `queryGSI` in `postgres.go` (section 5i): MATERIALIZED CTE pattern.
5. Implement `queryLSI` in `postgres.go`: same CTE pattern but against `_lsi` table.
6. Update `Scan` in `postgres.go` to route to per-table main, GSI, or LSI table.
7. Update `Query` in `table.go` to resolve `IndexKeyRef` and `ProjectionAttrs` (section 6g).
8. Update `Scan` in `table.go` similarly.

**Acceptance criteria:**
- Integration test: `Query` with `IndexName` returns only items matching the GSI partition key.
- Integration test: `Query` with GSI sort key `BETWEEN` returns correct subset.
- Integration test: `Query` with LSI returns correct sort order (different from table SK order).
- Integration test: pagination — `LastEvaluatedKey` returned when result is truncated at `Limit`;
  second call with `ExclusiveStartKey` returns the next page without duplicates or gaps.

---

### Phase 8 — Query/Scan index routing (memory store)
**Estimated effort:** 0.5 day

**Tasks:**
1. Implement the filter-first, copy-last `Query` pattern in `memory.go` (section 4f).
2. Add GSI routing: use `s.gsiIdx` bucket when `q.IndexSchema != nil && !q.IndexSchema.IsLSI`.
3. Add LSI routing: filter `items` by `pkVal` equality then re-sort by LSI SK attribute.
4. Apply projection via `applyProjection` helper.
5. Apply same filter-first pattern to `Scan`.

**Acceptance criteria:**
- Unit tests for GSI query and LSI query using the memory store.
- Existing unit tests for Query/Scan still pass.

---

### Phase 9 — UpdateTable GSI management
**Estimated effort:** 0.5 day

**Tasks:**
1. Implement `parseOneGSI` and `gsiToIndexDef` helpers in `table.go`.
2. Implement `removeGSI` helper.
3. Wire full `UpdateTable` GSI create/delete logic in `table.go` (section 6h).

**Acceptance criteria:**
- Integration test: create table, add a GSI via `UpdateTable`, verify existing items appear in the
  new GSI query.
- Integration test: delete a GSI via `UpdateTable`, verify queries on that index return
  `ValidationException`.
- Integration test: `UpdateTable` with LSI changes returns `ValidationException`.

---

### Phase 10 — Projection pushdown
**Estimated effort:** 0.5 day

**Tasks:**
1. Implement `buildProjectionSQL` in `postgres.go` (section 6g).
2. Wire `ProjectionAttrs` into `queryGSI`, `queryLSI`, `queryMainTable` SQL generation.
3. Implement `resolveProjectionAttrs` in `table.go`.
4. Wire `ProjectionAttrs` into `table.go` `Query` and `Scan` (section 6g).

**Acceptance criteria:**
- Integration test: GSI with `ProjectionType=KEYS_ONLY` returns only key attributes; no
  non-key attributes in any returned item.
- Integration test: GSI with `ProjectionType=INCLUDE` returns key attributes plus the listed
  non-key attributes only.

---

### Phase 11 — Enforce limits
**Estimated effort:** 0.5 day

**Tasks:**
1. Add GSI count check in `CreateTable` (section 6b).
2. Add LSI count check in `CreateTable`.
3. Add LSI-requires-sort-key check in `CreateTable`.
4. Add GSI cap check in `UpdateTable` (section 6h).
5. Add LSI-change rejection in `UpdateTable`.

**Acceptance criteria:**
- Unit test: `CreateTable` with 21 GSIs returns `ValidationException`.
- Unit test: `CreateTable` with 6 LSIs returns `ValidationException`.
- Unit test: `CreateTable` with LSI on a table without a sort key returns `ValidationException`.
- Unit test: `UpdateTable` adding a GSI to a table already at 20 returns `LimitExceededException`.
- Unit test: `UpdateTable` with `LocalSecondaryIndexUpdates` returns `ValidationException`.

---

## 10. Test File Map

| Test file | Phase | What it covers |
|---|---|---|
| `internal/store/aws/dynamodb/schema_test.go` | 1 | `AttrVal`, `AttrType`, `ParseNumeric`, `applyProjection` |
| `internal/store/aws/dynamodb/expressions_test.go` | 6 | All expression operators, numeric-aware comparisons |
| `internal/store/aws/dynamodb/memory_test.go` | 5, 8 | Write amplification, GSI/LSI query routing |
| `internal/store/aws/dynamodb/postgres_test.go` | 3, 4, 7 | DDL lifecycle, write amplification, SQL query correctness |
| `tests/integration/dynamodb_gsi_test.go` | 7, 9, 10 | End-to-end GSI queries via aws-sdk-go-v2 |
| `tests/integration/dynamodb_lsi_test.go` | 7, 10 | End-to-end LSI queries |
| `tests/integration/dynamodb_update_table_test.go` | 9 | UpdateTable add/delete GSI |
| `tests/integration/dynamodb_limits_test.go` | 11 | Limit enforcement errors |

---

## 11. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| `pkHashToVals` cannot decode legacy hashes (old format "PK=x\|SK=y") for items written before the migration | During Phase 3–4, also write items to the legacy `jc_dynamodb_items` table for one release; remove in a follow-up |
| PostgreSQL DDL lock contention when many `CreateTable` calls happen concurrently in tests | Each test calls `POST /_jaiscloud/reset` which wipes per-table tables; use `DROP TABLE ... CASCADE` in `DropTableSchema` |
| `MATERIALIZED CTE` not available in PostgreSQL < 12 | JaisCloud's minimum PostgreSQL version is 14 (used in docker-compose); no issue |
| `buildSKPredicate` parses the SK clause of `KeyConditionExpression` — fragile string parsing | Write comprehensive unit tests for `buildSKPredicate` covering all operator forms before integrating into the query path |
