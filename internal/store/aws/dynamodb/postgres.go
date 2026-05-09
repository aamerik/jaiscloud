package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDynamoDBItemStore implements DynamoDBItemStore against PostgreSQL.
// Main item data lives in jc_dynamodb_items (legacy global table).
// Per-table GSI/LSI index rows live in jc_dt_{suffix}_gsi and jc_dt_{suffix}_lsi.
type PostgresDynamoDBItemStore struct {
	pool *pgxpool.Pool
}

func NewPostgresDynamoDBItemStore(pool *pgxpool.Pool) *PostgresDynamoDBItemStore {
	return &PostgresDynamoDBItemStore{pool: pool}
}

// pgSuffix returns a short stable hex suffix derived from the table name.
func pgSuffix(tableName string) string {
	h := sha256.Sum256([]byte(tableName))
	return fmt.Sprintf("%x", h[:8])
}

func hashKey(table string, item map[string]any) string {
	b, _ := json.Marshal(item)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(table+":"+string(b))))
}

// ─── Data-plane methods ───────────────────────────────────────────────────────

func (s *PostgresDynamoDBItemStore) PutItem(ctx context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error) {
	h := pkHash
	if h == "" {
		h = hashKey(table, item)
	}
	var oldItem map[string]any
	if cond.ConditionExpression != "" || cond.ReturnValues == "ALL_OLD" {
		oldItem, _ = s.GetItem(ctx, table, h)
		if cond.ConditionExpression != "" {
			existing := oldItem
			if existing == nil {
				existing = map[string]any{}
			}
			if !matchesFilter(existing, cond.ConditionExpression, cond.ExpressionAttributeNames, cond.ExpressionAttributeValues, nil) {
				return nil, &conditionFailedError{}
			}
		}
	}
	raw, _ := json.Marshal(item)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_dynamodb_items (table_name, pk_hash, item)
		VALUES ($1, $2, $3)
		ON CONFLICT (table_name, pk_hash) DO UPDATE
			SET item=$3, updated_at=now()
	`, table, h, json.RawMessage(raw))
	if err != nil {
		return nil, err
	}
	// Maintain GSI/LSI index rows when schema is available.
	if cond.Schema != nil {
		if upsertErr := s.upsertIndexRows(ctx, table, h, item, cond.Schema); upsertErr != nil {
			return nil, fmt.Errorf("index maintenance failed for table %s: %w", table, upsertErr)
		}
	}
	if cond.ReturnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

func (s *PostgresDynamoDBItemStore) GetItem(ctx context.Context, table, pkHash string) (map[string]any, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT item FROM jc_dynamodb_items WHERE table_name=$1 AND pk_hash=$2
	`, table, pkHash).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var item map[string]any
	return item, json.Unmarshal(raw, &item)
}

func (s *PostgresDynamoDBItemStore) DeleteItem(ctx context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error) {
	var oldItem map[string]any
	if cond.ConditionExpression != "" || cond.ReturnValues == "ALL_OLD" || cond.Schema != nil {
		oldItem, _ = s.GetItem(ctx, table, pkHash)
		if cond.ConditionExpression != "" {
			existing := oldItem
			if existing == nil {
				existing = map[string]any{}
			}
			if !matchesFilter(existing, cond.ConditionExpression, cond.ExpressionAttributeNames, cond.ExpressionAttributeValues, nil) {
				return nil, &conditionFailedError{}
			}
		}
	}
	if cond.Schema != nil {
		s.deleteIndexRows(ctx, table, pkHash)
	}
	_, err := s.pool.Exec(ctx, `
		DELETE FROM jc_dynamodb_items WHERE table_name=$1 AND pk_hash=$2
	`, table, pkHash)
	if err != nil {
		return nil, err
	}
	if cond.ReturnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

func (s *PostgresDynamoDBItemStore) UpdateItem(ctx context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error) {
	existing, err := s.GetItem(ctx, table, pkHash)
	if err != nil {
		return nil, err
	}
	if spec.ConditionExpression != "" {
		check := existing
		if check == nil {
			check = map[string]any{}
		}
		if !matchesFilter(check, spec.ConditionExpression, spec.ExpressionAttributeNames, spec.ExpressionAttributeValues, nil) {
			return nil, &conditionFailedError{}
		}
	}
	if existing == nil {
		existing = copyItem(item)
	}
	if spec.UpdateExpression != "" {
		applyUpdateExpression(existing, spec.UpdateExpression, spec.ExpressionAttributeNames, spec.ExpressionAttributeValues)
	} else {
		for k, v := range item {
			existing[k] = v
		}
	}
	// PutItem will maintain indexes via cond.Schema.
	putCond := ConditionSpec{Schema: spec.Schema}
	if _, err := s.PutItem(ctx, table, pkHash, existing, putCond); err != nil {
		return nil, err
	}
	return existing, nil
}

func (s *PostgresDynamoDBItemStore) Query(ctx context.Context, table string, q QuerySpec) ([]map[string]any, int, string, error) {
	if q.IndexSchema != nil {
		return s.queryIndex(ctx, table, q)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT item FROM jc_dynamodb_items WHERE table_name=$1 ORDER BY pk_hash
	`, table)
	if err != nil {
		return nil, 0, "", err
	}
	defer rows.Close()
	return s.filterRows(rows, q.KeyConditionExpression, q.FilterExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues, q.ExclusiveStartKey, q.Limit)
}

func (s *PostgresDynamoDBItemStore) Scan(ctx context.Context, table string, sc ScanSpec) ([]map[string]any, int, string, error) {
	if sc.IndexSchema != nil {
		return s.scanIndex(ctx, table, sc)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT item FROM jc_dynamodb_items WHERE table_name=$1 ORDER BY pk_hash
	`, table)
	if err != nil {
		return nil, 0, "", err
	}
	defer rows.Close()
	return s.filterRows(rows, "", sc.FilterExpression, sc.ExpressionAttributeNames, sc.ExpressionAttributeValues, sc.ExclusiveStartKey, sc.Limit)
}

func (s *PostgresDynamoDBItemStore) filterRows(rows pgx.Rows, keyExpr, filterExpr string, names map[string]string, values map[string]any, exclusiveStartKey string, limit int) ([]map[string]any, int, string, error) {
	// Collect items matching key condition.
	var keyMatched []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, 0, "", err
		}
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		if !matchesKeyCondition(item, keyExpr, names, values, nil) {
			continue
		}
		keyMatched = append(keyMatched, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", err
	}

	// Paginate on key-condition-matched items; scannedCount = items in page.
	page := paginateItems(keyMatched, exclusiveStartKey, limit)
	scannedCount := len(page)

	var lastKey string
	if limit > 0 && len(page) == limit {
		b, _ := json.Marshal(page[len(page)-1])
		lastKey = string(b)
	}

	// Apply FilterExpression on the page.
	var result []map[string]any
	for _, item := range page {
		if filterExpr == "" || matchesFilter(item, filterExpr, names, values, nil) {
			result = append(result, item)
		}
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result, scannedCount, lastKey, nil
}

func (s *PostgresDynamoDBItemStore) BatchWriteItems(ctx context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error) {
	for _, req := range reqs {
		if req.PutItem != nil {
			cond := ConditionSpec{Schema: req.Schema}
			if _, err := s.PutItem(ctx, req.Table, req.PutHash, req.PutItem, cond); err != nil {
				return nil, err
			}
		} else if req.DeleteKey != nil {
			h := req.DeleteHash
			if h == "" {
				h = hashKey(req.Table, req.DeleteKey)
			}
			deleteCond := ConditionSpec{Schema: req.Schema}
			if _, err := s.DeleteItem(ctx, req.Table, h, deleteCond); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (s *PostgresDynamoDBItemStore) BatchGetItems(ctx context.Context, reqs []BatchGetRequest) (map[string][]map[string]any, error) {
	result := make(map[string][]map[string]any)
	for _, req := range reqs {
		for _, key := range req.Keys {
			h := itemPKHash(key)
			item, err := s.GetItem(ctx, req.Table, h)
			if err != nil {
				return nil, err
			}
			if item != nil {
				result[req.Table] = append(result[req.Table], item)
			}
		}
	}
	return result, nil
}

// TransactWriteItems wraps all condition checks and writes in a single Postgres transaction.
func (s *PostgresDynamoDBItemStore) TransactWriteItems(ctx context.Context, ops []TransactWriteOp) ([]CancellationReason, error) {
	// Phase 1: evaluate all conditions (reads can run outside the write tx).
	reasons := make([]CancellationReason, len(ops))
	anyFailed := false
	for i, op := range ops {
		condExpr := op.Cond.ConditionExpression
		if condExpr == "" && op.Type != "ConditionCheck" {
			reasons[i] = CancellationReason{Code: "None"}
			continue
		}
		item, _ := s.GetItem(ctx, op.Table, op.PKHash)
		if item == nil {
			item = map[string]any{}
		}
		if condExpr != "" && !matchesFilter(item, condExpr, op.Cond.ExpressionAttributeNames, op.Cond.ExpressionAttributeValues, nil) {
			reasons[i] = CancellationReason{Code: "ConditionalCheckFailed", Message: "The conditional request failed"}
			anyFailed = true
		} else {
			reasons[i] = CancellationReason{Code: "None"}
		}
	}
	if anyFailed {
		return reasons, nil
	}

	// Phase 2: apply all writes in a single DB transaction.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	for _, op := range ops {
		switch op.Type {
		case "Put":
			raw, _ := json.Marshal(op.Item)
			if _, err := tx.Exec(ctx, `
				INSERT INTO jc_dynamodb_items (table_name, pk_hash, item)
				VALUES ($1, $2, $3)
				ON CONFLICT (table_name, pk_hash) DO UPDATE
					SET item=$3, updated_at=now()
			`, op.Table, op.PKHash, json.RawMessage(raw)); err != nil {
				return nil, err
			}
		case "Delete":
			if _, err := tx.Exec(ctx, `
				DELETE FROM jc_dynamodb_items WHERE table_name=$1 AND pk_hash=$2
			`, op.Table, op.PKHash); err != nil {
				return nil, err
			}
		case "Update":
			existing, err := s.GetItem(ctx, op.Table, op.PKHash)
			if err != nil {
				return nil, err
			}
			if existing == nil {
				existing = copyItem(op.Key)
			}
			if op.Update.UpdateExpression != "" {
				applyUpdateExpression(existing, op.Update.UpdateExpression, op.Update.ExpressionAttributeNames, op.Update.ExpressionAttributeValues)
			} else {
				for k, v := range op.Item {
					existing[k] = v
				}
			}
			raw, _ := json.Marshal(existing)
			if _, err := tx.Exec(ctx, `
				INSERT INTO jc_dynamodb_items (table_name, pk_hash, item)
				VALUES ($1, $2, $3)
				ON CONFLICT (table_name, pk_hash) DO UPDATE
					SET item=$3, updated_at=now()
			`, op.Table, op.PKHash, json.RawMessage(raw)); err != nil {
				return nil, err
			}
		}
	}
	return nil, tx.Commit(ctx)
}

func (s *PostgresDynamoDBItemStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_dynamodb_items`)
	// Drop all per-table index tables created by CreateTableSchema.
	// Use current_schema() so the query works regardless of cloud (aws/azure/gcp).
	rows, err := s.pool.Query(ctx, `
		SELECT tablename FROM pg_tables
		WHERE schemaname=current_schema() AND tablename LIKE 'jc_dt_%'
	`)
	if err != nil {
		return
	}
	var names []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			names = append(names, n)
		}
	}
	rows.Close()
	for _, n := range names {
		s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, n))
	}
}

// ─── Table-lifecycle methods ──────────────────────────────────────────────────

// CreateTableSchema creates GSI and LSI index tables for a DynamoDB table.
// Main item storage stays in the global jc_dynamodb_items table.
func (s *PostgresDynamoDBItemStore) CreateTableSchema(ctx context.Context, schema TableSchema) error {
	suffix := pgSuffix(schema.TableName)
	main := "jc_dt_" + suffix

	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s_gsi (
    index_name TEXT NOT NULL,
    gsi_pk_val TEXT NOT NULL,
    gsi_sk_val TEXT NOT NULL DEFAULT '',
    gsi_sk_num NUMERIC,
    pk_hash    TEXT NOT NULL,
    PRIMARY KEY (index_name, pk_hash)
);
CREATE INDEX IF NOT EXISTS %s_gsi_q ON %s_gsi (index_name, gsi_pk_val, gsi_sk_val);
CREATE TABLE IF NOT EXISTS %s_lsi (
    index_name TEXT NOT NULL,
    pk_val     TEXT NOT NULL,
    lsi_sk_val TEXT NOT NULL DEFAULT '',
    lsi_sk_num NUMERIC,
    pk_hash    TEXT NOT NULL,
    PRIMARY KEY (index_name, pk_hash)
);
CREATE INDEX IF NOT EXISTS %s_lsi_q ON %s_lsi (index_name, pk_val, lsi_sk_val);
`, main, main, main, main, main, main)

	_, err := s.pool.Exec(ctx, ddl)
	return err
}

// DropTableSchema drops per-table index tables and removes items from jc_dynamodb_items.
func (s *PostgresDynamoDBItemStore) DropTableSchema(ctx context.Context, tableName string) error {
	suffix := pgSuffix(tableName)
	main := "jc_dt_" + suffix
	ddl := fmt.Sprintf(`
DROP TABLE IF EXISTS %s_lsi;
DROP TABLE IF EXISTS %s_gsi;
`, main, main)
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_dynamodb_items WHERE table_name=$1`, tableName)
	return err
}

// AddGSI backfills GSI index rows from existing items in jc_dynamodb_items.
// Items are processed in batches of 200 to avoid N+1 round-trips.
func (s *PostgresDynamoDBItemStore) AddGSI(ctx context.Context, tableName string, schema TableSchema, idx IndexDef) error {
	suffix := pgSuffix(tableName)
	gsiTable := "jc_dt_" + suffix + "_gsi"

	rows, err := s.pool.Query(ctx, `SELECT pk_hash, item FROM jc_dynamodb_items WHERE table_name=$1`, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	type gsiRow struct {
		indexName string
		pkVal     string
		skVal     string
		skNum     *float64
		pkHash    string
	}

	const batchSize = 200
	batch := make([]gsiRow, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Build multi-row INSERT: VALUES ($1,$2,$3,$4,$5),($6,$7,$8,$9,$10),...
		placeholders := make([]string, 0, len(batch))
		args := make([]any, 0, len(batch)*5)
		for i, r := range batch {
			base := i * 5
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4, base+5))
			args = append(args, r.indexName, r.pkVal, r.skVal, r.skNum, r.pkHash)
		}
		_, err := s.pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, pk_hash) VALUES %s
			 ON CONFLICT (index_name, pk_hash) DO UPDATE
			   SET gsi_pk_val=EXCLUDED.gsi_pk_val, gsi_sk_val=EXCLUDED.gsi_sk_val, gsi_sk_num=EXCLUDED.gsi_sk_num`,
			gsiTable, strings.Join(placeholders, ",")), args...)
		batch = batch[:0]
		return err
	}

	for rows.Next() {
		var pkHash string
		var raw []byte
		if err := rows.Scan(&pkHash, &raw); err != nil {
			continue
		}
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		gsiPKVal, ok := AttrVal(item[idx.PKAttr])
		if !ok {
			continue // sparse index — item has no GSI PK attribute
		}
		gsiSKVal := ""
		var gsiSKNum *float64
		if idx.SKAttr != "" {
			gsiSKVal, _ = AttrVal(item[idx.SKAttr])
			if idx.SKType == "N" {
				if n, ok2 := ParseNumeric(item[idx.SKAttr]); ok2 {
					gsiSKNum = &n
				}
			}
		}
		batch = append(batch, gsiRow{idx.IndexName, gsiPKVal, gsiSKVal, gsiSKNum, pkHash})
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return flush()
}

// DeleteGSI removes all rows for the named GSI.
func (s *PostgresDynamoDBItemStore) DeleteGSI(ctx context.Context, tableName string, schema TableSchema, indexName string) error {
	suffix := pgSuffix(tableName)
	gsiTable := "jc_dt_" + suffix + "_gsi"
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE index_name=$1`, gsiTable), indexName)
	return err
}

// ─── Index write helpers ──────────────────────────────────────────────────────

// upsertIndexRows maintains GSI and LSI index rows for the given item.
func (s *PostgresDynamoDBItemStore) upsertIndexRows(ctx context.Context, table, pkHash string, item map[string]any, schema *TableSchema) error {
	if schema == nil {
		return nil
	}
	suffix := pgSuffix(table)
	main := "jc_dt_" + suffix

	// Remove stale index rows for this pkHash first.
	s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s_gsi WHERE pk_hash=$1`, main), pkHash)
	s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s_lsi WHERE pk_hash=$1`, main), pkHash)

	// Insert GSI rows.
	for _, gsi := range schema.GSIs {
		gsiPKVal, ok := AttrVal(item[gsi.PKAttr])
		if !ok {
			continue
		}
		gsiSKVal := ""
		var gsiSKNum *float64
		if gsi.SKAttr != "" {
			gsiSKVal, _ = AttrVal(item[gsi.SKAttr])
			if gsi.SKType == "N" {
				if n, ok2 := ParseNumeric(item[gsi.SKAttr]); ok2 {
					gsiSKNum = &n
				}
			}
		}
		_, err := s.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s_gsi (index_name, gsi_pk_val, gsi_sk_val, gsi_sk_num, pk_hash)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (index_name, pk_hash) DO UPDATE
				SET gsi_pk_val=$2, gsi_sk_val=$3, gsi_sk_num=$4
		`, main), gsi.IndexName, gsiPKVal, gsiSKVal, gsiSKNum, pkHash)
		if err != nil {
			return err
		}
	}

	// Insert LSI rows.
	pkVal, _ := AttrVal(item[schema.PKAttr])
	for _, lsi := range schema.LSIs {
		lsiSKVal := ""
		var lsiSKNum *float64
		if lsi.SKAttr != "" {
			lsiSKVal, _ = AttrVal(item[lsi.SKAttr])
			if lsi.SKType == "N" {
				if n, ok2 := ParseNumeric(item[lsi.SKAttr]); ok2 {
					lsiSKNum = &n
				}
			}
		}
		_, err := s.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s_lsi (index_name, pk_val, lsi_sk_val, lsi_sk_num, pk_hash)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (index_name, pk_hash) DO UPDATE
				SET pk_val=$2, lsi_sk_val=$3, lsi_sk_num=$4
		`, main), lsi.IndexName, pkVal, lsiSKVal, lsiSKNum, pkHash)
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteIndexRows removes all GSI/LSI index rows for the given pkHash.
func (s *PostgresDynamoDBItemStore) deleteIndexRows(ctx context.Context, table, pkHash string) {
	suffix := pgSuffix(table)
	main := "jc_dt_" + suffix
	s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s_gsi WHERE pk_hash=$1`, main), pkHash)
	s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s_lsi WHERE pk_hash=$1`, main), pkHash)
}

// ─── Index query helpers ──────────────────────────────────────────────────────

// queryIndex routes a Query to the appropriate GSI or LSI index table.
func (s *PostgresDynamoDBItemStore) queryIndex(ctx context.Context, table string, q QuerySpec) ([]map[string]any, int, string, error) {
	idx := q.IndexSchema
	suffix := pgSuffix(table)
	main := "jc_dt_" + suffix

	var pkHashes []string
	var err error

	lsiOrder := lsiSortOrder(idx)
	gsiOrder := gsiSortOrder(idx)

	if idx.IsLSI {
		pkVal, _ := extractEqValue(q.KeyConditionExpression, idx.PKAttr, q.ExpressionAttributeNames, q.ExpressionAttributeValues)
		if pkVal == "" {
			pkHashes, err = s.collectPKHashes(ctx,
				fmt.Sprintf(`SELECT pk_hash FROM %s_lsi WHERE index_name=$1 ORDER BY %s`, main, lsiOrder),
				idx.IndexName)
		} else {
			pkHashes, err = s.collectPKHashes(ctx,
				fmt.Sprintf(`SELECT pk_hash FROM %s_lsi WHERE index_name=$1 AND pk_val=$2 ORDER BY %s`, main, lsiOrder),
				idx.IndexName, pkVal)
		}
	} else {
		gsiPKVal, _ := extractEqValue(q.KeyConditionExpression, idx.PKAttr, q.ExpressionAttributeNames, q.ExpressionAttributeValues)
		if gsiPKVal == "" {
			pkHashes, err = s.collectPKHashes(ctx,
				fmt.Sprintf(`SELECT pk_hash FROM %s_gsi WHERE index_name=$1 ORDER BY %s`, main, gsiOrder),
				idx.IndexName)
		} else {
			pkHashes, err = s.collectPKHashes(ctx,
				fmt.Sprintf(`SELECT pk_hash FROM %s_gsi WHERE index_name=$1 AND gsi_pk_val=$2 ORDER BY %s`, main, gsiOrder),
				idx.IndexName, gsiPKVal)
		}
	}
	if err != nil {
		return nil, 0, "", err
	}

	items, err := s.fetchByPKHashes(ctx, table, pkHashes)
	if err != nil {
		return nil, 0, "", err
	}
	items = sortByIndexSK(items, idx, q.ScanIndexForward)
	return s.filterAndPaginate(items, q.KeyConditionExpression, q.FilterExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues, q.ExclusiveStartKey, q.Limit)
}

// lsiSortOrder returns the ORDER BY clause for LSI queries, using numeric column when SK type is N.
func lsiSortOrder(idx *IndexKeyRef) string {
	if idx != nil && idx.SKType == "N" {
		return "lsi_sk_num NULLS LAST, pk_hash"
	}
	return "lsi_sk_val, pk_hash"
}

// gsiSortOrder returns the ORDER BY clause for GSI queries, using numeric column when SK type is N.
func gsiSortOrder(idx *IndexKeyRef) string {
	if idx != nil && idx.SKType == "N" {
		return "gsi_sk_num NULLS LAST, pk_hash"
	}
	return "gsi_sk_val, pk_hash"
}

// sortByIndexSK sorts items by the index SK attribute after fetching from jc_dynamodb_items.
// This restores ordering lost by the IN (...) clause in fetchByPKHashes.
func sortByIndexSK(items []map[string]any, idx *IndexKeyRef, scanFwd bool) []map[string]any {
	if idx == nil || idx.SKAttr == "" || len(items) == 0 {
		return items
	}
	sort.SliceStable(items, func(i, j int) bool {
		vi := items[i][idx.SKAttr]
		vj := items[j][idx.SKAttr]
		ni, iIsN := ParseNumeric(vi)
		nj, jIsN := ParseNumeric(vj)
		var less bool
		if idx.SKType == "N" && iIsN && jIsN {
			less = ni < nj
		} else {
			si, _ := AttrVal(vi)
			sj, _ := AttrVal(vj)
			less = si < sj
		}
		if !scanFwd {
			return !less
		}
		return less
	})
	return items
}

// scanIndex routes a Scan to the appropriate GSI index table.
func (s *PostgresDynamoDBItemStore) scanIndex(ctx context.Context, table string, sc ScanSpec) ([]map[string]any, int, string, error) {
	idx := sc.IndexSchema
	suffix := pgSuffix(table)
	main := "jc_dt_" + suffix

	var pkHashes []string
	var err error
	if idx.IsLSI {
		pkHashes, err = s.collectPKHashes(ctx,
			fmt.Sprintf(`SELECT pk_hash FROM %s_lsi WHERE index_name=$1 ORDER BY %s`, main, lsiSortOrder(idx)),
			idx.IndexName)
	} else {
		pkHashes, err = s.collectPKHashes(ctx,
			fmt.Sprintf(`SELECT pk_hash FROM %s_gsi WHERE index_name=$1 ORDER BY %s`, main, gsiSortOrder(idx)),
			idx.IndexName)
	}
	if err != nil {
		return nil, 0, "", err
	}

	items, err := s.fetchByPKHashes(ctx, table, pkHashes)
	if err != nil {
		return nil, 0, "", err
	}
	return s.filterAndPaginate(items, "", sc.FilterExpression, sc.ExpressionAttributeNames, sc.ExpressionAttributeValues, sc.ExclusiveStartKey, sc.Limit)
}

func (s *PostgresDynamoDBItemStore) collectPKHashes(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			continue
		}
		hashes = append(hashes, h)
	}
	return hashes, rows.Err()
}

func (s *PostgresDynamoDBItemStore) fetchByPKHashes(ctx context.Context, table string, hashes []string) ([]map[string]any, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(hashes)+1)
	args = append(args, table)
	placeholders := make([]string, len(hashes))
	for i, h := range hashes {
		args = append(args, h)
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}
	query := fmt.Sprintf(`SELECT item FROM jc_dynamodb_items WHERE table_name=$1 AND pk_hash IN (%s)`,
		strings.Join(placeholders, ","))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var item map[string]any
		if json.Unmarshal(raw, &item) == nil {
			items = append(items, item)
		}
	}
	return items, rows.Err()
}

func (s *PostgresDynamoDBItemStore) filterAndPaginate(items []map[string]any, keyExpr, filterExpr string, names map[string]string, values map[string]any, exclusiveStartKey string, limit int) ([]map[string]any, int, string, error) {
	// Apply key condition first to get key-matched items for scannedCount.
	var keyMatched []map[string]any
	for _, item := range items {
		if keyExpr != "" && !matchesKeyCondition(item, keyExpr, names, values, nil) {
			continue
		}
		keyMatched = append(keyMatched, item)
	}

	page := paginateItems(keyMatched, exclusiveStartKey, limit)
	scannedCount := len(page)

	var lastKey string
	if limit > 0 && len(page) == limit {
		b, _ := json.Marshal(page[len(page)-1])
		lastKey = string(b)
	}

	var result []map[string]any
	for _, item := range page {
		if filterExpr == "" || matchesFilter(item, filterExpr, names, values, nil) {
			result = append(result, item)
		}
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result, scannedCount, lastKey, nil
}
