package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryDynamoDBItemStore is an in-memory DynamoDBItemStore.
// Items are stored per table keyed by pkHash (caller-computed primary key string).
type MemoryDynamoDBItemStore struct {
	mu      sync.RWMutex
	schemas map[string]TableSchema                    // tableName → schema
	tables  map[string]map[string]map[string]any      // table → pkHash → item
	gsiIdx  map[string]map[string]map[string][]string // tableName → indexName → gsiPKVal → []pkHash
}

func NewMemoryDynamoDBItemStore() *MemoryDynamoDBItemStore {
	return &MemoryDynamoDBItemStore{
		schemas: make(map[string]TableSchema),
		tables:  make(map[string]map[string]map[string]any),
		gsiIdx:  make(map[string]map[string]map[string][]string),
	}
}

func (s *MemoryDynamoDBItemStore) tableMap(table string) map[string]map[string]any {
	if s.tables[table] == nil {
		s.tables[table] = make(map[string]map[string]any)
	}
	return s.tables[table]
}

func (s *MemoryDynamoDBItemStore) PutItem(_ context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tableMap(table)
	oldItem := t[pkHash]
	if cond.ConditionExpression != "" {
		existing := oldItem
		if existing == nil {
			existing = map[string]any{}
		}
		ok, err := matchesFilter(existing, cond.ConditionExpression, cond.ExpressionAttributeNames, cond.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, &conditionFailedError{}
		}
	}
	var returnOld map[string]any
	if cond.ReturnValues == "ALL_OLD" && oldItem != nil {
		returnOld = copyItem(oldItem)
	}
	// Remove old GSI entries before overwriting.
	if oldItem != nil && cond.Schema != nil {
		s.removeGSIEntries(table, pkHash, oldItem, cond.Schema)
	}
	t[pkHash] = copyItem(item)
	// Add new GSI entries.
	if cond.Schema != nil {
		s.addGSIEntries(table, pkHash, item, cond.Schema)
	}
	return returnOld, nil
}

func (s *MemoryDynamoDBItemStore) GetItem(_ context.Context, table, pkHash string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.tables[table]
	if t == nil {
		return nil, nil
	}
	item, ok := t[pkHash]
	if !ok {
		return nil, nil
	}
	return copyItem(item), nil
}

func (s *MemoryDynamoDBItemStore) DeleteItem(_ context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tables[table]
	var oldItem map[string]any
	if t != nil {
		oldItem = t[pkHash]
	}
	if cond.ConditionExpression != "" {
		existing := oldItem
		if existing == nil {
			existing = map[string]any{}
		}
		ok, err := matchesFilter(existing, cond.ConditionExpression, cond.ExpressionAttributeNames, cond.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, &conditionFailedError{}
		}
	}
	var returnOld map[string]any
	if cond.ReturnValues == "ALL_OLD" && oldItem != nil {
		returnOld = copyItem(oldItem)
	}
	if t != nil {
		if cond.Schema != nil && oldItem != nil {
			s.removeGSIEntries(table, pkHash, oldItem, cond.Schema)
		}
		delete(t, pkHash)
	}
	return returnOld, nil
}

// conditionFailedError is returned when a ConditionExpression is not satisfied.
type conditionFailedError struct{}

func (e *conditionFailedError) Error() string { return "ConditionalCheckFailedException" }

func (s *MemoryDynamoDBItemStore) UpdateItem(_ context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tableMap(table)
	existing := t[pkHash]

	if spec.ConditionExpression != "" {
		check := existing
		if check == nil {
			check = map[string]any{}
		}
		ok, err := matchesFilter(check, spec.ConditionExpression, spec.ExpressionAttributeNames, spec.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, &conditionFailedError{}
		}
	}

	oldItem := existing
	if existing == nil {
		existing = copyItem(item)
	} else {
		existing = copyItem(existing)
	}

	if spec.UpdateExpression != "" {
		if err := applyUpdateExpression(existing, spec.UpdateExpression,
			spec.ExpressionAttributeNames, spec.ExpressionAttributeValues); err != nil {
			return nil, err
		}
	} else {
		for k, v := range item {
			existing[k] = v
		}
	}

	if spec.Schema != nil && oldItem != nil {
		s.removeGSIEntries(table, pkHash, oldItem, spec.Schema)
	}
	t[pkHash] = existing
	if spec.Schema != nil {
		s.addGSIEntries(table, pkHash, existing, spec.Schema)
	}
	return copyItem(existing), nil
}

func (s *MemoryDynamoDBItemStore) Query(_ context.Context, table string, q QuerySpec) ([]map[string]any, int, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.tables[table]
	if t == nil {
		return []map[string]any{}, 0, "", nil
	}
	schema := s.schemas[table]
	attrTypes := buildAttrTypes(schema)
	_ = attrTypes // retained for build-attrTypes call; type info is now embedded in DynamoDB values

	// Collect candidates based on index routing.
	var candidates []map[string]any
	var sortAttr string
	var sortIsNumeric bool
	var sortForward = true
	if q.ScanIndexForward {
		sortForward = true
	}

	if q.IndexSchema != nil {
		idx := q.IndexSchema
		sortAttr = idx.SKAttr
		sortIsNumeric = idx.SKType == "N"
		if !sortForward {
			sortForward = false
		}

		if !idx.IsLSI {
			// GSI: use gsiIdx to narrow candidates to matching GSI partition.
			gsiPKVal, found := extractEqValue(q.KeyConditionExpression, idx.PKAttr, q.ExpressionAttributeNames, q.ExpressionAttributeValues)
			if found {
				for _, ph := range s.gsiIdx[table][idx.IndexName][gsiPKVal] {
					if item, ok := t[ph]; ok {
						candidates = append(candidates, item)
					}
				}
			} else {
				for _, item := range t {
					candidates = append(candidates, item)
				}
			}
		} else {
			// LSI: filter by main table PK value.
			mainPKVal, found := extractEqValue(q.KeyConditionExpression, schema.PKAttr, q.ExpressionAttributeNames, q.ExpressionAttributeValues)
			if found {
				for _, item := range t {
					if v, ok := AttrVal(item[schema.PKAttr]); ok && v == mainPKVal {
						candidates = append(candidates, item)
					}
				}
			} else {
				for _, item := range t {
					candidates = append(candidates, item)
				}
			}
		}
	} else {
		sortAttr = schema.SKAttr
		sortIsNumeric = schema.SKType == "N"
		for _, item := range t {
			candidates = append(candidates, item)
		}
	}

	// Apply key condition only → keyMatched (used for ScannedCount and pagination).
	var keyMatched []map[string]any
	for _, item := range candidates {
		ok, err := matchesKeyCondition(item, q.KeyConditionExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues)
		if err != nil {
			return nil, 0, "", err
		}
		if !ok {
			continue
		}
		keyMatched = append(keyMatched, item)
	}

	// Sort by sort key if available, otherwise by pkHash for stable order.
	if sortAttr != "" {
		skAttr := sortAttr
		numeric := sortIsNumeric
		fwd := sortForward
		sort.Slice(keyMatched, func(i, j int) bool {
			if numeric {
				ni, _ := ParseNumeric(keyMatched[i][skAttr])
				nj, _ := ParseNumeric(keyMatched[j][skAttr])
				if fwd {
					return ni < nj
				}
				return ni > nj
			}
			vi, _ := AttrVal(keyMatched[i][skAttr])
			vj, _ := AttrVal(keyMatched[j][skAttr])
			if fwd {
				return vi < vj
			}
			return vi > vj
		})
	} else {
		sort.Slice(keyMatched, func(i, j int) bool { return itemPKHash(keyMatched[i]) < itemPKHash(keyMatched[j]) })
	}

	// Paginate on key-condition-matched items; scannedCount = items in page.
	page := paginateItems(keyMatched, q.ExclusiveStartKey, q.Limit)
	scannedCount := len(page)

	var lastKey string
	if q.Limit > 0 && len(page) == q.Limit {
		b, _ := json.Marshal(page[len(page)-1])
		lastKey = string(b)
	}

	// Apply FilterExpression on the page.
	var result []map[string]any
	for _, item := range page {
		if q.FilterExpression == "" {
			result = append(result, copyItem(item))
			continue
		}
		ok, err := matchesFilter(item, q.FilterExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues)
		if err != nil {
			return nil, 0, "", err
		}
		if ok {
			result = append(result, copyItem(item))
		}
	}
	if len(result) == 0 {
		result = []map[string]any{}
	}
	return result, scannedCount, lastKey, nil
}

func (s *MemoryDynamoDBItemStore) Scan(_ context.Context, table string, sc ScanSpec) ([]map[string]any, int, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.tables[table]
	if t == nil {
		return []map[string]any{}, 0, "", nil
	}
	schema := s.schemas[table]
	attrTypes := buildAttrTypes(schema)
	_ = attrTypes

	var candidates []map[string]any
	if sc.IndexSchema != nil && !sc.IndexSchema.IsLSI {
		// GSI scan: iterate all buckets in the GSI index.
		if idx := s.gsiIdx[table][sc.IndexSchema.IndexName]; idx != nil {
			seen := make(map[string]bool)
			for _, hashes := range idx {
				for _, ph := range hashes {
					if !seen[ph] {
						seen[ph] = true
						if item, ok := t[ph]; ok {
							candidates = append(candidates, item)
						}
					}
				}
			}
		}
	} else {
		for _, item := range t {
			candidates = append(candidates, item)
		}
	}

	// Sort all candidates stably before pagination.
	sort.Slice(candidates, func(i, j int) bool { return itemPKHash(candidates[i]) < itemPKHash(candidates[j]) })

	// Paginate candidates; scannedCount = items in page (before filter).
	page := paginateItems(candidates, sc.ExclusiveStartKey, sc.Limit)
	scannedCount := len(page)

	var lastKey string
	if sc.Limit > 0 && len(page) == sc.Limit {
		b, _ := json.Marshal(page[len(page)-1])
		lastKey = string(b)
	}

	// Apply FilterExpression on the page.
	var result []map[string]any
	for _, item := range page {
		if sc.FilterExpression == "" {
			result = append(result, copyItem(item))
			continue
		}
		ok, err := matchesFilter(item, sc.FilterExpression, sc.ExpressionAttributeNames, sc.ExpressionAttributeValues)
		if err != nil {
			return nil, 0, "", err
		}
		if ok {
			result = append(result, copyItem(item))
		}
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result, scannedCount, lastKey, nil
}

// paginateItems skips items up to and including the ExclusiveStartKey item.
func paginateItems(items []map[string]any, exclusiveStartKey string, limit int) []map[string]any {
	start := 0
	if exclusiveStartKey != "" {
		var startKey map[string]any
		if json.Unmarshal([]byte(exclusiveStartKey), &startKey) == nil {
			startHash := itemPKHash(startKey)
			for i, item := range items {
				if itemPKHash(item) == startHash {
					start = i + 1
					break
				}
			}
		}
	}
	items = items[start:]
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

func (s *MemoryDynamoDBItemStore) BatchWriteItems(_ context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range reqs {
		t := s.tableMap(req.Table)
		if req.PutItem != nil {
			if req.Schema != nil {
				if old, ok := t[req.PutHash]; ok {
					s.removeGSIEntries(req.Table, req.PutHash, old, req.Schema)
				}
			}
			t[req.PutHash] = copyItem(req.PutItem)
			if req.Schema != nil {
				s.addGSIEntries(req.Table, req.PutHash, req.PutItem, req.Schema)
			}
		} else if req.DeleteKey != nil {
			if req.Schema != nil {
				if old, ok := t[req.DeleteHash]; ok {
					s.removeGSIEntries(req.Table, req.DeleteHash, old, req.Schema)
				}
			}
			delete(t, req.DeleteHash)
		}
	}
	return nil, nil
}

func (s *MemoryDynamoDBItemStore) BatchGetItems(_ context.Context, reqs []BatchGetRequest) (map[string][]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]map[string]any)
	for _, req := range reqs {
		t := s.tables[req.Table]
		for _, key := range req.Keys {
			h := itemPKHash(key)
			if t != nil {
				if item, ok := t[h]; ok {
					result[req.Table] = append(result[req.Table], copyItem(item))
				}
			}
		}
	}
	return result, nil
}

// TransactWriteItems evaluates all conditions atomically (under a single lock),
// then applies all writes. Returns non-nil reasons if any condition failed.
func (s *MemoryDynamoDBItemStore) TransactWriteItems(_ context.Context, ops []TransactWriteOp) ([]CancellationReason, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Phase 1: check all conditions.
	reasons := make([]CancellationReason, len(ops))
	anyFailed := false
	for i, op := range ops {
		if op.Cond.ConditionExpression == "" && op.Type != "ConditionCheck" {
			reasons[i] = CancellationReason{Code: "None"}
			continue
		}
		existing := s.tableMap(op.Table)[op.PKHash]
		if existing == nil {
			existing = map[string]any{}
		}
		condExpr := op.Cond.ConditionExpression
		if op.Type == "ConditionCheck" && condExpr == "" {
			reasons[i] = CancellationReason{Code: "None"}
			continue
		}
		ok, err := matchesFilter(existing, condExpr, op.Cond.ExpressionAttributeNames, op.Cond.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		if !ok {
			reasons[i] = CancellationReason{Code: "ConditionalCheckFailed", Message: "The conditional request failed"}
			anyFailed = true
		} else {
			reasons[i] = CancellationReason{Code: "None"}
		}
	}
	if anyFailed {
		return reasons, nil
	}

	// Phase 2: apply all writes (conditions already satisfied).
	for _, op := range ops {
		t := s.tableMap(op.Table)
		switch op.Type {
		case "Put":
			oldItem := t[op.PKHash]
			if oldItem != nil && op.Cond.Schema != nil {
				s.removeGSIEntries(op.Table, op.PKHash, oldItem, op.Cond.Schema)
			}
			t[op.PKHash] = copyItem(op.Item)
			if op.Cond.Schema != nil {
				s.addGSIEntries(op.Table, op.PKHash, op.Item, op.Cond.Schema)
			}
		case "Delete":
			oldItem := t[op.PKHash]
			if oldItem != nil && op.Cond.Schema != nil {
				s.removeGSIEntries(op.Table, op.PKHash, oldItem, op.Cond.Schema)
			}
			delete(t, op.PKHash)
		case "Update":
			existing := t[op.PKHash]
			base := op.Key
			if existing != nil {
				base = copyItem(existing)
			}
			updated := copyItem(base)
			if err := applyUpdateExpression(updated, op.Update.UpdateExpression,
				op.Update.ExpressionAttributeNames, op.Update.ExpressionAttributeValues); err != nil {
				return nil, err
			}
			if op.Update.Schema != nil {
				if existing != nil {
					s.removeGSIEntries(op.Table, op.PKHash, existing, op.Update.Schema)
				}
				s.addGSIEntries(op.Table, op.PKHash, updated, op.Update.Schema)
			}
			t[op.PKHash] = updated
		}
		// ConditionCheck: condition already evaluated — no write needed.
	}
	return nil, nil
}

func (s *MemoryDynamoDBItemStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas = make(map[string]TableSchema)
	s.tables = make(map[string]map[string]map[string]any)
	s.gsiIdx = make(map[string]map[string]map[string][]string)
}

func (s *MemoryDynamoDBItemStore) CreateTableSchema(_ context.Context, schema TableSchema) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[schema.TableName] = schema
	if s.tables[schema.TableName] == nil {
		s.tables[schema.TableName] = make(map[string]map[string]any)
	}
	s.gsiIdx[schema.TableName] = make(map[string]map[string][]string)
	for _, gsi := range schema.GSIs {
		s.gsiIdx[schema.TableName][gsi.IndexName] = make(map[string][]string)
	}
	return nil
}

func (s *MemoryDynamoDBItemStore) DropTableSchema(_ context.Context, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.schemas, tableName)
	delete(s.tables, tableName)
	delete(s.gsiIdx, tableName)
	return nil
}

func (s *MemoryDynamoDBItemStore) AddGSI(_ context.Context, tableName string, schema TableSchema, idx IndexDef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[tableName] = schema
	if s.gsiIdx[tableName] == nil {
		s.gsiIdx[tableName] = make(map[string]map[string][]string)
	}
	bucket := make(map[string][]string)
	for pkHash, item := range s.tables[tableName] {
		if gsiPKVal, ok := AttrVal(item[idx.PKAttr]); ok {
			bucket[gsiPKVal] = append(bucket[gsiPKVal], pkHash)
		}
	}
	s.gsiIdx[tableName][idx.IndexName] = bucket
	return nil
}

func (s *MemoryDynamoDBItemStore) DeleteGSI(_ context.Context, tableName string, schema TableSchema, indexName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[tableName] = schema
	if idx := s.gsiIdx[tableName]; idx != nil {
		delete(idx, indexName)
	}
	return nil
}

// ─── GSI index helpers ────────────────────────────────────────────────────────

// removeGSIEntries removes pkHash from all GSI index slices for the old item.
// Must be called with s.mu held (write).
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

// addGSIEntries inserts pkHash into GSI index buckets for each GSI whose PK attr is present.
// Must be called with s.mu held (write).
func (s *MemoryDynamoDBItemStore) addGSIEntries(table, pkHash string, item map[string]any, schema *TableSchema) {
	if s.gsiIdx[table] == nil {
		s.gsiIdx[table] = make(map[string]map[string][]string)
	}
	for _, gsi := range schema.GSIs {
		gsiPKVal, ok := AttrVal(item[gsi.PKAttr])
		if !ok {
			continue
		}
		if s.gsiIdx[table][gsi.IndexName] == nil {
			s.gsiIdx[table][gsi.IndexName] = make(map[string][]string)
		}
		s.gsiIdx[table][gsi.IndexName][gsiPKVal] = append(
			s.gsiIdx[table][gsi.IndexName][gsiPKVal], pkHash,
		)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// MemoryItemPKHash is the exported version used by the table provider as fallback.
func MemoryItemPKHash(item map[string]any) string { return itemPKHash(item) }

func itemPKHash(item map[string]any) string {
	var parts []string
	for k, v := range item {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	result := strings.Join(sortStrings(parts), "|")
	return result
}

func sortStrings(ss []string) []string {
	sort.Strings(ss)
	return ss
}

func copyItem(item map[string]any) map[string]any {
	cp := make(map[string]any, len(item))
	for k, v := range item {
		cp[k] = v
	}
	return cp
}

// buildAttrTypes returns a map of attribute name → type ("S", "N", "B") from the table schema.
func buildAttrTypes(schema TableSchema) map[string]string {
	m := make(map[string]string)
	if schema.PKAttr != "" {
		m[schema.PKAttr] = schema.PKType
	}
	if schema.SKAttr != "" {
		m[schema.SKAttr] = schema.SKType
	}
	for _, gsi := range schema.GSIs {
		if gsi.PKAttr != "" {
			m[gsi.PKAttr] = gsi.PKType
		}
		if gsi.SKAttr != "" {
			m[gsi.SKAttr] = gsi.SKType
		}
	}
	for _, lsi := range schema.LSIs {
		if lsi.SKAttr != "" {
			m[lsi.SKAttr] = lsi.SKType
		}
	}
	return m
}

// extractEqValue finds the equality value for attrName in a key condition expression.
func extractEqValue(expr, attrName string, names map[string]string, values map[string]any) (string, bool) {
	for _, cond := range splitAND(expr) {
		cond = strings.TrimSpace(cond)
		if !strings.Contains(cond, " = ") {
			continue
		}
		parts := strings.SplitN(cond, " = ", 2)
		attr := resolveExprName(strings.TrimSpace(parts[0]), names)
		if attr != attrName {
			continue
		}
		val := resolveExprValue(strings.TrimSpace(parts[1]), values)
		s, ok := AttrVal(val)
		if !ok {
			s = fmt.Sprintf("%v", extractDynamoString(val))
		}
		return s, true
	}
	return "", false
}

func splitComma(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range s {
		switch c {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(s) {
		parts = append(parts, strings.TrimSpace(s[start:]))
	}
	return parts
}

func resolveExprName(ref string, names map[string]string) string {
	if strings.HasPrefix(ref, "#") {
		if n, ok := names[ref]; ok {
			return n
		}
		return ref[1:]
	}
	return ref
}

func resolveExprValue(ref string, values map[string]any) any {
	if strings.HasPrefix(ref, ":") {
		if v, ok := values[ref]; ok {
			return v
		}
	}
	return ref
}

// matchesKeyCondition evaluates a DynamoDB KeyConditionExpression against an item.
func matchesKeyCondition(item map[string]any, expr string, names map[string]string, values map[string]any) (bool, error) {
	return EvalFilter(item, expr, names, values)
}

// matchesFilter evaluates a DynamoDB FilterExpression or ConditionExpression against an item.
func matchesFilter(item map[string]any, expr string, names map[string]string, values map[string]any) (bool, error) {
	return EvalFilter(item, expr, names, values)
}

// splitAND splits a condition expression on top-level AND connectives,
// correctly handling "attr BETWEEN :lo AND :hi" patterns.
func splitAND(expr string) []string {
	upper := strings.ToUpper(expr)
	var parts []string
	partStart := 0
	i := 0
	for i+5 <= len(upper) {
		if upper[i:i+5] != " AND " {
			i++
			continue
		}
		clause := upper[partStart:i]
		betweenCount := strings.Count(clause, " BETWEEN ")
		andInClause := strings.Count(clause, " AND ")
		if betweenCount > andInClause {
			i += 5
			continue
		}
		parts = append(parts, expr[partStart:i])
		partStart = i + 5
		i = partStart
	}
	parts = append(parts, expr[partStart:])
	return parts
}

func extractDynamoString(v any) any {
	if m, ok := v.(map[string]any); ok {
		if s, ok := m["S"]; ok {
			return s
		}
		if n, ok := m["N"]; ok {
			return n
		}
	}
	return v
}
