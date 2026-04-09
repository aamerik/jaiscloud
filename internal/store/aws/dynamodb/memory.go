package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MemoryDynamoDBItemStore is an in-memory DynamoDBItemStore.
// Items are stored per table keyed by pkHash (caller-computed primary key string).
type MemoryDynamoDBItemStore struct {
	mu     sync.RWMutex
	tables map[string]map[string]map[string]any // table → pkHash → item
}

func NewMemoryDynamoDBItemStore() *MemoryDynamoDBItemStore {
	return &MemoryDynamoDBItemStore{
		tables: make(map[string]map[string]map[string]any),
	}
}

func (s *MemoryDynamoDBItemStore) tableMap(table string) map[string]map[string]any {
	if s.tables[table] == nil {
		s.tables[table] = make(map[string]map[string]any)
	}
	return s.tables[table]
}

func (s *MemoryDynamoDBItemStore) PutItem(_ context.Context, table, pkHash string, item map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tableMap(table)[pkHash] = copyItem(item)
	return nil
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

func (s *MemoryDynamoDBItemStore) DeleteItem(_ context.Context, table, pkHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tables[table]
	if t != nil {
		delete(t, pkHash)
	}
	return nil
}

func (s *MemoryDynamoDBItemStore) UpdateItem(_ context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tableMap(table)
	existing := t[pkHash]
	if existing == nil {
		existing = copyItem(item) // create new item from key
	} else {
		existing = copyItem(existing)
	}

	// Apply UpdateExpression (minimal: SET and REMOVE clauses).
	if spec.UpdateExpression != "" {
		applyUpdateExpression(existing, spec.UpdateExpression,
			spec.ExpressionAttributeNames, spec.ExpressionAttributeValues)
	} else {
		// If no expression, merge provided attributes (legacy).
		for k, v := range item {
			existing[k] = v
		}
	}

	t[pkHash] = existing
	return copyItem(existing), nil
}

func (s *MemoryDynamoDBItemStore) Query(_ context.Context, table string, q QuerySpec) ([]map[string]any, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.tables[table]
	if t == nil {
		return []map[string]any{}, "", nil
	}
	var results []map[string]any
	for _, item := range t {
		if matchesKeyCondition(item, q.KeyConditionExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues) {
			if q.FilterExpression == "" || matchesFilter(item, q.FilterExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues) {
				results = append(results, copyItem(item))
			}
		}
		if q.Limit > 0 && len(results) >= q.Limit {
			break
		}
	}
	if results == nil {
		results = []map[string]any{}
	}
	return results, "", nil
}

func (s *MemoryDynamoDBItemStore) Scan(_ context.Context, table string, sc ScanSpec) ([]map[string]any, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.tables[table]
	if t == nil {
		return []map[string]any{}, "", nil
	}
	var results []map[string]any
	for _, item := range t {
		if sc.FilterExpression == "" || matchesFilter(item, sc.FilterExpression, sc.ExpressionAttributeNames, sc.ExpressionAttributeValues) {
			results = append(results, copyItem(item))
		}
		if sc.Limit > 0 && len(results) >= sc.Limit {
			break
		}
	}
	if results == nil {
		results = []map[string]any{}
	}
	return results, "", nil
}

func (s *MemoryDynamoDBItemStore) BatchWriteItems(_ context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range reqs {
		t := s.tableMap(req.Table)
		if req.PutItem != nil {
			t[req.PutHash] = copyItem(req.PutItem)
		} else if req.DeleteKey != nil {
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

func (s *MemoryDynamoDBItemStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables = make(map[string]map[string]map[string]any)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// MemoryItemPKHash is the exported version used by the table provider as fallback.
func MemoryItemPKHash(item map[string]any) string { return itemPKHash(item) }

// itemPKHash computes a stable string key from a DynamoDB item/key.
// Uses a sorted concatenation of attribute name+value strings.
func itemPKHash(item map[string]any) string {
	// Collect all keys in sorted order for a stable hash.
	var parts []string
	for k, v := range item {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	// Simple stable sort by joining — good enough for an emulator.
	result := strings.Join(sortStrings(parts), "|")
	return result
}

func sortStrings(ss []string) []string {
	n := len(ss)
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if ss[i] > ss[j] {
				ss[i], ss[j] = ss[j], ss[i]
			}
		}
	}
	return ss
}

func copyItem(item map[string]any) map[string]any {
	cp := make(map[string]any, len(item))
	for k, v := range item {
		cp[k] = v
	}
	return cp
}

// applyUpdateExpression handles SET and REMOVE clauses.
// e.g. "SET #n = :val, #c = :cnt REMOVE #old"
func applyUpdateExpression(item map[string]any, expr string, names map[string]string, values map[string]any) {
	// Split on SET/REMOVE keywords (case-insensitive, rough parse).
	upper := strings.ToUpper(expr)
	setIdx := strings.Index(upper, "SET ")
	removeIdx := strings.Index(upper, " REMOVE ")
	addIdx := strings.Index(upper, " ADD ")

	if setIdx >= 0 {
		end := len(expr)
		if removeIdx > setIdx {
			end = removeIdx
		}
		if addIdx > setIdx && addIdx < end {
			end = addIdx
		}
		setPart := strings.TrimSpace(expr[setIdx+4 : end])
		for _, assignment := range splitComma(setPart) {
			parts := strings.SplitN(assignment, "=", 2)
			if len(parts) != 2 {
				continue
			}
			attrRef := strings.TrimSpace(parts[0])
			valRef := strings.TrimSpace(parts[1])
			attr := resolveExprName(attrRef, names)
			val := resolveExprValue(valRef, values)
			if attr != "" {
				item[attr] = val
			}
		}
	}

	if removeIdx >= 0 {
		end := len(expr)
		removePart := strings.TrimSpace(expr[removeIdx+8 : end])
		for _, attrRef := range splitComma(removePart) {
			attr := resolveExprName(strings.TrimSpace(attrRef), names)
			if attr != "" {
				delete(item, attr)
			}
		}
	}

	if addIdx >= 0 {
		addPart := strings.TrimSpace(expr[addIdx+5:])
		for _, assignment := range splitComma(addPart) {
			parts := strings.SplitN(assignment, " ", 2)
			if len(parts) != 2 {
				continue
			}
			attr := resolveExprName(strings.TrimSpace(parts[0]), names)
			valRef := strings.TrimSpace(parts[1])
			val := resolveExprValue(valRef, values)
			if attr != "" {
				// ADD for numbers: existing + val
				if existing, ok := item[attr]; ok {
					if em, ok := existing.(map[string]any); ok {
						if nv, ok := em["N"]; ok {
							if newMap, ok := val.(map[string]any); ok {
								if nv2, ok := newMap["N"]; ok {
									n1 := toFloat(nv)
									n2 := toFloat(nv2)
									item[attr] = map[string]any{"N": fmt.Sprintf("%g", n1+n2)}
									continue
								}
							}
						}
					}
				}
				item[attr] = val
			}
		}
	}
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

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case string:
		var f float64
		fmt.Sscanf(n, "%f", &f)
		return f
	}
	return 0
}

// matchesKeyCondition does a simple equality check for the partition key.
// Full expression parsing is deferred to Phase 2.
func matchesKeyCondition(item map[string]any, expr string, names map[string]string, values map[string]any) bool {
	if expr == "" {
		return true
	}
	// Handle "pk = :val" and "#pk = :val AND sk = :val2" patterns.
	conditions := splitAND(expr)
	for _, cond := range conditions {
		if !evalCondition(item, strings.TrimSpace(cond), names, values) {
			return false
		}
	}
	return true
}

func matchesFilter(item map[string]any, expr string, names map[string]string, values map[string]any) bool {
	if expr == "" {
		return true
	}
	return matchesKeyCondition(item, expr, names, values)
}

func splitAND(expr string) []string {
	upper := strings.ToUpper(expr)
	var parts []string
	start := 0
	for {
		idx := strings.Index(upper[start:], " AND ")
		if idx < 0 {
			parts = append(parts, expr[start:])
			break
		}
		parts = append(parts, expr[start:start+idx])
		start += idx + 5
	}
	return parts
}

func evalCondition(item map[string]any, cond string, names map[string]string, values map[string]any) bool {
	// "attr = :val" or "attr begins_with :val" etc.
	// Phase 1: support = and begins_with only.
	upper := strings.ToUpper(cond)

	if strings.Contains(upper, " BEGINS_WITH ") || strings.Contains(upper, "BEGINS_WITH(") {
		// begins_with(attr, :val)
		inner := strings.TrimSpace(cond)
		inner = strings.TrimPrefix(strings.ToLower(inner), "begins_with(")
		inner = strings.TrimSuffix(inner, ")")
		parts := strings.SplitN(inner, ",", 2)
		if len(parts) != 2 {
			return true
		}
		attr := resolveExprName(strings.TrimSpace(parts[0]), names)
		valRef := strings.TrimSpace(parts[1])
		val := resolveExprValue(valRef, values)
		itemVal := itemAttrString(item, attr)
		return strings.HasPrefix(itemVal, fmt.Sprintf("%v", extractDynamoString(val)))
	}

	if strings.Contains(cond, " = ") {
		parts := strings.SplitN(cond, " = ", 2)
		attr := resolveExprName(strings.TrimSpace(parts[0]), names)
		valRef := strings.TrimSpace(parts[1])
		val := resolveExprValue(valRef, values)
		itemVal := item[attr]
		return dynamoValuesEqual(itemVal, val)
	}

	return true
}

func itemAttrString(item map[string]any, attr string) string {
	v, ok := item[attr]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v", extractDynamoString(v))
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

func dynamoValuesEqual(a, b any) bool {
	as := fmt.Sprintf("%v", extractDynamoString(a))
	bs := fmt.Sprintf("%v", extractDynamoString(b))
	return as == bs
}
