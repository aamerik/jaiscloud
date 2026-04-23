package dynamodb

import "fmt"

// TableSchema is the parsed, normalised form of a DynamoDB table definition.
// It is built by the provider at CreateTable time and passed on every data-plane
// call so the store knows which attributes are key columns.
type TableSchema struct {
	TableName string     // e.g. "Orders"
	PKAttr    string     // partition key attribute name
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
	SKAttr     string // index sort key attribute name; "" if no sort key
	PKType     string // "S", "N", or "B"
	SKType     string // "S", "N", or "B"; "" if no index sort key
	Projection ProjectionDef
	IsLSI      bool // true for LocalSecondaryIndex, false for GlobalSecondaryIndex
}

// ProjectionDef controls which attributes an index query returns.
type ProjectionDef struct {
	// Type is "ALL", "KEYS_ONLY", or "INCLUDE".
	Type        string
	NonKeyAttrs []string // only meaningful when Type == "INCLUDE"
}

// IndexKeyRef is resolved by the provider from TableSchema and passed in
// QuerySpec/ScanSpec so the store selects the right index table without
// re-reading jc_resources.
type IndexKeyRef struct {
	IndexName string
	PKAttr    string
	SKAttr    string
	PKType    string
	SKType    string
	IsLSI     bool // routes to _lsi table instead of _gsi
}

// AttrVal extracts the raw string value from a DynamoDB typed attribute map.
// Input: {"S": "hello"} → "hello"
// Input: {"N": "42"}    → "42"
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

// ParseNumeric converts a DynamoDB number attribute to float64.
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
