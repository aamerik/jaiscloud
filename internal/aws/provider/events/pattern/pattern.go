// Package pattern implements the EventBridge / SNS / CW Logs event-pattern compiler and matcher.
package pattern

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// Mode controls which operators are allowed.
type Mode int

const (
	ModeEventBridge Mode = iota
	ModeSNS
	ModeCWLogs
)

// Kind identifies the operator for a Condition.
type Kind int

const (
	KindEqual Kind = iota
	KindOneOf
	KindPrefix
	KindSuffix
	KindEqualIgnoreCase
	KindExists
	KindNumeric
	KindCIDR
	KindWildcard
	KindAnythingBut
	KindNull
)

// NumOp is a single comparison operand inside a Numeric condition.
type NumOp struct {
	Op  string
	Val float64
}

// Condition is a single test applied to a field value.
type Condition struct {
	Kind       Kind
	StringVal  string
	ListVal    []any
	NumOps     []NumOp
	NestedNot  *Condition
	WildcardRE *regexp.Regexp
	CIDRBlock  *net.IPNet
	Exists     bool
	Null       bool
}

// Clause ties a dotted field path to one or more conditions.
type Clause struct {
	Path  []string
	Conds []Condition
}

// Pattern is a compiled event pattern.
// Branches is a list of AND-branches; any branch matching makes the whole pattern match.
type Pattern struct {
	Branches [][]Clause
	Mode     Mode
}

// Compile parses raw JSON pattern string into a Pattern.
// An empty string compiles to a pattern that matches everything.
func Compile(raw string, mode Mode) (*Pattern, error) {
	if raw == "" {
		return &Pattern{Mode: mode}, nil
	}
	var top map[string]any
	if err := json.Unmarshal([]byte(raw), &top); err != nil {
		return nil, fmt.Errorf("pattern: invalid JSON: %w", err)
	}
	p := &Pattern{Mode: mode}

	// Top-level $or: list of sub-patterns, each of which is AND-ed internally.
	if orList, ok := top["$or"].([]any); ok {
		for _, item := range orList {
			sub, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("pattern: $or elements must be objects")
			}
			branch, err := compileBranch(sub, nil, mode)
			if err != nil {
				return nil, err
			}
			p.Branches = append(p.Branches, branch)
		}
		return p, nil
	}

	// Normal case: single AND-branch.
	branch, err := compileBranch(top, nil, mode)
	if err != nil {
		return nil, err
	}
	p.Branches = append(p.Branches, branch)
	return p, nil
}

func compileBranch(obj map[string]any, prefix []string, mode Mode) ([]Clause, error) {
	var clauses []Clause
	for key, val := range obj {
		path := append(append([]string{}, prefix...), key)
		switch v := val.(type) {
		case []any:
			conds, err := compileConditions(v, mode)
			if err != nil {
				return nil, fmt.Errorf("pattern: field %q: %w", strings.Join(path, "."), err)
			}
			clauses = append(clauses, Clause{Path: path, Conds: conds})
		case map[string]any:
			sub, err := compileBranch(v, path, mode)
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, sub...)
		default:
			return nil, fmt.Errorf("pattern: field %q: value must be an array or object, got %T", strings.Join(path, "."), val)
		}
	}
	return clauses, nil
}

func compileConditions(list []any, mode Mode) ([]Condition, error) {
	var conds []Condition
	for _, item := range list {
		cond, err := compileCondition(item, mode)
		if err != nil {
			return nil, err
		}
		conds = append(conds, cond)
	}
	return conds, nil
}

func compileCondition(item any, mode Mode) (Condition, error) {
	switch v := item.(type) {
	case nil:
		return Condition{Kind: KindNull, Null: true}, nil
	case bool, float64, string:
		return Condition{Kind: KindEqual, StringVal: fmt.Sprintf("%v", v)}, nil
	case map[string]any:
		return compileOperator(v, mode)
	default:
		return Condition{}, fmt.Errorf("unsupported condition type %T", item)
	}
}

func compileOperator(m map[string]any, mode Mode) (Condition, error) {
	if pfx, ok := m["prefix"]; ok {
		s, ok := pfx.(string)
		if !ok {
			return Condition{}, fmt.Errorf("prefix value must be a string")
		}
		return Condition{Kind: KindPrefix, StringVal: s}, nil
	}
	if sfx, ok := m["suffix"]; ok {
		s, ok := sfx.(string)
		if !ok {
			return Condition{}, fmt.Errorf("suffix value must be a string")
		}
		return Condition{Kind: KindSuffix, StringVal: s}, nil
	}
	if eic, ok := m["equals-ignore-case"]; ok {
		s, ok := eic.(string)
		if !ok {
			return Condition{}, fmt.Errorf("equals-ignore-case value must be a string")
		}
		return Condition{Kind: KindEqualIgnoreCase, StringVal: strings.ToLower(s)}, nil
	}
	if ex, ok := m["exists"]; ok {
		b, ok := ex.(bool)
		if !ok {
			return Condition{}, fmt.Errorf("exists value must be a boolean")
		}
		return Condition{Kind: KindExists, Exists: b}, nil
	}
	if _, ok := m["numeric"]; ok {
		ops, err := compileNumeric(m["numeric"])
		if err != nil {
			return Condition{}, err
		}
		return Condition{Kind: KindNumeric, NumOps: ops}, nil
	}
	if cidrVal, ok := m["cidr"]; ok {
		s, ok := cidrVal.(string)
		if !ok {
			return Condition{}, fmt.Errorf("cidr value must be a string")
		}
		_, cidrBlock, err := net.ParseCIDR(s)
		if err != nil {
			return Condition{}, fmt.Errorf("cidr: invalid CIDR %q: %w", s, err)
		}
		return Condition{Kind: KindCIDR, CIDRBlock: cidrBlock}, nil
	}
	if wc, ok := m["wildcard"]; ok {
		if mode != ModeEventBridge {
			return Condition{}, fmt.Errorf("wildcard operator only allowed in EventBridge patterns")
		}
		s, ok := wc.(string)
		if !ok {
			return Condition{}, fmt.Errorf("wildcard value must be a string")
		}
		re, err := compileWildcard(s)
		if err != nil {
			return Condition{}, err
		}
		return Condition{Kind: KindWildcard, StringVal: s, WildcardRE: re}, nil
	}
	if ab, ok := m["anything-but"]; ok {
		return compileAnythingBut(ab, mode)
	}
	return Condition{}, fmt.Errorf("unknown operator in %v", m)
}

func compileNumeric(raw any) ([]NumOp, error) {
	list, ok := raw.([]any)
	if !ok || len(list) < 2 {
		return nil, fmt.Errorf("numeric requires an array with at least [op, num]")
	}
	if len(list) != 2 && len(list) != 4 {
		return nil, fmt.Errorf("numeric requires 2 or 4 elements, got %d", len(list))
	}
	var ops []NumOp
	for i := 0; i < len(list); i += 2 {
		op, ok := list[i].(string)
		if !ok {
			return nil, fmt.Errorf("numeric operator must be a string")
		}
		numVal, ok := list[i+1].(float64)
		if !ok {
			return nil, fmt.Errorf("numeric value must be a number")
		}
		if op != "=" && op != "!=" && op != "<" && op != "<=" && op != ">" && op != ">=" {
			return nil, fmt.Errorf("numeric: unknown operator %q", op)
		}
		ops = append(ops, NumOp{Op: op, Val: numVal})
	}
	if len(ops) == 2 {
		if ops[0].Val > ops[1].Val && (ops[0].Op == ">" || ops[0].Op == ">=") && (ops[1].Op == "<" || ops[1].Op == "<=") {
			return nil, fmt.Errorf("numeric range: lower bound must be less than upper bound")
		}
	}
	return ops, nil
}

func compileWildcard(s string) (*regexp.Regexp, error) {
	count := strings.Count(s, "*")
	if count > 5 {
		return nil, fmt.Errorf("wildcard: maximum 5 wildcards allowed, got %d", count)
	}
	var sb strings.Builder
	sb.WriteByte('^')
	for _, ch := range s {
		if ch == '*' {
			sb.WriteString(`[^"]*`)
		} else {
			sb.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	sb.WriteByte('$')
	return regexp.Compile(sb.String())
}

func compileAnythingBut(val any, mode Mode) (Condition, error) {
	switch v := val.(type) {
	case string:
		return Condition{Kind: KindAnythingBut, ListVal: []any{v}}, nil
	case float64:
		return Condition{Kind: KindAnythingBut, ListVal: []any{v}}, nil
	case []any:
		return Condition{Kind: KindAnythingBut, ListVal: v}, nil
	case map[string]any:
		if pfx, ok := v["prefix"].(string); ok {
			nested := Condition{Kind: KindPrefix, StringVal: pfx}
			return Condition{Kind: KindAnythingBut, NestedNot: &nested}, nil
		}
		if sfx, ok := v["suffix"].(string); ok {
			nested := Condition{Kind: KindSuffix, StringVal: sfx}
			return Condition{Kind: KindAnythingBut, NestedNot: &nested}, nil
		}
		return Condition{}, fmt.Errorf("anything-but object must have prefix or suffix key")
	default:
		return Condition{}, fmt.Errorf("anything-but value must be scalar, list, or {prefix/suffix} object")
	}
}

// Match returns true if envelope matches the pattern.
func (p *Pattern) Match(envelope map[string]any) bool {
	if len(p.Branches) == 0 {
		return true
	}
	flat := flattenMap(envelope, "")
	for _, branch := range p.Branches {
		if matchBranch(branch, flat, envelope) {
			return true
		}
	}
	return false
}

func matchBranch(branch []Clause, flat map[string]any, envelope map[string]any) bool {
	for _, clause := range branch {
		if !matchClause(clause, flat, envelope) {
			return false
		}
	}
	return true
}

func matchClause(clause Clause, flat map[string]any, envelope map[string]any) bool {
	path := strings.Join(clause.Path, ".")
	val, exists := flat[path]

	if len(clause.Conds) == 0 {
		return true
	}
	// AWS EventBridge/SNS semantics: conditions within a clause are OR-ed.
	// e.g. {"color":["red","blue"]} matches if color is "red" OR "blue".
	// AND-semantics live within a single Condition's NumOps (numeric range checks).
	for _, cond := range clause.Conds {
		if matchCondition(cond, val, exists) {
			return true
		}
	}
	return false
}

func matchCondition(cond Condition, val any, exists bool) bool {
	switch cond.Kind {
	case KindExists:
		return exists == cond.Exists
	case KindNull:
		return val == nil && exists
	case KindEqual:
		if !exists {
			return false
		}
		return fmt.Sprintf("%v", val) == cond.StringVal
	case KindOneOf:
		if !exists {
			return false
		}
		s := fmt.Sprintf("%v", val)
		for _, opt := range cond.ListVal {
			if fmt.Sprintf("%v", opt) == s {
				return true
			}
		}
		return false
	case KindPrefix:
		if !exists {
			return false
		}
		return strings.HasPrefix(fmt.Sprintf("%v", val), cond.StringVal)
	case KindSuffix:
		if !exists {
			return false
		}
		return strings.HasSuffix(fmt.Sprintf("%v", val), cond.StringVal)
	case KindEqualIgnoreCase:
		if !exists {
			return false
		}
		return strings.ToLower(fmt.Sprintf("%v", val)) == cond.StringVal
	case KindNumeric:
		if !exists {
			return false
		}
		f, err := toFloat(val)
		if err != nil {
			return false
		}
		for _, op := range cond.NumOps {
			if !applyNumOp(op, f) {
				return false
			}
		}
		return true
	case KindCIDR:
		if !exists {
			return false
		}
		ip := net.ParseIP(fmt.Sprintf("%v", val))
		if ip == nil {
			return false
		}
		return cond.CIDRBlock.Contains(ip)
	case KindWildcard:
		if !exists {
			return false
		}
		return cond.WildcardRE.MatchString(fmt.Sprintf("%v", val))
	case KindAnythingBut:
		if !exists {
			return false
		}
		if cond.NestedNot != nil {
			return !matchCondition(*cond.NestedNot, val, exists)
		}
		s := fmt.Sprintf("%v", val)
		for _, opt := range cond.ListVal {
			if fmt.Sprintf("%v", opt) == s {
				return false
			}
		}
		return true
	}
	return false
}

func applyNumOp(op NumOp, val float64) bool {
	switch op.Op {
	case "=":
		return val == op.Val
	case "!=":
		return val != op.Val
	case "<":
		return val < op.Val
	case "<=":
		return val <= op.Val
	case ">":
		return val > op.Val
	case ">=":
		return val >= op.Val
	}
	return false
}

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("cannot convert %T to float64", v)
}

// flattenMap converts a nested map into a dot-path keyed flat map.
func flattenMap(m map[string]any, prefix string) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if sub, ok := v.(map[string]any); ok {
			for sk, sv := range flattenMap(sub, key) {
				result[sk] = sv
			}
		}
		result[key] = v
	}
	return result
}
