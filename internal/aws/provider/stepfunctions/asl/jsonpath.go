package asl

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// EvalPath evaluates a JSONPath expression against doc (unmarshalled JSON as any).
// Returns the matched value or nil if path evaluates to nothing.
// AWS SFN uses the Stefan Goessner JSONPath dialect.
func EvalPath(doc any, path string) (any, error) {
	if path == "$" {
		return doc, nil
	}
	if !strings.HasPrefix(path, "$") {
		return nil, fmt.Errorf("JSONPath must start with $: %q", path)
	}
	tokens, err := tokenize(path[1:]) // strip leading $
	if err != nil {
		return nil, fmt.Errorf("jsonpath parse error: %w", err)
	}
	results, err := evalTokens([]any{doc}, tokens)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	if len(results) == 1 {
		return results[0], nil
	}
	return results, nil
}

// SetPath sets value at the given reference path (used for ResultPath).
// Path must be a simple dot-notation path (no arrays/filters).
// Returns a new document with the value set.
func SetPath(doc any, path string, value any) (any, error) {
	if path == "$" {
		return value, nil
	}
	if path == "null" || path == "" {
		// ResultPath of null discards the output entirely
		return doc, nil
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, fmt.Errorf("ResultPath must start with $.: %q", path)
	}
	// Walk into the document, creating maps as needed
	docMap, ok := toMap(doc)
	if !ok {
		docMap = make(map[string]any)
	}
	parts := strings.Split(path[2:], ".") // strip "$."
	setNested(docMap, parts, value)
	return docMap, nil
}

func setNested(m map[string]any, parts []string, value any) {
	if len(parts) == 1 {
		m[parts[0]] = value
		return
	}
	child, ok := m[parts[0]]
	childMap, isMap := toMap(child)
	if !ok || !isMap {
		childMap = make(map[string]any)
	}
	setNested(childMap, parts[1:], value)
	m[parts[0]] = childMap
}

// EvalParameters processes a Parameters/ResultSelector object.
// Keys ending in ".$" have their value treated as a JSONPath (or intrinsic) expression.
func EvalParameters(params any, input any, contextObj any) (any, error) {
	switch p := params.(type) {
	case map[string]any:
		result := make(map[string]any, len(p))
		for k, v := range p {
			if strings.HasSuffix(k, ".$") {
				newKey := strings.TrimSuffix(k, ".$")
				pathStr, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("value of %q must be a string", k)
				}
				var val any
				var err error
				switch {
				case strings.HasPrefix(pathStr, "$$"):
					// Context object reference ($$. prefix)
					val, err = EvalPath(contextObj, "$"+pathStr[2:])
				case strings.HasPrefix(pathStr, "States."):
					val, err = EvalIntrinsic(pathStr, input)
				default:
					val, err = EvalPath(input, pathStr)
				}
				if err != nil {
					return nil, fmt.Errorf("key %q: %w", k, err)
				}
				result[newKey] = val
			} else {
				// Recurse for nested structures
				val, err := EvalParameters(v, input, contextObj)
				if err != nil {
					return nil, err
				}
				result[k] = val
			}
		}
		return result, nil
	case []any:
		result := make([]any, len(p))
		for i, v := range p {
			r, err := EvalParameters(v, input, contextObj)
			if err != nil {
				return nil, err
			}
			result[i] = r
		}
		return result, nil
	default:
		return p, nil
	}
}

// ─── tokenizer ───────────────────────────────────────────────────────────────

type tokenKind int

const (
	tokField     tokenKind = iota // .fieldName
	tokIndex                      // [N]
	tokSlice                      // [N:M]
	tokWildcard                   // [*] or .*
	tokRecurse                    // ..
	tokFilter                     // [?(...)]
)

type token struct {
	kind  tokenKind
	field string
	n, m  int
	expr  string // for filter
}

// tokenize parses the suffix of a JSONPath expression (after the leading $).
func tokenize(s string) ([]token, error) {
	var tokens []token
	for len(s) > 0 {
		switch {
		case strings.HasPrefix(s, ".."):
			// Recursive descent followed by field or wildcard
			s = s[2:]
			if s == "" {
				return nil, fmt.Errorf(".. must be followed by a field name")
			}
			field, rest, err := consumeField(s)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token{kind: tokRecurse, field: field})
			s = rest
		case strings.HasPrefix(s, "."):
			s = s[1:]
			if s == "" {
				return nil, fmt.Errorf("trailing dot in path")
			}
			if s == "*" {
				tokens = append(tokens, token{kind: tokWildcard})
				s = ""
				continue
			}
			field, rest, err := consumeField(s)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token{kind: tokField, field: field})
			s = rest
		case strings.HasPrefix(s, "["):
			tok, rest, err := parseBracket(s)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, tok)
			s = rest
		default:
			return nil, fmt.Errorf("unexpected character %q in path", s[0])
		}
	}
	return tokens, nil
}

// consumeField reads an identifier (field name) from s.
func consumeField(s string) (field, rest string, err error) {
	i := 0
	for i < len(s) && (unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i])) || s[i] == '_' || s[i] == '-' || s[i] == '@') {
		i++
	}
	if i == 0 {
		return "", s, fmt.Errorf("expected identifier, got %q", s)
	}
	return s[:i], s[i:], nil
}

// parseBracket parses a [...] segment.
func parseBracket(s string) (token, string, error) {
	end := strings.Index(s, "]")
	if end < 0 {
		return token{}, "", fmt.Errorf("unclosed [")
	}
	inner := s[1:end]
	rest := s[end+1:]

	if inner == "*" {
		return token{kind: tokWildcard}, rest, nil
	}
	if strings.HasPrefix(inner, "?") {
		return token{kind: tokFilter, expr: inner[1:]}, rest, nil
	}
	if strings.Contains(inner, ":") {
		parts := strings.SplitN(inner, ":", 2)
		n, err1 := parseOptionalInt(parts[0], 0)
		m, err2 := parseOptionalInt(parts[1], -1)
		if err1 != nil || err2 != nil {
			return token{}, "", fmt.Errorf("invalid slice %q", inner)
		}
		return token{kind: tokSlice, n: n, m: m}, rest, nil
	}
	// Quoted string key: ['fieldName']
	if (strings.HasPrefix(inner, "'") && strings.HasSuffix(inner, "'")) ||
		(strings.HasPrefix(inner, `"`) && strings.HasSuffix(inner, `"`)) {
		field := inner[1 : len(inner)-1]
		return token{kind: tokField, field: field}, rest, nil
	}
	// Integer index
	n, err := strconv.Atoi(strings.TrimSpace(inner))
	if err != nil {
		return token{}, "", fmt.Errorf("invalid bracket expression %q", inner)
	}
	return token{kind: tokIndex, n: n}, rest, nil
}

func parseOptionalInt(s string, defaultVal int) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultVal, nil
	}
	return strconv.Atoi(s)
}

// ─── evaluator ───────────────────────────────────────────────────────────────

func evalTokens(docs []any, tokens []token) ([]any, error) {
	current := docs
	for _, tok := range tokens {
		var next []any
		for _, doc := range current {
			results, err := evalToken(doc, tok)
			if err != nil {
				return nil, err
			}
			next = append(next, results...)
		}
		current = next
	}
	return current, nil
}

func evalToken(doc any, tok token) ([]any, error) {
	switch tok.kind {
	case tokField:
		m, ok := toMap(doc)
		if !ok {
			return nil, nil
		}
		v, exists := m[tok.field]
		if !exists {
			return nil, nil
		}
		return []any{v}, nil

	case tokWildcard:
		switch d := doc.(type) {
		case map[string]any:
			out := make([]any, 0, len(d))
			for _, v := range d {
				out = append(out, v)
			}
			return out, nil
		case []any:
			return d, nil
		}
		return nil, nil

	case tokIndex:
		arr, ok := doc.([]any)
		if !ok {
			return nil, nil
		}
		idx := tok.n
		if idx < 0 {
			idx = len(arr) + idx
		}
		if idx < 0 || idx >= len(arr) {
			return nil, nil
		}
		return []any{arr[idx]}, nil

	case tokSlice:
		arr, ok := doc.([]any)
		if !ok {
			return nil, nil
		}
		n := tok.n
		m := tok.m
		if m < 0 || m > len(arr) {
			m = len(arr)
		}
		if n < 0 {
			n = 0
		}
		if n >= m {
			return nil, nil
		}
		return arr[n:m], nil

	case tokRecurse:
		// Recursive descent: collect all values of tok.field at any depth
		return recurseField(doc, tok.field), nil

	case tokFilter:
		// Basic filter support: [?(@.field)] or [?(@.field == 'value')]
		arr, ok := doc.([]any)
		if !ok {
			return nil, nil
		}
		return evalFilter(arr, tok.expr)
	}
	return nil, fmt.Errorf("unknown token kind %d", tok.kind)
}

func recurseField(doc any, field string) []any {
	var results []any
	switch d := doc.(type) {
	case map[string]any:
		if v, ok := d[field]; ok {
			results = append(results, v)
		}
		for _, v := range d {
			results = append(results, recurseField(v, field)...)
		}
	case []any:
		for _, v := range d {
			results = append(results, recurseField(v, field)...)
		}
	}
	return results
}

// evalFilter handles simple [?(@.field)] and [?(@.field == value)] filters.
func evalFilter(arr []any, expr string) ([]any, error) {
	// Strip outer parens: (@.field == 'x')
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		expr = expr[1 : len(expr)-1]
	}
	expr = strings.TrimSpace(expr)

	// Simple existence check: @.field
	if strings.HasPrefix(expr, "@.") && !strings.Contains(expr, " ") {
		field := expr[2:]
		var out []any
		for _, item := range arr {
			m, ok := toMap(item)
			if !ok {
				continue
			}
			if _, exists := m[field]; exists {
				out = append(out, item)
			}
		}
		return out, nil
	}

	// Equality check: @.field == 'value' or @.field == 42
	if idx := strings.Index(expr, "=="); idx >= 0 {
		lhs := strings.TrimSpace(expr[:idx])
		rhs := strings.TrimSpace(expr[idx+2:])
		if !strings.HasPrefix(lhs, "@.") {
			return nil, fmt.Errorf("filter LHS must start with @.: %q", lhs)
		}
		field := lhs[2:]
		var out []any
		for _, item := range arr {
			m, ok := toMap(item)
			if !ok {
				continue
			}
			v, exists := m[field]
			if !exists {
				continue
			}
			if matchesLiteral(v, rhs) {
				out = append(out, item)
			}
		}
		return out, nil
	}

	return nil, fmt.Errorf("unsupported filter expression: %q", expr)
}

func matchesLiteral(v any, rhs string) bool {
	// String literal
	if (strings.HasPrefix(rhs, "'") && strings.HasSuffix(rhs, "'")) ||
		(strings.HasPrefix(rhs, `"`) && strings.HasSuffix(rhs, `"`)) {
		s, ok := v.(string)
		if !ok {
			return false
		}
		return s == rhs[1:len(rhs)-1]
	}
	// Numeric literal
	if n, err := strconv.ParseFloat(rhs, 64); err == nil {
		switch vv := v.(type) {
		case float64:
			return vv == n
		case int:
			return float64(vv) == n
		}
	}
	// Boolean
	if rhs == "true" {
		b, ok := v.(bool)
		return ok && b
	}
	if rhs == "false" {
		b, ok := v.(bool)
		return ok && !b
	}
	return false
}

// toMap safely casts any to map[string]any.
func toMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}
