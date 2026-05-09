package dynamodb

import (
	"strings"
)

// ParseProjection parses a ProjectionExpression string, resolves #name tokens,
// and returns a slice of attribute paths. Returns nil if expr is empty (meaning
// "return all attributes").
func ParseProjection(expr string, exprNames map[string]string) []string {
	if expr == "" {
		return nil
	}
	parts := splitComma(expr)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, resolveProjectionPath(p, exprNames))
	}
	return out
}

// ApplyProjection filters item to only the attributes named in attrs.
// Returns item unchanged if attrs is nil or empty.
// Supports dotted paths (e.g. "address.city") and list indexes (e.g. "tags[0]").
func ApplyProjection(item map[string]any, attrs []string) map[string]any {
	if len(attrs) == 0 || item == nil {
		return item
	}
	out := map[string]any{}
	for _, path := range attrs {
		segments := splitPath(path)
		if len(segments) == 0 {
			continue
		}
		top := segments[0]
		if _, exists := item[top]; !exists {
			continue
		}
		if len(segments) == 1 {
			out[top] = item[top]
		} else {
			// Nested path: merge into existing top-level entry.
			val := getNestedValue(item, segments)
			if val != nil {
				setNestedValue(out, segments, val)
			}
		}
	}
	return out
}

// resolveProjectionPath replaces #name tokens in a path.
func resolveProjectionPath(path string, names map[string]string) string {
	segments := strings.Split(path, ".")
	resolved := make([]string, len(segments))
	for i, seg := range segments {
		// Strip list index for name resolution (e.g. "tags[0]" → "tags").
		base := seg
		suffix := ""
		if idx := strings.Index(seg, "["); idx >= 0 {
			base = seg[:idx]
			suffix = seg[idx:]
		}
		if strings.HasPrefix(base, "#") {
			if v, ok := names[base]; ok {
				base = v
			}
		}
		resolved[i] = base + suffix
	}
	return strings.Join(resolved, ".")
}

// splitPath splits a dotted path into segments, preserving list indexes.
func splitPath(path string) []string {
	return strings.Split(path, ".")
}

// getNestedValue traverses segments into item and returns the leaf value.
func getNestedValue(item map[string]any, segments []string) any {
	var cur any = item
	for _, seg := range segments {
		base := seg
		listIdx := -1
		if idx := strings.Index(seg, "["); idx >= 0 {
			base = seg[:idx]
			listIdx = parseListIndex(seg[idx:])
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[base]
		if listIdx >= 0 {
			list, ok := cur.([]any)
			if !ok || listIdx >= len(list) {
				return nil
			}
			cur = list[listIdx]
		}
	}
	return cur
}

// setNestedValue sets val at the leaf of segments in out, creating intermediate maps.
func setNestedValue(out map[string]any, segments []string, val any) {
	if len(segments) == 1 {
		out[segments[0]] = val
		return
	}
	top := segments[0]
	sub, ok := out[top].(map[string]any)
	if !ok {
		sub = map[string]any{}
		out[top] = sub
	}
	setNestedValue(sub, segments[1:], val)
}

// parseListIndex extracts the integer from "[N]".
func parseListIndex(s string) int {
	if len(s) < 3 || s[0] != '[' || s[len(s)-1] != ']' {
		return -1
	}
	n := 0
	for _, c := range s[1 : len(s)-1] {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}
