package dynamodb

import (
	"fmt"
	"strconv"
	"strings"
)

const noIndex = -1

// PathPart is one step in a nested attribute path.
// Name != "" → named key step. Index >= 0 → list index step.
type PathPart struct {
	Name  string
	Index int
}

// ParsePathParts parses an attribute reference like "#a.b[0].c" into PathParts.
func ParsePathParts(ref string) []PathPart {
	var parts []PathPart
	for _, seg := range splitPathSegments(ref) {
		idx := strings.Index(seg, "[")
		if idx >= 0 {
			name := seg[:idx]
			if name != "" {
				parts = append(parts, PathPart{Name: name, Index: noIndex})
			}
			rest := seg[idx:]
			for len(rest) > 0 && rest[0] == '[' {
				end := strings.Index(rest, "]")
				if end < 0 {
					break
				}
				n, err := strconv.Atoi(rest[1:end])
				if err != nil {
					break
				}
				parts = append(parts, PathPart{Name: "", Index: n})
				rest = rest[end+1:]
			}
		} else {
			parts = append(parts, PathPart{Name: seg, Index: noIndex})
		}
	}
	return parts
}

// splitPathSegments splits "a.b[0].c" by "." not inside brackets.
func splitPathSegments(ref string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range ref {
		switch c {
		case '[':
			depth++
		case ']':
			depth--
		case '.':
			if depth == 0 {
				parts = append(parts, ref[start:i])
				start = i + 1
			}
		}
	}
	if start < len(ref) {
		parts = append(parts, ref[start:])
	}
	return parts
}

// ResolveParts replaces #name placeholders using the names map.
func ResolveParts(parts []PathPart, names map[string]string) []PathPart {
	out := make([]PathPart, len(parts))
	copy(out, parts)
	for i, p := range out {
		if strings.HasPrefix(p.Name, "#") {
			if n, ok := names[p.Name]; ok {
				out[i].Name = n
			} else {
				out[i].Name = p.Name[1:]
			}
		}
	}
	return out
}

// ResolvePath navigates a top-level DynamoDB item using parts.
// The top-level item maps attribute names to DynamoDB typed values (e.g. {"S":"hello"}).
// Returns (nil, false) if any step is missing.
func ResolvePath(item map[string]any, parts []PathPart) (any, bool) {
	if len(parts) == 0 {
		return nil, false
	}
	first := parts[0]
	if first.Name == "" {
		return nil, false
	}
	v, ok := item[first.Name]
	if !ok {
		return nil, false
	}
	if len(parts) == 1 {
		return v, true
	}
	return resolveInValue(v, parts[1:])
}

// resolveInValue navigates inside a typed DynamoDB value {"M":{...}} or {"L":[...]}.
func resolveInValue(v any, parts []PathPart) (any, bool) {
	if len(parts) == 0 {
		return v, true
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	part := parts[0]
	rest := parts[1:]

	if part.Name != "" {
		inner, ok := m["M"]
		if !ok {
			return nil, false
		}
		innerMap, ok := inner.(map[string]any)
		if !ok {
			return nil, false
		}
		attr, ok := innerMap[part.Name]
		if !ok {
			return nil, false
		}
		return resolveInValue(attr, rest)
	}
	// List index step.
	inner, ok := m["L"]
	if !ok {
		return nil, false
	}
	list, ok := inner.([]any)
	if !ok {
		return nil, false
	}
	if part.Index < 0 || part.Index >= len(list) {
		return nil, false
	}
	return resolveInValue(list[part.Index], rest)
}

// SetPath sets a nested path in the item, creating intermediate M nodes as needed.
func SetPath(item map[string]any, parts []PathPart, val any) error {
	if len(parts) == 0 {
		return fmt.Errorf("empty path")
	}
	first := parts[0]
	if first.Name == "" {
		return fmt.Errorf("top-level path must be a named key")
	}
	if len(parts) == 1 {
		item[first.Name] = val
		return nil
	}
	existing := item[first.Name]
	newVal, err := setInValue(existing, parts[1:], val)
	if err != nil {
		return err
	}
	item[first.Name] = newVal
	return nil
}

func setInValue(v any, parts []PathPart, val any) (any, error) {
	if len(parts) == 0 {
		return val, nil
	}
	part := parts[0]
	rest := parts[1:]

	if part.Name != "" {
		var innerMap map[string]any
		if v != nil {
			m, ok := v.(map[string]any)
			if ok {
				if inner, ok := m["M"].(map[string]any); ok {
					innerMap = inner
				}
			}
		}
		if innerMap == nil {
			innerMap = make(map[string]any)
		}
		newChild, err := setInValue(innerMap[part.Name], rest, val)
		if err != nil {
			return nil, err
		}
		innerMap[part.Name] = newChild
		return map[string]any{"M": innerMap}, nil
	}

	// List index step.
	var list []any
	if v != nil {
		if m, ok := v.(map[string]any); ok {
			if inner, ok := m["L"].([]any); ok {
				list = make([]any, len(inner))
				copy(list, inner)
			}
		}
	}
	if part.Index < 0 {
		return nil, fmt.Errorf("negative list index")
	}
	for len(list) <= part.Index {
		list = append(list, nil)
	}
	newChild, err := setInValue(list[part.Index], rest, val)
	if err != nil {
		return nil, err
	}
	list[part.Index] = newChild
	return map[string]any{"L": list}, nil
}

// RemovePath removes the attribute at the given path.
func RemovePath(item map[string]any, parts []PathPart) {
	if len(parts) == 0 {
		return
	}
	first := parts[0]
	if first.Name == "" {
		return
	}
	if len(parts) == 1 {
		delete(item, first.Name)
		return
	}
	v, ok := item[first.Name]
	if !ok {
		return
	}
	newV, changed := removeInValue(v, parts[1:])
	if changed {
		item[first.Name] = newV
	}
}

func removeInValue(v any, parts []PathPart) (any, bool) {
	if len(parts) == 0 {
		return nil, false
	}
	m, ok := v.(map[string]any)
	if !ok {
		return v, false
	}
	part := parts[0]
	rest := parts[1:]

	if part.Name != "" {
		inner, ok := m["M"]
		if !ok {
			return v, false
		}
		innerMap, ok := inner.(map[string]any)
		if !ok {
			return v, false
		}
		if len(rest) == 0 {
			delete(innerMap, part.Name)
			return map[string]any{"M": innerMap}, true
		}
		child, ok := innerMap[part.Name]
		if !ok {
			return v, false
		}
		newChild, changed := removeInValue(child, rest)
		if !changed {
			return v, false
		}
		innerMap[part.Name] = newChild
		return map[string]any{"M": innerMap}, true
	}

	// List index step.
	inner, ok := m["L"]
	if !ok {
		return v, false
	}
	list, ok := inner.([]any)
	if !ok {
		return v, false
	}
	if part.Index < 0 || part.Index >= len(list) {
		return v, false
	}
	if len(rest) == 0 {
		newList := make([]any, 0, len(list)-1)
		newList = append(newList, list[:part.Index]...)
		newList = append(newList, list[part.Index+1:]...)
		return map[string]any{"L": newList}, true
	}
	newChild, changed := removeInValue(list[part.Index], rest)
	if !changed {
		return v, false
	}
	newList := make([]any, len(list))
	copy(newList, list)
	newList[part.Index] = newChild
	return map[string]any{"L": newList}, true
}
