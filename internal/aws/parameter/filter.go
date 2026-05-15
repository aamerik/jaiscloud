package parameter

import "strings"

// ParameterFilter represents a single DescribeParameters filter.
type ParameterFilter struct {
	Key    string   // "Name" | "Type" | "Path" | "KeyId" | "Tier" | "Label"
	Option string   // "Equals" | "BeginsWith" | "Contains" | "Recursive" | "OneLevel"
	Values []string
}

// ApplyFilters returns only the entries that match all supplied filters.
// If filters is empty the full slice is returned unchanged.
func ApplyFilters(params []ParameterEntry, filters []ParameterFilter) []ParameterEntry {
	if len(filters) == 0 {
		return params
	}
	var result []ParameterEntry
	for _, p := range params {
		if matchesAllFilters(p, filters) {
			result = append(result, p)
		}
	}
	return result
}

func matchesAllFilters(p ParameterEntry, filters []ParameterFilter) bool {
	for _, f := range filters {
		if !matchesFilter(p, f) {
			return false
		}
	}
	return true
}

func matchesFilter(p ParameterEntry, f ParameterFilter) bool {
	var field string
	switch f.Key {
	case "Name":
		field = p.Name
	case "Type":
		field = p.Type
	case "Path":
		// "Path" with "Recursive" or "OneLevel" is handled by ListParameters;
		// treat as a BeginsWith match here for completeness.
		field = p.Name
	case "KeyId":
		field = p.KMSKeyID
	case "Tier":
		field = p.Tier
	default:
		// Unknown key — skip (do not exclude).
		return true
	}

	for _, v := range f.Values {
		switch f.Option {
		case "Equals", "":
			if field == v {
				return true
			}
		case "BeginsWith":
			if strings.HasPrefix(field, v) {
				return true
			}
		case "Contains":
			if strings.Contains(field, v) {
				return true
			}
		case "Recursive":
			// Treat as prefix match with trailing slash normalisation.
			prefix := v
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			if strings.HasPrefix(field, prefix) || field == v {
				return true
			}
		case "OneLevel":
			prefix := v
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			if strings.HasPrefix(field, prefix) {
				rest := strings.TrimPrefix(field, prefix)
				if rest != "" && !strings.Contains(rest, "/") {
					return true
				}
			}
		}
	}
	return false
}
