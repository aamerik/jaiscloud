package monitoring

import (
	"fmt"
	"strings"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// tsFilter is the subset of the Cloud Monitoring filter grammar that
// ListTimeSeries supports: a conjunction (AND) of equality clauses on
// `metric.type` and `resource.type`. Anything else fails closed with an error
// rather than silently matching everything.
type tsFilter struct {
	metricType   string
	resourceType string
}

// compileTSFilter parses a minimal ListTimeSeries filter of the form
//
//	metric.type = "custom.googleapis.com/foo" AND resource.type = "global"
//
// Both clauses are optional individually, but the filter string must be
// non-empty and every clause must be a supported equality.
func compileTSFilter(s string) (tsFilter, error) {
	var f tsFilter
	s = strings.TrimSpace(s)
	if s == "" {
		return f, fmt.Errorf("filter must not be empty")
	}
	for _, clause := range strings.Split(s, " AND ") {
		key, val, ok := parseEquality(strings.TrimSpace(clause))
		if !ok {
			return f, fmt.Errorf("unsupported filter clause %q", clause)
		}
		switch key {
		case "metric.type":
			f.metricType = val
		case "resource.type":
			f.resourceType = val
		default:
			return f, fmt.Errorf("unsupported filter key %q (only metric.type and resource.type are supported)", key)
		}
	}
	return f, nil
}

// parseEquality parses `key = "value"` into key and the unquoted value.
func parseEquality(clause string) (key, val string, ok bool) {
	key, rhs, found := strings.Cut(clause, "=")
	if !found {
		return "", "", false
	}
	key = strings.TrimSpace(key)
	rhs = strings.TrimSpace(rhs)
	if len(rhs) < 2 || rhs[0] != '"' || rhs[len(rhs)-1] != '"' {
		return "", "", false
	}
	return key, rhs[1 : len(rhs)-1], true
}

// match reports whether a time series satisfies the filter.
func (f tsFilter) match(ts monitoringstore.TimeSeries) bool {
	if f.metricType != "" && ts.MetricType != f.metricType {
		return false
	}
	if f.resourceType != "" && ts.ResourceType != f.resourceType {
		return false
	}
	return true
}

// ─── descriptor-list filter (metric + monitored-resource descriptors) ─────────

// descriptorField selects which descriptor attribute a filter clause tests.
type descriptorField int

const (
	descriptorType descriptorField = iota
	descriptorName
)

// metricDescriptorFilterKeys are the filter keys accepted by
// ListMetricDescriptors. Unknown keys fail closed.
var metricDescriptorFilterKeys = map[string]descriptorField{
	"metric.type": descriptorType,
	"type":        descriptorType,
	"name":        descriptorName,
}

// monitoredResourceFilterKeys are the filter keys accepted by
// ListMonitoredResourceDescriptors. Unknown keys fail closed.
var monitoredResourceFilterKeys = map[string]descriptorField{
	"resource.type": descriptorType,
	"type":          descriptorType,
	"name":          descriptorName,
}

// descriptorClause is one parsed predicate: field equals value, or field starts
// with value.
type descriptorClause struct {
	field  descriptorField
	prefix bool
	value  string
}

// descriptorFilter is a conjunction of descriptorClause predicates. An empty
// filter matches everything.
type descriptorFilter struct {
	clauses []descriptorClause
}

// compileDescriptorFilter parses the discovery subset of the Cloud Monitoring
// filter grammar:
//
//	type = "x"
//	metric.type = "custom.googleapis.com/foo"     (ListMetricDescriptors)
//	resource.type = "gce_instance"                (ListMonitoredResourceDescriptors)
//	type = starts_with("gce_")
//	name = starts_with("projects/p/monitoredResourceDescriptors/k8s")
//
// Clauses are joined by " AND " and each is an equality or starts_with over a
// descriptor attribute. Any other key, operator, or malformed clause fails
// closed with an error (mapped to InvalidArgument by the service) instead of
// silently returning everything. keys is the surface-specific allowlist of
// accepted key spellings.
func compileDescriptorFilter(s string, keys map[string]descriptorField) (descriptorFilter, error) {
	var f descriptorFilter
	s = strings.TrimSpace(s)
	if s == "" {
		return f, nil
	}
	for _, raw := range strings.Split(s, " AND ") {
		clause := strings.TrimSpace(raw)
		if clause == "" {
			return f, fmt.Errorf("empty filter clause")
		}
		c, err := parseDescriptorClause(clause, keys)
		if err != nil {
			return f, err
		}
		f.clauses = append(f.clauses, c)
	}
	return f, nil
}

// parseDescriptorClause parses one `key = "value"` or
// `key = starts_with("prefix")` clause.
func parseDescriptorClause(clause string, keys map[string]descriptorField) (descriptorClause, error) {
	key, rhs, found := strings.Cut(clause, "=")
	if !found {
		return descriptorClause{}, fmt.Errorf("unsupported filter clause %q", clause)
	}
	key = strings.TrimSpace(key)
	rhs = strings.TrimSpace(rhs)
	field, ok := keys[key]
	if !ok {
		return descriptorClause{}, fmt.Errorf("unsupported filter key %q", key)
	}
	if inner, ok := parseStartsWith(rhs); ok {
		return descriptorClause{field: field, prefix: true, value: inner}, nil
	}
	if val, ok := unquote(rhs); ok {
		return descriptorClause{field: field, value: val}, nil
	}
	return descriptorClause{}, fmt.Errorf("unsupported filter operator in clause %q (only equality and starts_with are supported)", clause)
}

// parseStartsWith parses rhs of the form starts_with("prefix").
func parseStartsWith(rhs string) (string, bool) {
	const fn = "starts_with("
	if !strings.HasPrefix(rhs, fn) || !strings.HasSuffix(rhs, ")") {
		return "", false
	}
	return unquote(strings.TrimSpace(rhs[len(fn) : len(rhs)-1]))
}

// unquote returns the contents of a double-quoted string literal.
func unquote(s string) (string, bool) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	return s[1 : len(s)-1], true
}

// match reports whether a descriptor with the given type and resource name
// satisfies every clause.
func (f descriptorFilter) match(typ, name string) bool {
	for _, c := range f.clauses {
		actual := typ
		if c.field == descriptorName {
			actual = name
		}
		if c.prefix {
			if !strings.HasPrefix(actual, c.value) {
				return false
			}
			continue
		}
		if actual != c.value {
			return false
		}
	}
	return true
}
