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
