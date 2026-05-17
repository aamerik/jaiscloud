package logs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

// LambdaRawInvoker is the interface logs uses to invoke Lambda subscription destinations.
type LambdaRawInvoker interface {
	InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error
}

// MetricDatum is a single metric observation passed to CloudWatch.
type MetricDatum struct {
	Name  string
	Value float64
	Unit  string
}

// MetricDataPutter is the interface logs uses to push extracted metric filter values to CloudWatch.
type MetricDataPutter interface {
	InternalPutMetricData(ctx context.Context, namespace string, data []MetricDatum) error
}

// SetSubscriptionDispatcher wires the Lambda invoker for subscription filter delivery.
func (p *Provider) SetSubscriptionDispatcher(inv LambdaRawInvoker) { p.subInvoker = inv }

// SetMetricDataPutter wires the CloudWatch provider for metric filter extraction.
func (p *Provider) SetMetricDataPutter(m MetricDataPutter) { p.cwMetrics = m }

// dispatchSubscriptionFilters fans out matching log events to all subscription filters
// for the given log group. Called inside PutLogEvents after appending to the ring.
// Caller must NOT hold p.store.mu.
func (p *Provider) dispatchSubscriptionFilters(ctx context.Context, accountID, groupName, streamName string, events []LogEvent) {
	if p.subInvoker == nil {
		return
	}

	p.store.mu.RLock()
	filters := make([]*SubscriptionFilter, 0)
	for _, f := range p.store.subscriptionFilters[groupName] {
		filters = append(filters, f)
	}
	p.store.mu.RUnlock()

	if len(filters) == 0 {
		return
	}

	for _, f := range filters {
		matched := matchLogEvents(events, f.FilterPattern)
		if len(matched) == 0 {
			continue
		}
		payload, err := buildSubscriptionPayload(accountID, groupName, streamName, f.FilterName, matched)
		if err != nil {
			slog.Warn("cwlogs: build subscription payload failed", "filter", f.FilterName, "err", err)
			continue
		}
		destARN := f.DestinationArn
		go func(arn string, p2 []byte) {
			if err := p.subInvoker.InternalInvokeRaw(ctx, arn, p2); err != nil {
				slog.Warn("cwlogs: subscription delivery failed", "dest", arn, "err", err)
			}
		}(destARN, payload)
	}
}

// matchLogEvents returns the subset of events matching the filter pattern.
// Supports: empty pattern (all pass), quoted string literal, ?term substring, [a,b] space-separated tokens.
func matchLogEvents(events []LogEvent, pattern string) []LogEvent {
	if pattern == "" {
		return events
	}
	var out []LogEvent
	for _, e := range events {
		if logEventMatches(e.Message, pattern) {
			out = append(out, e)
		}
	}
	return out
}

func logEventMatches(msg, pattern string) bool {
	if pattern == "" {
		return true
	}
	// ?term → substring match (case-insensitive prefix with ?)
	if len(pattern) > 1 && pattern[0] == '?' {
		term := pattern[1:]
		return containsIgnoreCase(msg, term)
	}
	// Simple substring
	return containsIgnoreCase(msg, pattern)
}

// jsonFieldRe matches CWL JSON field filter patterns: { $.fieldPath OP "value" }
// e.g. { $.level = "error" } or { $.code != "0" }
var jsonFieldRe = regexp.MustCompile(`^\s*\{\s*\$\.([A-Za-z0-9_.]+)\s*(=|!=|>|>=|<|<=)\s*"([^"]*)"\s*\}\s*$`)

// compileLogFilter returns a function that tests whether a log message matches
// the given filter pattern. Supports JSON field filters ({ $.field = "v" })
// and falls back to simple substring matching.
func compileLogFilter(pattern string) func(msg string) bool {
	if pattern == "" {
		return func(string) bool { return true }
	}
	// JSON field filter: { $.fieldPath = "value" }
	if m := jsonFieldRe.FindStringSubmatch(pattern); m != nil {
		field, op, want := m[1], m[2], m[3]
		return func(msg string) bool {
			var obj map[string]any
			if json.Unmarshal([]byte(msg), &obj) != nil {
				return false
			}
			// Resolve potentially dotted field path
			var v any = obj
			for _, part := range strings.Split(field, ".") {
				if mv, ok := v.(map[string]any); ok {
					v = mv[part]
				} else {
					return false
				}
			}
			got := fmt.Sprintf("%v", v)
			switch op {
			case "=":
				return got == want
			case "!=":
				return got != want
			case ">":
				return got > want
			case ">=":
				return got >= want
			case "<":
				return got < want
			case "<=":
				return got <= want
			}
			return false
		}
	}
	// Fallback: substring match
	return func(msg string) bool {
		return strings.Contains(msg, pattern)
	}
}

func containsIgnoreCase(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	sl, subl := len(s), len(sub)
	for i := 0; i <= sl-subl; i++ {
		if equalFoldN(s[i:i+subl], sub) {
			return true
		}
	}
	return false
}

func equalFoldN(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// buildSubscriptionPayload builds the gzip+base64 DATA_MESSAGE envelope required by
// CloudWatch Logs → Lambda subscriptions.
func buildSubscriptionPayload(accountID, groupName, streamName, filterName string, events []LogEvent) ([]byte, error) {
	evList := make([]map[string]any, 0, len(events))
	for _, e := range events {
		evList = append(evList, map[string]any{
			"id":        timestampID(e.Timestamp),
			"timestamp": e.Timestamp,
			"message":   e.Message,
		})
	}
	inner := map[string]any{
		"messageType":         "DATA_MESSAGE",
		"owner":               accountID,
		"logGroup":            groupName,
		"logStream":           streamName,
		"subscriptionFilters": []string{filterName},
		"logEvents":           evList,
	}
	innerJSON, err := json.Marshal(inner)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(innerJSON); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	b64 := base64.StdEncoding.EncodeToString(buf.Bytes())

	outer := map[string]any{
		"awslogs": map[string]any{
			"data": b64,
		},
	}
	return json.Marshal(outer)
}

// extractMetricFilters iterates metric filters for a log group, matches events against
// each filter pattern, and calls InternalPutMetricData for matched events.
func (p *Provider) extractMetricFilters(ctx context.Context, groupName string, filters []*MetricFilter, events []LogEvent) {
	if p.cwMetrics == nil || len(filters) == 0 {
		return
	}
	for _, mf := range filters {
		matched := matchLogEvents(events, mf.FilterPattern)
		if len(matched) == 0 {
			continue
		}
		for _, xf := range mf.MetricTransformations {
			ns, _ := xf["metricNamespace"].(string)
			name, _ := xf["metricName"].(string)
			unit, _ := xf["unit"].(string)
			if unit == "" {
				unit = "None"
			}
			val := 1.0
			if v, ok := xf["metricValue"].(string); ok && v != "" {
				// metricValue may be a literal number string or a field reference like $fieldName
				if v[0] != '$' {
					if f, err := parseFloat(v); err == nil {
						val = f
					}
				}
			}
			data := make([]MetricDatum, len(matched))
			for i := range matched {
				data[i] = MetricDatum{Name: name, Value: val, Unit: unit}
			}
			if err := p.cwMetrics.InternalPutMetricData(ctx, ns, data); err != nil {
				// best-effort; log but don't block
				_ = err
			}
		}
	}
}

func parseFloat(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%g", &f)
	return f, err
}

func timestampID(ts int64) string {
	// Produce a deterministic-ish event ID from the timestamp.
	return json.Number(itoa64(ts)).String() + "000000000000000000"
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 20)
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
