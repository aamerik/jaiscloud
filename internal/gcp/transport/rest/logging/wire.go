package logging

import (
	"net/http"
	"time"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"
)

// ─── request helpers ──────────────────────────────────────────────────────────

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// queryToParams copies single-valued query parameters into params as strings.
// Repeated parameters keep their slice so list params (e.g. resourceNames) are
// not silently dropped.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) == 0 {
			continue
		}
		if len(vs) == 1 {
			params[k] = vs[0]
			continue
		}
		vals := make([]any, 0, len(vs))
		for _, v := range vs {
			vals = append(vals, v)
		}
		params[k] = vals
	}
}

func strFrom(v any) string {
	s, _ := v.(string)
	return s
}

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

// strListFrom converts a JSON array of strings into a []string, tolerating a
// single string value (the REST client may send either).
func strListFrom(v any) []string {
	switch x := v.(type) {
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if x == "" {
			return nil
		}
		return []string{x}
	default:
		return nil
	}
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case string:
		var n int
		for i := 0; i < len(x); i++ {
			if x[i] < '0' || x[i] > '9' {
				return 0
			}
		}
		for i := 0; i < len(x); i++ {
			n = n*10 + int(x[i]-'0')
		}
		return n
	default:
		return 0
	}
}

func boolFrom(v any) bool {
	b, _ := v.(bool)
	return b
}

func stringMapFrom(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// ─── entry transcoding ────────────────────────────────────────────────────────

// entryFromWire decodes a JSON LogEntry into the neutral stored form. Only the
// text and JSON payload variants are supported (protoPayload is ignored, which
// the emulator's gRPC surface does not implement either).
func entryFromWire(v any) loggingstore.LogEntry {
	var e loggingstore.LogEntry
	m, ok := v.(map[string]any)
	if !ok {
		return e
	}
	e.LogName = strFrom(m["logName"])
	e.InsertID = strFrom(m["insertId"])
	e.Labels = stringMapFrom(m["labels"])
	if res, ok := m["resource"].(map[string]any); ok {
		e.ResourceType = strFrom(res["type"])
		e.ResourceLabels = stringMapFrom(res["labels"])
	}
	switch sv := m["severity"].(type) {
	case string:
		if n, ok := core.SeverityValue(sv); ok {
			e.Severity = n
		}
	case float64:
		e.Severity = int(sv)
	}
	if ts := strFrom(m["timestamp"]); ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			e.Timestamp = t
		}
	}
	switch {
	case hasKey(m, "textPayload"):
		e.PayloadType = "text"
		e.TextPayload = strFrom(m["textPayload"])
	case hasKey(m, "jsonPayload"):
		e.PayloadType = "json"
		if jp, ok := m["jsonPayload"].(map[string]any); ok {
			e.JsonPayload = jp
		}
	}
	return e
}

// entryToWire encodes a neutral stored entry as the Discovery LogEntry JSON.
// Default-valued fields (zero severity, zero timestamp, empty payload) are
// omitted, matching protojson.
func entryToWire(e loggingstore.LogEntry) map[string]any {
	out := map[string]any{}
	if e.LogName != "" {
		out["logName"] = e.LogName
	}
	if e.ResourceType != "" || len(e.ResourceLabels) > 0 {
		res := map[string]any{}
		if e.ResourceType != "" {
			res["type"] = e.ResourceType
		}
		if len(e.ResourceLabels) > 0 {
			res["labels"] = e.ResourceLabels
		}
		out["resource"] = res
	}
	if !e.Timestamp.IsZero() {
		out["timestamp"] = e.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if e.Severity != 0 {
		out["severity"] = core.SeverityName(e.Severity)
	}
	switch e.PayloadType {
	case "json":
		if len(e.JsonPayload) > 0 {
			out["jsonPayload"] = e.JsonPayload
		}
	case "text":
		out["textPayload"] = e.TextPayload
	}
	if e.InsertID != "" {
		out["insertId"] = e.InsertID
	}
	if len(e.Labels) > 0 {
		out["labels"] = e.Labels
	}
	return out
}

// descriptorToWire encodes a neutral monitored resource descriptor as the
// Discovery MonitoredResourceDescriptor JSON. The Logging surface leaves `name`
// unset.
func descriptorToWire(d core.MonitoredResourceDescriptor) map[string]any {
	labels := make([]any, 0, len(d.Labels))
	for _, l := range d.Labels {
		labels = append(labels, map[string]any{
			"key":         l.Key,
			"valueType":   l.ValueType,
			"description": l.Description,
		})
	}
	out := map[string]any{
		"type":        d.Type,
		"displayName": d.DisplayName,
	}
	if d.Description != "" {
		out["description"] = d.Description
	}
	if len(labels) > 0 {
		out["labels"] = labels
	}
	return out
}
