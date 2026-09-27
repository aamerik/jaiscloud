package functions

import "strings"

// Runtime is one Cloud Functions v2 runtime descriptor
// (google.cloud.functions.v2.ListRuntimesResponse.Runtime). It is the
// transport-neutral shape shared by the REST and gRPC adapters: the REST render
// emits RuntimeJSON directly, and the gRPC transcode maps it onto the generated
// proto.
//
// The catalog is a curated static snapshot, not a live query: real Cloud
// Functions derives it from its build images, which an emulator does not have.
// The field names/enums are exactly the Discovery `Runtime` schema
// (name/displayName/stage/environment/warnings/deprecationDate/
// decommissionDate), so a client that validates against the official schema
// (gcloud `functions deploy --gen2` resolves a runtime from this list, filtered
// to environment GEN_2) accepts it unchanged.
type Runtime struct {
	// Name is the runtime id, e.g. "nodejs20".
	Name string
	// DisplayName is the user-facing name, e.g. "Node.js 20".
	DisplayName string
	// Stage is a ListRuntimesResponse_RuntimeStage enum value
	// (DEVELOPMENT/ALPHA/BETA/GA/DEPRECATED/DECOMMISSIONED).
	Stage string
	// Environment is an Environment enum value (GEN_1/GEN_2).
	Environment string
	// Warnings are deprecation/advisory messages for the runtime.
	Warnings []string
	// DeprecationDate and DecommissionDate are optional google.type.Date
	// objects ({year, month, day}); nil omits the field.
	DeprecationDate  map[string]any
	DecommissionDate map[string]any
}

// runtimeCatalog is the stable set of runtimes the emulator advertises. It
// covers the GEN_2 runtimes gcloud/Cloud Functions clients validate against
// (gcloud filters ListRuntimes to environment GEN_2 when deploying a gen2
// function). Deprecated entries carry a warning and a deprecation date, matching
// the shape of real GCP's response.
var runtimeCatalog = []Runtime{
	// Node.js.
	{Name: "nodejs18", DisplayName: "Node.js 18", Stage: "DEPRECATED", Environment: "GEN_2",
		Warnings:        []string{"This runtime is deprecated. See https://cloud.google.com/functions/docs/runtime-support for more information."},
		DeprecationDate: date(2025, 4, 30)},
	{Name: "nodejs20", DisplayName: "Node.js 20", Stage: "GA", Environment: "GEN_2"},
	{Name: "nodejs22", DisplayName: "Node.js 22", Stage: "GA", Environment: "GEN_2"},
	{Name: "nodejs24", DisplayName: "Node.js 24", Stage: "GA", Environment: "GEN_2"},

	// Python.
	{Name: "python310", DisplayName: "Python 3.10", Stage: "GA", Environment: "GEN_2"},
	{Name: "python311", DisplayName: "Python 3.11", Stage: "GA", Environment: "GEN_2"},
	{Name: "python312", DisplayName: "Python 3.12", Stage: "GA", Environment: "GEN_2"},
	{Name: "python313", DisplayName: "Python 3.13", Stage: "GA", Environment: "GEN_2"},

	// Go.
	{Name: "go121", DisplayName: "Go 1.21", Stage: "GA", Environment: "GEN_2"},
	{Name: "go122", DisplayName: "Go 1.22", Stage: "GA", Environment: "GEN_2"},
	{Name: "go123", DisplayName: "Go 1.23", Stage: "GA", Environment: "GEN_2"},
	{Name: "go124", DisplayName: "Go 1.24", Stage: "GA", Environment: "GEN_2"},

	// Java.
	{Name: "java17", DisplayName: "Java 17", Stage: "GA", Environment: "GEN_2"},
	{Name: "java21", DisplayName: "Java 21", Stage: "GA", Environment: "GEN_2"},

	// Ruby.
	{Name: "ruby32", DisplayName: "Ruby 3.2", Stage: "GA", Environment: "GEN_2"},
	{Name: "ruby33", DisplayName: "Ruby 3.3", Stage: "GA", Environment: "GEN_2"},

	// PHP.
	{Name: "php82", DisplayName: "PHP 8.2", Stage: "GA", Environment: "GEN_2"},
	{Name: "php83", DisplayName: "PHP 8.3", Stage: "GA", Environment: "GEN_2"},
	{Name: "php84", DisplayName: "PHP 8.4", Stage: "GA", Environment: "GEN_2"},

	// .NET.
	{Name: "dotnet6", DisplayName: ".NET 6", Stage: "DEPRECATED", Environment: "GEN_2",
		Warnings:        []string{"This runtime is deprecated. See https://cloud.google.com/functions/docs/runtime-support for more information."},
		DeprecationDate: date(2025, 2, 28)},
	{Name: "dotnet8", DisplayName: ".NET 8", Stage: "GA", Environment: "GEN_2"},
}

// date builds a google.type.Date object.
func date(year, month, day int) map[string]any {
	return map[string]any{"year": year, "month": month, "day": day}
}

// ListRuntimes returns the runtimes available in a project/location, honoring an
// optional AIP-160 filter over name/displayName/environment/stage. The catalog
// is project- and location-independent (as in the emulator's other synthesized
// catalogs), so the parent only validates the request.
//
// Unlike the emulator's other list surfaces, ListRuntimes has no pagination:
// the real v2 Discovery method declares only `parent` and `filter` (no
// pageSize/pageToken, no nextPageToken in the response).
func (s *Service) ListRuntimes(project, location, filter string) ([]Runtime, error) {
	if location == "" {
		return nil, invalidArgument("missing location")
	}
	return filterRuntimes(runtimeCatalog, filter)
}

// RuntimeJSON renders a Runtime as the Discovery-shaped map both transports
// agree on.
func RuntimeJSON(rt Runtime) map[string]any {
	out := map[string]any{
		"name":        rt.Name,
		"displayName": rt.DisplayName,
		"stage":       rt.Stage,
		"environment": rt.Environment,
	}
	if len(rt.Warnings) > 0 {
		out["warnings"] = rt.Warnings
	}
	if rt.DeprecationDate != nil {
		out["deprecationDate"] = rt.DeprecationDate
	}
	if rt.DecommissionDate != nil {
		out["decommissionDate"] = rt.DecommissionDate
	}
	return out
}

// filterRuntimes applies an AIP-160-style filter over the catalog. Only the
// fields the Runtime schema exposes are supported; an unknown field or a
// malformed expression is InvalidArgument, matching real Cloud Functions.
func filterRuntimes(catalog []Runtime, filter string) ([]Runtime, error) {
	clauses, err := parseRuntimeFilter(filter)
	if err != nil {
		return nil, err
	}
	// Always return a fresh slice: callers must not be able to mutate the
	// package-level catalog (or its Warnings sub-slices) through the result.
	out := make([]Runtime, 0, len(catalog))
	for _, rt := range catalog {
		ok := true
		for _, c := range clauses {
			if !c.matches(rt) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, rt)
		}
	}
	return out, nil
}

// runtimeFilterClause is one `field = "value"` equality clause.
type runtimeFilterClause struct {
	field string
	value string
}

func (c runtimeFilterClause) matches(rt Runtime) bool {
	switch c.field {
	case "name":
		return rt.Name == c.value
	case "displayName":
		return rt.DisplayName == c.value
	case "stage":
		return rt.Stage == c.value
	case "environment":
		return rt.Environment == c.value
	}
	return false
}

// runtimeFilterFields is the set of filterable Runtime fields.
var runtimeFilterFields = map[string]bool{
	"name":        true,
	"displayName": true,
	"stage":       true,
	"environment": true,
}

// parseRuntimeFilter parses the AIP-160 subset real clients use for runtimes:
// one or more `field = "value"` equality clauses joined by AND (the keyword is
// case-insensitive). An empty filter yields no clauses. A clause without `=`, an
// unknown field, an unsupported operator (OR/NOT/parentheses), or a malformed
// value is InvalidArgument — a filter the emulator cannot evaluate is rejected
// rather than silently returning a wrong (e.g. empty) result.
func parseRuntimeFilter(filter string) ([]runtimeFilterClause, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return nil, nil
	}
	// Split on a top-level AND, case-insensitively, outside quotes.
	parts := splitFilterAnd(filter)
	clauses := make([]runtimeFilterClause, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, invalidArgument("malformed runtimes filter " + filter)
		}
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			return nil, invalidArgument("unsupported runtimes filter " + filter)
		}
		field := strings.TrimSpace(p[:eq])
		if !runtimeFilterFields[field] {
			return nil, invalidArgument("unsupported runtimes filter field " + field)
		}
		value, err := parseFilterValue(strings.TrimSpace(p[eq+1:]))
		if err != nil {
			return nil, invalidArgument("malformed runtimes filter " + filter)
		}
		clauses = append(clauses, runtimeFilterClause{field: field, value: value})
	}
	return clauses, nil
}

// parseFilterValue parses a single equality value: either a double-quoted
// string (with no embedded quote) or a single bare token with no whitespace,
// quote, parenthesis, or '='. Anything else is rejected so unsupported
// operators (OR/NOT/parentheses) cannot be mistaken for a value.
func parseFilterValue(raw string) (string, error) {
	if raw == "" {
		return "", invalidArgument("empty value")
	}
	if strings.HasPrefix(raw, `"`) {
		if len(raw) < 2 || !strings.HasSuffix(raw, `"`) {
			return "", invalidArgument("unterminated quoted value")
		}
		value := raw[1 : len(raw)-1]
		if strings.ContainsAny(value, `"`) {
			return "", invalidArgument("malformed quoted value")
		}
		return value, nil
	}
	if strings.ContainsAny(raw, ` "()=`) {
		return "", invalidArgument("malformed unquoted value")
	}
	return raw, nil
}

// splitFilterAnd splits an AIP-160 expression on the top-level "AND" operator
// (case-insensitive), ignoring AND inside double quotes.
func splitFilterAnd(s string) []string {
	var parts []string
	var b strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			inQuote = !inQuote
			b.WriteByte(c)
			continue
		}
		if !inQuote && i+3 <= len(s) && strings.EqualFold(s[i:i+3], "and") {
			// Require word boundaries around AND.
			leftOK := i == 0 || isSpace(s[i-1])
			rightOK := i+3 == len(s) || isSpace(s[i+3])
			if leftOK && rightOK {
				parts = append(parts, b.String())
				b.Reset()
				i += 2 // skip "AND"; loop increments past the third char
				continue
			}
		}
		b.WriteByte(c)
	}
	parts = append(parts, b.String())
	return parts
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
