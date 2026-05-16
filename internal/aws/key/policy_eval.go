package key

import (
	"encoding/json"
	"strings"
)

// policyStatement is a single statement in a key policy document.
type policyStatement struct {
	Effect    string `json:"Effect"`
	Principal any    `json:"Principal"` // string | {"AWS": string|[]string} | {"Service": ...}
	Action    any    `json:"Action"`    // string | []string
	Resource  any    `json:"Resource"`  // string | []string (ignored here — key policies are always resource-scoped)
}

// evalKeyPolicy returns true if the given action is allowed for the given caller principal.
// An empty policy string defaults to allow-all (emulator permissive default).
func evalKeyPolicy(policyJSON, callerPrincipal, action string) bool {
	if policyJSON == "" {
		return true
	}
	var doc struct {
		Statement []policyStatement `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return true // malformed policy → allow
	}

	// Evaluate Deny statements first (always wins).
	for _, s := range doc.Statement {
		if !strings.EqualFold(s.Effect, "Deny") {
			continue
		}
		if matchesPrincipal(s.Principal, callerPrincipal) && matchesAction(s.Action, action) {
			return false
		}
	}

	// If there are any Allow statements, the caller must match at least one.
	hasAllows := false
	for _, s := range doc.Statement {
		if !strings.EqualFold(s.Effect, "Allow") {
			continue
		}
		hasAllows = true
		if matchesPrincipal(s.Principal, callerPrincipal) && matchesAction(s.Action, action) {
			return true
		}
	}

	// No matching Allow → deny (if there were any Allow statements in the policy).
	if hasAllows {
		return false
	}
	// No Allow statements at all → permissive default.
	return true
}

func matchesPrincipal(principal any, caller string) bool {
	switch p := principal.(type) {
	case string:
		return p == "*" || strings.EqualFold(p, caller)
	case map[string]any:
		for _, v := range p {
			if matchesPrincipal(v, caller) {
				return true
			}
		}
	case []any:
		for _, item := range p {
			if matchesPrincipal(item, caller) {
				return true
			}
		}
	}
	return false
}

func matchesAction(action any, target string) bool {
	target = strings.ToLower(target)
	switch a := action.(type) {
	case string:
		return actionMatches(strings.ToLower(a), target)
	case []any:
		for _, item := range a {
			if s, ok := item.(string); ok && actionMatches(strings.ToLower(s), target) {
				return true
			}
		}
	}
	return false
}

// actionMatches handles exact match and wildcard (* suffix only, e.g. "kms:*").
func actionMatches(pattern, target string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, ":*") {
		prefix := pattern[:len(pattern)-1] // "kms:"
		return strings.HasPrefix(target, prefix)
	}
	return pattern == target
}
