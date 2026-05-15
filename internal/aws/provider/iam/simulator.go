package iam

import (
	"encoding/json"
	"strings"
)

// EvalResult holds the outcome of a single (action, resource) evaluation.
type EvalResult struct {
	EvalActionName   string
	EvalResourceName string
	EvalDecision     string // "allowed" | "explicitDeny" | "implicitDeny"
}

// SimulatePolicies evaluates each (action, resource) pair against the provided
// policy document strings. Decision: explicit Deny wins; otherwise first Allow wins;
// default implicitDeny.
func SimulatePolicies(policyDocs []string, actions []string, resources []string) []EvalResult {
	var results []EvalResult
	for _, action := range actions {
		for _, resource := range resources {
			decision := "implicitDeny"
			for _, doc := range policyDocs {
				var pd policyDoc
				if err := json.Unmarshal([]byte(doc), &pd); err != nil {
					continue
				}
				for _, stmt := range pd.Statement {
					if matchesAction(stmt.Action, action) && matchesResource(stmt.Resource, resource) {
						if stmt.Effect == "Deny" {
							decision = "explicitDeny"
							goto done
						}
						if stmt.Effect == "Allow" && decision == "implicitDeny" {
							decision = "allowed"
						}
					}
				}
			}
		done:
			results = append(results, EvalResult{
				EvalActionName:   action,
				EvalResourceName: resource,
				EvalDecision:     decision,
			})
		}
	}
	return results
}

func matchesAction(actionField any, action string) bool {
	return matchesGlobList(actionField, action)
}

func matchesResource(resourceField any, resource string) bool {
	if resource == "*" {
		return true
	}
	if s, ok := resourceField.(string); ok && s == "*" {
		return true
	}
	return matchesGlobList(resourceField, resource)
}

func matchesGlobList(field any, target string) bool {
	var patterns []string
	switch v := field.(type) {
	case string:
		patterns = []string{v}
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				patterns = append(patterns, s)
			}
		}
	}
	target = strings.ToLower(target)
	for _, p := range patterns {
		if globMatch(strings.ToLower(p), target) {
			return true
		}
	}
	return false
}

// globMatch supports * and ? wildcards.
func globMatch(pattern, str string) bool {
	if pattern == "*" {
		return true
	}
	if len(pattern) == 0 {
		return len(str) == 0
	}
	if pattern[0] == '*' {
		for i := 0; i <= len(str); i++ {
			if globMatch(pattern[1:], str[i:]) {
				return true
			}
		}
		return false
	}
	if len(str) == 0 {
		return false
	}
	if pattern[0] == '?' || pattern[0] == str[0] {
		return globMatch(pattern[1:], str[1:])
	}
	return false
}
