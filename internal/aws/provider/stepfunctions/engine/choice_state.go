package engine

import (
	"fmt"
	"math"
	"strings"
	"time"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

func (e *ExecutionEngine) evalChoice(state *asl.ChoiceState, input any) (string, any, error) {
	effective := applyInputPath2(input, state.InputPath)

	for _, choice := range state.Choices {
		matched, err := evalChoiceRule(&choice, effective)
		if err != nil {
			return "", nil, err
		}
		if matched {
			output := applyOutputPath2(effective, state.OutputPath)
			return choice.Next, output, nil
		}
	}
	if state.Default != "" {
		output := applyOutputPath2(effective, state.OutputPath)
		return state.Default, output, nil
	}
	return "", nil, &sfnError{code: "States.NoChoiceMatched", cause: "no choice rule matched and no Default transition"}
}

// evalChoiceRule evaluates a single ChoiceRule (may be compound).
func evalChoiceRule(rule *asl.ChoiceRule, input any) (bool, error) {
	// Compound rules (no Variable)
	if len(rule.And) > 0 {
		for _, sub := range rule.And {
			m, err := evalChoiceRule(&sub, input)
			if err != nil || !m {
				return false, err
			}
		}
		return true, nil
	}
	if len(rule.Or) > 0 {
		for _, sub := range rule.Or {
			m, err := evalChoiceRule(&sub, input)
			if err != nil {
				return false, err
			}
			if m {
				return true, nil
			}
		}
		return false, nil
	}
	if rule.Not != nil {
		m, err := evalChoiceRule(rule.Not, input)
		if err != nil {
			return false, err
		}
		return !m, nil
	}

	// Leaf rule — evaluate Variable
	actual, _ := asl.EvalPath(input, rule.Variable)

	// ─── String comparisons ───────────────────────────────────────────────────
	if rule.StringEquals != "" {
		s, ok := actual.(string)
		return ok && s == rule.StringEquals, nil
	}
	if rule.StringEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.StringEqualsPath)
		return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", exp), nil
	}
	if rule.StringLessThan != "" {
		s, ok := actual.(string)
		return ok && s < rule.StringLessThan, nil
	}
	if rule.StringLessThanPath != "" {
		exp, _ := asl.EvalPath(input, rule.StringLessThanPath)
		es, ok := exp.(string)
		s, sok := actual.(string)
		return ok && sok && s < es, nil
	}
	if rule.StringGreaterThan != "" {
		s, ok := actual.(string)
		return ok && s > rule.StringGreaterThan, nil
	}
	if rule.StringGreaterThanPath != "" {
		exp, _ := asl.EvalPath(input, rule.StringGreaterThanPath)
		es, ok := exp.(string)
		s, sok := actual.(string)
		return ok && sok && s > es, nil
	}
	if rule.StringLessThanEquals != "" {
		s, ok := actual.(string)
		return ok && s <= rule.StringLessThanEquals, nil
	}
	if rule.StringLessThanEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.StringLessThanEqualsPath)
		es, ok := exp.(string)
		s, sok := actual.(string)
		return ok && sok && s <= es, nil
	}
	if rule.StringGreaterThanEquals != "" {
		s, ok := actual.(string)
		return ok && s >= rule.StringGreaterThanEquals, nil
	}
	if rule.StringGreaterThanEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.StringGreaterThanEqualsPath)
		es, ok := exp.(string)
		s, sok := actual.(string)
		return ok && sok && s >= es, nil
	}
	if rule.StringMatches != "" {
		s, ok := actual.(string)
		if !ok {
			return false, nil
		}
		return wildcardMatch(s, rule.StringMatches), nil
	}

	// ─── Numeric comparisons ──────────────────────────────────────────────────
	if rule.NumericEquals != nil {
		n, ok := toNumber(actual)
		return ok && floatEq(n, *rule.NumericEquals), nil
	}
	if rule.NumericEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.NumericEqualsPath)
		en, ek := toNumber(exp)
		n, ok := toNumber(actual)
		return ok && ek && floatEq(n, en), nil
	}
	if rule.NumericLessThan != nil {
		n, ok := toNumber(actual)
		return ok && n < *rule.NumericLessThan, nil
	}
	if rule.NumericLessThanPath != "" {
		exp, _ := asl.EvalPath(input, rule.NumericLessThanPath)
		en, ek := toNumber(exp)
		n, ok := toNumber(actual)
		return ok && ek && n < en, nil
	}
	if rule.NumericGreaterThan != nil {
		n, ok := toNumber(actual)
		return ok && n > *rule.NumericGreaterThan, nil
	}
	if rule.NumericGreaterThanPath != "" {
		exp, _ := asl.EvalPath(input, rule.NumericGreaterThanPath)
		en, ek := toNumber(exp)
		n, ok := toNumber(actual)
		return ok && ek && n > en, nil
	}
	if rule.NumericLessThanEquals != nil {
		n, ok := toNumber(actual)
		return ok && n <= *rule.NumericLessThanEquals, nil
	}
	if rule.NumericLessThanEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.NumericLessThanEqualsPath)
		en, ek := toNumber(exp)
		n, ok := toNumber(actual)
		return ok && ek && n <= en, nil
	}
	if rule.NumericGreaterThanEquals != nil {
		n, ok := toNumber(actual)
		return ok && n >= *rule.NumericGreaterThanEquals, nil
	}
	if rule.NumericGreaterThanEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.NumericGreaterThanEqualsPath)
		en, ek := toNumber(exp)
		n, ok := toNumber(actual)
		return ok && ek && n >= en, nil
	}

	// ─── Boolean comparisons ──────────────────────────────────────────────────
	if rule.BooleanEquals != nil {
		b, ok := actual.(bool)
		return ok && b == *rule.BooleanEquals, nil
	}
	if rule.BooleanEqualsPath != "" {
		exp, _ := asl.EvalPath(input, rule.BooleanEqualsPath)
		eb, ek := exp.(bool)
		b, ok := actual.(bool)
		return ok && ek && b == eb, nil
	}

	// ─── Timestamp comparisons ────────────────────────────────────────────────
	if rule.TimestampEquals != "" {
		at, ok := parseTimestamp(actual)
		et, err := time.Parse(time.RFC3339, rule.TimestampEquals)
		return ok && err == nil && at.Equal(et), nil
	}
	if rule.TimestampEqualsPath != "" {
		at, aok := parseTimestamp(actual)
		exp, _ := asl.EvalPath(input, rule.TimestampEqualsPath)
		et, eok := parseTimestamp(exp)
		return aok && eok && at.Equal(et), nil
	}
	if rule.TimestampLessThan != "" {
		at, ok := parseTimestamp(actual)
		et, err := time.Parse(time.RFC3339, rule.TimestampLessThan)
		return ok && err == nil && at.Before(et), nil
	}
	if rule.TimestampLessThanPath != "" {
		at, aok := parseTimestamp(actual)
		exp, _ := asl.EvalPath(input, rule.TimestampLessThanPath)
		et, eok := parseTimestamp(exp)
		return aok && eok && at.Before(et), nil
	}
	if rule.TimestampGreaterThan != "" {
		at, ok := parseTimestamp(actual)
		et, err := time.Parse(time.RFC3339, rule.TimestampGreaterThan)
		return ok && err == nil && at.After(et), nil
	}
	if rule.TimestampGreaterThanPath != "" {
		at, aok := parseTimestamp(actual)
		exp, _ := asl.EvalPath(input, rule.TimestampGreaterThanPath)
		et, eok := parseTimestamp(exp)
		return aok && eok && at.After(et), nil
	}
	if rule.TimestampLessThanEquals != "" {
		at, ok := parseTimestamp(actual)
		et, err := time.Parse(time.RFC3339, rule.TimestampLessThanEquals)
		return ok && err == nil && !at.After(et), nil
	}
	if rule.TimestampLessThanEqualsPath != "" {
		at, aok := parseTimestamp(actual)
		exp, _ := asl.EvalPath(input, rule.TimestampLessThanEqualsPath)
		et, eok := parseTimestamp(exp)
		return aok && eok && !at.After(et), nil
	}
	if rule.TimestampGreaterThanEquals != "" {
		at, ok := parseTimestamp(actual)
		et, err := time.Parse(time.RFC3339, rule.TimestampGreaterThanEquals)
		return ok && err == nil && !at.Before(et), nil
	}
	if rule.TimestampGreaterThanEqualsPath != "" {
		at, aok := parseTimestamp(actual)
		exp, _ := asl.EvalPath(input, rule.TimestampGreaterThanEqualsPath)
		et, eok := parseTimestamp(exp)
		return aok && eok && !at.Before(et), nil
	}

	// ─── Type checks ──────────────────────────────────────────────────────────
	if rule.IsNull != nil {
		return (actual == nil) == *rule.IsNull, nil
	}
	if rule.IsPresent != nil {
		// IsPresent checks whether the path resolved to a value (non-nil)
		return (actual != nil) == *rule.IsPresent, nil
	}
	if rule.IsString != nil {
		_, ok := actual.(string)
		return ok == *rule.IsString, nil
	}
	if rule.IsNumeric != nil {
		_, ok := toNumber(actual)
		return ok == *rule.IsNumeric, nil
	}
	if rule.IsBoolean != nil {
		_, ok := actual.(bool)
		return ok == *rule.IsBoolean, nil
	}
	if rule.IsTimestamp != nil {
		_, ok := parseTimestamp(actual)
		return ok == *rule.IsTimestamp, nil
	}

	return false, fmt.Errorf("States.Runtime: ChoiceRule has no comparison operator")
}

// wildcardMatch matches s against pattern where * matches any sequence and ? matches one char.
func wildcardMatch(s, pattern string) bool {
	// Simple recursive wildcard
	if pattern == "*" {
		return true
	}
	if pattern == "" {
		return s == ""
	}
	if pattern[0] == '*' {
		return wildcardMatch(s, pattern[1:]) || (len(s) > 0 && wildcardMatch(s[1:], pattern))
	}
	if len(s) == 0 {
		return false
	}
	if pattern[0] == '?' || strings.EqualFold(string(s[0]), string(pattern[0])) {
		return wildcardMatch(s[1:], pattern[1:])
	}
	return false
}

func toNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

func floatEq(a, b float64) bool {
	return math.Abs(a-b) < 1e-10
}

func parseTimestamp(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}
