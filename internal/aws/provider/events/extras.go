package events

import (
	"context"
	"encoding/json"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// TestEventPattern does a best-effort match of an event against a pattern.
func (p *EventBridgeProvider) TestEventPattern(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pattern := strParam(nr.Params, "EventPattern")
	eventStr := strParam(nr.Params, "Event")

	var envelope map[string]any
	if err := json.Unmarshal([]byte(eventStr), &envelope); err != nil {
		return nil, &model.ProviderError{Code: "InvalidEventPatternException", Message: "Event must be valid JSON", HTTPStatus: 400}
	}

	result := matchesPattern(pattern, envelope)
	return provider.OK(map[string]any{"Result": result}), nil
}

// ListRuleNamesByTarget returns rule names that have the given target ARN.
func (p *EventBridgeProvider) ListRuleNamesByTarget(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	targetArn := strParam(nr.Params, "TargetArn")
	busFilter := normBus(strParam(nr.Params, "EventBusName"))

	// Scan all targets under the bus; collect rule names that match the ARN.
	// Store ID format: "busName/ruleName/targetID"
	storePrefix := busFilter + "/"
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeTarget, storePrefix)
	ruleNames := []string{}
	seen := map[string]bool{}
	for _, e := range entries {
		var td targetData
		if err := json.Unmarshal(e.Data, &td); err != nil {
			continue
		}
		if td.Arn != targetArn {
			continue
		}
		// Strip bus prefix, then take the rule name (up to the next slash).
		rest := e.ID[len(storePrefix):]
		ruleName := rest
		if idx := strings.Index(rest, "/"); idx >= 0 {
			ruleName = rest[:idx]
		}
		if ruleName != "" && !seen[ruleName] {
			seen[ruleName] = true
			ruleNames = append(ruleNames, ruleName)
		}
	}
	return provider.OK(map[string]any{"RuleNames": ruleNames}), nil
}
