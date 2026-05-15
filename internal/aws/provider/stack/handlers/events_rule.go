package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	eventsprovider "jaiscloud/internal/aws/provider/events"
	"jaiscloud/internal/model"
)

// NewEventsRuleHandler returns a ResourceHandler for AWS::Events::Rule.
func NewEventsRuleHandler(eventsP *eventsprovider.EventBridgeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["Name"] = name
			resp, err := eventsP.PutRule(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			ruleArn, _ := resp.Data["RuleArn"].(string)
			return name, map[string]any{"Arn": ruleArn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			busName := propStr(props, "EventBusName", "default")
			_, err := eventsP.DeleteRule(ctx, &model.NormalizedRequest{
				Params: map[string]any{"Name": physicalID, "EventBusName": busName},
			})
			return err
		},
	}
}
