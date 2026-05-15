package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	eventsprovider "jaiscloud/internal/aws/provider/events"
	"jaiscloud/internal/model"
)

// NewEventsEventBusHandler returns a ResourceHandler for AWS::Events::EventBus.
func NewEventsEventBusHandler(eventsP *eventsprovider.EventBridgeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			resp, err := eventsP.CreateEventBus(ctx, child(nr, map[string]any{"Name": name}))
			if err != nil {
				return "", nil, err
			}
			busArn, _ := resp.Data["EventBusArn"].(string)
			return name, map[string]any{"Arn": busArn, "Name": name}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := eventsP.DeleteEventBus(ctx, &model.NormalizedRequest{Params: map[string]any{"Name": physicalID}})
			return err
		},
	}
}
