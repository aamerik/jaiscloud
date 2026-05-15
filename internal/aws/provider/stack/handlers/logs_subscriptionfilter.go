package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/model"
)

// NewLogsSubscriptionFilterHandler returns a ResourceHandler for AWS::Logs::SubscriptionFilter.
func NewLogsSubscriptionFilterHandler(logsP *cwlogs.Provider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			logGroupName := propStr(props, "LogGroupName", "")
			filterName := propStr(props, "FilterName", logicalID)
			filterPattern := propStr(props, "FilterPattern", "")
			destinationArn := propStr(props, "DestinationArn", "")
			if _, err := logsP.PutSubscriptionFilter(ctx, child(nr, map[string]any{
				"logGroupName":   logGroupName,
				"filterName":     filterName,
				"filterPattern":  filterPattern,
				"destinationArn": destinationArn,
			})); err != nil {
				return "", nil, err
			}
			physicalID := logGroupName + "/" + filterName
			return physicalID, map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			logGroupName := propStr(props, "LogGroupName", "")
			filterName := propStr(props, "FilterName", "")
			_, err := logsP.DeleteSubscriptionFilter(ctx, &model.NormalizedRequest{
				Params: map[string]any{
					"logGroupName": logGroupName,
					"filterName":   filterName,
				},
			})
			return err
		},
	}
}
