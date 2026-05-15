package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/model"
)

// NewLogsLogStreamHandler returns a ResourceHandler for AWS::Logs::LogStream.
func NewLogsLogStreamHandler(logsP *cwlogs.Provider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			groupName := propStr(props, "LogGroupName", "")
			streamName := propStr(props, "LogStreamName", logicalID)
			if _, err := logsP.CreateLogStream(ctx, child(nr, map[string]any{
				"logGroupName":  groupName,
				"logStreamName": streamName,
			})); err != nil {
				return "", nil, err
			}
			return streamName, map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			groupName := propStr(props, "LogGroupName", "")
			_, err := logsP.DeleteLogStream(ctx, &model.NormalizedRequest{
				Params: map[string]any{"logGroupName": groupName, "logStreamName": physicalID},
			})
			return err
		},
	}
}
