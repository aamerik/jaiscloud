package handlers

import (
	"context"
	"fmt"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/model"
)

// NewLogsLogGroupHandler returns a ResourceHandler for AWS::Logs::LogGroup.
func NewLogsLogGroupHandler(logsP *cwlogs.Provider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "LogGroupName", "/cfn/"+logicalID)
			params := copyProps(props)
			params["logGroupName"] = name
			if _, err := logsP.CreateLogGroup(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			arn := fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s:*", nr.Region, nr.AccountID, name)
			return name, map[string]any{"Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := logsP.DeleteLogGroup(ctx, &model.NormalizedRequest{Params: map[string]any{"logGroupName": physicalID}})
			return err
		},
	}
}
