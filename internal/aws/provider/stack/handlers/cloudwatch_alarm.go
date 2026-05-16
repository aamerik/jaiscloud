package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	cloudwatchprovider "jaiscloud/internal/aws/provider/cloudwatch"
	"jaiscloud/internal/model"
)

// NewCloudWatchAlarmHandler returns a ResourceHandler for AWS::CloudWatch::Alarm.
func NewCloudWatchAlarmHandler(cwP *cloudwatchprovider.Provider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "AlarmName", logicalID)
			params := copyProps(props)
			params["AlarmName"] = name
			if _, err := cwP.PutMetricAlarm(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			arn := nr.ResourceID("cloudwatch-alarm", name)
			return name, map[string]any{"Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := cwP.DeleteAlarms(ctx, &model.NormalizedRequest{
				Params: map[string]any{"AlarmNames": []any{physicalID}},
			})
			return err
		},
	}
}
