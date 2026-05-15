package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/model"
)

// NewSNSTopicHandler returns a ResourceHandler for AWS::SNS::Topic.
func NewSNSTopicHandler(notifP *notification.SNSProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "TopicName", logicalID)
			resp, err := notifP.CreateTopic(ctx, child(nr, map[string]any{"Name": name}))
			if err != nil {
				return "", nil, err
			}
			arn := resp.Data["TopicArn"].(string)
			return arn, map[string]any{"TopicArn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := notifP.DeleteTopic(ctx, &model.NormalizedRequest{Params: map[string]any{"TopicArn": physicalID}})
			return err
		},
	}
}
