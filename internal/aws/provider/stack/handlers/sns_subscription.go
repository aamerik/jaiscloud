package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/model"
)

// NewSNSSubscriptionHandler returns a ResourceHandler for AWS::SNS::Subscription.
func NewSNSSubscriptionHandler(notifP *notification.SNSProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := notifP.Subscribe(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			subArn, _ := resp.Data["SubscriptionArn"].(string)
			return subArn, map[string]any{"SubscriptionArn": subArn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := notifP.Unsubscribe(ctx, &model.NormalizedRequest{Params: map[string]any{"SubscriptionArn": physicalID}})
			return err
		},
	}
}
