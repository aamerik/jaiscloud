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
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "TopicName", logicalID) != propStr(newProps, "TopicName", logicalID) {
				return "", nil, true, nil
			}
			// In-place: update display name if changed
			if propStr(oldProps, "DisplayName", "") != propStr(newProps, "DisplayName", "") {
				if _, err := notifP.SetTopicAttributes(ctx, child(nr, map[string]any{
					"TopicArn":       physicalID,
					"AttributeName":  "DisplayName",
					"AttributeValue": propStr(newProps, "DisplayName", ""),
				})); err != nil {
					return "", nil, false, err
				}
			}
			return physicalID, map[string]any{"TopicArn": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := notifP.DeleteTopic(ctx, &model.NormalizedRequest{Params: map[string]any{"TopicArn": physicalID}})
			return err
		},
		RefAttr:     "Arn",
		GetAttAttrs: []string{"TopicArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"TopicName"},
			RequireUpdate:      []string{"DisplayName", "Subscription", "Tags"},
		},
	}
}
