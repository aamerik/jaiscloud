package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	iamprovider "jaiscloud/internal/aws/provider/iam"
	"jaiscloud/internal/model"
)

// NewIAMPolicyHandler returns a ResourceHandler for AWS::IAM::Policy.
func NewIAMPolicyHandler(iamP *iamprovider.IAMProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "PolicyName", logicalID)
			params := copyProps(props)
			params["PolicyName"] = name
			resp, err := iamP.CreatePolicy(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if pm, ok := resp.Data["Policy"].(map[string]any); ok {
				arn, _ = pm["Arn"].(string)
			}
			return arn, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "PolicyName", logicalID) != propStr(newProps, "PolicyName", logicalID) {
				return "", nil, true, nil
			}
			// PolicyDocument / Description changes require replacement (no update API for inline policy doc)
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := iamP.DeletePolicy(ctx, &model.NormalizedRequest{Params: map[string]any{"PolicyArn": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"PolicyName"},
			RequireUpdate:      []string{"PolicyDocument", "Description"},
		},
	}
}
