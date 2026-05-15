package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	iamprovider "jaiscloud/internal/aws/provider/iam"
	"jaiscloud/internal/model"
)

// NewIAMRoleHandler returns a ResourceHandler for AWS::IAM::Role.
func NewIAMRoleHandler(iamP *iamprovider.IAMProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "RoleName", logicalID)
			params := copyProps(props)
			params["RoleName"] = name
			resp, err := iamP.CreateRole(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if rm, ok := resp.Data["Role"].(map[string]any); ok {
				arn, _ = rm["Arn"].(string)
			}
			return name, map[string]any{"Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := iamP.DeleteRole(ctx, &model.NormalizedRequest{
				AccountID: "000000000000",
				Params:    map[string]any{"RoleName": physicalID},
			})
			return err
		},
	}
}
