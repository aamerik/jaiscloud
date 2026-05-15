package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/model"
)

// NewEC2SecurityGroupHandler returns a ResourceHandler for AWS::EC2::SecurityGroup.
func NewEC2SecurityGroupHandler(computeP *compute.ComputeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			groupName := propStr(props, "GroupName", logicalID)
			resp, err := computeP.CreateSecurityGroup(ctx, child(nr, map[string]any{
				"GroupName":        groupName,
				"GroupDescription": propStr(props, "GroupDescription", groupName),
				"VpcId":            propStr(props, "VpcId", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			groupID, _ := resp.Data["GroupId"].(string)
			return groupID, map[string]any{"GroupId": groupID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := computeP.DeleteSecurityGroup(ctx, &model.NormalizedRequest{Params: map[string]any{"GroupId": physicalID}})
			return err
		},
	}
}
