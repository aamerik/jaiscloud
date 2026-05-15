package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/model"
)

// NewEC2SubnetHandler returns a ResourceHandler for AWS::EC2::Subnet.
func NewEC2SubnetHandler(computeP *compute.ComputeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			resp, err := computeP.CreateSubnet(ctx, child(nr, map[string]any{
				"VpcId":            propStr(props, "VpcId", ""),
				"CidrBlock":        propStr(props, "CidrBlock", ""),
				"AvailabilityZone": propStr(props, "AvailabilityZone", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			subnetID := ""
			if sm, ok := resp.Data["Subnet"].(map[string]any); ok {
				subnetID, _ = sm["SubnetId"].(string)
			}
			return subnetID, map[string]any{"SubnetId": subnetID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := computeP.DeleteSubnet(ctx, &model.NormalizedRequest{Params: map[string]any{"SubnetId": physicalID}})
			return err
		},
	}
}
