package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/model"
)

// NewEC2VPCHandler returns a ResourceHandler for AWS::EC2::VPC.
func NewEC2VPCHandler(computeP *compute.ComputeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			cidr := propStr(props, "CidrBlock", "")
			resp, err := computeP.CreateVpc(ctx, child(nr, map[string]any{
				"CidrBlock": cidr,
			}))
			if err != nil {
				return "", nil, err
			}
			vpcID := ""
			if vm, ok := resp.Data["Vpc"].(map[string]any); ok {
				vpcID, _ = vm["VpcId"].(string)
			}
			return vpcID, map[string]any{"VpcId": vpcID, "CidrBlock": cidr, "DefaultSecurityGroup": ""}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := computeP.DeleteVpc(ctx, &model.NormalizedRequest{Params: map[string]any{"VpcId": physicalID}})
			return err
		},
	}
}
