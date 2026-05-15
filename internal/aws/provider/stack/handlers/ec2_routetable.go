package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/model"
)

// NewEC2RouteTableHandler returns a ResourceHandler for AWS::EC2::RouteTable.
func NewEC2RouteTableHandler(computeP *compute.ComputeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			resp, err := computeP.CreateRouteTable(ctx, child(nr, map[string]any{
				"VpcId": propStr(props, "VpcId", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			rtID := ""
			if rm, ok := resp.Data["RouteTable"].(map[string]any); ok {
				rtID, _ = rm["RouteTableId"].(string)
			}
			return rtID, map[string]any{"RouteTableId": rtID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := computeP.DeleteRouteTable(ctx, &model.NormalizedRequest{Params: map[string]any{"RouteTableId": physicalID}})
			return err
		},
	}
}
