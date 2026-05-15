package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/model"
)

// NewEC2InternetGatewayHandler returns a ResourceHandler for AWS::EC2::InternetGateway.
func NewEC2InternetGatewayHandler(computeP *compute.ComputeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			resp, err := computeP.CreateInternetGateway(ctx, child(nr, map[string]any{}))
			if err != nil {
				return "", nil, err
			}
			igwID := ""
			if im, ok := resp.Data["InternetGateway"].(map[string]any); ok {
				igwID, _ = im["InternetGatewayId"].(string)
			}
			return igwID, map[string]any{"InternetGatewayId": igwID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := computeP.DeleteInternetGateway(ctx, &model.NormalizedRequest{Params: map[string]any{"InternetGatewayId": physicalID}})
			return err
		},
	}
}
