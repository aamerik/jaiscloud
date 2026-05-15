package handlers

import (
	"context"
	"fmt"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	apigwprovider "jaiscloud/internal/aws/provider/apigw"
	"jaiscloud/internal/model"
)

// NewAPIGatewayRestApiHandler returns a ResourceHandler for AWS::ApiGateway::RestApi.
func NewAPIGatewayRestApiHandler(apigwP *apigwprovider.GatewayProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["name"] = name
			resp, err := apigwP.CreateRestApi(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			apiID, _ := resp.Data["id"].(string)
			arn := fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", nr.Region, apiID)
			return apiID, map[string]any{"RestApiId": apiID, "RootResourceId": resp.Data["rootResourceId"], "Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := apigwP.DeleteRestApi(ctx, &model.NormalizedRequest{Params: map[string]any{"restApiId": physicalID}})
			return err
		},
	}
}
