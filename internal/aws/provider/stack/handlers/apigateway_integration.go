package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	apigwprovider "jaiscloud/internal/aws/provider/apigw"
	"jaiscloud/internal/model"
)

// NewAPIGatewayIntegrationHandler returns a ResourceHandler for AWS::ApiGateway::Integration.
func NewAPIGatewayIntegrationHandler(apigwP *apigwprovider.GatewayProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			resourceID := propStr(props, "ResourceId", "")
			httpMethod := propStr(props, "HttpMethod", "")
			intType := propStr(props, "Type", "")
			uri := propStr(props, "Uri", "")
			if _, err := apigwP.PutIntegration(ctx, child(nr, map[string]any{
				"restApiId":  restAPIID,
				"resourceId": resourceID,
				"httpMethod": httpMethod,
				"type":       intType,
				"uri":        uri,
			})); err != nil {
				return "", nil, err
			}
			physicalID := restAPIID + "/" + resourceID + "/" + httpMethod + "/integration"
			return physicalID, map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			resourceID := propStr(props, "ResourceId", "")
			httpMethod := propStr(props, "HttpMethod", "")
			_, err := apigwP.DeleteIntegration(ctx, &model.NormalizedRequest{
				Params: map[string]any{
					"restApiId":  restAPIID,
					"resourceId": resourceID,
					"httpMethod": httpMethod,
				},
			})
			return err
		},
	}
}
