package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	apigwprovider "jaiscloud/internal/aws/provider/apigw"
	"jaiscloud/internal/model"
)

// NewAPIGatewayDeploymentHandler returns a ResourceHandler for AWS::ApiGateway::Deployment.
func NewAPIGatewayDeploymentHandler(apigwP *apigwprovider.GatewayProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			stageName := propStr(props, "StageName", "")
			resp, err := apigwP.CreateDeployment(ctx, child(nr, map[string]any{
				"restApiId": restAPIID,
				"stageName": stageName,
			}))
			if err != nil {
				return "", nil, err
			}
			deploymentID, _ := resp.Data["id"].(string)
			return deploymentID, map[string]any{"DeploymentId": deploymentID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			_, err := apigwP.DeleteDeployment(ctx, &model.NormalizedRequest{
				Params: map[string]any{"restApiId": restAPIID, "deploymentId": physicalID},
			})
			return err
		},
	}
}
