package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	apigwprovider "jaiscloud/internal/aws/provider/apigw"
	"jaiscloud/internal/model"
)

// NewAPIGatewayStageHandler returns a ResourceHandler for AWS::ApiGateway::Stage.
func NewAPIGatewayStageHandler(apigwP *apigwprovider.GatewayProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			stageName := propStr(props, "StageName", logicalID)
			deploymentID := propStr(props, "DeploymentId", "")
			resp, err := apigwP.CreateStage(ctx, child(nr, map[string]any{
				"restApiId":    restAPIID,
				"stageName":    stageName,
				"deploymentId": deploymentID,
			}))
			if err != nil {
				return "", nil, err
			}
			_ = resp
			physicalID := restAPIID + "/" + stageName
			return physicalID, map[string]any{"StageName": stageName}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			stageName := propStr(props, "StageName", "")
			_, err := apigwP.DeleteStage(ctx, &model.NormalizedRequest{
				Params: map[string]any{"restApiId": restAPIID, "stageName": stageName},
			})
			return err
		},
	}
}
