package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	containerprovider "jaiscloud/internal/aws/provider/container"
	"jaiscloud/internal/model"
)

// NewECSServiceHandler returns a ResourceHandler for AWS::ECS::Service.
func NewECSServiceHandler(ecsP *containerprovider.ContainerProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "ServiceName", logicalID)
			params := copyProps(props)
			params["serviceName"] = name
			resp, err := ecsP.CreateService(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			serviceArn := ""
			if sm, ok := resp.Data["service"].(map[string]any); ok {
				serviceArn, _ = sm["serviceArn"].(string)
			}
			return name, map[string]any{"ServiceArn": serviceArn, "Name": name}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			clusterName := propStr(props, "Cluster", "default")
			_, err := ecsP.DeleteService(ctx, &model.NormalizedRequest{
				Params: map[string]any{"cluster": clusterName, "service": physicalID},
			})
			return err
		},
	}
}
