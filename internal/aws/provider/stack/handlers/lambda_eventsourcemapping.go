package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	"jaiscloud/internal/model"
)

// NewLambdaEventSourceMappingHandler returns a ResourceHandler for AWS::Lambda::EventSourceMapping.
func NewLambdaEventSourceMappingHandler(esmP *lambdaesm.Provider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := esmP.CreateEventSourceMapping(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			uuid, _ := resp.Data["UUID"].(string)
			return uuid, map[string]any{"UUID": uuid}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := esmP.DeleteEventSourceMapping(ctx, &model.NormalizedRequest{
				Params: map[string]any{"_esm_uuid": physicalID},
			})
			return err
		},
	}
}
