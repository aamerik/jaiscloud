package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	functionprovider "jaiscloud/internal/aws/provider/function"
	"jaiscloud/internal/model"
)

// NewLambdaFunctionHandler returns a ResourceHandler for AWS::Lambda::Function.
func NewLambdaFunctionHandler(funcP *functionprovider.FunctionProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "FunctionName", logicalID)
			params := copyProps(props)
			params["FunctionName"] = name
			resp, err := funcP.CreateFunction(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["FunctionArn"].(string)
			return name, map[string]any{"Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := funcP.DeleteFunction(ctx, &model.NormalizedRequest{Params: map[string]any{"_function_name": physicalID}})
			return err
		},
	}
}
