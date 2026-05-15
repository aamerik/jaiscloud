package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	functionprovider "jaiscloud/internal/aws/provider/function"
	"jaiscloud/internal/model"
)

// NewLambdaUrlHandler returns a ResourceHandler for AWS::Lambda::Url.
func NewLambdaUrlHandler(funcP *functionprovider.FunctionProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			targetFunc := propStr(props, "TargetFunctionArn", "")
			authType := propStr(props, "AuthType", "NONE")
			params := map[string]any{
				"_function_name": targetFunc,
				"AuthType":       authType,
			}
			if cors, ok := props["Cors"]; ok {
				params["Cors"] = cors
			}
			resp, err := funcP.CreateFunctionUrlConfig(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			funcURL, _ := resp.Data["FunctionUrl"].(string)
			return funcURL, map[string]any{"FunctionUrl": funcURL}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			targetFunc := propStr(props, "TargetFunctionArn", "")
			_, err := funcP.DeleteFunctionUrlConfig(ctx, &model.NormalizedRequest{
				Params: map[string]any{"_function_name": targetFunc},
			})
			return err
		},
	}
}
