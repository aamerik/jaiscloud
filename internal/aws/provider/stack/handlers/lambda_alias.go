package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	functionprovider "jaiscloud/internal/aws/provider/function"
	"jaiscloud/internal/model"
)

// NewLambdaAliasHandler returns a ResourceHandler for AWS::Lambda::Alias.
func NewLambdaAliasHandler(funcP *functionprovider.FunctionProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			funcName := propStr(props, "FunctionName", "")
			aliasName := propStr(props, "Name", logicalID)
			funcVersion := propStr(props, "FunctionVersion", "$LATEST")
			resp, err := funcP.CreateAlias(ctx, child(nr, map[string]any{
				"_function_name":  funcName,
				"Name":            aliasName,
				"FunctionVersion": funcVersion,
				"Description":     propStr(props, "Description", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["AliasArn"].(string)
			return arn, map[string]any{"AliasArn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			funcName := propStr(props, "FunctionName", "")
			aliasName := propStr(props, "Name", "")
			_, err := funcP.DeleteAlias(ctx, &model.NormalizedRequest{
				Params: map[string]any{"_function_name": funcName, "_alias_name": aliasName},
			})
			return err
		},
	}
}
