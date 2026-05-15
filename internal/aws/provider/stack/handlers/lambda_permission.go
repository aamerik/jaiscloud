package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	functionprovider "jaiscloud/internal/aws/provider/function"
	"jaiscloud/internal/model"
)

// NewLambdaPermissionHandler returns a ResourceHandler for AWS::Lambda::Permission.
func NewLambdaPermissionHandler(funcP *functionprovider.FunctionProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			funcName := propStr(props, "FunctionName", "")
			stmtID := propStr(props, "StatementId", logicalID)
			params := copyProps(props)
			params["_function_name"] = funcName
			params["StatementId"] = stmtID
			if _, err := funcP.AddPermission(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			return funcName + "/policy/" + stmtID, map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			funcName := propStr(props, "FunctionName", "")
			stmtID := propStr(props, "StatementId", "")
			_, err := funcP.RemovePermission(ctx, &model.NormalizedRequest{
				Params: map[string]any{"_function_name": funcName, "_statement_id": stmtID},
			})
			return err
		},
	}
}
