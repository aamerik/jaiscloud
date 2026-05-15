package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	functionprovider "jaiscloud/internal/aws/provider/function"
	"jaiscloud/internal/model"
)

// NewLambdaVersionHandler returns a ResourceHandler for AWS::Lambda::Version.
func NewLambdaVersionHandler(funcP *functionprovider.FunctionProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			funcName := propStr(props, "FunctionName", "")
			desc := propStr(props, "Description", "")
			resp, err := funcP.PublishVersion(ctx, child(nr, map[string]any{
				"_function_name": funcName,
				"Description":    desc,
			}))
			if err != nil {
				return "", nil, err
			}
			versionedArn, _ := resp.Data["FunctionArn"].(string)
			return versionedArn, map[string]any{"FunctionArn": versionedArn}, nil
		},
		// Lambda versions cannot be deleted directly via CloudFormation; no-op.
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			return nil
		},
	}
}
