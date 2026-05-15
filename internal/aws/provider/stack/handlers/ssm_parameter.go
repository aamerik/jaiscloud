package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	paramprovider "jaiscloud/internal/aws/parameter"
	"jaiscloud/internal/model"
)

// NewSSMParameterHandler returns a ResourceHandler for AWS::SSM::Parameter.
func NewSSMParameterHandler(paramP *paramprovider.ParameterProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", "/cfn/"+logicalID)
			params := copyProps(props)
			params["Name"] = name
			if _, err := paramP.PutParameter(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			return name, map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := paramP.DeleteParameter(ctx, &model.NormalizedRequest{Params: map[string]any{"Name": physicalID}})
			return err
		},
	}
}
