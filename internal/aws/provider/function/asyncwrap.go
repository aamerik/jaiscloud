package function

import (
	"context"

	"jaiscloud/internal/model"
)

// InvokeAsync wraps InvokeFunction with an Event (async) invocation type and
// always returns 202. The AWS SDK's InvokeAsync action is deprecated but still
// used by some SDKs.
func (p *FunctionProvider) InvokeAsync(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Force async invocation type so InvokeFunction returns 202.
	if nr.Params == nil {
		nr.Params = map[string]any{}
	}
	// Map FunctionName from path params if present.
	if fn, ok := nr.Params["FunctionName"].(string); ok && fn != "" {
		nr.Params["_function_name"] = fn
	}
	nr.Params["_invocation_type"] = "Event"
	_, err := p.InvokeFunction(ctx, nr)
	if err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{"StatusCode": 202}}, nil
}
