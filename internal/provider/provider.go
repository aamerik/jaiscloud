package provider

import (
	"context"
	"jaiscloud/internal/model"
)

// HandlerFunc is the signature every provider operation must implement.
type HandlerFunc func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

// OK is a convenience constructor for a successful 200 response.
func OK(data map[string]any) *model.ProviderResponse {
	return &model.ProviderResponse{HTTPStatus: 200, Data: data}
}
