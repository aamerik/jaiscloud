// Types moved to internal/model to avoid import cycles.
package gateway

import "jaiscloud/internal/model"

type ProviderError = model.ProviderError

func NewProviderError(code, message string, httpStatus int) *ProviderError {
	return model.NewProviderError(code, message, httpStatus)
}
