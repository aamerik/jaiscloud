// Types moved to internal/model to avoid import cycles.
// Re-exported here for backwards-compatibility within the gateway package.
package gateway

import "jaiscloud/internal/model"

type NormalizedRequest = model.NormalizedRequest
type ProviderResponse = model.ProviderResponse
