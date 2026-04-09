package adapter

import (
	"net/http"

	"jaiscloud/internal/model"
)

// Codec handles encode/decode for one AWS service (e.g. SQS).
type Codec interface {
	ServiceName() string
	Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error)
	Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (status int, headers http.Header, body []byte)
	EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (status int, headers http.Header, body []byte)
}
