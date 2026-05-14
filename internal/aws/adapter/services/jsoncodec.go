package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// GenericJSONTargetCodec handles any JSON/Target service where the only
// per-service difference is the X-Amz-Target prefix and SigV4 service name.
// Content-Type is always application/x-amz-json-1.1.
type GenericJSONTargetCodec struct {
	Service      string // SigV4 service name (e.g. "cognito-idp")
	TargetPrefix string // e.g. "AWSCognitoIdentityProviderService."
}

var _ adapter.Codec = (*GenericJSONTargetCodec)(nil)

func (c *GenericJSONTargetCodec) ServiceName() string { return c.Service }

func (c *GenericJSONTargetCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, c.TargetPrefix)
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for %s: %q", c.Service, target)
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("invalid JSON body: %w", err)
		}
	} else {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		Service: c.Service,
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *GenericJSONTargetCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *GenericJSONTargetCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(map[string]any{
		"__type":  perr.Code,
		"Message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
