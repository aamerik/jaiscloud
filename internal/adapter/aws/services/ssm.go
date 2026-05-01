package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// SSMCodec handles the SSM Parameter Store JSON/Target wire protocol.
// X-Amz-Target: AmazonSSM.<Action>
type SSMCodec struct{}

var _ adapter.Codec = (*SSMCodec)(nil)

func (c *SSMCodec) ServiceName() string { return "ssm" }

func (c *SSMCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "AmazonSSM.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for SSM: %q", target)
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("ssm: invalid JSON body: %w", err)
		}
	} else {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		Service: "ssm",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *SSMCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *SSMCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	out := map[string]any{
		"__type":  perr.Code,
		"message": perr.Message,
	}
	for k, v := range perr.Data {
		out[k] = v
	}
	body, _ := json.Marshal(out)
	return perr.HTTPStatus, h, body
}
