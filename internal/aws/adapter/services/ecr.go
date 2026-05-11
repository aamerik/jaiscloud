package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// ECRCodec handles the ECR JSON/Target wire format.
// Protocol: JSON with X-Amz-Target: AmazonEC2ContainerRegistry_V20150921.<Action>
type ECRCodec struct{}

var _ adapter.Codec = (*ECRCodec)(nil)

func (c *ECRCodec) ServiceName() string { return "ecr" }

func (c *ECRCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "AmazonEC2ContainerRegistry_V20150921.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for ECR: %q", target)
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
		Service: "ecr",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *ECRCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *ECRCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(map[string]any{
		"__type":  perr.Code,
		"message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
