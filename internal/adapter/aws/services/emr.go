package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// EMRCodec handles the EMR JSON/Target wire protocol.
// X-Amz-Target: ElasticMapReduce.<Action>
type EMRCodec struct{}

var _ adapter.Codec = (*EMRCodec)(nil)

func (c *EMRCodec) ServiceName() string { return "emr" }

func (c *EMRCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "ElasticMapReduce.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for EMR: %q", target)
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
		Service: "emr",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *EMRCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *EMRCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(map[string]any{
		"__type":  perr.Code,
		"message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
