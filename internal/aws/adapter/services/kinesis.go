package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// KinesisCodec handles the Kinesis JSON/Target wire format.
// Protocol: JSON with X-Amz-Target: Kinesis_20131202.<Action>
type KinesisCodec struct{}

var _ adapter.Codec = (*KinesisCodec)(nil)

func (c *KinesisCodec) ServiceName() string { return "kinesis" }

func (c *KinesisCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "Kinesis_20131202.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for Kinesis: %q", target)
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
		Service: "kinesis",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *KinesisCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *KinesisCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(map[string]any{
		"__type":  perr.Code,
		"Message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
