package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// KMSCodec handles the KMS JSON/Target wire protocol.
// X-Amz-Target: TrentService.<Action>
type KMSCodec struct{}

var _ adapter.Codec = (*KMSCodec)(nil)

func (c *KMSCodec) ServiceName() string { return "kms" }

func (c *KMSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "TrentService.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for KMS: %q", target)
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("kms: invalid JSON body: %w", err)
		}
	} else {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		Service: "kms",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *KMSCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *KMSCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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
