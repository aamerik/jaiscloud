package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// GlueCodec handles AWS Glue wire format.
// Protocol: JSON with X-Amz-Target: AWSGlue.<Action>
type GlueCodec struct{}

var _ adapter.Codec = (*GlueCodec)(nil)

func (c *GlueCodec) ServiceName() string { return "glue" }

func (c *GlueCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "AWSGlue.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for Glue: %q", target)
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
		Service: "glue",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *GlueCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	b, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, b
}

func (c *GlueCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	code := glueErrorCodeMap[perr.Code]
	if code == "" {
		code = perr.Code
	}
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	out := map[string]any{
		"__type":  code,
		"Message": perr.Message,
	}
	for k, v := range perr.Data {
		out[k] = v
	}
	b, _ := json.Marshal(out)
	return perr.HTTPStatus, h, b
}

var glueErrorCodeMap = map[string]string{
	"NotFound":      "EntityNotFoundException",
	"AlreadyExists": "AlreadyExistsException",
	"InvalidInput":  "InvalidInputException",
}
