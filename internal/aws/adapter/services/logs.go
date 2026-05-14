package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// LogsCodec handles the CloudWatch Logs JSON/Target wire protocol.
// X-Amz-Target: Logs_20140328.<Action>
type LogsCodec struct{}

var _ adapter.Codec = (*LogsCodec)(nil)

func (c *LogsCodec) ServiceName() string { return "logs" }

func (c *LogsCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "Logs_20140328.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for logs: %q", target)
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("logs: invalid JSON body: %w", err)
		}
	} else {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		Service: "logs",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *LogsCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *LogsCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	out := map[string]any{
		"__type":  perr.Code,
		"Message": perr.Message,
	}
	body, _ := json.Marshal(out)
	return perr.HTTPStatus, h, body
}
