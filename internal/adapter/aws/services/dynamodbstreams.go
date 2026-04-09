package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// DynamoDBStreamsCodec handles the DynamoDB Streams JSON/Target wire protocol.
// X-Amz-Target: DynamoDBStreams_20120810.<Action>
type DynamoDBStreamsCodec struct{}

var _ adapter.Codec = (*DynamoDBStreamsCodec)(nil)

func (c *DynamoDBStreamsCodec) ServiceName() string { return "dynamodbstreams" }

func (c *DynamoDBStreamsCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "DynamoDBStreams_20120810.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for DynamoDB Streams: %q", target)
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
		Service: "dynamodbstreams",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *DynamoDBStreamsCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.0")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *DynamoDBStreamsCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.0")
	body, _ := json.Marshal(map[string]any{
		"__type":  perr.Code,
		"message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
