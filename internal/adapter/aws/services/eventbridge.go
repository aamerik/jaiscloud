package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// EventBridgeCodec handles the EventBridge (CloudWatch Events) JSON/Target wire protocol.
// X-Amz-Target: AmazonCloudWatchEvents.<Action>
type EventBridgeCodec struct{}

var _ adapter.Codec = (*EventBridgeCodec)(nil)

func (c *EventBridgeCodec) ServiceName() string { return "events" }

func (c *EventBridgeCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	// AWS SDK v2 eventbridge package uses "AWSEvents." prefix;
	// older cloudwatchevents package uses "AmazonCloudWatchEvents." — support both.
	action := strings.TrimPrefix(target, "AWSEvents.")
	if action == target {
		action = strings.TrimPrefix(target, "AmazonCloudWatchEvents.")
	}
	if action == "" || action == target {
		return nil, model.NewProviderError("InvalidRequest",
			"missing or invalid X-Amz-Target for events: "+target, 400)
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
		Service: "events",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *EventBridgeCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	var body []byte
	if resp.Data != nil {
		body, _ = json.Marshal(resp.Data)
	} else {
		body = []byte("{}")
	}
	return resp.HTTPStatus, h, body
}

func (c *EventBridgeCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.1")
	body, _ := json.Marshal(map[string]any{
		"__type":  perr.Code,
		"message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
