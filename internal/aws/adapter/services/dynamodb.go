package services

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// DynamoDBCodec handles DynamoDB wire format.
// Protocol: JSON with X-Amz-Target: DynamoDB_20120810.<Action>
// Both request and response bodies are JSON.
type DynamoDBCodec struct{}

var _ adapter.Codec = (*DynamoDBCodec)(nil)

func (c *DynamoDBCodec) ServiceName() string { return "dynamodb" }

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *DynamoDBCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	target := r.Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, "DynamoDB_20120810.")
	if action == "" || action == target {
		return nil, fmt.Errorf("missing or invalid X-Amz-Target for DynamoDB: %q", target)
	}

	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("invalid JSON body: %w", err)
		}
	} else {
		params = map[string]any{}
	}

	nr := &model.NormalizedRequest{
		Service: "dynamodb",
		Action:  action,
		Params:  params,
		Raw:     r,
	}
	return nr, nil
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *DynamoDBCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.0")
	b, _ := json.Marshal(resp.Data)
	h.Set("x-amz-crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(b)), 10))
	return resp.HTTPStatus, h, b
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *DynamoDBCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	code := dynamoErrorCodeMap[perr.Code]
	if code == "" {
		code = perr.Code
	}
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.0")
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

var dynamoErrorCodeMap = map[string]string{
	"NotFound":             "ResourceNotFoundException",
	"AlreadyExists":        "ResourceInUseException",
	"ValidationException":  "ValidationException",
}
