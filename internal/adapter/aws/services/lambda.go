package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// LambdaCodec handles the Lambda REST-JSON wire protocol.
// Paths follow the pattern /2015-03-31/functions[/{name}[/invocations|/configuration|/code]]
type LambdaCodec struct{}

func (c *LambdaCodec) ServiceName() string { return "lambda" }

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *LambdaCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action, functionName := lambdaDetectAction(r.Method, r.URL.Path)

	params := map[string]any{
		"_function_name": functionName,
		"_body":          body,
	}
	// Parse JSON body into params (for CreateFunction, UpdateFunctionConfiguration, etc.)
	if len(body) > 0 {
		var decoded map[string]any
		if json.Unmarshal(body, &decoded) == nil {
			for k, v := range decoded {
				params[k] = v
			}
		}
	}
	// InvokeFunction: also store raw body as payload
	if action == "InvokeFunction" {
		params["_payload"] = body
		// X-Amz-Invocation-Type header (RequestResponse, Event, DryRun)
		if it := r.Header.Get("X-Amz-Invocation-Type"); it != "" {
			params["_invocation_type"] = it
		}
	}

	return &model.NormalizedRequest{
		Service: "lambda",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func lambdaDetectAction(method, path string) (action, functionName string) {
	// Strip version prefix: /2015-03-31/functions[/...]
	path = strings.TrimPrefix(path, "/2015-03-31/")
	segments := strings.SplitN(path, "/", 3)

	if len(segments) == 0 || segments[0] != "functions" {
		return "Unknown", ""
	}

	if len(segments) == 1 || segments[1] == "" {
		switch method {
		case http.MethodGet:
			return "ListFunctions", ""
		case http.MethodPost:
			return "CreateFunction", ""
		}
		return "Unknown", ""
	}

	name := segments[1]

	if len(segments) == 2 || segments[2] == "" {
		switch method {
		case http.MethodGet:
			return "GetFunction", name
		case http.MethodDelete:
			return "DeleteFunction", name
		case http.MethodPut:
			return "UpdateFunctionConfiguration", name
		}
		return "Unknown", name
	}

	switch segments[2] {
	case "invocations":
		if method == http.MethodPost {
			return "InvokeFunction", name
		}
	case "configuration":
		switch method {
		case http.MethodGet:
			return "GetFunctionConfiguration", name
		case http.MethodPut:
			return "UpdateFunctionConfiguration", name
		}
	case "code":
		if method == http.MethodPut {
			return "UpdateFunctionCode", name
		}
	}
	return "Unknown", name
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *LambdaCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}

	// InvokeFunction: body is raw payload bytes, not JSON
	if nr.Action == "InvokeFunction" {
		h.Set("X-Amz-Executed-Version", "$LATEST")
		if ferr, ok := resp.Data["_function_error"].(string); ok && ferr != "" {
			h.Set("X-Amz-Function-Error", ferr)
		}
		payload, _ := resp.Data["_payload"].([]byte)
		if len(payload) > 0 {
			h.Set("Content-Type", "application/json")
		}
		return resp.HTTPStatus, h, payload
	}

	// No-body responses (DeleteFunction → 204)
	if resp.HTTPStatus == 204 {
		return 204, h, nil
	}

	h.Set("Content-Type", "application/json")
	b, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, b
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *LambdaCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body := fmt.Sprintf(`{"message":%q,"__type":%q}`, perr.Message, perr.Code)
	return perr.HTTPStatus, h, []byte(body)
}
