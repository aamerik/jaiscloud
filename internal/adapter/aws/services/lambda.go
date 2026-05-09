package services

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// LambdaCodec handles the Lambda REST-JSON wire protocol.
// Paths follow the pattern /2015-03-31/functions[/{name}[/invocations|/configuration|/code]]
type LambdaCodec struct{}

func (c *LambdaCodec) ServiceName() string { return "lambda" }

// ─── Decode ───────────────────────────────────────────────────────────────────

// isESMAction returns true for actions that operate on event-source-mappings.
func isESMAction(action string) bool {
	switch action {
	case "CreateEventSourceMapping", "GetEventSourceMapping",
		"ListEventSourceMappings", "UpdateEventSourceMapping",
		"DeleteEventSourceMapping":
		return true
	}
	return false
}

func (c *LambdaCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action, resourceID := lambdaDetectAction(r.Method, r.URL.Path)

	params := map[string]any{
		"_body": body,
	}

	// For ESM actions, store the resource ID as _esm_uuid rather than _function_name
	// to avoid confusion between function names and ESM UUIDs.
	if isESMAction(action) {
		if resourceID != "" {
			params["_esm_uuid"] = resourceID
		}
	} else {
		params["_function_name"] = resourceID
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

func lambdaDetectAction(method, path string) (action, resourceID string) {
	// Strip version prefix: /2015-03-31/...
	path = strings.TrimPrefix(path, "/2015-03-31/")

	// Handle event-source-mappings FIRST (before functions check)
	// /event-source-mappings                  GET=List, POST=Create
	// /event-source-mappings/{uuid}           GET=Get, PUT=Update, DELETE=Delete
	if strings.HasPrefix(path, "event-source-mappings") {
		rest := strings.TrimPrefix(path, "event-source-mappings")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			switch method {
			case http.MethodGet:
				return "ListEventSourceMappings", ""
			case http.MethodPost:
				return "CreateEventSourceMapping", ""
			}
			return "Unknown", ""
		}
		// rest is the UUID
		uuid := strings.SplitN(rest, "/", 2)[0]
		switch method {
		case http.MethodGet:
			return "GetEventSourceMapping", uuid
		case http.MethodPut:
			return "UpdateEventSourceMapping", uuid
		case http.MethodDelete:
			return "DeleteEventSourceMapping", uuid
		}
		return "Unknown", uuid
	}

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
	out := map[string]any{
		"message": perr.Message,
		"__type":  perr.Code,
	}
	for k, v := range perr.Data {
		out[k] = v
	}
	body, _ := json.Marshal(out)
	return perr.HTTPStatus, h, body
}
