package services

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// LambdaCodec handles the Lambda REST-JSON wire protocol.
// Paths follow /2015-03-31/{resource} conventions.
type LambdaCodec struct{}

func (c *LambdaCodec) ServiceName() string { return "lambda" }

// ─── Decode ───────────────────────────────────────────────────────────────────

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
	action, pathParams := lambdaDetectAction(r.Method, r.URL.Path)

	params := map[string]any{"_body": body}
	for k, v := range pathParams {
		params[k] = v
	}

	if len(body) > 0 {
		var decoded map[string]any
		if json.Unmarshal(body, &decoded) == nil {
			for k, v := range decoded {
				params[k] = v
			}
		}
	}

	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	if action == "InvokeFunction" {
		params["_payload"] = body
		if it := r.Header.Get("X-Amz-Invocation-Type"); it != "" {
			params["_invocation_type"] = it
		}
		if lt := r.Header.Get("X-Amz-Log-Type"); lt != "" {
			params["_log_type"] = lt
		}
	}

	return &model.NormalizedRequest{
		Service: "lambda",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func lambdaDetectAction(method, path string) (action string, params map[string]string) {
	params = map[string]string{}
	for _, prefix := range []string{"/2015-03-31/", "/2016-08-19/", "/2017-03-31/", "/2017-10-31/", "/2018-10-31/", "/2019-09-25/", "/2019-09-30/"} {
		if strings.HasPrefix(path, prefix) {
			path = path[len(prefix):]
			break
		}
	}

	// event-source-mappings handled before generic split
	if strings.HasPrefix(path, "event-source-mappings") {
		rest := strings.TrimPrefix(path, "event-source-mappings")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			switch method {
			case http.MethodGet:
				return "ListEventSourceMappings", params
			case http.MethodPost:
				return "CreateEventSourceMapping", params
			}
			return "Unknown", params
		}
		uuid := strings.SplitN(rest, "/", 2)[0]
		params["_esm_uuid"] = uuid
		switch method {
		case http.MethodGet:
			return "GetEventSourceMapping", params
		case http.MethodPut:
			return "UpdateEventSourceMapping", params
		case http.MethodDelete:
			return "DeleteEventSourceMapping", params
		}
		return "Unknown", params
	}

	segs := strings.Split(strings.Trim(path, "/"), "/")
	if len(segs) == 0 || segs[0] == "" {
		return "Unknown", params
	}

	switch segs[0] {
	case "account-settings":
		return "GetAccountSettings", params
	case "tags":
		if len(segs) > 1 {
			params["_resource_arn"] = segs[1]
		}
		switch method {
		case http.MethodGet:
			return "ListTags", params
		case http.MethodPost:
			return "TagResource", params
		case http.MethodDelete:
			return "UntagResource", params
		}
	case "layers":
		return lambdaLayerDetect(method, segs[1:], params)
	case "functions":
		return lambdaFunctionDetect(method, segs[1:], params)
	}
	return "Unknown", params
}

func lambdaFunctionDetect(method string, segs []string, params map[string]string) (string, map[string]string) {
	if len(segs) == 0 || segs[0] == "" {
		switch method {
		case http.MethodGet:
			return "ListFunctions", params
		case http.MethodPost:
			return "CreateFunction", params
		}
		return "Unknown", params
	}

	name := segs[0]
	params["_function_name"] = name

	if len(segs) == 1 {
		switch method {
		case http.MethodGet:
			return "GetFunction", params
		case http.MethodDelete:
			return "DeleteFunction", params
		case http.MethodPut:
			return "UpdateFunctionConfiguration", params
		}
		return "Unknown", params
	}

	sub := segs[1]
	rest := segs[2:]

	switch sub {
	case "invocations":
		if method == http.MethodPost {
			return "InvokeFunction", params
		}
	case "configuration":
		switch method {
		case http.MethodGet:
			return "GetFunctionConfiguration", params
		case http.MethodPut:
			return "UpdateFunctionConfiguration", params
		}
	case "code":
		if method == http.MethodPut {
			return "UpdateFunctionCode", params
		}
	case "versions":
		switch method {
		case http.MethodPost:
			return "PublishVersion", params
		case http.MethodGet:
			return "ListVersionsByFunction", params
		}
	case "aliases":
		if len(rest) == 0 {
			switch method {
			case http.MethodPost:
				return "CreateAlias", params
			case http.MethodGet:
				return "ListAliases", params
			}
		} else {
			params["_alias_name"] = rest[0]
			switch method {
			case http.MethodGet:
				return "GetAlias", params
			case http.MethodPut:
				return "UpdateAlias", params
			case http.MethodDelete:
				return "DeleteAlias", params
			}
		}
	case "policy":
		if len(rest) == 0 {
			switch method {
			case http.MethodPost:
				return "AddPermission", params
			case http.MethodGet:
				return "GetPolicy", params
			}
		} else {
			params["_statement_id"] = rest[0]
			if method == http.MethodDelete {
				return "RemovePermission", params
			}
		}
	case "url":
		switch method {
		case http.MethodPost:
			return "CreateFunctionUrlConfig", params
		case http.MethodGet:
			return "GetFunctionUrlConfig", params
		case http.MethodPut:
			return "UpdateFunctionUrlConfig", params
		case http.MethodDelete:
			return "DeleteFunctionUrlConfig", params
		}
	case "concurrency":
		switch method {
		case http.MethodPut:
			return "PutFunctionConcurrency", params
		case http.MethodGet:
			return "GetFunctionConcurrency", params
		case http.MethodDelete:
			return "DeleteFunctionConcurrency", params
		}
	case "event-invoke-config":
		if len(rest) == 0 {
			switch method {
			case http.MethodPut:
				return "PutFunctionEventInvokeConfig", params
			case http.MethodPost:
				return "UpdateFunctionEventInvokeConfig", params
			case http.MethodGet:
				return "GetFunctionEventInvokeConfig", params
			case http.MethodDelete:
				return "DeleteFunctionEventInvokeConfig", params
			}
		}
	}
	return "Unknown", params
}

func lambdaLayerDetect(method string, segs []string, params map[string]string) (string, map[string]string) {
	if len(segs) == 0 || segs[0] == "" {
		if method == http.MethodGet {
			return "ListLayers", params
		}
		return "Unknown", params
	}
	params["_layer_name"] = segs[0]
	if len(segs) < 2 || segs[1] != "versions" {
		return "Unknown", params
	}
	if len(segs) == 2 {
		switch method {
		case http.MethodPost:
			return "PublishLayerVersion", params
		case http.MethodGet:
			return "ListLayerVersions", params
		}
		return "Unknown", params
	}
	params["_layer_version"] = segs[2]
	switch method {
	case http.MethodGet:
		return "GetLayerVersion", params
	case http.MethodDelete:
		return "DeleteLayerVersion", params
	}
	return "Unknown", params
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *LambdaCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}

	if nr.Action == "InvokeFunction" {
		h.Set("X-Amz-Executed-Version", "$LATEST")
		if ferr, ok := resp.Data["_function_error"].(string); ok && ferr != "" {
			h.Set("X-Amz-Function-Error", ferr)
		}
		if logResult, ok := resp.Data["LogResult"].(string); ok && logResult != "" {
			h.Set("X-Amz-Log-Result", logResult)
		}
		payload, _ := resp.Data["_payload"].([]byte)
		if len(payload) > 0 {
			h.Set("Content-Type", "application/json")
		}
		return resp.HTTPStatus, h, payload
	}

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
