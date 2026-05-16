package services

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// ExecuteAPICodec handles the API Gateway execute-api invoke plane.
// SigV4 service name: execute-api
//
// The SDK calls execute-api endpoints with URL paths like:
//
//	/{stage}/{resource+}
//
// where the Host header identifies the API ID:
//
//	{apiId}.execute-api.{region}.amazonaws.com
//
// In JaisCloud the request reaches this codec via the standard SigV4 service
// detection — no special host routing is needed. The apiId is extracted from
// the first path segment if the Host header is absent (local testing scenario)
// or from the Host header subdomain.
//
// Action is always "Invoke" — the GatewayProvider.Invoke handler then performs
// the resource/method lookup.
type ExecuteAPICodec struct{}

func (c *ExecuteAPICodec) ServiceName() string { return "execute-api" }

func (c *ExecuteAPICodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	params := map[string]any{
		"_body":       body,
		"_httpMethod": r.Method,
	}

	// Extract apiId from Host header (e.g. "abc123.execute-api.us-east-1.amazonaws.com").
	apiID := ""
	host := r.Host
	if idx := strings.Index(host, ".execute-api."); idx > 0 {
		apiID = host[:idx]
	}

	// Path: /{stage}/{resource+}
	// Strip leading slash and split.
	rawPath := strings.TrimPrefix(r.URL.Path, "/")
	pathParts := strings.SplitN(rawPath, "/", 2)

	stageName := ""
	resourcePath := "/"
	if len(pathParts) >= 1 && pathParts[0] != "" {
		stageName = pathParts[0]
	}
	if len(pathParts) >= 2 {
		resourcePath = "/" + pathParts[1]
	}

	// Fall back: if no apiId from Host, try first path segment as apiId when
	// path looks like /{apiId}/{stage}/{resource+} (standard AWS API Gateway path routing).
	if apiID == "" && len(pathParts) >= 1 {
		// Heuristic: treat first segment as apiId only if it is short (≤12 chars)
		// and second+ segments are present.  Otherwise treat first as stage.
		if len(pathParts[0]) <= 12 && len(pathParts) >= 2 {
			apiID = pathParts[0]
			subParts := strings.SplitN(pathParts[1], "/", 2)
			if len(subParts) >= 1 {
				stageName = subParts[0]
			}
			if len(subParts) >= 2 {
				resourcePath = "/" + subParts[1]
			} else {
				resourcePath = "/"
			}
		}
	}

	params["_apiId"] = apiID
	params["_stageName"] = stageName
	params["_resourcePath"] = resourcePath

	// Forward query string and headers as params.
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params["_query_"+k] = vs[0]
		}
	}

	return &model.NormalizedRequest{
		Service: "execute-api",
		Action:  "Invoke",
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *ExecuteAPICodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	// If the upstream integration returned raw bytes, pass them through.
	if raw, ok := resp.Data["_raw"].([]byte); ok {
		if ct, ok := resp.Data["_contentType"].(string); ok {
			h.Set("Content-Type", ct)
		}
		return resp.HTTPStatus, h, raw
	}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *ExecuteAPICodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(map[string]any{
		"message": perr.Message,
		"code":    perr.Code,
	})
	return perr.HTTPStatus, h, body
}
