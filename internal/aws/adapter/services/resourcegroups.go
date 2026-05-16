package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// ResourceGroupsCodec handles the AWS Resource Groups REST/JSON wire protocol.
// SigV4 service name: resource-groups
type ResourceGroupsCodec struct{}

var _ adapter.Codec = (*ResourceGroupsCodec)(nil)

func (c *ResourceGroupsCodec) ServiceName() string { return "resource-groups" }

func (c *ResourceGroupsCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action, pathParams := resourceGroupsActionFromRequest(r)
	if action == "" {
		return nil, model.NewProviderError("InvalidRequest",
			fmt.Sprintf("unrecognised Resource Groups request: %s %s", r.Method, r.URL.Path), 400)
	}

	params := make(map[string]any)
	for k, v := range pathParams {
		params[k] = v
	}

	if len(body) > 0 {
		var bodyParams map[string]any
		if err := json.Unmarshal(body, &bodyParams); err != nil {
			return nil, fmt.Errorf("invalid JSON body: %w", err)
		}
		for k, v := range bodyParams {
			params[k] = v
		}
	}

	// Include query params
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	return &model.NormalizedRequest{
		Service: "resource-groups",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

// resourceGroupsActionFromRequest maps Resource Groups REST method+path to action + params.
//
// Supported path shapes:
//
//	POST   /groups                  → CreateGroup
//	DELETE /groups/{GroupName}       → DeleteGroup
//	GET    /groups/{GroupName}       → GetGroup
//	POST   /groups-list              → ListGroups
//	PUT    /groups/{GroupName}       → UpdateGroup
//	PUT    /resources/{Arn}/tags     → Tag
//	DELETE /resources/{Arn}/tags     → Untag
//	GET    /resources/{Arn}/tags     → GetTags
func resourceGroupsActionFromRequest(r *http.Request) (string, map[string]any) {
	method := r.Method
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.SplitN(path, "/", 3)

	if len(segments) == 0 {
		return "", nil
	}

	switch segments[0] {
	case "groups":
		if len(segments) == 1 {
			if method == http.MethodPost {
				return "CreateGroup", nil
			}
		} else if len(segments) == 2 {
			groupName := segments[1]
			params := map[string]any{"Group": groupName}
			switch method {
			case http.MethodDelete:
				return "DeleteGroup", params
			case http.MethodGet:
				return "GetGroup", params
			case http.MethodPut:
				return "UpdateGroup", params
			}
		}
	case "groups-list":
		if method == http.MethodPost {
			return "ListGroups", nil
		}
	case "resources":
		// /resources/{Arn}/tags
		if len(segments) >= 3 && segments[2] == "tags" {
			arn := segments[1]
			params := map[string]any{"Arn": arn}
			switch method {
			case http.MethodPut:
				return "Tag", params
			case http.MethodDelete:
				return "Untag", params
			case http.MethodGet:
				return "GetTags", params
			}
		}
	}

	return "", nil
}

func (c *ResourceGroupsCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	var body []byte
	if resp.Data != nil {
		body, _ = json.Marshal(resp.Data)
	} else {
		body = []byte("{}")
	}
	return resp.HTTPStatus, h, body
}

func (c *ResourceGroupsCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(map[string]any{
		"Message": perr.Message,
		"Code":    perr.Code,
	})
	return perr.HTTPStatus, h, body
}
