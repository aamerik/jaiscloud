package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// EKSCodec handles EKS REST/JSON wire protocol.
// SigV4 service name: eks
// Endpoint is empty — all requests go to the local emulator.
type EKSCodec struct{}

var _ adapter.Codec = (*EKSCodec)(nil)

func (c *EKSCodec) ServiceName() string { return "eks" }

func (c *EKSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action, params := eksActionFromRequest(r)
	if action == "" {
		return nil, model.NewProviderError("InvalidRequest",
			fmt.Sprintf("unrecognised EKS request: %s %s", r.Method, r.URL.Path), 400)
	}
	if params == nil {
		params = map[string]any{}
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
	return &model.NormalizedRequest{
		Service: "eks",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

// eksActionFromRequest maps EKS REST method+path to action + initial params.
//
// Supported path shapes:
//
//	/clusters
//	/clusters/{name}
//	/clusters/{name}/node-groups
//	/clusters/{name}/node-groups/{ng}
//	/clusters/{name}/addons
//	/clusters/{name}/addons/{addon}
//	/clusters/{name}/fargate-profiles
//	/clusters/{name}/fargate-profiles/{profile}
//	/clusters/{name}/access-entries
//	/clusters/{name}/access-entries/{principal}
//	/clusters/{name}/updates
//	/clusters/{name}/updates/{update}
func eksActionFromRequest(r *http.Request) (string, map[string]any) {
	method := r.Method
	// Strip leading slash and split.
	segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// segments[0] must be "clusters"
	if len(segments) == 0 || segments[0] != "clusters" {
		return "", nil
	}
	// /clusters
	if len(segments) == 1 {
		switch method {
		case http.MethodPost:
			return "CreateCluster", nil
		case http.MethodGet:
			return "ListClusters", nil
		}
		return "", nil
	}
	clusterName := segments[1]
	params := map[string]any{"name": clusterName, "clusterName": clusterName}
	// /clusters/{name}
	if len(segments) == 2 {
		switch method {
		case http.MethodGet:
			return "DescribeCluster", params
		case http.MethodDelete:
			return "DeleteCluster", params
		case http.MethodPost:
			return "UpdateClusterVersion", params
		}
		return "", nil
	}
	sub := segments[2]
	// /clusters/{name}/{sub}
	if len(segments) == 3 {
		switch sub {
		case "node-groups":
			switch method {
			case http.MethodPost:
				return "CreateNodegroup", params
			case http.MethodGet:
				return "ListNodegroups", params
			}
		case "addons":
			switch method {
			case http.MethodPost:
				return "CreateAddon", params
			case http.MethodGet:
				return "ListAddons", params
			}
		case "fargate-profiles":
			switch method {
			case http.MethodPost:
				return "CreateFargateProfile", params
			case http.MethodGet:
				return "ListFargateProfiles", params
			}
		case "access-entries":
			switch method {
			case http.MethodPost:
				return "CreateAccessEntry", params
			case http.MethodGet:
				return "ListAccessEntries", params
			}
		case "updates":
			if method == http.MethodGet {
				return "ListUpdates", params
			}
		}
		return "", nil
	}
	subName := segments[3]
	// /clusters/{name}/{sub}/{subName}
	if len(segments) == 4 {
		switch sub {
		case "node-groups":
			params["nodegroupName"] = subName
			switch method {
			case http.MethodGet:
				return "DescribeNodegroup", params
			case http.MethodDelete:
				return "DeleteNodegroup", params
			case http.MethodPost:
				return "UpdateNodegroupVersion", params
			case http.MethodPatch:
				return "UpdateNodegroupConfig", params
			}
		case "addons":
			params["addonName"] = subName
			switch method {
			case http.MethodGet:
				return "DescribeAddon", params
			case http.MethodDelete:
				return "DeleteAddon", params
			case http.MethodPost:
				return "UpdateAddon", params
			}
		case "fargate-profiles":
			params["fargateProfileName"] = subName
			switch method {
			case http.MethodGet:
				return "DescribeFargateProfile", params
			case http.MethodDelete:
				return "DeleteFargateProfile", params
			}
		case "access-entries":
			params["principalArn"] = subName
			switch method {
			case http.MethodGet:
				return "DescribeAccessEntry", params
			case http.MethodDelete:
				return "DeleteAccessEntry", params
			}
		case "updates":
			params["updateId"] = subName
			if method == http.MethodGet {
				return "DescribeUpdate", params
			}
		}
	}
	return "", nil
}

func (c *EKSCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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

func (c *EKSCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(map[string]any{
		"message": perr.Message,
	})
	return perr.HTTPStatus, h, body
}
