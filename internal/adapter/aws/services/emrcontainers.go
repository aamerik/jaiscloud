package services

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// EMRContainersCodec handles the EMR on EKS REST JSON wire format.
// SigV4 service name: emr-containers
//
// URL patterns:
//
//	POST   /virtualclusters                                       → CreateVirtualCluster
//	GET    /virtualclusters                                       → ListVirtualClusters
//	GET    /virtualclusters/{vcId}                                → DescribeVirtualCluster
//	DELETE /virtualclusters/{vcId}                                → DeleteVirtualCluster
//	POST   /virtualclusters/{vcId}/jobruns                       → StartJobRun
//	GET    /virtualclusters/{vcId}/jobruns                       → ListJobRuns
//	GET    /virtualclusters/{vcId}/jobruns/{runId}               → DescribeJobRun
//	DELETE /virtualclusters/{vcId}/jobruns/{runId}               → CancelJobRun
//	POST   /virtualclusters/{vcId}/endpoints                     → CreateManagedEndpoint
//	GET    /virtualclusters/{vcId}/endpoints                     → ListManagedEndpoints
//	GET    /virtualclusters/{vcId}/endpoints/{epId}              → DescribeManagedEndpoint
//	DELETE /virtualclusters/{vcId}/endpoints/{epId}              → DeleteManagedEndpoint
//	POST   /tags/{resourceArn}                                    → TagResource
//	GET    /tags/{resourceArn}                                    → ListTagsForResource
//	DELETE /tags/{resourceArn}                                    → UntagResource
type EMRContainersCodec struct{}

func (c *EMRContainersCodec) ServiceName() string { return "emr-containers" }

func (c *EMRContainersCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	// Strip leading slash and tokenise
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	params := map[string]any{}

	// Parse JSON body
	if len(body) > 0 {
		json.Unmarshal(body, &params)
	}

	// Propagate query params (list filters etc.)
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	action := emrcDetectAction(r.Method, parts, params)

	return &model.NormalizedRequest{
		Service: "emr-containers",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

// emrcDetectAction maps (method, path segments) → action name and injects path
// parameters into params as _path_<key>.
func emrcDetectAction(method string, parts []string, params map[string]any) string {
	// parts[0] is the first segment after the leading slash has been removed
	if len(parts) == 0 {
		return "Unknown"
	}

	switch parts[0] {
	case "virtualclusters":
		switch len(parts) {
		case 1:
			// /virtualclusters
			if method == http.MethodPost {
				return "CreateVirtualCluster"
			}
			return "ListVirtualClusters"

		case 2:
			// /virtualclusters/{vcId}
			params["_path_virtualClusterId"] = parts[1]
			if method == http.MethodDelete {
				return "DeleteVirtualCluster"
			}
			return "DescribeVirtualCluster"

		case 3:
			// /virtualclusters/{vcId}/jobruns  OR  /virtualclusters/{vcId}/endpoints
			params["_path_virtualClusterId"] = parts[1]
			switch parts[2] {
			case "jobruns":
				if method == http.MethodPost {
					return "StartJobRun"
				}
				return "ListJobRuns"
			case "endpoints":
				if method == http.MethodPost {
					return "CreateManagedEndpoint"
				}
				return "ListManagedEndpoints"
			}

		case 4:
			// /virtualclusters/{vcId}/jobruns/{runId}
			// /virtualclusters/{vcId}/endpoints/{epId}
			params["_path_virtualClusterId"] = parts[1]
			switch parts[2] {
			case "jobruns":
				params["_path_jobRunId"] = parts[3]
				if method == http.MethodDelete {
					return "CancelJobRun"
				}
				return "DescribeJobRun"
			case "endpoints":
				params["_path_endpointId"] = parts[3]
				if method == http.MethodDelete {
					return "DeleteManagedEndpoint"
				}
				return "DescribeManagedEndpoint"
			}
		}

	case "tags":
		// /tags/{resourceArn}  — ARN may contain slashes so rejoin
		if len(parts) >= 2 {
			params["_path_resourceArn"] = strings.Join(parts[1:], "/")
		}
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		default:
			return "ListTagsForResource"
		}
	}

	return "Unknown"
}

func (c *EMRContainersCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *EMRContainersCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	out := map[string]any{
		"message": perr.Message,
		"code":    perr.Code,
	}
	for k, v := range perr.Data {
		out[k] = v
	}
	body, _ := json.Marshal(out)
	return perr.HTTPStatus, h, body
}
