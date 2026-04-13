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
	path := r.URL.Path

	// Detect action from HTTP method + path
	// POST /clusters → CreateCluster
	// GET  /clusters → ListClusters
	// GET  /clusters/{name} → DescribeCluster
	// DELETE /clusters/{name} → DeleteCluster
	var action string
	var params map[string]any

	trimmed := strings.TrimPrefix(path, "/clusters")
	clusterName := strings.Trim(trimmed, "/")

	switch {
	case r.Method == http.MethodPost && clusterName == "":
		action = "CreateCluster"
	case r.Method == http.MethodGet && clusterName == "":
		action = "ListClusters"
	case r.Method == http.MethodGet && clusterName != "":
		action = "DescribeCluster"
		params = map[string]any{"name": clusterName}
	case r.Method == http.MethodDelete && clusterName != "":
		action = "DeleteCluster"
		params = map[string]any{"name": clusterName}
	default:
		return nil, model.NewProviderError("InvalidRequest",
			fmt.Sprintf("unrecognised EKS request: %s %s", r.Method, path), 400)
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
