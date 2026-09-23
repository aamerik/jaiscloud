package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ResourceManagerCodec decodes the Cloud Resource Manager v1 project surface
// (cloudresourcemanager.googleapis.com/v1) that the hashicorp/google Terraform
// provider requires:
//
//	GET  /v1/projects/{project}                     (projects.get)
//	POST /v1/projects/{project}:getIamPolicy        (projects.getIamPolicy)
//	POST /v1/projects/{project}:setIamPolicy        (projects.setIamPolicy)
//	POST /v1/projects/{project}:testIamPermissions   (projects.testIamPermissions)
//
// A dedicated codec is required because project-level IAM has no resource
// segment after the project: the custom verb attaches directly to the project
// segment, so the generic JSONCodec (which derives resourceType/name from the
// segments after projects/{project}) has nothing to work with.
type ResourceManagerCodec struct {
	Service string
}

func (c *ResourceManagerCodec) ServiceName() string { return c.Service }

func (c *ResourceManagerCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+1 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}
	// The router only claims the project-last shape, but stay defensive: a
	// trailing resource segment is not part of the supported surface.
	if pi+1 != len(seg)-1 {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

	projectSeg := seg[pi+1]
	project := projectSeg
	custom := ""
	if i := strings.IndexByte(projectSeg, ':'); i >= 0 {
		project = projectSeg[:i]
		custom = projectSeg[i+1:]
	}
	if project == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = project
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	// The project IAM custom methods are POST-only per Discovery; a bare
	// project read is GET.
	switch {
	case custom == "getIamPolicy" && r.Method == http.MethodPost:
		nr.Action = "ProjectGetIamPolicy"
	case custom == "setIamPolicy" && r.Method == http.MethodPost:
		nr.Action = "ProjectSetIamPolicy"
	case custom == "testIamPermissions" && r.Method == http.MethodPost:
		nr.Action = "ProjectTestIamPermissions"
	case custom == "" && r.Method == http.MethodGet:
		nr.Action = "ProjectGet"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// Encode serialises a provider response as JSON.
func (c *ResourceManagerCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	if raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage); ok {
		return status, headers, raw
	}
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCP error envelope.
func (c *ResourceManagerCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	env := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  statusStr,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}
