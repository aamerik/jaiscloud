package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// DataprocCodec decodes the Dataproc v1 REST surface
// (dataproc.googleapis.com/v1). Unlike the other /v1/projects/{project}/...
// services, Dataproc resources live under /v1/projects/{project}/regions/{region}
// and expose clusters, jobs, and long-running operations (also a first-class
// REST method — not gRPC-only). Custom methods are suffix-call on a resource
// (:start, :stop, :diagnose, :cancel) or on the collection (jobs:submit,
// jobs:submitAsOperation).
type DataprocCodec struct {
	Service string
}

func (c *DataprocCodec) ServiceName() string { return c.Service }

func (c *DataprocCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
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

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	// rest = ["regions", "{region}", ...] after projects/{project}.
	rest := seg[pi+2:]
	if len(rest) < 2 || rest[0] != "regions" {
		return nil, model.NewProviderError("InvalidRequest", "expected regions/{region} in dataproc path", 404)
	}
	nr.Params["region"] = rest[1]
	tail := rest[2:]

	if len(tail) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "missing dataproc resource", 404)
	}

	resourceType := tail[0]
	resourceType = strings.SplitN(resourceType, ":", 2)[0]
	nr.Params["resourceType"] = resourceType
	nr.Params["name"] = strings.Join(rest, "/")

	custom := ""
	last := tail[len(tail)-1]
	if i := strings.IndexByte(last, ':'); i >= 0 {
		custom = last[i+1:]
		tail[len(tail)-1] = last[:i]
	}

	switch resourceType {
	case "clusters":
		if len(tail) >= 2 {
			nr.Params["clusterName"] = tail[1]
		}
	case "jobs":
		if len(tail) >= 2 {
			nr.Params["jobId"] = tail[1]
		}
	case "operations":
		if len(tail) >= 2 {
			nr.Params["operationId"] = tail[1]
		}
	}

	isCollection := len(tail) == 1
	nr.Action = deriveDataprocAction(resourceType, isCollection, r.Method, custom)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// deriveDataprocAction maps (resourceType, isCollection, method, custom) to the
// action name.
func deriveDataprocAction(resourceType string, isCollection bool, method, custom string) string {
	if custom != "" {
		switch resourceType {
		case "clusters":
			switch custom {
			case "start":
				return "StartCluster"
			case "stop":
				return "StopCluster"
			case "diagnose":
				return "DiagnoseCluster"
			}
		case "jobs":
			switch custom {
			case "submit":
				return "SubmitJob"
			case "submitAsOperation":
				return "SubmitJobAsOperation"
			case "cancel":
				return "CancelJob"
			}
		}
	}

	switch resourceType {
	case "clusters":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateCluster"
		case isCollection && method == http.MethodGet:
			return "ListClusters"
		case method == http.MethodGet:
			return "GetCluster"
		case method == http.MethodPatch:
			return "UpdateCluster"
		case method == http.MethodDelete:
			return "DeleteCluster"
		}
	case "jobs":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListJobs"
		case method == http.MethodGet:
			return "GetJob"
		case method == http.MethodDelete:
			return "DeleteJob"
		}
	case "operations":
		switch {
		case method == http.MethodGet:
			return "GetOperation"
		}
	}
	return ""
}

// Encode serialises a provider response as JSON (shared with JSONCodec).
func (c *DataprocCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
func (c *DataprocCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr := perr.Status
	if statusStr == "" {
		statusStr = gcpStatusString(status)
	}
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
