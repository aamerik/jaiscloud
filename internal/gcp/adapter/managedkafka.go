package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ManagedKafkaCodec decodes the Managed Kafka v1 REST surface
// (managedkafka.googleapis.com/v1). Resources live under
// /v1/projects/{project}/locations/{location}/clusters[/{id}][/topics[/{topicId}]|/consumerGroups].
// Unlike Dataproc, there are no custom ":verb" methods and no long-running
// operation resource — cluster create/update/delete return the resource inline
// (wrapped in a done operation by the provider), and topic CRUD is fully
// synchronous.
type ManagedKafkaCodec struct {
	Service string
}

func (c *ManagedKafkaCodec) ServiceName() string { return c.Service }

func (c *ManagedKafkaCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
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

	// rest = ["locations", "{location}", "clusters", ...] after projects/{project}.
	rest := seg[pi+2:]
	if len(rest) < 3 || rest[0] != "locations" {
		return nil, model.NewProviderError("InvalidRequest", "expected locations/{location}/clusters in managed kafka path", 404)
	}
	nr.Params["location"] = rest[1]
	tail := rest[2:]

	if len(tail) == 0 || tail[0] != "clusters" {
		return nil, model.NewProviderError("InvalidRequest", "expected clusters in managed kafka path", 404)
	}

	var resourceType, clusterID, topicID string
	isCollection := false
	switch len(tail) {
	case 1: // ["clusters"]
		resourceType = "clusters"
		isCollection = true
	case 2: // ["clusters", id]
		resourceType = "clusters"
		clusterID = tail[1]
	case 3: // ["clusters", id, "topics"|"consumerGroups"]
		resourceType = tail[2]
		clusterID = tail[1]
		isCollection = true
	case 4: // ["clusters", id, "topics"|"consumerGroups", id]
		resourceType = tail[2]
		clusterID = tail[1]
		topicID = tail[3]
	default:
		return nil, model.NewProviderError("InvalidRequest", "unrecognized managed kafka path", 404)
	}

	nr.Params["resourceType"] = resourceType
	nr.Params["name"] = strings.Join(rest, "/")
	if clusterID != "" {
		nr.Params["clusterId"] = clusterID
	}
	if topicID != "" {
		nr.Params["topicId"] = topicID
	}

	nr.Action = deriveManagedKafkaAction(resourceType, isCollection, r.Method)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// deriveManagedKafkaAction maps (resourceType, isCollection, method) to the
// action name. Create is POST on the collection; delete is DELETE on the
// resource; there are no custom-method verbs.
func deriveManagedKafkaAction(resourceType string, isCollection bool, method string) string {
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
	case "topics":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateTopic"
		case isCollection && method == http.MethodGet:
			return "ListTopics"
		case method == http.MethodGet:
			return "GetTopic"
		case method == http.MethodPatch:
			return "UpdateTopic"
		case method == http.MethodDelete:
			return "DeleteTopic"
		}
	case "consumerGroups":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListConsumerGroups"
		case method == http.MethodGet:
			return "GetConsumerGroup"
		case method == http.MethodPatch:
			return "UpdateConsumerGroup"
		case method == http.MethodDelete:
			return "DeleteConsumerGroup"
		}
	}
	return ""
}

// Encode serialises a provider response as JSON.
func (c *ManagedKafkaCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
func (c *ManagedKafkaCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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
