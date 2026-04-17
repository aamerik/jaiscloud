package services

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// APIGatewayCodec handles the API Gateway management plane REST/JSON wire protocol.
// SigV4 service name: apigateway
//
// URL patterns (management plane, hosted at execute.amazonaws.com):
//
//	POST   /restapis                                                    → CreateRestApi
//	GET    /restapis                                                    → GetRestApis
//	GET    /restapis/{restApiId}                                        → GetRestApi
//	PATCH  /restapis/{restApiId}                                        → UpdateRestApi
//	DELETE /restapis/{restApiId}                                        → DeleteRestApi
//	GET    /restapis/{restApiId}/resources                              → GetResources
//	GET    /restapis/{restApiId}/resources/{resourceId}                 → GetResource
//	POST   /restapis/{restApiId}/resources/{resourceId}                 → CreateResource
//	DELETE /restapis/{restApiId}/resources/{resourceId}                 → DeleteResource
//	PUT    /restapis/{restApiId}/resources/{resourceId}/methods/{method} → PutMethod
//	GET    /restapis/{restApiId}/resources/{resourceId}/methods/{method} → GetMethod
//	DELETE /restapis/{restApiId}/resources/{resourceId}/methods/{method} → DeleteMethod
//	PUT    /restapis/{restApiId}/resources/{resourceId}/methods/{method}/integration → PutIntegration
//	GET    /restapis/{restApiId}/resources/{resourceId}/methods/{method}/integration → GetIntegration
//	DELETE /restapis/{restApiId}/resources/{resourceId}/methods/{method}/integration → DeleteIntegration
//	PUT    /restapis/{restApiId}/resources/{resourceId}/methods/{method}/responses/{status} → PutMethodResponse
//	PUT    /restapis/{restApiId}/resources/{resourceId}/methods/{method}/integration/responses/{status} → PutIntegrationResponse
//	POST   /restapis/{restApiId}/deployments                            → CreateDeployment
//	GET    /restapis/{restApiId}/deployments                            → GetDeployments
//	DELETE /restapis/{restApiId}/deployments/{deploymentId}             → DeleteDeployment
//	POST   /restapis/{restApiId}/stages                                 → CreateStage
//	GET    /restapis/{restApiId}/stages                                 → GetStages
//	GET    /restapis/{restApiId}/stages/{stageName}                     → GetStage
//	PATCH  /restapis/{restApiId}/stages/{stageName}                     → UpdateStage
//	DELETE /restapis/{restApiId}/stages/{stageName}                     → DeleteStage
type APIGatewayCodec struct{}

func (c *APIGatewayCodec) ServiceName() string { return "apigateway" }

func (c *APIGatewayCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	params := map[string]any{}

	// Parse JSON body.
	if len(body) > 0 {
		json.Unmarshal(body, &params)
	}

	// Propagate query params.
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	action := apigwDetectAction(r.Method, parts, params)

	return &model.NormalizedRequest{
		Service: "apigateway",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

// apigwDetectAction maps (method, path segments) → action name, injecting path
// parameters into params.
//
// Path segment layout:
//
//	[0]="restapis"  [1]={restApiId}  [2]=sub-resource  [3]={id}  [4]=methods  [5]={httpMethod}  [6]=integration|responses  ...
func apigwDetectAction(method string, parts []string, params map[string]any) string {
	if len(parts) == 0 || parts[0] != "restapis" {
		return "Unknown"
	}

	// /restapis
	if len(parts) == 1 || parts[1] == "" {
		switch method {
		case http.MethodPost:
			return "CreateRestApi"
		case http.MethodGet:
			return "GetRestApis"
		}
		return "Unknown"
	}

	// /restapis/{restApiId}[/...]
	params["restApiId"] = parts[1]

	if len(parts) == 2 {
		switch method {
		case http.MethodGet:
			return "GetRestApi"
		case http.MethodPatch:
			return "UpdateRestApi"
		case http.MethodDelete:
			return "DeleteRestApi"
		}
		return "Unknown"
	}

	switch parts[2] {
	case "resources":
		return apigwResourcesAction(method, parts, params)
	case "deployments":
		return apigwDeploymentsAction(method, parts, params)
	case "stages":
		return apigwStagesAction(method, parts, params)
	}

	return "Unknown"
}

func apigwResourcesAction(method string, parts []string, params map[string]any) string {
	// /restapis/{id}/resources
	if len(parts) == 3 {
		switch method {
		case http.MethodGet:
			return "GetResources"
		}
		return "Unknown"
	}

	// /restapis/{id}/resources/{resourceId}[/...]
	params["resourceId"] = parts[3]

	if len(parts) == 4 {
		switch method {
		case http.MethodGet:
			return "GetResource"
		case http.MethodPost:
			return "CreateResource"
		case http.MethodDelete:
			return "DeleteResource"
		}
		return "Unknown"
	}

	if parts[4] != "methods" {
		return "Unknown"
	}

	// /restapis/{id}/resources/{resId}/methods[/{httpMethod}[/...]]
	if len(parts) == 5 {
		return "Unknown"
	}
	params["httpMethod"] = parts[5]

	if len(parts) == 6 {
		switch method {
		case http.MethodPut:
			return "PutMethod"
		case http.MethodGet:
			return "GetMethod"
		case http.MethodDelete:
			return "DeleteMethod"
		}
		return "Unknown"
	}

	// /...methods/{httpMethod}/{sub}[/{statusCode}]
	switch parts[6] {
	case "integration":
		if len(parts) == 7 {
			switch method {
			case http.MethodPut:
				return "PutIntegration"
			case http.MethodGet:
				return "GetIntegration"
			case http.MethodDelete:
				return "DeleteIntegration"
			}
		}
		// /integration/responses/{statusCode}
		if len(parts) >= 9 && parts[7] == "responses" {
			params["statusCode"] = parts[8]
			if method == http.MethodPut {
				return "PutIntegrationResponse"
			}
		}
	case "responses":
		// /methods/{httpMethod}/responses/{statusCode}
		if len(parts) >= 8 {
			params["statusCode"] = parts[7]
			if method == http.MethodPut {
				return "PutMethodResponse"
			}
		}
	}

	return "Unknown"
}

func apigwDeploymentsAction(method string, parts []string, params map[string]any) string {
	// /restapis/{id}/deployments
	if len(parts) == 3 {
		switch method {
		case http.MethodPost:
			return "CreateDeployment"
		case http.MethodGet:
			return "GetDeployments"
		}
		return "Unknown"
	}
	// /restapis/{id}/deployments/{deploymentId}
	params["deploymentId"] = parts[3]
	if method == http.MethodDelete {
		return "DeleteDeployment"
	}
	return "Unknown"
}

func apigwStagesAction(method string, parts []string, params map[string]any) string {
	// /restapis/{id}/stages
	if len(parts) == 3 {
		switch method {
		case http.MethodPost:
			return "CreateStage"
		case http.MethodGet:
			return "GetStages"
		}
		return "Unknown"
	}
	// /restapis/{id}/stages/{stageName}
	params["stageName"] = parts[3]
	switch method {
	case http.MethodGet:
		return "GetStage"
	case http.MethodPatch:
		return "UpdateStage"
	case http.MethodDelete:
		return "DeleteStage"
	}
	return "Unknown"
}

func (c *APIGatewayCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	if resp.HTTPStatus == 202 || resp.HTTPStatus == 204 {
		return resp.HTTPStatus, h, nil
	}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, body
}

func (c *APIGatewayCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body, _ := json.Marshal(map[string]any{
		"message": perr.Message,
		"code":    perr.Code,
	})
	return perr.HTTPStatus, h, body
}
