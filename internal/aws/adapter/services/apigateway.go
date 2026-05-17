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
//	POST   /restapis/{restApiId}/requestvalidators                      → CreateRequestValidator
//	GET    /restapis/{restApiId}/requestvalidators                      → GetRequestValidators
//	GET    /restapis/{restApiId}/requestvalidators/{validatorId}        → GetRequestValidator
//	PATCH  /restapis/{restApiId}/requestvalidators/{validatorId}        → UpdateRequestValidator
//	DELETE /restapis/{restApiId}/requestvalidators/{validatorId}        → DeleteRequestValidator
//	GET    /restapis/{restApiId}/stages/{stageName}/exports/{exportType} → GetExport
//	POST   /domainnames                                                 → CreateDomainName
//	GET    /domainnames                                                 → GetDomainNames
//	GET    /domainnames/{domainName}                                    → GetDomainName
//	PATCH  /domainnames/{domainName}                                    → UpdateDomainName
//	DELETE /domainnames/{domainName}                                    → DeleteDomainName
//	POST   /domainnames/{domainName}/basepathmappings                   → CreateBasePathMapping
//	GET    /domainnames/{domainName}/basepathmappings                   → GetBasePathMappings
//	GET    /domainnames/{domainName}/basepathmappings/{basePath}        → GetBasePathMapping
//	DELETE /domainnames/{domainName}/basepathmappings/{basePath}        → DeleteBasePathMapping
//	POST   /usageplans                                                  → CreateUsagePlan
//	GET    /usageplans                                                  → GetUsagePlans
//	GET    /usageplans/{usagePlanId}                                    → GetUsagePlan
//	PATCH  /usageplans/{usagePlanId}                                    → UpdateUsagePlan
//	DELETE /usageplans/{usagePlanId}                                    → DeleteUsagePlan
//	POST   /usageplans/{usagePlanId}/keys                               → CreateUsagePlanKey
//	GET    /usageplans/{usagePlanId}/keys                               → GetUsagePlanKeys
//	DELETE /usageplans/{usagePlanId}/keys/{keyId}                       → DeleteUsagePlanKey
//	POST   /apikeys                                                     → CreateApiKey
//	GET    /apikeys                                                     → GetApiKeys
//	GET    /apikeys/{apiKey}                                            → GetApiKey
//	PATCH  /apikeys/{apiKey}                                            → UpdateApiKey
//	DELETE /apikeys/{apiKey}                                            → DeleteApiKey
//	GET    /tags/{resourceArn}                                          → GetTags
//	PUT    /tags/{resourceArn}                                          → TagResource
//	DELETE /tags/{resourceArn}                                          → UntagResource
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

	// Propagate query params. For multi-value params (e.g. tagKeys), keep all values.
	for k, vs := range r.URL.Query() {
		if len(vs) == 1 {
			params[k] = vs[0]
		} else if len(vs) > 1 {
			params[k] = vs
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

// apigwTopLevelAction handles paths not under /restapis: /domainnames, /usageplans, /apikeys, /tags.
func apigwTopLevelAction(method string, parts []string, params map[string]any) string {
	switch parts[0] {
	case "domainnames":
		return apigwDomainNamesAction(method, parts, params)
	case "usageplans":
		return apigwUsagePlansAction(method, parts, params)
	case "apikeys":
		return apigwAPIKeysAction(method, parts, params)
	case "tags":
		// /tags/{resourceArn}
		if len(parts) >= 2 {
			params["resourceArn"] = strings.Join(parts[1:], "/")
		}
		switch method {
		case http.MethodGet:
			return "GetTags"
		case http.MethodPut:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	}
	return "Unknown"
}

func apigwDomainNamesAction(method string, parts []string, params map[string]any) string {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return "CreateDomainName"
		case http.MethodGet:
			return "GetDomainNames"
		}
		return "Unknown"
	}
	params["domainName"] = parts[1]
	if len(parts) == 2 {
		switch method {
		case http.MethodGet:
			return "GetDomainName"
		case http.MethodPatch:
			return "UpdateDomainName"
		case http.MethodDelete:
			return "DeleteDomainName"
		}
		return "Unknown"
	}
	if parts[2] == "basepathmappings" {
		if len(parts) == 3 {
			switch method {
			case http.MethodPost:
				return "CreateBasePathMapping"
			case http.MethodGet:
				return "GetBasePathMappings"
			}
			return "Unknown"
		}
		params["basePath"] = parts[3]
		switch method {
		case http.MethodGet:
			return "GetBasePathMapping"
		case http.MethodDelete:
			return "DeleteBasePathMapping"
		}
	}
	return "Unknown"
}

func apigwUsagePlansAction(method string, parts []string, params map[string]any) string {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return "CreateUsagePlan"
		case http.MethodGet:
			return "GetUsagePlans"
		}
		return "Unknown"
	}
	params["usagePlanId"] = parts[1]
	if len(parts) == 2 {
		switch method {
		case http.MethodGet:
			return "GetUsagePlan"
		case http.MethodPatch:
			return "UpdateUsagePlan"
		case http.MethodDelete:
			return "DeleteUsagePlan"
		}
		return "Unknown"
	}
	if parts[2] == "keys" {
		if len(parts) == 3 {
			switch method {
			case http.MethodPost:
				return "CreateUsagePlanKey"
			case http.MethodGet:
				return "GetUsagePlanKeys"
			}
			return "Unknown"
		}
		params["keyId"] = parts[3]
		if method == http.MethodDelete {
			return "DeleteUsagePlanKey"
		}
	}
	return "Unknown"
}

func apigwAPIKeysAction(method string, parts []string, params map[string]any) string {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return "CreateApiKey"
		case http.MethodGet:
			return "GetApiKeys"
		}
		return "Unknown"
	}
	params["apiKey"] = parts[1]
	switch method {
	case http.MethodGet:
		return "GetApiKey"
	case http.MethodPatch:
		return "UpdateApiKey"
	case http.MethodDelete:
		return "DeleteApiKey"
	}
	return "Unknown"
}

// apigwDetectAction maps (method, path segments) → action name, injecting path
// parameters into params.
//
// Path segment layout:
//
//	[0]="restapis"  [1]={restApiId}  [2]=sub-resource  [3]={id}  [4]=methods  [5]={httpMethod}  [6]=integration|responses  ...
func apigwDetectAction(method string, parts []string, params map[string]any) string {
	if len(parts) == 0 {
		return "Unknown"
	}
	if parts[0] != "restapis" {
		return apigwTopLevelAction(method, parts, params)
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
	case "requestvalidators":
		return apigwRequestValidatorsAction(method, parts, params)
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
			// For CreateResource the {resourceId} path segment IS the parentId.
			params["parentId"] = parts[3]
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
	if len(parts) == 4 {
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
	// /restapis/{id}/stages/{stageName}/exports/{exportType}
	if len(parts) >= 6 && parts[4] == "exports" {
		params["exportType"] = parts[5]
		if method == http.MethodGet {
			return "GetExport"
		}
	}
	return "Unknown"
}

func apigwRequestValidatorsAction(method string, parts []string, params map[string]any) string {
	// /restapis/{id}/requestvalidators
	if len(parts) == 3 {
		switch method {
		case http.MethodPost:
			return "CreateRequestValidator"
		case http.MethodGet:
			return "GetRequestValidators"
		}
		return "Unknown"
	}
	// /restapis/{id}/requestvalidators/{validatorId}
	params["requestValidatorId"] = parts[3]
	switch method {
	case http.MethodGet:
		return "GetRequestValidator"
	case http.MethodPatch:
		return "UpdateRequestValidator"
	case http.MethodDelete:
		return "DeleteRequestValidator"
	}
	return "Unknown"
}

func (c *APIGatewayCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	// HTTP/HTTP_PROXY integration: pass upstream body verbatim.
	if raw, ok := resp.Data["_raw"].([]byte); ok {
		if ct, _ := resp.Data["_contentType"].(string); ct != "" {
			h.Set("Content-Type", ct)
		}
		status := 200
		if s, ok := resp.Data["_status"].(int); ok {
			status = s
		}
		// Selectively pass through cacheable upstream headers.
		// (hop-by-hop headers are not present since we read them from a completed response)
		for _, passHeader := range []string{"Cache-Control", "Etag", "Last-Modified", "Vary"} {
			if nr != nil && nr.Raw != nil {
				// headers came from provider Data — nothing to copy here
				_ = passHeader
			}
		}
		return status, h, raw
	}
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
