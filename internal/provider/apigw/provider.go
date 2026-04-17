// Package apigw implements the API Gateway management plane provider.
// Supports REST APIs, resources, methods, integrations, stages, and deployments.
// The execute-api invoke path is handled by a separate codec that calls
// GatewayProvider.Dispatch.
package apigw

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtAPI        = "apigw_api"
	rtResource   = "apigw_resource"
	rtStage      = "apigw_stage"
	rtDeployment = "apigw_deployment"
)

// GatewayProvider handles API Gateway management operations and execute-api dispatch.
type GatewayProvider struct {
	resources store.ResourceStore
	// httpClient is used for HTTP_PROXY integrations.
	httpClient *http.Client
}

// New constructs a GatewayProvider.
func New(resources store.ResourceStore) *GatewayProvider {
	return &GatewayProvider{
		resources:  resources,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Routes returns all API Gateway management plane handler registrations.
func (p *GatewayProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// REST APIs
		"Gateway.CreateRestApi":    p.CreateRestApi,
		"Gateway.GetRestApi":       p.GetRestApi,
		"Gateway.GetRestApis":      p.GetRestApis,
		"Gateway.UpdateRestApi":    p.UpdateRestApi,
		"Gateway.DeleteRestApi":    p.DeleteRestApi,
		// Resources
		"Gateway.GetResources":     p.GetResources,
		"Gateway.GetResource":      p.GetResource,
		"Gateway.CreateResource":   p.CreateResource,
		"Gateway.DeleteResource":   p.DeleteResource,
		// Methods
		"Gateway.PutMethod":        p.PutMethod,
		"Gateway.GetMethod":        p.GetMethod,
		"Gateway.DeleteMethod":     p.DeleteMethod,
		// Integrations
		"Gateway.PutIntegration":   p.PutIntegration,
		"Gateway.GetIntegration":   p.GetIntegration,
		"Gateway.DeleteIntegration": p.DeleteIntegration,
		// Method/Integration responses
		"Gateway.PutMethodResponse":       p.PutMethodResponse,
		"Gateway.PutIntegrationResponse":  p.PutIntegrationResponse,
		// Deployments
		"Gateway.CreateDeployment": p.CreateDeployment,
		"Gateway.GetDeployments":   p.GetDeployments,
		"Gateway.DeleteDeployment": p.DeleteDeployment,
		// Stages
		"Gateway.CreateStage":      p.CreateStage,
		"Gateway.GetStage":         p.GetStage,
		"Gateway.GetStages":        p.GetStages,
		"Gateway.UpdateStage":      p.UpdateStage,
		"Gateway.DeleteStage":      p.DeleteStage,
		// Execute-API (invoke plane)
		"Gateway.Invoke":           p.Invoke,
	}
}

// ─── REST API CRUD ────────────────────────────────────────────────────────────

type restAPI struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	CreatedDate int64             `json:"createdDate"`
	Tags        map[string]string `json:"tags,omitempty"`
}

func (p *GatewayProvider) CreateRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	if name == "" {
		return nil, model.NewProviderError("BadRequestException", "name is required", 400)
	}
	apiID := shortID()
	api := restAPI{
		ID:          apiID,
		Name:        name,
		Description: strParam(nr.Params, "description"),
		CreatedDate: time.Now().Unix(),
	}
	if err := p.save(ctx, rtAPI, apiID, api); err != nil {
		return nil, fmt.Errorf("apigw: create api: %w", err)
	}
	// Create root resource "/" automatically.
	rootID := shortID()
	root := apiResource{ID: rootID, APIID: apiID, Path: "/", PathPart: ""}
	p.save(ctx, rtResource, rootID, root)

	return &model.ProviderResponse{HTTPStatus: 201, Data: apiToWire(api)}, nil
}

func (p *GatewayProvider) GetRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	var api restAPI
	if err := p.load(ctx, rtAPI, apiID, &api); err != nil {
		return nil, p.notFound(err, "Rest API not found: "+apiID)
	}
	return provider.OK(apiToWire(api)), nil
}

func (p *GatewayProvider) GetRestApis(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtAPI, "")
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var api restAPI
		if json.Unmarshal(e.Data, &api) == nil {
			items = append(items, apiToWire(api))
		}
	}
	return provider.OK(map[string]any{"item": items}), nil
}

func (p *GatewayProvider) UpdateRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	var api restAPI
	if err := p.load(ctx, rtAPI, apiID, &api); err != nil {
		return nil, p.notFound(err, "Rest API not found: "+apiID)
	}
	applyPatchOps(&api, nr.Params)
	if err := p.save(ctx, rtAPI, apiID, api); err != nil {
		return nil, fmt.Errorf("apigw: update api: %w", err)
	}
	return provider.OK(apiToWire(api)), nil
}

func (p *GatewayProvider) DeleteRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	if err := p.resources.Delete(ctx, rtAPI, apiID); err != nil {
		return nil, p.notFound(err, "Rest API not found: "+apiID)
	}
	// Cascade-delete all child entities associated with this API.
	for _, rt := range []string{rtResource, rtStage, rtDeployment} {
		entries, _ := p.resources.List(ctx, rt, "")
		for _, e := range entries {
			var m map[string]any
			if json.Unmarshal(e.Data, &m) == nil {
				if aid, _ := m["apiId"].(string); aid == apiID {
					p.resources.Delete(ctx, rt, e.ID) //nolint:errcheck
				}
			}
		}
	}
	return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{}}, nil
}

// ─── Resources ────────────────────────────────────────────────────────────────

type apiResource struct {
	ID              string                     `json:"id"`
	APIID           string                     `json:"apiId"`
	ParentID        string                     `json:"parentId"`
	Path            string                     `json:"path"`
	PathPart        string                     `json:"pathPart"`
	ResourceMethods map[string]resourceMethod  `json:"resourceMethods,omitempty"`
}

type resourceMethod struct {
	HTTPMethod          string                 `json:"httpMethod"`
	AuthorizationType   string                 `json:"authorizationType"`
	Integration         *methodIntegration     `json:"methodIntegration,omitempty"`
	MethodResponses     map[string]any         `json:"methodResponses,omitempty"`
}

type methodIntegration struct {
	Type            string            `json:"type"`
	URI             string            `json:"uri"`
	HTTPMethod      string            `json:"httpMethod"`
	PassthroughBehavior string        `json:"passthroughBehavior"`
	Responses       map[string]any    `json:"integrationResponses,omitempty"`
}

func (p *GatewayProvider) GetResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	entries, _ := p.resources.List(ctx, rtResource, "")
	var items []map[string]any
	for _, e := range entries {
		var r apiResource
		if json.Unmarshal(e.Data, &r) == nil && r.APIID == apiID {
			items = append(items, resourceToWire(r))
		}
	}
	return provider.OK(map[string]any{"item": items}), nil
}

func (p *GatewayProvider) GetResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found: "+resourceID)
	}
	return provider.OK(resourceToWire(r)), nil
}

func (p *GatewayProvider) CreateResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	parentID, _ := nr.Params["parentId"].(string)
	pathPart, _ := nr.Params["pathPart"].(string)

	var parent apiResource
	if err := p.load(ctx, rtResource, parentID, &parent); err != nil {
		return nil, p.notFound(err, "Parent resource not found: "+parentID)
	}
	path := strings.TrimRight(parent.Path, "/") + "/" + pathPart

	r := apiResource{
		ID: shortID(), APIID: apiID, ParentID: parentID,
		Path: path, PathPart: pathPart,
		ResourceMethods: make(map[string]resourceMethod),
	}
	if err := p.save(ctx, rtResource, r.ID, r); err != nil {
		return nil, fmt.Errorf("apigw: create resource: %w", err)
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: resourceToWire(r)}, nil
}

func (p *GatewayProvider) DeleteResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	if err := p.resources.Delete(ctx, rtResource, resourceID); err != nil {
		return nil, p.notFound(err, "Resource not found: "+resourceID)
	}
	return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{}}, nil
}

// ─── Methods ──────────────────────────────────────────────────────────────────

func (p *GatewayProvider) PutMethod(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	authType, _ := nr.Params["authorizationType"].(string)

	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found: "+resourceID)
	}
	if r.ResourceMethods == nil {
		r.ResourceMethods = make(map[string]resourceMethod)
	}
	r.ResourceMethods[httpMethod] = resourceMethod{
		HTTPMethod: httpMethod, AuthorizationType: authType,
	}
	p.save(ctx, rtResource, resourceID, r)
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{
		"httpMethod": httpMethod, "authorizationType": authType,
	}}, nil
}

func (p *GatewayProvider) GetMethod(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, model.NewProviderError("NotFoundException", "Method not found: "+httpMethod, 404)
	}
	return provider.OK(map[string]any{"httpMethod": m.HTTPMethod, "authorizationType": m.AuthorizationType}), nil
}

func (p *GatewayProvider) DeleteMethod(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	delete(r.ResourceMethods, httpMethod)
	p.save(ctx, rtResource, resourceID, r)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── Integrations ─────────────────────────────────────────────────────────────

func (p *GatewayProvider) PutIntegration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	intType, _ := nr.Params["type"].(string)
	uri, _ := nr.Params["uri"].(string)
	intHTTPMethod, _ := nr.Params["integrationHttpMethod"].(string)

	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	m := r.ResourceMethods[httpMethod]
	m.Integration = &methodIntegration{
		Type: intType, URI: uri, HTTPMethod: intHTTPMethod,
		PassthroughBehavior: "WHEN_NO_MATCH",
	}
	r.ResourceMethods[httpMethod] = m
	p.save(ctx, rtResource, resourceID, r)
	return &model.ProviderResponse{HTTPStatus: 201, Data: integrationToWire(m.Integration)}, nil
}

func (p *GatewayProvider) GetIntegration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok || m.Integration == nil {
		return nil, model.NewProviderError("NotFoundException", "Integration not found", 404)
	}
	return provider.OK(integrationToWire(m.Integration)), nil
}

func (p *GatewayProvider) DeleteIntegration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	m := r.ResourceMethods[httpMethod]
	m.Integration = nil
	r.ResourceMethods[httpMethod] = m
	p.save(ctx, rtResource, resourceID, r)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── Method / Integration responses ──────────────────────────────────────────

func (p *GatewayProvider) PutMethodResponse(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	statusCode, _ := nr.Params["statusCode"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	m := r.ResourceMethods[httpMethod]
	if m.MethodResponses == nil {
		m.MethodResponses = make(map[string]any)
	}
	m.MethodResponses[statusCode] = map[string]any{"statusCode": statusCode}
	r.ResourceMethods[httpMethod] = m
	p.save(ctx, rtResource, resourceID, r)
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"statusCode": statusCode}}, nil
}

func (p *GatewayProvider) PutIntegrationResponse(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceID, _ := nr.Params["resourceId"].(string)
	httpMethod, _ := nr.Params["httpMethod"].(string)
	statusCode, _ := nr.Params["statusCode"].(string)
	var r apiResource
	if err := p.load(ctx, rtResource, resourceID, &r); err != nil {
		return nil, p.notFound(err, "Resource not found")
	}
	m := r.ResourceMethods[httpMethod]
	if m.Integration == nil {
		m.Integration = &methodIntegration{}
	}
	if m.Integration.Responses == nil {
		m.Integration.Responses = make(map[string]any)
	}
	m.Integration.Responses[statusCode] = map[string]any{"statusCode": statusCode}
	r.ResourceMethods[httpMethod] = m
	p.save(ctx, rtResource, resourceID, r)
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{"statusCode": statusCode}}, nil
}

// ─── Deployments ──────────────────────────────────────────────────────────────

type deployment struct {
	ID          string `json:"id"`
	APIID       string `json:"apiId"`
	Description string `json:"description"`
	CreatedDate int64  `json:"createdDate"`
}

func (p *GatewayProvider) CreateDeployment(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	stageName, _ := nr.Params["stageName"].(string)
	desc, _ := nr.Params["description"].(string)

	d := deployment{
		ID: shortID(), APIID: apiID, Description: desc,
		CreatedDate: time.Now().Unix(),
	}
	p.save(ctx, rtDeployment, d.ID, d)

	// Auto-create or update the stage if stageName is provided.
	if stageName != "" {
		stageKey := apiID + "/" + stageName
		st := apiStage{Name: stageName, APIID: apiID, DeploymentID: d.ID}
		p.save(ctx, rtStage, stageKey, st)
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{
		"id": d.ID, "createdDate": d.CreatedDate, "description": d.Description,
	}}, nil
}

func (p *GatewayProvider) GetDeployments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	entries, _ := p.resources.List(ctx, rtDeployment, "")
	var items []map[string]any
	for _, e := range entries {
		var d deployment
		if json.Unmarshal(e.Data, &d) == nil && d.APIID == apiID {
			items = append(items, map[string]any{"id": d.ID, "createdDate": d.CreatedDate})
		}
	}
	return provider.OK(map[string]any{"item": items}), nil
}

func (p *GatewayProvider) DeleteDeployment(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	deploymentID, _ := nr.Params["deploymentId"].(string)
	p.resources.Delete(ctx, rtDeployment, deploymentID)
	return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{}}, nil
}

// ─── Stages ───────────────────────────────────────────────────────────────────

type apiStage struct {
	Name         string            `json:"stageName"`
	APIID        string            `json:"apiId"`
	DeploymentID string            `json:"deploymentId"`
	Description  string            `json:"description"`
	Variables    map[string]string `json:"variables,omitempty"`
	CreatedDate  int64             `json:"createdDate"`
}

func (p *GatewayProvider) CreateStage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	stageName, _ := nr.Params["stageName"].(string)
	deploymentID, _ := nr.Params["deploymentId"].(string)

	st := apiStage{
		Name: stageName, APIID: apiID, DeploymentID: deploymentID,
		CreatedDate: time.Now().Unix(),
	}
	stageKey := apiID + "/" + stageName
	p.save(ctx, rtStage, stageKey, st)
	return &model.ProviderResponse{HTTPStatus: 201, Data: stageToWire(st)}, nil
}

func (p *GatewayProvider) GetStage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	stageName, _ := nr.Params["stageName"].(string)
	var st apiStage
	if err := p.load(ctx, rtStage, apiID+"/"+stageName, &st); err != nil {
		return nil, p.notFound(err, "Stage not found: "+stageName)
	}
	return provider.OK(stageToWire(st)), nil
}

func (p *GatewayProvider) GetStages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	entries, _ := p.resources.List(ctx, rtStage, apiID+"/")
	var items []map[string]any
	for _, e := range entries {
		var st apiStage
		if json.Unmarshal(e.Data, &st) == nil {
			items = append(items, stageToWire(st))
		}
	}
	return provider.OK(map[string]any{"item": items}), nil
}

func (p *GatewayProvider) UpdateStage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	stageName, _ := nr.Params["stageName"].(string)
	var st apiStage
	stageKey := apiID + "/" + stageName
	if err := p.load(ctx, rtStage, stageKey, &st); err != nil {
		return nil, p.notFound(err, "Stage not found: "+stageName)
	}
	applyPatchOpsStage(&st, nr.Params)
	p.save(ctx, rtStage, stageKey, st)
	return provider.OK(stageToWire(st)), nil
}

func (p *GatewayProvider) DeleteStage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["restApiId"].(string)
	stageName, _ := nr.Params["stageName"].(string)
	p.resources.Delete(ctx, rtStage, apiID+"/"+stageName)
	return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{}}, nil
}

// ─── Execute-API invoke ───────────────────────────────────────────────────────

// Invoke handles execute-api requests: routes the incoming HTTP call to the
// configured integration (currently HTTP_PROXY and MOCK are supported).
func (p *GatewayProvider) Invoke(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apiID, _ := nr.Params["_apiId"].(string)
	stageName, _ := nr.Params["_stageName"].(string)
	resourcePath, _ := nr.Params["_resourcePath"].(string)
	httpMethod, _ := nr.Params["_httpMethod"].(string)
	body, _ := nr.Params["_body"].([]byte)

	r, err := p.findResource(ctx, apiID, resourcePath)
	if err != nil {
		return nil, model.NewProviderError("NotFoundException", "no matching resource for "+resourcePath, 404)
	}

	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		m, ok = r.ResourceMethods["ANY"]
	}
	if !ok {
		return nil, model.NewProviderError("MethodNotAllowedException", httpMethod+" not configured", 405)
	}
	if m.Integration == nil {
		return nil, model.NewProviderError("InternalServerErrorException", "no integration configured", 500)
	}

	// Stage variable interpolation.
	stageKey := apiID + "/" + stageName
	var st apiStage
	p.load(ctx, rtStage, stageKey, &st)
	uri := interpolateStageVars(m.Integration.URI, st.Variables)

	switch strings.ToUpper(m.Integration.Type) {
	case "MOCK":
		return provider.OK(map[string]any{"statusCode": 200}), nil
	case "HTTP", "HTTP_PROXY":
		return p.invokeHTTP(ctx, m.Integration.HTTPMethod, uri, body, nr.Raw)
	default:
		return nil, model.NewProviderError("InternalServerErrorException",
			"unsupported integration type: "+m.Integration.Type, 500)
	}
}

func (p *GatewayProvider) invokeHTTP(ctx context.Context, method, uri string, body []byte, orig *http.Request) (*model.ProviderResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, uri, nil)
	if err != nil {
		return nil, model.NewProviderError("InternalServerErrorException", "build upstream request: "+err.Error(), 500)
	}
	if len(body) > 0 {
		req.Body = io.NopCloser(strings.NewReader(string(body)))
		req.ContentLength = int64(len(body))
	}
	// Forward selected headers.
	if orig != nil {
		for _, h := range []string{"Content-Type", "Accept", "Authorization"} {
			if v := orig.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, model.NewProviderError("InternalServerErrorException", "upstream call failed: "+err.Error(), 502)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return &model.ProviderResponse{
		HTTPStatus: resp.StatusCode,
		Data:       map[string]any{"_raw": respBody, "_status": resp.StatusCode},
	}, nil
}

// findResource matches a request path against stored resources for an API,
// supporting path parameters ({param}) and greedy {proxy+} variables.
func (p *GatewayProvider) findResource(ctx context.Context, apiID, requestPath string) (apiResource, error) {
	entries, _ := p.resources.List(ctx, rtResource, "")
	// First pass: exact match.
	for _, e := range entries {
		var r apiResource
		if json.Unmarshal(e.Data, &r) != nil || r.APIID != apiID {
			continue
		}
		if r.Path == requestPath {
			return r, nil
		}
	}
	// Second pass: pattern match ({param} and {proxy+}).
	for _, e := range entries {
		var r apiResource
		if json.Unmarshal(e.Data, &r) != nil || r.APIID != apiID {
			continue
		}
		if pathMatches(r.Path, requestPath) {
			return r, nil
		}
	}
	return apiResource{}, errors.New("not found")
}

// pathMatches returns true if pattern (e.g. /items/{id} or /files/{proxy+})
// matches the given request path.
func pathMatches(pattern, path string) bool {
	pp := strings.Split(strings.Trim(pattern, "/"), "/")
	rp := strings.Split(strings.Trim(path, "/"), "/")
	for i, seg := range pp {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "+}") {
			// Greedy segment — matches everything remaining.
			return i < len(rp)
		}
		if i >= len(rp) {
			return false
		}
		if !strings.HasPrefix(seg, "{") && seg != rp[i] {
			return false
		}
	}
	return len(pp) == len(rp)
}

func interpolateStageVars(uri string, vars map[string]string) string {
	for k, v := range vars {
		uri = strings.ReplaceAll(uri, "${stageVariables."+k+"}", v)
	}
	return uri
}

// ─── persistence helpers ──────────────────────────────────────────────────────

func (p *GatewayProvider) save(ctx context.Context, rtype, id string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e := store.ResourceEntry{Type: rtype, ID: id, Data: data}
	if err := p.resources.Create(ctx, e); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return p.resources.Update(ctx, e)
		}
		return err
	}
	return nil
}

func (p *GatewayProvider) load(ctx context.Context, rtype, id string, v any) error {
	e, err := p.resources.Get(ctx, rtype, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(e.Data, v)
}

func (p *GatewayProvider) notFound(err error, msg string) error {
	if errors.Is(err, store.ErrNotFound) {
		return model.NewProviderError("NotFoundException", msg, 404)
	}
	return err
}

// ─── wire helpers ─────────────────────────────────────────────────────────────

func apiToWire(a restAPI) map[string]any {
	return map[string]any{
		"id": a.ID, "name": a.Name, "description": a.Description,
		"createdDate": a.CreatedDate,
	}
}

func resourceToWire(r apiResource) map[string]any {
	return map[string]any{
		"id": r.ID, "parentId": r.ParentID, "path": r.Path, "pathPart": r.PathPart,
		"resourceMethods": r.ResourceMethods,
	}
}

func integrationToWire(i *methodIntegration) map[string]any {
	if i == nil {
		return map[string]any{}
	}
	return map[string]any{
		"type": i.Type, "uri": i.URI, "httpMethod": i.HTTPMethod,
		"passthroughBehavior": i.PassthroughBehavior,
	}
}

func stageToWire(st apiStage) map[string]any {
	return map[string]any{
		"stageName": st.Name, "deploymentId": st.DeploymentID,
		"description": st.Description, "createdDate": st.CreatedDate,
		"variables": st.Variables,
	}
}

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func applyPatchOps(api *restAPI, params map[string]any) {
	if ops, ok := params["patchOperations"].([]any); ok {
		for _, op := range ops {
			if m, ok := op.(map[string]any); ok {
				path, _ := m["path"].(string)
				value, _ := m["value"].(string)
				switch path {
				case "/name":
					api.Name = value
				case "/description":
					api.Description = value
				}
			}
		}
	}
}

func applyPatchOpsStage(st *apiStage, params map[string]any) {
	if ops, ok := params["patchOperations"].([]any); ok {
		for _, op := range ops {
			if m, ok := op.(map[string]any); ok {
				path, _ := m["path"].(string)
				value, _ := m["value"].(string)
				switch {
				case path == "/description":
					st.Description = value
				case strings.HasPrefix(path, "/variables/"):
					key := strings.TrimPrefix(path, "/variables/")
					if st.Variables == nil {
						st.Variables = make(map[string]string)
					}
					st.Variables[key] = value
				}
			}
		}
	}
}

func shortID() string {
	b := make([]byte, 5)
	io.ReadFull(rand.Reader, b)
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	out := make([]byte, 10)
	for i, byt := range b {
		out[i*2] = chars[byt>>4%36]
		out[i*2+1] = chars[byt&0xf%36]
	}
	return string(out)
}
