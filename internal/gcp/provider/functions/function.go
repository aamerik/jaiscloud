// Package functions implements the Google Cloud Functions v1 provider.
package functions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// rtFunctionPolicy is the generic ResourceStore type for function IAM policies.
const rtFunctionPolicy = "gcp_function_policy"

// operationMetadataType is the proto @type of the Cloud Functions v1
// OperationMetadata carried on the long-running operations that Create, Update,
// and Delete return.
const operationMetadataType = "type.googleapis.com/google.cloud.functions.v1.OperationMetadata"

// functionRegions is the synthesized set of locations the emulator advertises
// for functions.location discovery. Real Cloud Functions v1 is available in
// more regions; this is a stable subset. Region discovery itself is a shared,
// project-wide surface: the bare /v1/projects/{p}/locations[/{l}] paths are
// owned by the Memorystore detector (see adapter/detectV1Service) and return the
// same google.cloud.location.Location records, so a Cloud Functions SDK
// locations.list/get resolves through that handler. These records back the
// Function.ListLocations/GetLocation handlers for direct dispatch.
var functionRegions = []string{
	"asia-east1",
	"asia-east2",
	"asia-northeast1",
	"asia-northeast2",
	"asia-northeast3",
	"asia-south1",
	"asia-southeast1",
	"asia-southeast2",
	"australia-southeast1",
	"europe-central2",
	"europe-north1",
	"europe-west1",
	"europe-west2",
	"europe-west3",
	"europe-west6",
	"northamerica-northeast1",
	"southamerica-east1",
	"us-central1",
	"us-east1",
	"us-east4",
	"us-west1",
	"us-west2",
	"us-west3",
	"us-west4",
}

// Provider handles Cloud Functions v1 functions.
type Provider struct {
	functions functionsstore.Store
	resources store.ResourceStore // IAM policies (control-plane)
	executor  lambdaexec.LambdaExecutor
}

// New returns a Provider backed by the given store. executor defaults to a
// MockExecutor (echo) when nil so CallFunction works out of the box.
func New(functions functionsstore.Store, resources store.ResourceStore, executor lambdaexec.LambdaExecutor) *Provider {
	if executor == nil {
		executor = &lambdaexec.MockExecutor{}
	}
	return &Provider{functions: functions, resources: resources, executor: executor}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Function.CreateFunction":             p.CreateFunction,
		"Function.GetFunction":                p.GetFunction,
		"Function.ListFunctions":              p.ListFunctions,
		"Function.UpdateFunction":             p.UpdateFunction,
		"Function.DeleteFunction":             p.DeleteFunction,
		"Function.CallFunction":               p.CallFunction,
		"Function.GenerateUploadUrl":          p.GenerateUploadUrl,
		"Function.GenerateDownloadUrl":        p.GenerateDownloadUrl,
		"Function.ListLocations":              p.ListLocations,
		"Function.GetLocation":                p.GetLocation,
		"Function.FunctionGetIamPolicy":       p.FunctionGetIamPolicy,
		"Function.FunctionSetIamPolicy":       p.FunctionSetIamPolicy,
		"Function.FunctionTestIamPermissions": p.FunctionTestIamPermissions,
	}
}

// defaultHttpsTriggerURL derives the deployed HTTPS URL for an HTTP-triggered
// function (region-project.cloudfunctions.net/{name}).
func defaultHttpsTriggerURL(project, location, id string) string {
	return fmt.Sprintf("https://%s-%s.cloudfunctions.net/%s", location, project, id)
}

// functionID extracts the function ID from a relative name
// ("locations/{l}/functions/{id}") or a full GCP name.
func functionID(name string) string {
	if i := strings.Index(name, "/functions/"); i >= 0 {
		return name[i+len("/functions/"):]
	}
	return strings.TrimPrefix(name, "functions/")
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidArgument", "missing resource name", 400)
	}
	return n, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	if body == nil {
		return nil
	}
	m, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func bodyEventTrigger(body map[string]any) *functionsstore.EventTrigger {
	if body == nil {
		return nil
	}
	et, ok := body["eventTrigger"].(map[string]any)
	if !ok {
		return nil
	}
	out := &functionsstore.EventTrigger{
		EventType: stringOf(et["eventType"]),
		Resource:  stringOf(et["resource"]),
		Service:   stringOf(et["service"]),
	}
	return out
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// functionFromBody builds the store Function from a request body, deriving the
// HTTPS trigger URL when no eventTrigger is present.
func functionFromBody(nr *model.NormalizedRequest, body map[string]any, location, id string) functionsstore.Function {
	now := clock.Now().UTC()
	f := functionsstore.Function{
		ID:                   id,
		Location:             location,
		Runtime:              bodyString(body, "runtime"),
		EntryPoint:           bodyString(body, "entryPoint"),
		SourceUploadURL:      bodyString(body, "sourceUploadUrl"),
		SourceArchiveURL:     bodyString(body, "sourceArchiveUrl"),
		EnvironmentVariables: bodyStringMap(body, "environmentVariables"),
		Status:               "ACTIVE",
		CreateTime:           now,
		UpdateTime:           now,
		Labels:               bodyStringMap(body, "labels"),
		AvailableMemoryMB:    256,
		Timeout:              "60s",
		Description:          bodyString(body, "description"),
	}
	if v := bodyString(body, "timeout"); v != "" {
		f.Timeout = v
	}
	if n, ok := body["availableMemoryMb"].(float64); ok && n > 0 {
		f.AvailableMemoryMB = int(n)
	}
	if et := bodyEventTrigger(body); et != nil {
		f.EventTrigger = et
	} else {
		f.HttpsTriggerURL = defaultHttpsTriggerURL(nr.AccountID, location, id)
	}
	return f
}

// functionToMap renders a store Function as a CloudFunction wire map.
func functionToMap(nr *model.NormalizedRequest, f functionsstore.Function) map[string]any {
	out := map[string]any{
		"name":   nr.ResourceID("cloud-function", f.Location+"/"+f.ID),
		"status": f.Status,
	}
	if !f.UpdateTime.IsZero() {
		out["updateTime"] = f.UpdateTime.Format(time.RFC3339Nano)
	}
	if f.Runtime != "" {
		out["runtime"] = f.Runtime
	}
	if f.EntryPoint != "" {
		out["entryPoint"] = f.EntryPoint
	}
	if f.SourceUploadURL != "" {
		out["sourceUploadUrl"] = f.SourceUploadURL
	}
	if f.SourceArchiveURL != "" {
		out["sourceArchiveUrl"] = f.SourceArchiveURL
	}
	if f.EventTrigger != nil {
		out["eventTrigger"] = map[string]any{
			"eventType": f.EventTrigger.EventType,
			"resource":  f.EventTrigger.Resource,
			"service":   f.EventTrigger.Service,
		}
	} else if f.HttpsTriggerURL != "" {
		out["httpsTrigger"] = map[string]any{
			"url":           f.HttpsTriggerURL,
			"securityLevel": "SECURE_ALWAYS",
		}
	}
	if f.EnvironmentVariables != nil {
		out["environmentVariables"] = f.EnvironmentVariables
	}
	if f.Labels != nil {
		out["labels"] = f.Labels
	}
	if f.AvailableMemoryMB > 0 {
		out["availableMemoryMb"] = f.AvailableMemoryMB
	}
	if f.Timeout != "" {
		out["timeout"] = f.Timeout
	}
	if f.Description != "" {
		out["description"] = f.Description
	}
	return out
}

func mapErr(err error) error {
	if errors.Is(err, functionsstore.ErrNoSuchFunction) {
		return model.NewProviderError("NotFound", "function not found", 404)
	}
	return err
}

// functionOperation renders a completed google.longrunning.Operation for a
// function mutation. The emulator completes Create/Update/Delete synchronously,
// so done is always true and the operation is never persisted or pollable (the
// shared locations/{location}/operations/{id} path is path-identical to Cloud
// Workflows' LRO surface and routes there). response is the updated Function for
// create/update and an empty object for delete, matching real Cloud Functions v1.
func functionOperation(nr *model.NormalizedRequest, location, verb, target string, response map[string]any) map[string]any {
	now := clock.Now().UTC()
	return map[string]any{
		"name": nr.ResourceID("cloud-function-operation", location+"/"+uuid.New().String()),
		"metadata": map[string]any{
			"@type":         operationMetadataType,
			"createTime":    now.Format(time.RFC3339Nano),
			"endTime":       now.Format(time.RFC3339Nano),
			"target":        target,
			"verb":          verb,
			"operationType": strings.ToUpper(verb) + "_FUNCTION",
			"apiVersion":    "v1",
		},
		"done":     true,
		"response": response,
	}
}

// parseFunctionResourceName validates a function resource name and returns its
// location and id. Both the full form
// ("projects/{p}/locations/{l}/functions/{id}") and the relative form
// ("locations/{l}/functions/{id}") are accepted; anything missing the required
// "locations/{l}/functions/{id}" shape is rejected. Source archive references
// are deliberately not validated (see README-GCP.md).
func parseFunctionResourceName(name string) (location, id string, err error) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "locations":
			location = parts[i+1]
		case "functions":
			id = parts[i+1]
		}
	}
	if location == "" || id == "" {
		return "", "", model.NewProviderError("InvalidArgument",
			"malformed function resource name "+name, 400)
	}
	return location, id, nil
}

// validateCreateName cross-checks an optional request-body name against the
// requested location and derives the function id when functionId is absent.
func validateCreateName(name, location string) (id string, err error) {
	if name == "" {
		return "", nil
	}
	loc, fid, err := parseFunctionResourceName(name)
	if err != nil {
		return "", err
	}
	if location != "" && loc != location {
		return "", model.NewProviderError("InvalidArgument",
			"function name location "+loc+" does not match request location "+location, 400)
	}
	return fid, nil
}

// functionUpdateFields maps every accepted updateMask path (lowerCamel or
// snake_case) to its canonical CloudFunction field name. A path absent from
// this set fails loud with Unimplemented rather than being silently ignored.
var functionUpdateFields = map[string]string{
	"runtime":               "runtime",
	"entrypoint":            "entryPoint",
	"entry_point":           "entryPoint",
	"sourceuploadurl":       "sourceUploadUrl",
	"source_upload_url":     "sourceUploadUrl",
	"sourcearchiveurl":      "sourceArchiveUrl",
	"source_archive_url":    "sourceArchiveUrl",
	"environmentvariables":  "environmentVariables",
	"environment_variables": "environmentVariables",
	"labels":                "labels",
	"description":           "description",
	"timeout":               "timeout",
	"availablememorymb":     "availableMemoryMb",
	"available_memory_mb":   "availableMemoryMb",
	"eventtrigger":          "eventTrigger",
	"event_trigger":         "eventTrigger",
}

// canonicalMaskField normalizes an updateMask path to its canonical field name.
// A leading "function." (the proto request wraps the resource) is stripped.
func canonicalMaskField(path string) (string, bool) {
	p := strings.ToLower(strings.TrimSpace(path))
	p = strings.TrimPrefix(p, "function.")
	f, ok := functionUpdateFields[p]
	return f, ok
}

// splitMask splits a comma-separated updateMask into non-empty paths.
func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// applyFunctionUpdate merges a PATCH body into f. A non-empty mask names the
// paths to overlay (masked paths take the incoming body value, unmasked paths
// retain the stored value); an empty mask overlays every mutable field present
// in the body. Every masked path is validated up front so an unsupported field
// fails loud with Unimplemented instead of being silently no-op-ed.
func applyFunctionUpdate(f *functionsstore.Function, body map[string]any, mask string) error {
	masked := splitMask(mask)
	for _, m := range masked {
		if _, ok := canonicalMaskField(m); !ok {
			return model.NewProviderError("Unimplemented", "unsupported updateMask field "+m, 501)
		}
	}
	apply := func(field string) bool {
		if len(masked) == 0 {
			return true
		}
		for _, m := range masked {
			if c, ok := canonicalMaskField(m); ok && c == field {
				return true
			}
		}
		return false
	}
	if apply("runtime") {
		if v := bodyString(body, "runtime"); v != "" {
			f.Runtime = v
		}
	}
	if apply("entryPoint") {
		if v := bodyString(body, "entryPoint"); v != "" {
			f.EntryPoint = v
		}
	}
	if apply("sourceUploadUrl") {
		if v := bodyString(body, "sourceUploadUrl"); v != "" {
			f.SourceUploadURL = v
		}
	}
	if apply("sourceArchiveUrl") {
		if v := bodyString(body, "sourceArchiveUrl"); v != "" {
			f.SourceArchiveURL = v
		}
	}
	if apply("environmentVariables") {
		if env := bodyStringMap(body, "environmentVariables"); env != nil {
			f.EnvironmentVariables = env
		}
	}
	if apply("labels") {
		if labels := bodyStringMap(body, "labels"); labels != nil {
			f.Labels = labels
		}
	}
	if apply("description") {
		if v := bodyString(body, "description"); v != "" {
			f.Description = v
		}
	}
	if apply("timeout") {
		if v := bodyString(body, "timeout"); v != "" {
			f.Timeout = v
		}
	}
	if apply("availableMemoryMb") {
		if n, ok := body["availableMemoryMb"].(float64); ok && n > 0 {
			f.AvailableMemoryMB = int(n)
		}
	}
	if apply("eventTrigger") {
		if et := bodyEventTrigger(body); et != nil {
			f.EventTrigger = et
		}
	}
	return nil
}

func (p *Provider) CreateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	id, err := validateCreateName(bodyString(body, "name"), location)
	if err != nil {
		return nil, err
	}
	if fid := strParam(nr, "functionId"); fid != "" {
		id = fid
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing functionId", 400)
	}
	if bodyString(body, "runtime") == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing runtime", 400)
	}
	f := functionFromBody(nr, body, location, id)
	if err := p.functions.CreateFunction(ctx, nr.AccountID, location, id, f); err != nil {
		if errors.Is(err, functionsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "function already exists", 409)
		}
		return nil, err
	}
	return provider.OK(functionOperation(nr, location, "create",
		nr.ResourceID("cloud-function", location+"/"+id), functionToMap(nr, f))), nil
}

func (p *Provider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if _, _, err := parseFunctionResourceName(name); err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	f, err := p.functions.GetFunction(ctx, nr.AccountID, location, functionID(name))
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(functionToMap(nr, f)), nil
}

func (p *Provider) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	// "-" is GCP's all-locations wildcard (locations/-/functions): aggregate
	// across every region for the project instead of an exact-match lookup,
	// which would always be empty since no function is ever stored under the
	// literal location "-".
	var fns []functionsstore.Function
	var err error
	if location == "-" {
		fns, err = p.functions.ListFunctionsAllLocations(ctx, nr.AccountID)
	} else {
		fns, err = p.functions.ListFunctions(ctx, nr.AccountID, location)
	}
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(fns, func(f functionsstore.Function) string { return f.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, f := range page {
		items = append(items, functionToMap(nr, f))
	}
	resp := map[string]any{"functions": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if _, _, err := parseFunctionResourceName(name); err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	id := functionID(name)
	body, _ := nr.Params["body"].(map[string]any)
	mask := strParam(nr, "updateMask")
	if mask == "" {
		mask = strParam(nr, "update_mask")
	}
	f, err := p.functions.UpdateFunctionAtomic(ctx, nr.AccountID, location, id, func(f functionsstore.Function) (functionsstore.Function, error) {
		if err := applyFunctionUpdate(&f, body, mask); err != nil {
			return functionsstore.Function{}, err
		}
		f.UpdateTime = clock.Now().UTC()
		return f, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(functionOperation(nr, location, "update",
		nr.ResourceID("cloud-function", location+"/"+id), functionToMap(nr, f))), nil
}

func (p *Provider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if _, _, err := parseFunctionResourceName(name); err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	id := functionID(name)
	if err := p.functions.DeleteFunction(ctx, nr.AccountID, location, id); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(functionOperation(nr, location, "delete",
		nr.ResourceID("cloud-function", location+"/"+id), map[string]any{})), nil
}

// CallFunction invokes a function synchronously via the Lambda executor. The
// request payload is the CallFunctionRequest.data string; the executor (mock
// echo by default, Docker/K8s under JAISCLOUD_EXECUTOR_MODE) runs the
// function's entryPoint and returns the result as a string.
func (p *Provider) CallFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	id := functionID(name)
	f, err := p.functions.GetFunction(ctx, nr.AccountID, location, id)
	if err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)
	data := bodyString(body, "data")

	timeout, err := time.ParseDuration(f.Timeout)
	if err != nil || timeout <= 0 {
		timeout = 60 * time.Second
	}
	invCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := lambdaexec.InvokeRequest{
		FunctionName: f.ID,
		Runtime:      f.Runtime,
		Handler:      f.EntryPoint,
		EnvVars:      f.EnvironmentVariables,
		Payload:      []byte(data),
		AccountID:    nr.AccountID,
		MemoryMB:     f.AvailableMemoryMB,
		TimeoutSecs:  int(timeout.Seconds()),
	}
	executionID := uuid.New().String()
	result, err := p.executor.Invoke(invCtx, req)
	if err != nil {
		return provider.OK(map[string]any{
			"executionId": executionID,
			"error":       err.Error(),
		}), nil
	}
	return provider.OK(map[string]any{
		"executionId": executionID,
		"result":      string(result.Payload),
	}), nil
}

// GenerateUploadUrl returns a fake signed upload URL for source deployment.
func (p *Provider) GenerateUploadUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	project := nr.AccountID
	return provider.OK(map[string]any{
		"uploadUrl": fmt.Sprintf("https://storage.googleapis.com/uploads/%s/%s/%s.zip",
			project, location, uuid.New().String()),
	}), nil
}

// GenerateDownloadUrl returns a fake signed download URL for a function's
// source archive. The function must exist (NotFound otherwise), matching real
// Cloud Functions v1. The URL is synthesized and nothing is served from it.
func (p *Provider) GenerateDownloadUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if _, _, err := parseFunctionResourceName(name); err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"downloadUrl": fmt.Sprintf("https://storage.googleapis.com/%s-cloudfunctions/%s/%s.zip",
			nr.AccountID, strParam(nr, "location"), functionID(name)),
	}), nil
}

// GetLocation returns a synthesized google.cloud.location.Location for a
// region. The bare locations paths are owned by the Memorystore detector on the
// shared emulator host; this handler backs direct Function.GetLocation dispatch.
func (p *Provider) GetLocation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	return provider.OK(functionLocationMap(nr, location)), nil
}

// ListLocations returns the synthesized function region set, honoring
// pageSize/pageToken via the shared paging helper.
func (p *Provider) ListLocations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	locations := make([]map[string]any, 0, len(functionRegions))
	for _, r := range functionRegions {
		locations = append(locations, functionLocationMap(nr, r))
	}
	page, next := paging.Page(locations, func(m map[string]any) string { return m["locationId"].(string) }, nr.Params)
	items := make([]any, 0, len(page))
	for _, l := range page {
		items = append(items, l)
	}
	resp := map[string]any{"locations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// functionLocationMap renders a google.cloud.location.Location for a region.
func functionLocationMap(nr *model.NormalizedRequest, location string) map[string]any {
	return map[string]any{
		"name":        nr.ResourceID("cloud-function-location", location),
		"locationId":  location,
		"displayName": location,
	}
}

func (p *Provider) requireFunction(ctx context.Context, nr *model.NormalizedRequest) error {
	name, err := resourceName(nr)
	if err != nil {
		return err
	}
	if _, _, err := parseFunctionResourceName(name); err != nil {
		return err
	}
	if _, err := p.functions.GetFunction(ctx, nr.AccountID, strParam(nr, "location"), functionID(name)); err != nil {
		return mapErr(err)
	}
	return nil
}

func (p *Provider) FunctionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	id := strParam(nr, "location") + "/" + functionID(name)
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtFunctionPolicy, id))), nil
}

func (p *Provider) FunctionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtFunctionPolicy, strParam(nr, "location")+"/"+functionID(name), body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) FunctionTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := resourceName(nr); err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}
