package functions

import (
	"time"

	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// FunctionJSON renders a stored Function in the wire shape matching the given
// API version. It is the single source of truth for the function shape, shared
// by the REST provider and the gRPC transport.
func FunctionJSON(v Version, project string, f functionsstore.Function) map[string]any {
	if v == V2 {
		return functionJSONV2(project, f)
	}
	return functionJSONV1(project, f)
}

// functionJSONV1 renders the Cloud Functions v1 CloudFunction shape.
func functionJSONV1(project string, f functionsstore.Function) map[string]any {
	out := map[string]any{
		"name":   resourceID(project)("cloud-function", f.Location+"/"+f.ID),
		"status": f.Status,
	}
	if !f.UpdateTime.IsZero() {
		out["updateTime"] = formatTimestamp(f.UpdateTime)
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

// functionSourceV2 renders the v2 BuildConfig.source oneof from the stored
// source reference. A gs:// archive becomes a storageSource; a stored signed
// sourceUploadUrl becomes a sourceUploadUrl; anything else yields nil (the
// field is omitted).
func functionSourceV2(f functionsstore.Function) map[string]any {
	ref := f.SourceArchiveURL
	if !hasGSPrefix(ref) {
		if f.SourceUploadURL != "" {
			return map[string]any{"sourceUploadUrl": f.SourceUploadURL}
		}
		return nil
	}
	rest := ref[len("gs://"):]
	i := indexByte(rest, '/')
	if i <= 0 || i >= len(rest)-1 {
		return nil
	}
	return map[string]any{
		"storageSource": map[string]any{
			"bucket": rest[:i],
			"object": rest[i+1:],
		},
	}
}

// functionJSONV2 renders the Cloud Functions v2 Function shape: state,
// environment, buildConfig (runtime/entryPoint/source), serviceConfig
// (service/uri), and the shared metadata. functionJSONV1 is deliberately left
// unchanged.
func functionJSONV2(project string, f functionsstore.Function) map[string]any {
	out := map[string]any{
		"name":  resourceID(project)("cloud-function", f.Location+"/"+f.ID),
		"state": f.Status,
		// Cloud Functions v2 runs on Cloud Run (GEN_2); the field is
		// output-only and every emulated v2 function is GEN_2.
		"environment": "GEN_2",
	}
	if !f.CreateTime.IsZero() {
		out["createTime"] = formatTimestamp(f.CreateTime)
	}
	if !f.UpdateTime.IsZero() {
		out["updateTime"] = formatTimestamp(f.UpdateTime)
	}
	if f.HttpsTriggerURL != "" {
		out["url"] = f.HttpsTriggerURL
	}
	build := map[string]any{}
	if f.Runtime != "" {
		build["runtime"] = f.Runtime
	}
	if f.EntryPoint != "" {
		build["entryPoint"] = f.EntryPoint
	}
	if src := functionSourceV2(f); src != nil {
		build["source"] = src
	}
	if f.EnvironmentVariables != nil {
		build["environmentVariables"] = f.EnvironmentVariables
	}
	if len(build) > 0 {
		out["buildConfig"] = build
	}
	svc := map[string]any{
		// The backing Cloud Run service is named after the function. It is
		// output-only and always rendered (even without a trigger) so clients
		// that read serviceConfig.service get a stable value.
		"service": resourceID(project)("cloud-run-service", f.Location+"/"+f.ID),
	}
	// A deployed function has one synthesized revision (derived from its stored
	// source hash); all traffic serves it. Persisted revision history + traffic
	// splitting are FD5 concerns.
	if rev := functionRevision(f); rev != "" {
		svc["revision"] = resourceID(project)("cloud-function-revision", f.Location+"/"+f.ID+"/"+rev)
		svc["allTrafficOnLatestRevision"] = true
	}
	if f.HttpsTriggerURL != "" {
		svc["uri"] = f.HttpsTriggerURL
	}
	if f.EnvironmentVariables != nil {
		svc["environmentVariables"] = f.EnvironmentVariables
	}
	if f.AvailableMemoryMB > 0 {
		svc["availableMemory"] = fmtIntM(f.AvailableMemoryMB)
	}
	if secs := timeoutSeconds(f.Timeout); secs > 0 {
		svc["timeoutSeconds"] = secs
	}
	if len(svc) > 0 {
		out["serviceConfig"] = svc
	}
	if f.Labels != nil {
		out["labels"] = f.Labels
	}
	if f.Description != "" {
		out["description"] = f.Description
	}
	if f.EventTrigger != nil {
		out["eventTrigger"] = map[string]any{
			"eventType":   f.EventTrigger.EventType,
			"pubsubTopic": f.EventTrigger.Resource,
		}
	}
	return out
}

// Operation is a completed Cloud Functions long-running operation. Function
// mutations complete synchronously, so Done is always true; the operation is
// persisted (see store/functions) so operations.get/list and REST :wait can read
// it back. Function is the create/update response; it is nil for a delete (whose
// response is a google.protobuf.Empty Any).
type Operation struct {
	ID         string
	Location   string
	Verb       string // "create" | "update" | "delete"
	Target     string // full function resource name
	Function   *functionsstore.Function
	CreateTime time.Time
	EndTime    time.Time
}

// NewOperation builds a completed operation for a function mutation. The
// timestamps default to the business clock and are stable once stored.
func NewOperation(location, verb, target string, f *functionsstore.Function) Operation {
	t := now()
	return Operation{ID: newUUID(), Location: location, Verb: verb, Target: target, Function: f, CreateTime: t, EndTime: t}
}

// OperationName returns the full long-running-operation resource name for op.
func OperationName(project string, op Operation) string {
	return resourceID(project)("cloud-function-operation", op.Location+"/"+op.ID)
}

// OperationJSON renders a mutation Operation as a google.longrunning.Operation
// wire map. The response is a typed Any carrying the @type discriminator gax
// clients require to unpack it: the Function for create/update, or
// google.protobuf.Empty for a delete.
func OperationJSON(v Version, project string, op Operation) map[string]any {
	response := anyResponse(emptyTypeURL, nil)
	if op.Function != nil {
		response = anyResponse(functionTypeFor(v), FunctionJSON(v, project, *op.Function))
	}
	return map[string]any{
		"name":     OperationName(project, op),
		"metadata": operationMetadataMap(v, op),
		"done":     true,
		"response": response,
	}
}

// functionTypeFor returns the google.protobuf.Any type URL of the Function
// resource for the given API version.
func functionTypeFor(v Version) string {
	if v == V2 {
		return functionTypeURLV2
	}
	return functionTypeURLV1
}

// anyResponse wraps a rendered resource body as the Any-shaped JSON a
// google.longrunning.Operation response carries, adding the required @type
// discriminator (a missing type URL makes gax fail with "Missing type url when
// parsing"). body is copied, never mutated; a nil body yields the bare @type
// object (e.g. google.protobuf.Empty).
func anyResponse(typeURL string, body map[string]any) map[string]any {
	out := make(map[string]any, len(body)+1)
	out["@type"] = typeURL
	for k, val := range body {
		out[k] = val
	}
	return out
}

// operationMetadataMap renders the version-specific OperationMetadata carried
// on a function operation. v1 uses OperationMetadataV1 ({target, type,
// updateTime}); v2 uses OperationMetadata ({target, verb, operationType,
// apiVersion, createTime, endTime}). Both timestamps are the operation's stored
// values (falling back to the business clock when unset).
func operationMetadataMap(v Version, op Operation) map[string]any {
	start := op.CreateTime
	if start.IsZero() {
		start = now()
	}
	end := op.EndTime
	if end.IsZero() {
		end = start
	}
	if v == V2 {
		return map[string]any{
			"@type":         operationMetadataTypeV2,
			"createTime":    formatTimestamp(start),
			"endTime":       formatTimestamp(end),
			"target":        op.Target,
			"verb":          op.Verb,
			"operationType": operationTypeFor(op.Verb),
			"apiVersion":    string(V2),
		}
	}
	return map[string]any{
		"@type":      operationMetadataType,
		"target":     op.Target,
		"type":       operationTypeFor(op.Verb),
		"updateTime": formatTimestamp(end),
	}
}

// operationTypeFor maps an operation verb to the OperationMetadata
// operationType enum value.
func operationTypeFor(verb string) string {
	switch verb {
	case "create":
		return "CREATE_FUNCTION"
	case "update":
		return "UPDATE_FUNCTION"
	case "delete":
		return "DELETE_FUNCTION"
	}
	return ""
}

// functionRevision derives the rendered revision id for a deployed function from
// its persisted source hash. A function with no stored source has no revision
// (the pre-deploy metadata-only case). FD5 (revisions + traffic) replaces this
// derived value with a persisted revision record.
func functionRevision(f functionsstore.Function) string {
	if len(f.SourceSHA256) > 12 {
		return f.SourceSHA256[:12]
	}
	return f.SourceSHA256
}

// hasGSPrefix reports whether s starts with "gs://".
func hasGSPrefix(s string) bool {
	return len(s) >= 5 && s[:5] == "gs://"
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
