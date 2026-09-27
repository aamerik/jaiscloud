package gcp_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFunctionsV2StorageSourceResolution covers the FD1 source-resolution path
// over the wire: a v2 function whose buildConfig.source.storageSource points at
// an object in the emulated GCS is accepted (the archive is persisted), a
// missing object is rejected with NotFound, and :call still succeeds (mock
// echo in this binary).
func TestFunctionsV2StorageSourceResolution(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	v2base := "/v2/projects/" + project + "/locations/" + location + "/functions"

	createBucket(t, "functions-src-bucket")
	resp, body := do(t, "POST", "/upload/storage/v1/b/functions-src-bucket/o?uploadType=media&name=src.zip",
		[]byte("PK\x03\x04 fake archive"), map[string]string{"Content-Type": "application/zip"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload source: %s", body)

	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"main.handler",` +
		`"source":{"storageSource":{"bucket":"functions-src-bucket","object":"src.zip"}}}}`)
	resp, body = do(t, "POST", v2base+"?functionId=srcfn", create, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "create: %s", body)
	fn, _ := jsonMap(t, body)["response"].(map[string]any)
	bc, _ := fn["buildConfig"].(map[string]any)
	src, _ := bc["source"].(map[string]any)
	ss, _ := src["storageSource"].(map[string]any)
	require.Equal(t, "functions-src-bucket", ss["bucket"])

	// A source reference to a missing object is NotFound (deploy fails).
	missing := []byte(`{"buildConfig":{"runtime":"python312",` +
		`"source":{"storageSource":{"bucket":"functions-src-bucket","object":"nope.zip"}}}}`)
	resp, _ = do(t, "POST", v2base+"?functionId=missingfn", missing, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// The deployed function is invokable (mock echo returns the request data).
	resp, body = do(t, "POST", "/v1/projects/"+project+"/locations/"+location+"/functions/srcfn:call",
		[]byte(`{"data":"ping"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ping", jsonMap(t, body)["result"])
}

// TestFunctionsAcceptanceFlow covers the Cloud Functions v1 wire surface:
// create → get → list → generateDownloadUrl → locations.list → call (mock
// echo) → delete, over the JSON REST API. Create/Update/Delete return a done
// google.longrunning.Operation.
func TestFunctionsAcceptanceFlow(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	base := "/v1/projects/" + project + "/locations/" + location + "/functions"

	// Create (functionId query param) returns a done google.longrunning.Operation
	// whose response is the created Function.
	resp, body := do(t, "POST", base+"?functionId=hello",
		[]byte(`{"runtime":"nodejs20","entryPoint":"helloWorld"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	op := jsonMap(t, body)
	require.Equal(t, true, op["done"])
	fn, _ := op["response"].(map[string]any)
	require.Equal(t, "type.googleapis.com/google.cloud.functions.v1.CloudFunction", fn["@type"])
	require.Equal(t, "projects/proj/locations/us-central1/functions/hello", fn["name"])
	require.Equal(t, "ACTIVE", fn["status"])
	require.Equal(t, "nodejs20", fn["runtime"])

	// Get.
	resp, body = do(t, "GET", base+"/hello", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "helloWorld", jsonMap(t, body)["entryPoint"])

	// List.
	resp, body = do(t, "GET", base, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	items, _ := jsonMap(t, body)["functions"].([]any)
	require.Len(t, items, 1)

	// generateDownloadUrl (custom method) returns a synthesized URL.
	resp, body = do(t, "POST", base+"/hello:generateDownloadUrl",
		[]byte(`{}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, jsonMap(t, body)["downloadUrl"], "storage.googleapis.com")

	// locations.list resolves on the shared project-locations path.
	resp, body = do(t, "GET", "/v1/projects/"+project+"/locations", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	locations, _ := jsonMap(t, body)["locations"].([]any)
	require.NotEmpty(t, locations)

	// Call (mock echo returns the request data).
	resp, body = do(t, "POST", base+"/hello:call",
		[]byte(`{"data":"ping"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	call := jsonMap(t, body)
	require.Equal(t, "ping", call["result"])
	require.NotEmpty(t, call["executionId"])

	// Delete returns a done Operation whose response is a typed
	// google.protobuf.Empty Any (gax unpacks it as Empty).
	resp, body = do(t, "DELETE", base+"/hello", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	del := jsonMap(t, body)
	require.Equal(t, true, del["done"])
	delResp, ok := del["response"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "type.googleapis.com/google.protobuf.Empty", delResp["@type"])
	require.Len(t, delResp, 1)

	// Gone after delete.
	resp, _ = do(t, "GET", base+"/hello", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
