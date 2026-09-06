package gcp_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFunctionsAcceptanceFlow covers the Cloud Functions v1 wire surface:
// create → get → list → call (mock echo) → delete, over the JSON REST API.
func TestFunctionsAcceptanceFlow(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	base := "/v1/projects/" + project + "/locations/" + location + "/functions"

	// Create (functionId query param).
	resp, body := do(t, "POST", base+"?functionId=hello",
		[]byte(`{"runtime":"nodejs20","entryPoint":"helloWorld"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	fn := jsonMap(t, body)
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

	// Call (mock echo returns the request data).
	resp, body = do(t, "POST", base+"/hello:call",
		[]byte(`{"data":"ping"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	call := jsonMap(t, body)
	require.Equal(t, "ping", call["result"])
	require.NotEmpty(t, call["executionId"])

	// Delete.
	resp, _ = do(t, "DELETE", base+"/hello", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Gone after delete.
	resp, _ = do(t, "GET", base+"/hello", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
