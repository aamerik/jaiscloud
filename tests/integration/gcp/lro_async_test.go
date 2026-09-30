package gcp_test

import (
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file is the live end-to-end gate for the opt-in async long-running
// operation (LRO) mode. It is skipped unless JAISCLOUD_LRO_ASYNC=1, so the
// default (synchronous) `make test-integration-gcp` run stays green.
// `make test-lro-async-gcp` starts jaiscloud-gcp with
// JAISCLOUD_LRO_MODE=async and sets the variable, then runs TestLROAsync.

const (
	lroProject  = "proj"
	lroLocation = "us-central1"
)

// lroJSONHeaders is the create-content type every LRO create expects.
func lroJSONHeaders() map[string]string {
	return map[string]string{"Content-Type": "application/json"}
}

// TestLROAsync proves that in the opt-in async mode a create returns an
// in-flight operation (done:false) whose location-scoped name can be polled
// through the REST operations.get surface until it settles (done:true with the
// resource in response). Each service shares the
// projects/{project}/locations/{location}/operations/{id} namespace on the
// single emulator host.
func TestLROAsync(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	resetState(t)

	cases := []struct {
		name       string
		createPath string
		createBody string
		wantName   string
	}{
		{
			name:       "workflows",
			createPath: "/v1/projects/" + lroProject + "/locations/" + lroLocation + "/workflows?workflowId=async-wf",
			createBody: `{"sourceContents":"main:\n  steps:\n    - return: ok"}`,
			wantName:   "projects/" + lroProject + "/locations/" + lroLocation + "/workflows/async-wf",
		},
		{
			name:       "metastore",
			createPath: "/v1/projects/" + lroProject + "/locations/" + lroLocation + "/services?serviceId=async-svc",
			createBody: `{}`,
			wantName:   "projects/" + lroProject + "/locations/" + lroLocation + "/services/async-svc",
		},
		{
			name:       "managedkafka",
			createPath: "/v1/projects/" + lroProject + "/locations/" + lroLocation + "/clusters?clusterId=async-c1",
			createBody: `{}`,
			wantName:   "projects/" + lroProject + "/locations/" + lroLocation + "/clusters/async-c1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := do(t, "POST", tc.createPath, []byte(tc.createBody), lroJSONHeaders())
			require.Equal(t, http.StatusOK, resp.StatusCode, "create: %s", body)

			op := jsonMap(t, body)
			done, ok := op["done"].(bool)
			require.True(t, ok, "create operation has no boolean done: %s", body)
			require.False(t, done, "async create must return done:false: %s", body)

			name, _ := op["name"].(string)
			require.Regexp(t,
				regexp.MustCompile(`^projects/`+regexp.QuoteMeta(lroProject)+`/locations/`+regexp.QuoteMeta(lroLocation)+`/operations/[^/]+$`),
				name, "operation name must be location-scoped: %s", body)
			_, hasResponse := op["response"]
			require.False(t, hasResponse, "in-flight operation must not carry response: %s", body)

			// Poll operations.get until the operation settles. The window is
			// JAISCLOUD_LRO_DELAY (2s in the make target); allow far more than
			// that for scheduling jitter.
			pollPath := "/v1/" + name
			deadline := time.Now().Add(20 * time.Second)
			for {
				presp, pbody := do(t, "GET", pollPath, nil, nil)
				require.Equal(t, http.StatusOK, presp.StatusCode, "poll: %s", pbody)
				settled := jsonMap(t, pbody)
				if isDone, _ := settled["done"].(bool); isDone {
					response, ok := settled["response"].(map[string]any)
					require.True(t, ok, "settled operation must carry response: %s", pbody)
					require.Equal(t, tc.wantName, response["name"], "settled response name: %s", pbody)
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("operation %s did not settle within 20s; last: %s", name, pbody)
				}
				time.Sleep(200 * time.Millisecond)
			}
		})
	}
}

// TestLROAsyncServiceUsage proves the top-level operations/{id} namespace is
// pollable in async mode: a Service Usage enable returns an in-flight operation
// named operations/{id}, and GET /v1/operations/{id} resolves it through the
// Service Usage fallback wired into the Functions v1 REST route (which owns the
// shared path) until it settles with the typed EnableServiceResponse.
func TestLROAsyncServiceUsage(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	resetState(t)

	const service = "run.googleapis.com"
	resp, body := do(t, "POST", "/v1/projects/"+lroProject+"/services/"+service+":enable", nil, lroJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "enable: %s", body)

	op := jsonMap(t, body)
	done, ok := op["done"].(bool)
	require.True(t, ok, "enable operation has no boolean done: %s", body)
	require.False(t, done, "async enable must return done:false: %s", body)

	name, _ := op["name"].(string)
	require.Regexp(t, regexp.MustCompile(`^operations/[^/]+$`), name,
		"serviceusage operation name must be top-level: %s", body)
	_, hasResponse := op["response"]
	require.False(t, hasResponse, "in-flight operation must not carry response: %s", body)

	pollPath := "/v1/" + name
	deadline := time.Now().Add(20 * time.Second)
	for {
		presp, pbody := do(t, "GET", pollPath, nil, nil)
		require.Equal(t, http.StatusOK, presp.StatusCode, "poll: %s", pbody)
		settled := jsonMap(t, pbody)
		if isDone, _ := settled["done"].(bool); isDone {
			response, ok := settled["response"].(map[string]any)
			require.True(t, ok, "settled operation must carry response: %s", pbody)
			svc, ok := response["service"].(map[string]any)
			require.True(t, ok, "settled response must carry service: %s", pbody)
			require.Equal(t, "projects/"+lroProject+"/services/"+service, svc["name"],
				"settled service name: %s", pbody)
			require.Equal(t, "ENABLED", svc["state"], "settled service state: %s", pbody)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s did not settle within 20s; last: %s", name, pbody)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
