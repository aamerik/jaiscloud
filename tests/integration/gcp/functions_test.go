package gcp_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// functionDeliveryRecords parses the emulator export and returns the persisted
// Cloud Functions event-delivery records. Real Cloud Functions exposes no
// delivery API, so the export (the registered "functions" snapshotter) is the
// observation surface for retry / dead-letter outcomes.
func functionDeliveryRecords(t *testing.T) []map[string]any {
	t.Helper()
	resp, body := do(t, "GET", "/_jaiscloud/export", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "export: %s", body)
	gz, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	defer gz.Close()
	tr := tar.NewReader(gz)
	var env struct {
		Stores map[string]json.RawMessage `json:"stores"`
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr.Name != "envelope.json" {
			continue
		}
		raw, rerr := io.ReadAll(tr)
		require.NoError(t, rerr)
		require.NoError(t, json.Unmarshal(raw, &env))
	}
	raw := env.Stores["functions"]
	require.NotEmpty(t, raw, "functions snapshot missing from export")
	var snap struct {
		Deliveries map[string]map[string]struct {
			FunctionID string `json:"functionId"`
			Status     string `json:"status"`
			Result     string `json:"result"`
			Attempts   int    `json:"attempts"`
		} `json:"deliveries"`
	}
	require.NoError(t, json.Unmarshal(raw, &snap))
	var out []map[string]any
	for _, byID := range snap.Deliveries {
		for _, d := range byID {
			out = append(out, map[string]any{
				"functionId": d.FunctionID, "status": d.Status, "result": d.Result, "attempts": d.Attempts,
			})
		}
	}
	return out
}

// waitForDelivery polls until a delivery record for functionID appears.
func waitForDelivery(t *testing.T, functionID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range functionDeliveryRecords(t) {
			if d["functionId"] == functionID {
				return d
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no event delivery recorded for function %s", functionID)
	return nil
}

// TestFunctionsEventTriggerDelivery covers FD4 over the wire: a Pub/Sub publish,
// a GCS object finalize, and an Eventarc trigger (destination.cloudFunction) each
// invoke their function, and the delivery is recorded.
func TestFunctionsEventTriggerDelivery(t *testing.T) {
	resetState(t)
	const project = "proj"
	const location = "us-central1"
	fnBase := "/v1/projects/" + project + "/locations/" + location + "/functions"
	jsonHdr := map[string]string{"Content-Type": "application/json"}

	// A topic-triggered function.
	resp, body := do(t, "PUT", "/v1/projects/"+project+"/topics/events", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "topic create: %s", body)
	resp, body = do(t, "POST", fnBase+"?functionId=onsub",
		[]byte(`{"runtime":"nodejs20","entryPoint":"handler","eventTrigger":{"eventType":"google.pubsub.topic.publish","resource":"projects/proj/topics/events"}}`),
		jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create onsub: %s", body)

	// A bucket-triggered function (v1 object.change catch-all).
	const bucket = "evt-bucket"
	createBucket(t, bucket)
	resp, body = do(t, "POST", fnBase+"?functionId=onstor",
		[]byte(`{"runtime":"nodejs20","entryPoint":"handler","eventTrigger":{"eventType":"providers/cloud.storage/eventTypes/object.change","resource":"projects/_/buckets/evt-bucket"}}`),
		jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create onstor: %s", body)

	// A function with no trigger of its own, targeted by an Eventarc trigger.
	resp, body = do(t, "POST", fnBase+"?functionId=onevarc",
		[]byte(`{"runtime":"nodejs20","entryPoint":"handler"}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create onevarc: %s", body)
	resp, body = do(t, "PUT", "/v1/projects/"+project+"/topics/evarc", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "topic evarc: %s", body)
	resp, body = do(t, "POST", "/v1/projects/"+project+"/locations/"+location+"/triggers?triggerId=evarc-trigger",
		[]byte(`{"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/onevarc"},`+
			`"transport":{"pubsub":{"topic":"projects/proj/topics/evarc"}},`+
			`"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`),
		jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "eventarc trigger: %s", body)

	// Publish to the topic: the topic-subscribed function receives the payload.
	resp, body = do(t, "POST", "/v1/projects/"+project+"/topics/events:publish",
		[]byte(`{"messages":[{"data":"aGVsbG8="}]}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "publish: %s", body)
	d := waitForDelivery(t, "onsub")
	require.Equal(t, "delivered", d["status"])
	require.Equal(t, "hello", d["result"])

	// An object upload finalizes and triggers the bucket function (the payload is
	// the GCS object event JSON).
	resp, body = do(t, "POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name=obj.txt",
		[]byte("world"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload: %s", body)
	d = waitForDelivery(t, "onstor")
	require.Equal(t, "delivered", d["status"])
	require.Contains(t, d["result"], "obj.txt")

	// The Eventarc trigger routes a publish to its function.
	resp, body = do(t, "POST", "/v1/projects/"+project+"/topics/evarc:publish",
		[]byte(`{"messages":[{"data":"dmlh"}]}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "publish evarc: %s", body)
	d = waitForDelivery(t, "onevarc")
	require.Equal(t, "delivered", d["status"])
	require.Equal(t, "via", d["result"])
}

// zipArchive builds a minimal in-memory zip.
func zipArchive(t *testing.T, name, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create(name)
	require.NoError(t, err)
	_, err = f.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// TestFunctionsV2DeployUploadFlow covers FU4 over the wire: generateUploadUrl
// provisions a GCS bucket and returns storageSource; the archive PUT to the
// returned uploadUrl lands in the emulated GCS; a create referencing that
// storageSource persists the source and renders a revision with
// allTrafficOnLatestRevision; the deployed function is invokable.
func TestFunctionsV2DeployUploadFlow(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	base := gcpBase()
	v2base := "/v2/projects/" + project + "/locations/" + location + "/functions"

	// 1. generateUploadUrl (v2) → uploadUrl + storageSource.
	resp, body := do(t, "POST", v2base+":generateUploadUrl", []byte(`{}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "generateUploadUrl: %s", body)
	up := jsonMap(t, body)
	uploadURL, _ := up["uploadUrl"].(string)
	ss, _ := up["storageSource"].(map[string]any)
	require.NotEmpty(t, uploadURL)
	require.True(t, strings.HasPrefix(uploadURL, base+"/"), "uploadUrl %q must point at the emulator %q", uploadURL, base)
	require.NotNil(t, ss)
	bucket, _ := ss["bucket"].(string)
	object, _ := ss["object"].(string)
	require.True(t, strings.HasPrefix(bucket, "gcf-v2-sources-"), "bucket = %q", bucket)
	require.NotEmpty(t, object)

	// The bucket is provisioned lazily by generateUploadUrl.
	resp, _ = do(t, "GET", "/storage/v1/b/"+bucket, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "source bucket must exist")

	// 2. PUT the archive to the returned uploadUrl (a gcloud gen2 deploy does a
	// raw HTTP PUT here; the Storage endpoint override does not apply).
	uploadPath := strings.TrimPrefix(uploadURL, base)
	require.NotEqual(t, uploadURL, uploadPath, "uploadUrl must be under the emulator origin")
	resp, body = do(t, "PUT", uploadPath, zipArchive(t, "main.py", "def handler(e, c):\n    return e\n"),
		map[string]string{"Content-Type": "application/zip"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "source PUT: %s", body)

	// 3. Create the function referencing the uploaded storageSource.
	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"main.handler",` +
		`"source":{"storageSource":{"bucket":"` + bucket + `","object":"` + object + `"}}}}`)
	resp, body = do(t, "POST", v2base+"?functionId=srcfn", create, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "create: %s", body)

	// 4. Describe: the deployed function renders a revision and single-revision
	// traffic.
	resp, body = do(t, "GET", v2base+"/srcfn", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get: %s", body)
	fn := jsonMap(t, body)
	sc, _ := fn["serviceConfig"].(map[string]any)
	require.NotNil(t, sc)
	rev, _ := sc["revision"].(string)
	// The revision is the backing Cloud Run service revision (FD5).
	require.True(t, strings.HasPrefix(rev, "projects/"+project+"/locations/"+location+"/services/srcfn/revisions/srcfn-"),
		"revision = %q", rev)
	require.Equal(t, true, sc["allTrafficOnLatestRevision"])

	// The uploaded object is now a real GCS object.
	resp, _ = do(t, "GET", "/storage/v1/b/"+bucket+"/o/"+object, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 5. A source reference to a missing object is NotFound.
	missing := []byte(`{"buildConfig":{"runtime":"python312",` +
		`"source":{"storageSource":{"bucket":"` + bucket + `","object":"nope.zip"}}}}`)
	resp, _ = do(t, "POST", v2base+"?functionId=missingfn", missing, map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

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

// TestFunctionsInstanceConfig covers FD6 over the wire: the v2 ServiceConfig
// instance/concurrency settings round-trip through create/get/patch, and
// out-of-range values are 400 INVALID_ARGUMENT.
func TestFunctionsInstanceConfig(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	v2base := "/v2/projects/" + project + "/locations/" + location + "/functions"

	resp, body := do(t, "POST", v2base+"?functionId=cfg",
		[]byte(`{"buildConfig":{"runtime":"nodejs22","entryPoint":"h"},`+
			`"serviceConfig":{"minInstanceCount":2,"maxInstanceCount":10,`+
			`"maxInstanceRequestConcurrency":80,"availableCpu":"1"}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	op := jsonMap(t, body)
	fn, _ := op["response"].(map[string]any)
	sc, _ := fn["serviceConfig"].(map[string]any)
	require.Equal(t, float64(2), sc["minInstanceCount"])
	require.Equal(t, float64(10), sc["maxInstanceCount"])
	require.Equal(t, float64(80), sc["maxInstanceRequestConcurrency"])
	require.Equal(t, "1", sc["availableCpu"])

	// Get renders the persisted config.
	resp, body = do(t, "GET", v2base+"/cfg", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	sc, _ = jsonMap(t, body)["serviceConfig"].(map[string]any)
	require.Equal(t, "1", sc["availableCpu"])

	// A masked PATCH applies the new value.
	resp, body = do(t, "PATCH", v2base+"/cfg?updateMask=serviceConfig.maxInstanceCount",
		[]byte(`{"serviceConfig":{"maxInstanceCount":20}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	upd, _ := jsonMap(t, body)["response"].(map[string]any)
	require.Equal(t, float64(20), upd["serviceConfig"].(map[string]any)["maxInstanceCount"])

	// Out-of-range values are rejected.
	resp, body = do(t, "POST", v2base+"?functionId=bad",
		[]byte(`{"buildConfig":{"runtime":"nodejs22"},"serviceConfig":{"maxInstanceRequestConcurrency":1001}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", body)
	require.Contains(t, string(body), "INVALID_ARGUMENT")

	resp, body = do(t, "POST", "/v1/projects/"+project+"/locations/"+location+"/functions?functionId=badmem",
		[]byte(`{"runtime":"nodejs20","availableMemoryMb":300}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", body)
	require.Contains(t, string(body), "INVALID_ARGUMENT")
}

// doHost performs an HTTP request with an explicit Host header. It is used to
// invoke a deployed function at its synthesized HTTPS-trigger URL
// ({location}-{project}.cloudfunctions.net), which is host-scoped rather than
// path-scoped.
func doHost(t *testing.T, method, path, host string, body []byte, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, gcpBase()+path, rd)
	require.NoError(t, err)
	req.Host = host
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, b
}

// TestFunctionsHTTPSTrigger covers FD3 over the wire: a deployed HTTP-triggered
// function is invokable at its synthesized trigger host
// ({location}-{project}.cloudfunctions.net/{id}); the raw request body is the
// payload (any method), while an unknown function and an event-only function
// are 404 (event functions have no HTTPS endpoint).
func TestFunctionsHTTPSTrigger(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	triggerHost := location + "-" + project + ".cloudfunctions.net"
	base := "/v1/projects/" + project + "/locations/" + location + "/functions"

	// HTTP-triggered function.
	resp, body := do(t, "POST", base+"?functionId=web",
		[]byte(`{"runtime":"nodejs20","entryPoint":"handler"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "create: %s", body)

	// The synthesized httpsTrigger.url names the trigger host.
	resp, body = do(t, "GET", base+"/web", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	ht, _ := jsonMap(t, body)["httpsTrigger"].(map[string]any)
	require.Equal(t, "https://"+triggerHost+"/web", ht["url"])

	// Invoke it: the body is the payload and the mock executor echoes it.
	resp, body = doHost(t, "POST", "/web", triggerHost, []byte("ping"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "trigger: %s", body)
	require.Equal(t, "ping", string(body))

	// Any method is accepted (GET with no body).
	resp, _ = doHost(t, "GET", "/web", triggerHost, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Unknown function → 404.
	resp, _ = doHost(t, "POST", "/nope", triggerHost, []byte("x"), nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Event-only function → 404.
	resp, body = do(t, "POST", base+"?functionId=evt",
		[]byte(`{"runtime":"nodejs20","entryPoint":"handler","eventTrigger":{"eventType":"google.pubsub.topic.publish","resource":"projects/proj/topics/t"}}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "create event fn: %s", body)
	resp, _ = doHost(t, "POST", "/evt", triggerHost, []byte("x"), nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// A sub-path still addresses the function (real Cloud Functions route the
	// remainder to the function itself).
	resp, body = doHost(t, "POST", "/web/v1/route", triggerHost, []byte("sub"), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "sub-path trigger: %s", body)
	require.Equal(t, "sub", string(body))

	// A function created in a region outside the advertised catalog still serves
	// its synthesized URL (the ambiguous label is resolved against the store).
	resp, body = do(t, "POST", "/v1/projects/"+project+"/locations/me-west1/functions?functionId=regional",
		[]byte(`{"runtime":"nodejs20","entryPoint":"handler"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "create regional: %s", body)
	resp, body = doHost(t, "POST", "/regional", "me-west1-"+project+".cloudfunctions.net", []byte("regional"), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "regional trigger: %s", body)
	require.Equal(t, "regional", string(body))
}

// TestFunctionsV2UpgradeTraffic covers FD5 over the wire: the seven v2 1st→2nd
// gen upgrade/traffic methods (custom POST verbs on a function name) drive a
// persisted upgradeInfo state machine and flip allTrafficOnLatestRevision, while
// an invalid transition is a 400.
func TestFunctionsV2UpgradeTraffic(t *testing.T) {
	resetState(t)

	const project = "proj"
	const location = "us-central1"
	base := "/v2/projects/" + project + "/locations/" + location + "/functions"
	jsonHdr := map[string]string{"Content-Type": "application/json"}

	// Create a v2 function.
	resp, body := do(t, "POST", base+"?functionId=upg",
		[]byte(`{"buildConfig":{"runtime":"nodejs20","entryPoint":"handler"}}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create: %s", body)

	state := func() string {
		t.Helper()
		resp, body := do(t, "GET", base+"/upg", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, "get: %s", body)
		ui, _ := jsonMap(t, body)["upgradeInfo"].(map[string]any)
		s, _ := ui["upgradeState"].(string)
		return s
	}
	traffic := func() bool {
		t.Helper()
		resp, body := do(t, "GET", base+"/upg", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, "get: %s", body)
		sc, _ := jsonMap(t, body)["serviceConfig"].(map[string]any)
		b, _ := sc["allTrafficOnLatestRevision"].(bool)
		return b
	}

	// setupFunctionUpgradeConfig captures the Gen2 overrides without moving
	// traffic.
	resp, body = do(t, "POST", base+"/upg:setupFunctionUpgradeConfig",
		[]byte(`{"buildConfigOverrides":{"runtime":"nodejs22"},"serviceConfigOverrides":{"maxInstanceCount":4}}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "setup: %s", body)
	require.Equal(t, "SETUP_FUNCTION_UPGRADE_CONFIG_SUCCESSFUL", state())
	require.True(t, traffic())

	// redirect moves traffic to the Gen2 copy; rollback reverts it.
	resp, body = do(t, "POST", base+"/upg:redirectFunctionUpgradeTraffic", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "redirect: %s", body)
	require.Equal(t, "REDIRECT_FUNCTION_UPGRADE_TRAFFIC_SUCCESSFUL", state())
	require.False(t, traffic())

	resp, body = do(t, "POST", base+"/upg:rollbackFunctionUpgradeTraffic", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "rollback: %s", body)
	require.True(t, traffic())

	// abort discards the Gen2 copy.
	resp, body = do(t, "POST", base+"/upg:redirectFunctionUpgradeTraffic", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "re-redirect: %s", body)
	resp, body = do(t, "POST", base+"/upg:abortFunctionUpgrade", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "abort: %s", body)
	require.Empty(t, state())

	// commitAsGen2 is terminal; detach clears the upgrade state.
	resp, body = do(t, "POST", base+"/upg:setupFunctionUpgradeConfig", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "re-setup: %s", body)
	resp, body = do(t, "POST", base+"/upg:redirectFunctionUpgradeTraffic", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "re-redirect: %s", body)
	resp, body = do(t, "POST", base+"/upg:commitFunctionUpgradeAsGen2", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "commitAsGen2: %s", body)
	require.Equal(t, "COMMIT_FUNCTION_UPGRADE_AS_GEN2_SUCCESSFUL", state())
	resp, body = do(t, "POST", base+"/upg:detachFunction", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "detach: %s", body)
	require.Empty(t, state())

	// commitFunctionUpgrade (the plain variant) finalizes after a redirect.
	resp, body = do(t, "POST", base+"/upg:setupFunctionUpgradeConfig", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "setup: %s", body)
	resp, body = do(t, "POST", base+"/upg:redirectFunctionUpgradeTraffic", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "redirect: %s", body)
	resp, body = do(t, "POST", base+"/upg:commitFunctionUpgrade", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "commit: %s", body)
	require.Empty(t, state())

	// An invalid transition (redirect before setup) is a 400.
	resp, body = do(t, "POST", base+"?functionId=fresh",
		[]byte(`{"buildConfig":{"runtime":"nodejs20"}}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create fresh: %s", body)
	resp, body = do(t, "POST", base+"/fresh:redirectFunctionUpgradeTraffic", []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "redirect before setup: %s", body)
}
