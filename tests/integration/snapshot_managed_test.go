package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipIfNoDataDir calls GET /_jaiscloud/doctor?verbose=true and skips the test
// if the server is not running or if the data_dir field is empty.
func skipIfNoDataDir(t *testing.T) {
	t.Helper()
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/doctor?verbose=true")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skip("requires --data-dir")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Skip("requires --data-dir")
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Skip("requires --data-dir")
	}
	dataDir, _ := result["data_dir"].(string)
	if dataDir == "" {
		t.Skip("requires --data-dir")
	}
}

// createSnapshot POSTs {"name": name, "description": description} to /_jaiscloud/snapshot
// and returns the HTTP status code.
func createSnapshot(t *testing.T, name, description string) int {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"name":        name,
		"description": description,
	})
	require.NoError(t, err)
	resp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/snapshot",
		"application/json",
		bytes.NewReader(payload),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// listSnapshots GETs /_jaiscloud/snapshots and returns the parsed JSON array.
// Asserts 200 status code.
func listSnapshots(t *testing.T) []map[string]any {
	t.Helper()
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/snapshots")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result []map[string]any
	require.NoError(t, json.Unmarshal(body, &result))
	return result
}

// deleteSnapshot DELETEs /_jaiscloud/snapshot/{name}?yes=true and returns the HTTP status code.
func deleteSnapshot(t *testing.T, name string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/_jaiscloud/snapshot/%s?yes=true", jaiscloudEndpoint, name),
		nil,
	)
	require.NoError(t, err)
	client := &http.Client{}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// ─── Test 1: Create a named snapshot ─────────────────────────────────────────

func TestSnapshot_Create_Named_HTTP(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	// Seed a queue so there is something to snapshot.
	sqsClient := newSQSClient(t)
	_, err = sqsClient.CreateQueue(context.Background(), &sqssvc.CreateQueueInput{
		QueueName: aws.String("snap-create-seed"),
	})
	require.NoError(t, err)

	payload, err := json.Marshal(map[string]string{
		"name":        "test-snap-1",
		"description": "my first snapshot",
	})
	require.NoError(t, err)

	createResp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/snapshot",
		"application/json",
		bytes.NewReader(payload),
	)
	require.NoError(t, err)
	defer createResp.Body.Close()

	assert.True(t, createResp.StatusCode == http.StatusOK || createResp.StatusCode == http.StatusCreated,
		"expected 200 or 201, got %d", createResp.StatusCode)

	body, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Equal(t, "test-snap-1", result["name"])

	deleteSnapshot(t, "test-snap-1")
}

// ─── Test 2: List snapshots ────────────────────────────────────────────────────

func TestSnapshot_List_HTTP(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	statusA := createSnapshot(t, "list-snap-a", "snap a")
	assert.True(t, statusA == http.StatusOK || statusA == http.StatusCreated,
		"expected 200 or 201 creating list-snap-a, got %d", statusA)

	statusB := createSnapshot(t, "list-snap-b", "snap b")
	assert.True(t, statusB == http.StatusOK || statusB == http.StatusCreated,
		"expected 200 or 201 creating list-snap-b, got %d", statusB)

	snapshots := listSnapshots(t)

	var names []string
	for _, s := range snapshots {
		if n, ok := s["name"].(string); ok {
			names = append(names, n)
		}
	}
	assert.Contains(t, names, "list-snap-a")
	assert.Contains(t, names, "list-snap-b")

	deleteSnapshot(t, "list-snap-a")
	deleteSnapshot(t, "list-snap-b")
}

// ─── Test 3: List snapshots newest first ─────────────────────────────────────

func TestSnapshot_List_NewestFirst(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	statusOld := createSnapshot(t, "oldest-snap", "the older one")
	assert.True(t, statusOld == http.StatusOK || statusOld == http.StatusCreated,
		"expected 200 or 201 creating oldest-snap, got %d", statusOld)

	time.Sleep(10 * time.Millisecond)

	statusNew := createSnapshot(t, "newest-snap", "the newer one")
	assert.True(t, statusNew == http.StatusOK || statusNew == http.StatusCreated,
		"expected 200 or 201 creating newest-snap, got %d", statusNew)

	snapshots := listSnapshots(t)
	require.NotEmpty(t, snapshots, "expected at least one snapshot in list")

	firstName, _ := snapshots[0]["name"].(string)
	assert.Equal(t, "newest-snap", firstName,
		"expected newest-snap to be listed first (newest-first ordering)")

	deleteSnapshot(t, "oldest-snap")
	deleteSnapshot(t, "newest-snap")
}

// ─── Test 4: Inspect a snapshot ───────────────────────────────────────────────

func TestSnapshot_Inspect_HTTP(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	status := createSnapshot(t, "inspect-snap", "for inspection")
	require.True(t, status == http.StatusOK || status == http.StatusCreated,
		"expected 200 or 201 creating inspect-snap, got %d", status)

	inspectResp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/snapshot/inspect-snap")
	require.NoError(t, err)
	defer inspectResp.Body.Close()

	assert.Equal(t, http.StatusOK, inspectResp.StatusCode)

	body, err := io.ReadAll(inspectResp.Body)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Equal(t, "inspect-snap", result["name"])

	deleteSnapshot(t, "inspect-snap")
}

// ─── Test 5: Inspect a non-existent snapshot returns 404 ─────────────────────

func TestSnapshot_Inspect_NotFound(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)

	inspectResp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/snapshot/does-not-exist-12345")
	require.NoError(t, err)
	defer inspectResp.Body.Close()
	io.Copy(io.Discard, inspectResp.Body)

	assert.Equal(t, http.StatusNotFound, inspectResp.StatusCode)
}

// ─── Test 6: Delete a snapshot ────────────────────────────────────────────────

func TestSnapshot_Delete_HTTP(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	status := createSnapshot(t, "delete-me", "to be deleted")
	require.True(t, status == http.StatusOK || status == http.StatusCreated,
		"expected 200 or 201 creating delete-me, got %d", status)

	delStatus := deleteSnapshot(t, "delete-me")
	assert.True(t, delStatus == http.StatusOK || delStatus == http.StatusNoContent,
		"expected 200 or 204 deleting delete-me, got %d", delStatus)

	// Verify the snapshot is gone.
	inspectResp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/snapshot/delete-me")
	require.NoError(t, err)
	defer inspectResp.Body.Close()
	io.Copy(io.Discard, inspectResp.Body)

	assert.Equal(t, http.StatusNotFound, inspectResp.StatusCode,
		"snapshot should be gone after deletion")
}

// ─── Test 7: Delete a non-existent snapshot ───────────────────────────────────
// Note: the server uses os.RemoveAll which silently succeeds for missing dirs,
// so deleting a non-existent snapshot returns 200 (not 404).

func TestSnapshot_Delete_NotFound(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)

	// os.RemoveAll does not error on a missing path, so the handler returns 200.
	delStatus := deleteSnapshot(t, "no-such-snap")
	assert.Equal(t, http.StatusOK, delStatus,
		"deleting a non-existent snapshot should return 200 (os.RemoveAll is idempotent)")
}

// ─── Test 8: Revert a snapshot restores state ─────────────────────────────────

func TestSnapshot_Revert_HTTP(t *testing.T) {
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()

	skipIfNoDataDir(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	// Create a queue before taking the snapshot.
	sqsClient := newSQSClient(t)
	_, err = sqsClient.CreateQueue(context.Background(), &sqssvc.CreateQueueInput{
		QueueName: aws.String("pre-snapshot-queue"),
	})
	require.NoError(t, err)

	// Take the snapshot.
	status := createSnapshot(t, "revert-snap", "snapshot with queue")
	require.True(t, status == http.StatusOK || status == http.StatusCreated,
		"expected 200 or 201 creating revert-snap, got %d", status)

	// Wipe all state.
	resetState(t)

	// Verify the queue is gone before revert.
	listOut, err := sqsClient.ListQueues(context.Background(), &sqssvc.ListQueuesInput{
		QueueNamePrefix: aws.String("pre-snapshot-queue"),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.QueueUrls, "queue should be gone after reset")

	// Revert to the snapshot with reset_first=true.
	revertResp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/snapshot/revert-snap/revert?reset_first=true",
		"application/json",
		nil,
	)
	require.NoError(t, err)
	defer revertResp.Body.Close()
	revertBody, err := io.ReadAll(revertResp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, revertResp.StatusCode,
		"revert should return 200, body: %s", string(revertBody))

	// Verify the queue is restored.
	listAfter, err := sqsClient.ListQueues(context.Background(), &sqssvc.ListQueuesInput{
		QueueNamePrefix: aws.String("pre-snapshot-queue"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listAfter.QueueUrls, "queue should be restored after revert")

	found := false
	for _, u := range listAfter.QueueUrls {
		if strings.Contains(u, "pre-snapshot-queue") {
			found = true
			break
		}
	}
	assert.True(t, found, "pre-snapshot-queue should appear in queue list after revert")

	// Clean up.
	deleteSnapshot(t, "revert-snap")
}

// Ensure fmt is used (to suppress "imported and not used" if inlined calls are removed).
var _ = fmt.Sprintf
