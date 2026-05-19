package multiaccount

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/persistence/version"
)

// TestExport_SchemaVersion verifies the exported envelope is a gzip tarball
// containing envelope.json with schema_version=3.
func TestExport_SchemaVersion(t *testing.T) {
	resetState(t)

	resp, err := http.Get(endpoint + "/_jaiscloud/export")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The response is a gzip-compressed tar archive.
	gz, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err, "response must be a valid gzip stream")
	defer gz.Close()

	tr := tar.NewReader(gz)
	var envJSON []byte
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr.Name == "envelope.json" {
			envJSON, err = io.ReadAll(tr)
			require.NoError(t, err)
			break
		}
	}
	require.NotNil(t, envJSON, "tarball must contain envelope.json")

	var env version.Envelope
	require.NoError(t, json.Unmarshal(envJSON, &env))
	assert.Equal(t, 3, env.SchemaVersion, "exported envelope must be schema v3")
}

// TestExportImport_RoundTrip seeds two accounts with queues, exports, global-resets,
// imports, and verifies both accounts' queues are restored.
func TestExportImport_RoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	// Seed state in both accounts.
	_, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-round-trip-A")})
	require.NoError(t, err)
	_, err = sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-round-trip-B")})
	require.NoError(t, err)

	// Export.
	exportResp, err := http.Get(endpoint + "/_jaiscloud/export")
	require.NoError(t, err)
	snapshot, err := io.ReadAll(exportResp.Body)
	exportResp.Body.Close()
	require.NoError(t, err)

	// Global reset — wipes all state.
	resetState(t)

	listA, err := sqsA.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, listA.QueueUrls, "after reset A should have no queues")

	// Import the snapshot.
	importResp, err := http.Post(endpoint+"/_jaiscloud/import", "application/json",
		bytes.NewReader(snapshot))
	require.NoError(t, err)
	importResp.Body.Close()
	require.Equal(t, http.StatusOK, importResp.StatusCode)

	// Verify A's queue is restored.
	listA, err = sqsA.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	var foundA bool
	for _, u := range listA.QueueUrls {
		if strings.Contains(u, "q-round-trip-A") {
			foundA = true
		}
	}
	assert.True(t, foundA, "A's queue should be restored after import")

	// Verify B's queue is restored.
	listB, err := sqsB.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	var foundB bool
	for _, u := range listB.QueueUrls {
		if strings.Contains(u, "q-round-trip-B") {
			foundB = true
		}
	}
	assert.True(t, foundB, "B's queue should be restored after import")
}
