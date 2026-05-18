package multiaccount

import (
	"bytes"
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

	"jaiscloud/internal/admin"
)

// TestExport_SchemaVersion verifies the exported envelope uses schema v3 and
// includes a DefaultRegion field (renamed from Region in v2).
func TestExport_SchemaVersion(t *testing.T) {
	resetState(t)

	resp, err := http.Get(endpoint + "/_jaiscloud/export")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var env admin.SnapshotEnvelope
	require.NoError(t, json.Unmarshal(body, &env))

	assert.Equal(t, 3, env.SchemaVersion, "exported envelope must be schema v3")
	assert.NotContains(t, string(body), `"region":`,
		"v3 envelope must not have the old 'region' key (renamed to 'default_region')")
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
