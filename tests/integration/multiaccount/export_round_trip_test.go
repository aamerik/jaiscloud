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
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
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

// TestExportImport_ESMRoundTrip verifies that an ESM created before export is
// restored and its poller restarted after an import, so GetEventSourceMapping
// returns the mapping and it is still in an enabled state.
func TestExportImport_ESMRoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	lambdaC := newLambdaFor(t, AcctA)

	// Create the function and queue the ESM will reference.
	_, err := lambdaC.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("esm-export-fn"),
		Runtime:      lambdatypes.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::" + AcctA + ":role/lambda-role"),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: []byte("fake-zip")},
	})
	require.NoError(t, err)

	sqsC := newSQSFor(t, AcctA)
	_, err = sqsC.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("esm-export-queue")})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:" + AcctA + ":esm-export-queue"
	createOut, err := lambdaC.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-export-fn"),
		EventSourceArn: aws.String(queueARN),
		BatchSize:      aws.Int32(5),
	})
	require.NoError(t, err)
	esmUUID := aws.ToString(createOut.UUID)
	require.NotEmpty(t, esmUUID)

	// Export state.
	exportResp, err := http.Get(endpoint + "/_jaiscloud/export")
	require.NoError(t, err)
	snapshot, err := io.ReadAll(exportResp.Body)
	exportResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, exportResp.StatusCode)

	// Wipe all state.
	resetState(t)

	// Confirm the ESM is gone.
	_, err = lambdaC.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{UUID: aws.String(esmUUID)})
	require.Error(t, err, "ESM should not exist after reset")

	// Import the snapshot.
	importResp, err := http.Post(endpoint+"/_jaiscloud/import", "application/octet-stream",
		bytes.NewReader(snapshot))
	require.NoError(t, err)
	importResp.Body.Close()
	require.Equal(t, http.StatusOK, importResp.StatusCode)

	// ESM record should be restored.
	getOut, err := lambdaC.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err, "ESM should be restored after import")
	assert.Equal(t, esmUUID, aws.ToString(getOut.UUID))
	assert.Equal(t, queueARN, aws.ToString(getOut.EventSourceArn))
	assert.Contains(t, aws.ToString(getOut.FunctionArn), "esm-export-fn")
	assert.NotEqual(t, "Disabled", aws.ToString(getOut.State), "ESM should not be disabled after restore")
}
