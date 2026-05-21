package integration_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamo_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/persistence/version"
)

// skipIfNoServer skips the test if the jaiscloud server is not reachable.
func skipIfNoServer(t *testing.T) {
	t.Helper()
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Skip("jaiscloud server not running")
	}
	resp.Body.Close()
}

// exportSnapshot GETs /_jaiscloud/export and returns the raw bytes.
func exportSnapshot(t *testing.T) []byte {
	t.Helper()
	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/export")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "export must return 200")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body
}

// importSnapshot POSTs body to /_jaiscloud/import?reset_first=true and returns the status code.
// reset_first=true ensures provider-seeded resources (e.g. EC2 default VPC) don't trigger the
// non-empty guard afer a resetState call.
func importSnapshot(t *testing.T, body []byte) int {
	t.Helper()
	resp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/import?reset_first=true",
		"application/octet-stream",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	resp.Body.Close()
	return resp.StatusCode
}

// buildMinimalTarball creates a gzip tarball with only envelope.json from the given Envelope.
func buildMinimalTarball(t *testing.T, env version.Envelope) []byte {
	t.Helper()
	envJSON, err := json.Marshal(env)
	require.NoError(t, err)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	hdr := &tar.Header{
		Name:     "envelope.json",
		Typeflag: tar.TypeReg,
		Size:     int64(len(envJSON)),
		Mode:     0600,
		ModTime:  time.Now(),
	}
	require.NoError(t, tw.WriteHeader(hdr))
	_, err = tw.Write(envJSON)
	require.NoError(t, err)

	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

// TestImport_CloudMismatch_409 verifies that importing a snapshot with a
// mismatched cloud field returns HTTP 409 with a structured CloudMismatchError.
func TestImport_CloudMismatch_409(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	tarball := buildMinimalTarball(t, version.Envelope{
		SchemaVersion: version.CodeSnapshotVersion,
		Cloud:         "gcp",
		Stores:        map[string]json.RawMessage{},
	})

	resp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/import",
		"application/octet-stream",
		bytes.NewReader(tarball),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var mismatch admin.CloudMismatchError
	require.NoError(t, json.Unmarshal(body, &mismatch))
	assert.Equal(t, "cloud_mismatch", mismatch.Code)
	assert.Equal(t, "gcp", mismatch.EnvelopeCloud)
}

// TestImport_RefusesNonEmpty_StructuredError verifies that importing into a
// non-empty server returns HTTP 409 with a structured NonEmptyStateError.
func TestImport_RefusesNonEmpty_StructuredError(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	sqsClient := newSQSClient(t)
	ctx := context.Background()

	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("non-empty-guard-test"),
	})
	require.NoError(t, err)

	snapshot := exportSnapshot(t)

	// Attempt import without resetting — state is non-empty.
	resp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/import",
		"application/octet-stream",
		bytes.NewReader(snapshot),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var nonEmpty admin.NonEmptyStateError
	require.NoError(t, json.Unmarshal(body, &nonEmpty))
	assert.Equal(t, "non_empty_state", nonEmpty.Code)
	assert.NotEmpty(t, nonEmpty.NonEmptyStores)

	hasHint := strings.Contains(nonEmpty.Message, "POST /_jaiscloud/reset") ||
		strings.Contains(nonEmpty.Message, "--fresh-start") ||
		strings.Contains(nonEmpty.Message, "--data-dir")
	assert.True(t, hasHint, "message should contain remediation hint, got: %s", nonEmpty.Message)
}

// TestImport_PathTraversal_Rejected verifies that a tarball containing a
// path-traversal entry is rejected by the server.
func TestImport_PathTraversal_Rejected(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	validEnv := version.Envelope{
		SchemaVersion: version.CodeSnapshotVersion,
		Cloud:         "aws",
		Stores:        map[string]json.RawMessage{},
	}
	envJSON, err := json.Marshal(validEnv)
	require.NoError(t, err)

	traversalContent := []byte("pwned")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	// Write envelope.json.
	envHdr := &tar.Header{
		Name:     "envelope.json",
		Typeflag: tar.TypeReg,
		Size:     int64(len(envJSON)),
		Mode:     0600,
		ModTime:  time.Now(),
	}
	require.NoError(t, tw.WriteHeader(envHdr))
	_, err = tw.Write(envJSON)
	require.NoError(t, err)

	// Write a path-traversal blob entry.
	traversalHdr := &tar.Header{
		Name:     "blobs/../../tmp/pwned",
		Typeflag: tar.TypeReg,
		Size:     int64(len(traversalContent)),
		Mode:     0600,
		ModTime:  time.Now(),
	}
	require.NoError(t, tw.WriteHeader(traversalHdr))
	_, err = tw.Write(traversalContent)
	require.NoError(t, err)

	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	status := importSnapshot(t, buf.Bytes())
	assert.NotEqual(t, http.StatusOK, status,
		"server must reject path-traversal entries (got %d, want non-200)", status)
}

// TestExport_TarballStructure verifies that the exported tarball is a valid
// gzip archive with envelope.json as the first entry, containing the correct
// schema version and cloud field.
func TestExport_TarballStructure(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	resp, err := http.Get(jaiscloudEndpoint + "/_jaiscloud/export")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Verify it is a valid gzip stream.
	gz, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err, "response must be a valid gzip stream")
	defer gz.Close()

	tr := tar.NewReader(gz)

	// First entry must be envelope.json.
	hdr, err := tr.Next()
	require.NoError(t, err, "tarball must have at least one entry")
	assert.Equal(t, "envelope.json", hdr.Name, "first tarball entry must be envelope.json")

	envData, err := io.ReadAll(tr)
	require.NoError(t, err)

	var env version.Envelope
	require.NoError(t, json.Unmarshal(envData, &env))
	assert.Equal(t, version.CodeSnapshotVersion, env.SchemaVersion,
		"envelope schema_version must equal CodeSnapshotVersion (%d)", version.CodeSnapshotVersion)
	assert.Equal(t, "aws", env.Cloud, "envelope cloud must be 'aws'")
}

// TestImport_SchemaTooNew_Rejected verifies that a snapshot with a future
// schema version is rejected with a 4xx status code.
func TestImport_SchemaTooNew_Rejected(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	tarball := buildMinimalTarball(t, version.Envelope{
		SchemaVersion: 99,
		Cloud:         "aws",
		Stores:        map[string]json.RawMessage{},
	})

	resp, err := http.Post(
		jaiscloudEndpoint+"/_jaiscloud/import",
		"application/octet-stream",
		bytes.NewReader(tarball),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, resp.StatusCode >= 400 && resp.StatusCode < 500,
		"schema version check must return 4xx, got %d", resp.StatusCode)
}

// TestRoundTrip_SQS_VisibilityReset creates a queue, sends a message, exports,
// resets, imports, then verifies the queue and message are restored.
func TestRoundTrip_SQS_VisibilityReset(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	sqsClient := newSQSClient(t)
	ctx := context.Background()

	createOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("rt-sqs-test"),
	})
	require.NoError(t, err)
	queueURL := aws.ToString(createOut.QueueUrl)

	const msgBody = "hello-round-trip"
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(msgBody),
	})
	require.NoError(t, err)

	snapshot := exportSnapshot(t)
	resetState(t)

	status := importSnapshot(t, snapshot)
	require.Equal(t, http.StatusOK, status, "import must succeed")

	// Verify the queue is present after import.
	listOut, err := sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	var foundQueue bool
	for _, u := range listOut.QueueUrls {
		if strings.Contains(u, "rt-sqs-test") {
			foundQueue = true
			break
		}
	}
	assert.True(t, foundQueue, "queue 'rt-sqs-test' must be present after import")

	// Receive the message and verify its body.
	var receivedBody string
	waitFor(t, 5*time.Second, func() bool {
		recvOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     0,
		})
		if err != nil || len(recvOut.Messages) == 0 {
			return false
		}
		receivedBody = aws.ToString(recvOut.Messages[0].Body)
		return true
	})
	assert.Equal(t, msgBody, receivedBody, "restored message body must match original")
}

// TestRoundTrip_DynamoDB_Items creates a table, puts 3 items, exports, resets,
// imports, then verifies all items are restored correctly.
func TestRoundTrip_DynamoDB_Items(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	dynaClient := newDynamoClient(t)
	ctx := context.Background()

	const tableName = "rt-dynamo-test"
	_, err := dynaClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []dynamo_types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamo_types.ScalarAttributeTypeS},
		},
		KeySchema: []dynamo_types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamo_types.KeyTypeHash},
		},
		BillingMode: dynamo_types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	items := []struct{ id, val string }{
		{"1", "a"},
		{"2", "b"},
		{"3", "c"},
	}
	for _, item := range items {
		_, err := dynaClient.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamo_types.AttributeValue{
				"id":  &dynamo_types.AttributeValueMemberS{Value: item.id},
				"val": &dynamo_types.AttributeValueMemberS{Value: item.val},
			},
		})
		require.NoError(t, err, fmt.Sprintf("PutItem id=%s", item.id))
	}

	snapshot := exportSnapshot(t)
	resetState(t)

	status := importSnapshot(t, snapshot)
	require.Equal(t, http.StatusOK, status, "import must succeed")

	scanOut, err := dynaClient.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(scanOut.Items), "scan must return 3 items after import")

	// Verify item id=1 has val=a.
	getOut, err := dynaClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamo_types.AttributeValue{
			"id": &dynamo_types.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item, "item id=1 must exist after import")
	valAttr, ok := getOut.Item["val"]
	require.True(t, ok, "item id=1 must have 'val' attribute")
	valS, ok := valAttr.(*dynamo_types.AttributeValueMemberS)
	require.True(t, ok, "'val' must be a string attribute")
	assert.Equal(t, "a", valS.Value, "item id=1 val must be 'a'")
}

// Ensure s3 import is used (compile-time reference to suppress unused import).
var _ = (*s3.Client)(nil)
