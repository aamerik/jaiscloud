//go:build lambda_e2e

package lambda_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildZip creates an in-memory zip archive with the given file name and content.
func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		require.NoError(t, err, "zip.Create %s", name)
		_, err = w.Write([]byte(content))
		require.NoError(t, err, "zip.Write %s", name)
	}
	require.NoError(t, zw.Close(), "zip.Close")
	return buf.Bytes()
}

// TestLambdaDocker_LayerMount_PythonImport publishes a layer containing
// python/foo.py, creates a Python 3.11 function that imports foo, and
// invokes it — asserting the response carries the value returned by foo.get_value().
func TestLambdaDocker_LayerMount_PythonImport(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	// --- layer zip: python/foo.py ---
	layerZip := buildZip(t, map[string]string{
		"python/foo.py": `def get_value():
    return 42
`,
	})
	layerB64 := base64.StdEncoding.EncodeToString(layerZip)

	// PublishLayerVersion
	layerOut, err := c.PublishLayerVersion(ctx, &awslambda.PublishLayerVersionInput{
		LayerName:          aws.String("foo-layer"),
		CompatibleRuntimes: []types.Runtime{types.RuntimePython311},
		Content: &types.LayerVersionContentInput{
			ZipFile: []byte(layerB64),
		},
	})
	require.NoError(t, err, "PublishLayerVersion")
	require.NotEmpty(t, aws.ToString(layerOut.LayerVersionArn), "expected non-empty LayerVersionArn")

	// --- function zip: lambda_function.py ---
	fnZip := buildZip(t, map[string]string{
		"lambda_function.py": `import foo
def handler(event, context):
    return {"value": foo.get_value()}
`,
	})
	fnB64 := base64.StdEncoding.EncodeToString(fnZip)

	// CreateFunction with layer
	_, err = c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("layer-import-test"),
		Runtime:      types.RuntimePython311,
		Handler:      aws.String("lambda_function.handler"),
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Code: &types.FunctionCode{
			ZipFile: []byte(fnB64),
		},
		Layers: []string{aws.ToString(layerOut.LayerVersionArn)},
	})
	require.NoError(t, err, "CreateFunction")

	// Invoke
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("layer-import-test"),
		Payload:      []byte(`{}`),
	})
	require.NoError(t, err, "Invoke")
	assert.EqualValues(t, 200, out.StatusCode)
	assert.Empty(t, aws.ToString(out.FunctionError), "expected no function error")

	// Parse response
	var resp map[string]any
	require.NoError(t, json.Unmarshal(out.Payload, &resp), "parse response payload")
	value, ok := resp["value"]
	require.True(t, ok, "response must contain 'value' key, got: %s", string(out.Payload))
	assert.EqualValues(t, 42, value, "expected value=42 from foo.get_value()")
}
