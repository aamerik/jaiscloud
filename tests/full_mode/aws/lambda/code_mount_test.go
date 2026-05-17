//go:build lambda_e2e

package lambda_test

import (
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

// TestLambdaDocker_ZipCodeMount_ReturnsOutput creates a Python 3.11 function
// from a ZipFile, invokes it with a payload, and asserts the response contains
// the expected fields echoed back by the handler.
func TestLambdaDocker_ZipCodeMount_ReturnsOutput(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	// Build function zip: lambda_function.py that echoes event + adds "hello"
	fnZip := buildZip(t, map[string]string{
		"lambda_function.py": `def handler(event, context):
    return {"hello": "world", "input": event}
`,
	})
	fnB64 := base64.StdEncoding.EncodeToString(fnZip)

	// CreateFunction with ZipFile
	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("zip-code-mount-test"),
		Runtime:      types.RuntimePython311,
		Handler:      aws.String("lambda_function.handler"),
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Code: &types.FunctionCode{
			ZipFile: []byte(fnB64),
		},
	})
	require.NoError(t, err, "CreateFunction")

	// Invoke with payload
	payload := []byte(`{"key":"val"}`)
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("zip-code-mount-test"),
		Payload:      payload,
	})
	require.NoError(t, err, "Invoke")
	assert.EqualValues(t, 200, out.StatusCode)
	assert.Empty(t, aws.ToString(out.FunctionError), "expected no function error, got: %s", string(out.Payload))

	// Parse and assert response
	var resp map[string]any
	require.NoError(t, json.Unmarshal(out.Payload, &resp), "parse response payload: %s", string(out.Payload))

	assert.Equal(t, "world", resp["hello"], "expected hello=world")

	input, ok := resp["input"].(map[string]any)
	require.True(t, ok, "expected 'input' to be an object, got: %v", resp["input"])
	assert.Equal(t, "val", input["key"], "expected input.key=val")
}
