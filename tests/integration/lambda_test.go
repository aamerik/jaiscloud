package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLambdaClient(t *testing.T) *awslambda.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awslambda.NewFromConfig(cfg, func(o *awslambda.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func TestLambda_CreateGetDeleteFunction(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("my-func"),
		Runtime:      types.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("index.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("fake-zip")},
	})
	require.NoError(t, err)

	getOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("my-func"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-func", aws.ToString(getOut.Configuration.FunctionName))
	assert.Equal(t, types.StateActive, getOut.Configuration.State)

	_, err = c.DeleteFunction(ctx, &awslambda.DeleteFunctionInput{
		FunctionName: aws.String("my-func"),
	})
	require.NoError(t, err)

	_, err = c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("my-func"),
	})
	require.Error(t, err, "expected error for deleted function")
}

func TestLambda_ListFunctions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	for _, name := range []string{"fn-a", "fn-b", "fn-c"} {
		_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
			FunctionName: aws.String(name),
			Runtime:      types.RuntimePython312,
			Role:         aws.String("arn:aws:iam::000000000000:role/r"),
			Handler:      aws.String("main.handler"),
			Code:         &types.FunctionCode{ZipFile: []byte("x")},
		})
		require.NoError(t, err)
	}

	out, err := c.ListFunctions(ctx, &awslambda.ListFunctionsInput{})
	require.NoError(t, err)
	assert.Len(t, out.Functions, 3)
}

func TestLambda_InvokeEcho(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("echo-func"),
		Runtime:      types.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/r"),
		Handler:      aws.String("index.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("x")},
	})
	require.NoError(t, err)

	payload := map[string]any{"hello": "world", "count": 42}
	payloadBytes, _ := json.Marshal(payload)

	invokeOut, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("echo-func"),
		Payload:      payloadBytes,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(200), invokeOut.StatusCode)
	assert.Equal(t, payloadBytes, invokeOut.Payload)
}

func TestLambda_InvokeNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("no-such-function"),
		Payload:      []byte(`{}`),
	})
	require.Error(t, err)
}

func TestLambda_UpdateFunctionConfiguration(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("upd-func"),
		Runtime:      types.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/r"),
		Handler:      aws.String("old.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("x")},
	})
	require.NoError(t, err)

	updOut, err := c.UpdateFunctionConfiguration(ctx, &awslambda.UpdateFunctionConfigurationInput{
		FunctionName: aws.String("upd-func"),
		Handler:      aws.String("new.handler"),
		Description:  aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "new.handler", aws.ToString(updOut.Handler))
}

func TestLambda_UpdateFunctionCode(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("code-func"),
		Runtime:      types.RuntimePython312,
		Role:         aws.String("arn:aws:iam::000000000000:role/r"),
		Handler:      aws.String("main.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("original-zip")},
	})
	require.NoError(t, err)

	// UpdateFunctionCode with a new zip.
	updOut, err := c.UpdateFunctionCode(ctx, &awslambda.UpdateFunctionCodeInput{
		FunctionName: aws.String("code-func"),
		ZipFile:      []byte("new-zip-content-larger"),
	})
	require.NoError(t, err)
	assert.Equal(t, "code-func", aws.ToString(updOut.FunctionName))

	// GetFunction should reflect the update.
	getOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("code-func"),
	})
	require.NoError(t, err)
	assert.Equal(t, "code-func", aws.ToString(getOut.Configuration.FunctionName))
}

func TestLambda_EnvironmentVariables(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("env-func"),
		Runtime:      types.RuntimePython312,
		Role:         aws.String("arn:aws:iam::000000000000:role/r"),
		Handler:      aws.String("main.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("x")},
		Environment: &types.Environment{
			Variables: map[string]string{
				"DB_HOST": "localhost",
				"DB_PORT": "5432",
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("env-func"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Configuration.Environment)
	assert.Equal(t, "localhost", getOut.Configuration.Environment.Variables["DB_HOST"])
	assert.Equal(t, "5432", getOut.Configuration.Environment.Variables["DB_PORT"])

	// UpdateFunctionConfiguration can replace env vars.
	_, err = c.UpdateFunctionConfiguration(ctx, &awslambda.UpdateFunctionConfigurationInput{
		FunctionName: aws.String("env-func"),
		Environment: &types.Environment{
			Variables: map[string]string{
				"DB_HOST": "prod-db.example.com",
			},
		},
	})
	require.NoError(t, err)

	cfgOut, err := c.GetFunctionConfiguration(ctx, &awslambda.GetFunctionConfigurationInput{
		FunctionName: aws.String("env-func"),
	})
	require.NoError(t, err)
	require.NotNil(t, cfgOut.Environment)
	assert.Equal(t, "prod-db.example.com", cfgOut.Environment.Variables["DB_HOST"])
}

func TestLambda_GetFunctionConfiguration(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("cfg-func"),
		Runtime:      types.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/exec-role"),
		Handler:      aws.String("index.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("x")},
	})
	require.NoError(t, err)

	cfgOut, err := c.GetFunctionConfiguration(ctx, &awslambda.GetFunctionConfigurationInput{
		FunctionName: aws.String("cfg-func"),
	})
	require.NoError(t, err)
	assert.Equal(t, "cfg-func", aws.ToString(cfgOut.FunctionName))
	assert.Equal(t, "index.handler", aws.ToString(cfgOut.Handler))
	assert.Equal(t, "arn:aws:iam::000000000000:role/exec-role", aws.ToString(cfgOut.Role))
	assert.Equal(t, types.StateActive, cfgOut.State)
}

func TestLambda_AsyncInvoke(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("async-func"),
		Runtime:      types.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/exec-role"),
		Handler:      aws.String("index.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("x")},
	})
	require.NoError(t, err)

	// InvocationType=Event → 202, empty body
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName:   aws.String("async-func"),
		InvocationType: types.InvocationTypeEvent,
		Payload:        []byte(`{"key":"value"}`),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 202, out.StatusCode)
	assert.Empty(t, out.Payload)
}

// TestLambdaLayerMountMock verifies that CreateFunction with Layers does not error on
// Invoke in mock mode. The mock executor does not actually extract or mount layer zips;
// this test just confirms the plumbing does not break the invocation path.
func TestLambdaLayerMountMock(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	// Publish a layer so the ARN is resolvable.
	layerOut, err := c.PublishLayerVersion(ctx, &awslambda.PublishLayerVersionInput{
		LayerName:   aws.String("my-layer"),
		Description: aws.String("test layer"),
		Content: &types.LayerVersionContentInput{
			ZipFile: []byte("fake-layer-zip"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, layerOut.LayerVersionArn)

	layerARN := *layerOut.LayerVersionArn

	// Create function referencing the layer.
	_, err = c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("func-with-layer"),
		Runtime:      types.RuntimePython311,
		Role:         aws.String("arn:aws:iam::000000000000:role/exec-role"),
		Handler:      aws.String("handler.main"),
		Code:         &types.FunctionCode{ZipFile: []byte("fake-zip")},
		Layers:       []string{layerARN},
	})
	require.NoError(t, err)

	// Invoke should succeed — mock executor echoes payload regardless of layers.
	payload := []byte(`{"test":"layer-mount"}`)
	invokeOut, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("func-with-layer"),
		Payload:      payload,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, invokeOut.StatusCode)
	assert.Equal(t, payload, invokeOut.Payload)
}

