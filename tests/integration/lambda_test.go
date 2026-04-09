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
