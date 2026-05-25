//go:build sfn_e2e

package stepfunctions_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func sfnClient(t *testing.T) *awssfn.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, opts ...any) (aws.Endpoint, error) {
				return aws.Endpoint{URL: jaiscloudHost()}, nil
			},
		)),
	)
	require.NoError(t, err)
	return awssfn.NewFromConfig(cfg)
}

func createSM(t *testing.T, client *awssfn.Client, name, def string) string {
	t.Helper()
	out, err := client.CreateStateMachine(context.Background(), &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(def),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteStateMachine(context.Background(), &awssfn.DeleteStateMachineInput{
			StateMachineArn: out.StateMachineArn,
		})
	})
	return *out.StateMachineArn
}

func pollUntilTerminal(t *testing.T, client *awssfn.Client, execARN string, timeout time.Duration) *awssfn.DescribeExecutionOutput {
	t.Helper()
	deadline := clock.RealNow().Add(timeout)
	for clock.RealNow().Before(deadline) {
		out, err := client.DescribeExecution(context.Background(), &awssfn.DescribeExecutionInput{
			ExecutionArn: aws.String(execARN),
		})
		require.NoError(t, err)
		if out.Status != sfntypes.ExecutionStatusRunning {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("execution %s did not reach terminal state within %s", execARN, timeout)
	return nil
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestE2E_PassToSucceed(t *testing.T) {
	client := sfnClient(t)
	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Next":"S"},"S":{"Type":"Succeed"}}}`
	smARN := createSM(t, client, fmt.Sprintf("pass-succeed-%d", clock.RealNow().UnixNano()), def)

	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN),
		Input:           aws.String(`{"x":1}`),
	})
	require.NoError(t, err)

	exec := pollUntilTerminal(t, client, *out.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, exec.Status)
	assert.JSONEq(t, `{"x":1}`, *exec.Output)
}

func TestE2E_FailState(t *testing.T) {
	client := sfnClient(t)
	def := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"TestError","Cause":"intentional"}}}`
	smARN := createSM(t, client, fmt.Sprintf("fail-state-%d", clock.RealNow().UnixNano()), def)

	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN),
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	exec := pollUntilTerminal(t, client, *out.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusFailed, exec.Status)
	assert.Equal(t, "TestError", *exec.Error)
	assert.Equal(t, "intentional", *exec.Cause)
}

func TestE2E_ChoiceState_MatchesRule(t *testing.T) {
	client := sfnClient(t)
	def := `{
		"StartAt":"C",
		"States":{
			"C":{
				"Type":"Choice",
				"Choices":[
					{"Variable":"$.val","StringEquals":"go","Next":"Go"},
					{"Variable":"$.val","StringEquals":"stop","Next":"Stop"}
				],
				"Default":"Stop"
			},
			"Go":{"Type":"Pass","Result":{"result":"go"},"End":true},
			"Stop":{"Type":"Pass","Result":{"result":"stop"},"End":true}
		}
	}`
	smARN := createSM(t, client, fmt.Sprintf("choice-%d", clock.RealNow().UnixNano()), def)

	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN),
		Input:           aws.String(`{"val":"go"}`),
	})
	require.NoError(t, err)
	exec := pollUntilTerminal(t, client, *out.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, exec.Status)
	assert.Contains(t, *exec.Output, `"result":"go"`)
}

func TestE2E_ParallelBranches(t *testing.T) {
	client := sfnClient(t)
	def := `{
		"StartAt":"P",
		"States":{
			"P":{
				"Type":"Parallel",
				"Branches":[
					{"StartAt":"A","States":{"A":{"Type":"Pass","Result":{"b":"a"},"End":true}}},
					{"StartAt":"B","States":{"B":{"Type":"Pass","Result":{"b":"b"},"End":true}}}
				],
				"End":true
			}
		}
	}`
	smARN := createSM(t, client, fmt.Sprintf("parallel-%d", clock.RealNow().UnixNano()), def)
	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN), Input: aws.String(`{}`),
	})
	require.NoError(t, err)
	exec := pollUntilTerminal(t, client, *out.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, exec.Status)
}

func TestE2E_MapOverArray(t *testing.T) {
	client := sfnClient(t)
	def := `{
		"StartAt":"M",
		"States":{
			"M":{
				"Type":"Map",
				"ItemsPath":"$.items",
				"Iterator":{
					"StartAt":"I",
					"States":{"I":{"Type":"Pass","End":true}}
				},
				"End":true
			}
		}
	}`
	smARN := createSM(t, client, fmt.Sprintf("map-%d", clock.RealNow().UnixNano()), def)
	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN),
		Input:           aws.String(`{"items":["a","b","c"]}`),
	})
	require.NoError(t, err)
	exec := pollUntilTerminal(t, client, *out.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, exec.Status)
}

func TestE2E_StopExecution(t *testing.T) {
	client := sfnClient(t)
	// Long wait state so it won't finish before we stop it
	def := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":300,"End":true}}}`
	smARN := createSM(t, client, fmt.Sprintf("stop-exec-%d", clock.RealNow().UnixNano()), def)
	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN), Input: aws.String(`{}`),
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	_, err = client.StopExecution(context.Background(), &awssfn.StopExecutionInput{
		ExecutionArn: out.ExecutionArn,
		Error:        aws.String("ManualStop"),
		Cause:        aws.String("test stop"),
	})
	require.NoError(t, err)

	exec := pollUntilTerminal(t, client, *out.ExecutionArn, 5*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusAborted, exec.Status)
}

func TestE2E_GetExecutionHistory(t *testing.T) {
	client := sfnClient(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`
	smARN := createSM(t, client, fmt.Sprintf("history-%d", clock.RealNow().UnixNano()), def)
	out, err := client.StartExecution(context.Background(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(smARN), Input: aws.String(`{}`),
	})
	require.NoError(t, err)
	pollUntilTerminal(t, client, *out.ExecutionArn, 10*time.Second)

	hist, err := client.GetExecutionHistory(context.Background(), &awssfn.GetExecutionHistoryInput{
		ExecutionArn: out.ExecutionArn,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(hist.Events), 2)
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionStarted, hist.Events[0].Type)
	last := hist.Events[len(hist.Events)-1]
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionSucceeded, last.Type)
}

func TestE2E_ValidateDefinition_Invalid(t *testing.T) {
	client := sfnClient(t)
	out, err := client.ValidateStateMachineDefinition(context.Background(), &awssfn.ValidateStateMachineDefinitionInput{
		Definition: aws.String(`{"StartAt":"Missing","States":{"S":{"Type":"Succeed"}}}`),
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeFail, out.Result)
	assert.NotEmpty(t, out.Diagnostics)
}
