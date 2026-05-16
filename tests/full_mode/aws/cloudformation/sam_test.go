//go:build cfn_fullmode

package cloudformation_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const samTemplate = `
Transform: AWS::Serverless-2016-10-31
Resources:
  HelloFunc:
    Type: AWS::Serverless::Function
    Properties:
      Runtime: python3.11
      Handler: index.handler
      InlineCode: |
        def handler(event, context):
            return {"statusCode": 200, "body": "hello"}
      Events:
        Api:
          Type: Api
          Properties:
            Path: /hello
            Method: GET
`

// TestCFNSAMTransformFunction deploys a SAM template with Transform:
// AWS::Serverless-2016-10-31 and verifies the Lambda function is created.
func TestCFNSAMTransformFunction(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfClient := newCFClient(t)
	lambdaClient := newLambdaClient(t)

	stackName := "sam-transform-test"

	// CreateStack with SAM template
	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(samTemplate),
		Capabilities: []cftypes.Capability{cftypes.CapabilityCapabilityIam, cftypes.CapabilityCapabilityAutoExpand},
	})
	require.NoError(t, err, "CreateStack with SAM template")

	// Poll until terminal
	status := pollStackStatus(t, cfClient, stackName)
	require.Equal(t, "CREATE_COMPLETE", status,
		"expected SAM stack to reach CREATE_COMPLETE, got %s", status)

	// DescribeStackResources — assert Lambda function resource exists
	resOut, err := cfClient.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	funcPhysicalID := ""
	for _, r := range resOut.StackResources {
		if aws.ToString(r.ResourceType) == "AWS::Lambda::Function" {
			funcPhysicalID = aws.ToString(r.PhysicalResourceId)
			t.Logf("Lambda function physical ID: %s", funcPhysicalID)
			break
		}
	}
	require.NotEmpty(t, funcPhysicalID, "expected AWS::Lambda::Function in stack resources")

	// Directly verify the Lambda function exists via Lambda API
	funcOut, err := lambdaClient.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String(funcPhysicalID),
	})
	require.NoError(t, err, "Lambda function should exist after SAM stack creation")
	assert.Equal(t, "python3.11", string(funcOut.Configuration.Runtime),
		"Lambda runtime should match SAM template")
	assert.Equal(t, "index.handler", aws.ToString(funcOut.Configuration.Handler),
		"Lambda handler should match SAM template")

	// DeleteStack and wait
	_, err = cfClient.DeleteStack(ctx, &awscf.DeleteStackInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	deleteStatus := pollStackStatus(t, cfClient, stackName)
	assert.Equal(t, "DELETE_COMPLETE", deleteStatus,
		"expected stack to reach DELETE_COMPLETE after deletion, got %s", deleteStatus)
}
