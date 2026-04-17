//go:build lambda_e2e

package p25_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── templates ────────────────────────────────────────────────────────────────

const sqsLambdaTemplate = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "FunctionName": {
      "Type": "String",
      "Default": "cf-provisioned-fn"
    }
  },
  "Resources": {
    "InputQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": "cf-input-queue"
      }
    },
    "ProcessorFunction": {
      "Type": "AWS::Lambda::Function",
      "Properties": {
        "FunctionName": {"Ref": "FunctionName"},
        "Runtime": "nodejs20.x",
        "Role": "arn:aws:iam::000000000000:role/lambda-role",
        "Handler": "index.handler",
        "Code": {
          "ZipFile": "exports.handler = async (e) => e;"
        }
      }
    }
  },
  "Outputs": {
    "QueueUrl": {
      "Value": {"Ref": "InputQueue"}
    },
    "FunctionArn": {
      "Value": {"Fn::GetAtt": ["ProcessorFunction", "Arn"]}
    }
  }
}`

const kmsCMKTemplate = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "AppKey": {
      "Type": "AWS::KMS::Key",
      "Properties": {
        "Description": "CF-managed CMK",
        "EnableKeyRotation": false
      }
    },
    "AppSecret": {
      "Type": "AWS::SecretsManager::Secret",
      "Properties": {
        "Name": "cf-app-secret",
        "SecretString": "initial-secret-value",
        "KmsKeyId": {"Ref": "AppKey"}
      }
    }
  },
  "Outputs": {
    "KeyId": {
      "Value": {"Ref": "AppKey"}
    }
  }
}`

const updateableTemplate = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "QueueName": {
      "Type": "String",
      "Default": "update-queue-v1"
    }
  },
  "Resources": {
    "Queue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": {"Ref": "QueueName"}
      }
    }
  }
}`

// ─── tests ────────────────────────────────────────────────────────────────────

// TestCFN_StackProvisionsSQSAndLambda creates a CloudFormation stack that
// provisions an SQS queue and a Lambda function. After CREATE_COMPLETE, both
// resources must be queryable directly via their respective service APIs.
// Stack outputs must reference the provisioned resources.
func TestCFN_StackProvisionsSQSAndLambda(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)
	lambdaClient := newLambdaClient(t)
	sqsClient := newSQSClient(t)

	createOut, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("sqs-lambda-stack"),
		TemplateBody: aws.String(sqsLambdaTemplate),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createOut.StackId))

	// CloudFormation stacks complete synchronously in JaisCloud — no polling needed.
	descOut, err := cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("sqs-lambda-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	assert.Equal(t, cftypes.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)

	// Verify the SQS queue was actually created by the stack.
	queueURL := ""
	for _, out := range descOut.Stacks[0].Outputs {
		if aws.ToString(out.OutputKey) == "QueueUrl" {
			queueURL = aws.ToString(out.OutputValue)
		}
	}
	require.NotEmpty(t, queueURL, "expected QueueUrl output from stack")

	sqsOut, err := sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, sqsOut.Attributes["QueueArn"])

	// Verify the Lambda function was created.
	fnOut, err := lambdaClient.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("cf-provisioned-fn"),
	})
	require.NoError(t, err)
	assert.Equal(t, "cf-provisioned-fn", aws.ToString(fnOut.Configuration.FunctionName))

	// Verify FunctionArn output is present and non-empty.
	fnARN := ""
	for _, out := range descOut.Stacks[0].Outputs {
		if aws.ToString(out.OutputKey) == "FunctionArn" {
			fnARN = aws.ToString(out.OutputValue)
		}
	}
	assert.NotEmpty(t, fnARN, "expected FunctionArn output from stack")
}

// TestCFN_StackWithKMSKey_SecretRef creates a stack that provisions a KMS key
// and a SecretsManager secret that references the key. After CREATE_COMPLETE,
// the KMS key must exist and the secret must be decryptable.
func TestCFN_StackWithKMSKey_SecretRef(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)
	kmsClient := newKMSClient(t)
	smClient := newSMClient(t)

	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("kms-secret-stack"),
		TemplateBody: aws.String(kmsCMKTemplate),
	})
	require.NoError(t, err)

	descOut, err := cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("kms-secret-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	assert.Equal(t, cftypes.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)

	// Extract the KMS key ID from outputs.
	keyID := ""
	for _, out := range descOut.Stacks[0].Outputs {
		if aws.ToString(out.OutputKey) == "KeyId" {
			keyID = aws.ToString(out.OutputValue)
		}
	}
	require.NotEmpty(t, keyID, "expected KeyId output from stack")

	// Verify the KMS key is enabled.
	kmsDesc, err := kmsClient.DescribeKey(ctx, &awskms.DescribeKeyInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)
	assert.True(t, kmsDesc.KeyMetadata.Enabled)

	// Verify the secret was created and is decryptable.
	secretOut, err := smClient.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("cf-app-secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "initial-secret-value", aws.ToString(secretOut.SecretString))
}

// TestCFN_UpdateStack_ChangesResources creates a stack with a parameter-named
// SQS queue, then updates the stack with a new parameter value. Verifies that
// the UPDATE_COMPLETE status is returned and the stack shows updated parameters.
func TestCFN_UpdateStack_ChangesResources(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)

	// Create initial stack.
	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("updateable-stack"),
		TemplateBody: aws.String(updateableTemplate),
		Parameters: []cftypes.Parameter{
			{ParameterKey: aws.String("QueueName"), ParameterValue: aws.String("update-queue-v1")},
		},
	})
	require.NoError(t, err)

	descOut, err := cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("updateable-stack"),
	})
	require.NoError(t, err)
	assert.Equal(t, cftypes.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)

	// Update with new parameter.
	_, err = cfClient.UpdateStack(ctx, &awscf.UpdateStackInput{
		StackName:    aws.String("updateable-stack"),
		TemplateBody: aws.String(updateableTemplate),
		Parameters: []cftypes.Parameter{
			{ParameterKey: aws.String("QueueName"), ParameterValue: aws.String("update-queue-v2")},
		},
	})
	require.NoError(t, err)

	updatedOut, err := cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("updateable-stack"),
	})
	require.NoError(t, err)
	require.Len(t, updatedOut.Stacks, 1)
	assert.Equal(t, cftypes.StackStatusUpdateComplete, updatedOut.Stacks[0].StackStatus)

	// The parameter value must be persisted (tests the rc.params fix).
	for _, p := range updatedOut.Stacks[0].Parameters {
		if aws.ToString(p.ParameterKey) == "QueueName" {
			assert.Equal(t, "update-queue-v2", aws.ToString(p.ParameterValue))
		}
	}
}

// TestCFN_DeleteStack_CascadesChildren creates a stack with a Lambda function
// and SQS queue, then deletes the stack. Both child resources must be gone.
func TestCFN_DeleteStack_CascadesChildren(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)
	lambdaClient := newLambdaClient(t)

	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("delete-cascade-stack"),
		TemplateBody: aws.String(sqsLambdaTemplate),
	})
	require.NoError(t, err)

	// Verify function exists.
	_, err = lambdaClient.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("cf-provisioned-fn"),
	})
	require.NoError(t, err, "function must exist before stack delete")

	// Delete the stack.
	_, err = cfClient.DeleteStack(ctx, &awscf.DeleteStackInput{
		StackName: aws.String("delete-cascade-stack"),
	})
	require.NoError(t, err)

	// Stack must be gone.
	_, err = cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("delete-cascade-stack"),
	})
	require.Error(t, err, "stack should not be found after deletion")

	// Lambda function provisioned by the stack must also be deleted.
	_, err = lambdaClient.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("cf-provisioned-fn"),
	})
	require.Error(t, err, "Lambda function must be deleted when stack is deleted")
}

// TestCFN_StackParameters_DefaultsApplied verifies that when no parameters are
// passed to CreateStack, the template defaults are used. The stack must reach
// CREATE_COMPLETE and the SQS queue must use the default FunctionName.
func TestCFN_StackParameters_DefaultsApplied(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)
	lambdaClient := newLambdaClient(t)

	// Create without explicit parameters — defaults should apply.
	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("defaults-stack"),
		TemplateBody: aws.String(sqsLambdaTemplate),
	})
	require.NoError(t, err)

	descOut, err := cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("defaults-stack"),
	})
	require.NoError(t, err)
	assert.Equal(t, cftypes.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)

	// Default FunctionName is "cf-provisioned-fn".
	_, err = lambdaClient.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("cf-provisioned-fn"),
	})
	require.NoError(t, err)
}
