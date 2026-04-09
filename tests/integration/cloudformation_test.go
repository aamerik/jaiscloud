package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const simpleTemplate = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {}
    },
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  }
}`

func TestCF_CreateDescribeDeleteStack(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	out, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("my-stack"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.StackId))

	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("my-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	assert.Equal(t, "my-stack", aws.ToString(descOut.Stacks[0].StackName))
	assert.Equal(t, types.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)

	_, err = client.DeleteStack(ctx, &awscf.DeleteStackInput{
		StackName: aws.String("my-stack"),
	})
	require.NoError(t, err)

	_, err = client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("my-stack"),
	})
	require.Error(t, err)
}

func TestCF_ListStacks(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	for _, name := range []string{"stack-a", "stack-b", "stack-c"} {
		_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
			StackName:    aws.String(name),
			TemplateBody: aws.String(simpleTemplate),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListStacks(ctx, &awscf.ListStacksInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.StackSummaries, 3)
}

func TestCF_UpdateStack(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("my-stack"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	updatedTemplate := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {}
    }
  }
}`
	_, err = client.UpdateStack(ctx, &awscf.UpdateStackInput{
		StackName:    aws.String("my-stack"),
		TemplateBody: aws.String(updatedTemplate),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("my-stack"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.StackStatusUpdateComplete, descOut.Stacks[0].StackStatus)
}

func TestCF_DescribeStackResources(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("my-stack"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String("my-stack"),
	})
	require.NoError(t, err)
	assert.Len(t, resOut.StackResources, 2)

	types := map[string]bool{}
	for _, r := range resOut.StackResources {
		types[aws.ToString(r.ResourceType)] = true
	}
	assert.True(t, types["AWS::S3::Bucket"])
	assert.True(t, types["AWS::SQS::Queue"])
}

func TestCF_ValidateTemplate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	_, err := client.ValidateTemplate(ctx, &awscf.ValidateTemplateInput{
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)
}
