//go:build cfn_fullmode

package cloudformation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Templates ────────────────────────────────────────────────────────────────

const changeSetBaseTemplate = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "CSQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": "cs-test-queue"
      }
    }
  }
}`

const changeSetAddSNSTemplate = `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "CSQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": "cs-test-queue"
      }
    },
    "CSTopic": {
      "Type": "AWS::SNS::Topic",
      "Properties": {
        "TopicName": "cs-test-topic"
      }
    }
  }
}`

// ─── Helpers ─────────────────────────────────────────────────────────────────

// pollChangeSetStatus polls until the changeset reaches a terminal status.
func pollChangeSetStatus(t *testing.T, cfClient *awscf.Client, stackName, changeSetName string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := cfClient.DescribeChangeSet(context.Background(), &awscf.DescribeChangeSetInput{
			StackName:     aws.String(stackName),
			ChangeSetName: aws.String(changeSetName),
		})
		if err != nil {
			t.Logf("DescribeChangeSet error: %v", err)
			return "FAILED"
		}
		status := string(out.Status)
		t.Logf("changeset %s status: %s", changeSetName, status)
		switch status {
		case "CREATE_COMPLETE", "FAILED", "DELETE_COMPLETE":
			return status
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("changeset %s did not reach terminal status within 2m", changeSetName)
	return ""
}

// queueExists checks whether the named SQS queue is reachable.
func queueExists(ctx context.Context, sqsClient *awssqs.Client, queueName string) bool {
	out, err := sqsClient.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	return err == nil && out.QueueUrl != nil
}

// topicExists checks whether any SNS topic ARN contains the given name suffix.
func topicExists(ctx context.Context, snsClient *awssns.Client, topicName string) bool {
	out, err := snsClient.ListTopics(ctx, &awssns.ListTopicsInput{})
	if err != nil {
		return false
	}
	for _, topic := range out.Topics {
		if strings.Contains(aws.ToString(topic.TopicArn), topicName) {
			return true
		}
	}
	return false
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestCFNChangeSetAddResource creates a stack with an SQS queue, then uses a
// changeset to add an SNS topic. After executing the changeset, both the SQS
// queue and the SNS topic must be accessible via their respective service APIs.
func TestCFNChangeSetAddResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)
	sqsClient := newSQSClient(t)
	snsClient := newSNSClient(t)

	stackName := "cs-add-resource-stack"

	// Step 1 — create base stack with only the SQS queue.
	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(changeSetBaseTemplate),
	})
	require.NoError(t, err, "CreateStack should succeed")

	status := pollStackStatus(t, cfClient, stackName)
	require.Equal(t, "CREATE_COMPLETE", status, "base stack must reach CREATE_COMPLETE")

	// Verify SQS queue exists.
	assert.True(t, queueExists(ctx, sqsClient, "cs-test-queue"), "SQS queue must exist after base stack creation")
	// SNS topic must NOT exist yet.
	assert.False(t, topicExists(ctx, snsClient, "cs-test-topic"), "SNS topic must not exist before changeset")

	// Step 2 — create a changeset that adds the SNS topic.
	changeSetName := "add-sns-topic-cs"
	_, err = cfClient.CreateChangeSet(ctx, &awscf.CreateChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(changeSetName),
		TemplateBody:  aws.String(changeSetAddSNSTemplate),
		ChangeSetType: "UPDATE",
	})
	require.NoError(t, err, "CreateChangeSet should succeed")

	csStatus := pollChangeSetStatus(t, cfClient, stackName, changeSetName)
	require.Equal(t, "CREATE_COMPLETE", csStatus, "changeset must reach CREATE_COMPLETE before execution")

	// Step 3 — execute the changeset.
	_, err = cfClient.ExecuteChangeSet(ctx, &awscf.ExecuteChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(changeSetName),
	})
	require.NoError(t, err, "ExecuteChangeSet should succeed")

	updatedStatus := pollStackStatus(t, cfClient, stackName)
	require.Equal(t, "UPDATE_COMPLETE", updatedStatus, "stack must reach UPDATE_COMPLETE after changeset execution")

	// Step 4 — both resources must now exist.
	assert.True(t, queueExists(ctx, sqsClient, "cs-test-queue"), "SQS queue must still exist after changeset")
	assert.True(t, topicExists(ctx, snsClient, "cs-test-topic"), "SNS topic must exist after changeset execution")
}

// TestCFNChangeSetDeleteResource starts with a stack that has both an SQS queue
// and an SNS topic, creates a changeset that removes the SNS topic, executes it,
// and then asserts that only the SQS queue remains.
func TestCFNChangeSetDeleteResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfClient := newCFClient(t)
	sqsClient := newSQSClient(t)
	snsClient := newSNSClient(t)

	stackName := "cs-delete-resource-stack"

	// Step 1 — create stack with both SQS queue and SNS topic.
	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(changeSetAddSNSTemplate),
	})
	require.NoError(t, err, "CreateStack should succeed")

	status := pollStackStatus(t, cfClient, stackName)
	require.Equal(t, "CREATE_COMPLETE", status, "full stack must reach CREATE_COMPLETE")

	// Both resources must exist.
	require.True(t, queueExists(ctx, sqsClient, "cs-test-queue"), "SQS queue must exist in full stack")
	require.True(t, topicExists(ctx, snsClient, "cs-test-topic"), "SNS topic must exist in full stack")

	// Step 2 — create a changeset that removes the SNS topic.
	changeSetName := "remove-sns-topic-cs"
	_, err = cfClient.CreateChangeSet(ctx, &awscf.CreateChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(changeSetName),
		TemplateBody:  aws.String(changeSetBaseTemplate),
		ChangeSetType: "UPDATE",
	})
	require.NoError(t, err, "CreateChangeSet (removal) should succeed")

	csStatus := pollChangeSetStatus(t, cfClient, stackName, changeSetName)
	require.Equal(t, "CREATE_COMPLETE", csStatus, "removal changeset must reach CREATE_COMPLETE")

	// Step 3 — execute the changeset.
	_, err = cfClient.ExecuteChangeSet(ctx, &awscf.ExecuteChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(changeSetName),
	})
	require.NoError(t, err, "ExecuteChangeSet (removal) should succeed")

	updatedStatus := pollStackStatus(t, cfClient, stackName)
	require.Equal(t, "UPDATE_COMPLETE", updatedStatus, "stack must reach UPDATE_COMPLETE after removal changeset")

	// Step 4 — only the SQS queue should remain; SNS topic must be gone.
	assert.True(t, queueExists(ctx, sqsClient, "cs-test-queue"), "SQS queue must survive the removal changeset")
	assert.False(t, topicExists(ctx, snsClient, "cs-test-topic"), "SNS topic must be removed by the changeset")
}
