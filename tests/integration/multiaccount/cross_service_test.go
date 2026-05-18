package multiaccount

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSNS_CrossAccount_TopicAndQueue verifies that an SNS topic in account A
// can fan-out to a queue in account B when the queue ARN is supplied.
// This exercises the §11.1.1 cross-account SQS dispatch path.
func TestSNS_CrossAccount_TopicAndQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	snsA := newSNSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	// Create a topic in A.
	topicOut, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("cross-acct-topic"),
	})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	// Create a queue in B.
	qOut, err := sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("cross-acct-target-q"),
		Attributes: map[string]string{
			"Policy": crossAccountQueuePolicy(AcctA, AcctB, "cross-acct-target-q"),
		},
	})
	require.NoError(t, err)
	qURL := aws.ToString(qOut.QueueUrl)

	qARN := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:cross-acct-target-q", AcctB)

	// Subscribe B's queue to A's topic.
	_, err = snsA.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qARN),
	})
	require.NoError(t, err)

	// Publish from A.
	_, err = snsA.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("hello cross-account"),
	})
	require.NoError(t, err)

	// Poll B's queue — message should arrive within 3 seconds.
	var received bool
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		recv, rErr := sqsB.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		require.NoError(t, rErr)
		if len(recv.Messages) > 0 {
			received = true
			break
		}
	}
	assert.True(t, received, "B's queue should receive the SNS message published from A")
}

// TestSecretsManager_AccountIsolation verifies that secrets stored in A are
// not visible to B under the same secret name.
func TestSecretsManager_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	smA := newSMFor(t, AcctA)
	smB := newSMFor(t, AcctB)

	_, err := smA.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("my-secret"),
		SecretString: aws.String("valueA"),
	})
	require.NoError(t, err)

	// B should not be able to get A's secret by the same name.
	_, err = smB.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("my-secret"),
	})
	require.Error(t, err, "B should not see A's secret")
}

// crossAccountQueuePolicy returns a minimal SQS policy allowing SNS from
// account senderAccount to send to a queue owned by ownerAccount.
func crossAccountQueuePolicy(senderAccount, ownerAccount, queueName string) string {
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":    "Allow",
				"Principal": map[string]any{"Service": "sns.amazonaws.com"},
				"Action":    "sqs:SendMessage",
				"Resource":  fmt.Sprintf("arn:aws:sqs:us-east-1:%s:%s", ownerAccount, queueName),
				"Condition": map[string]any{
					"ArnLike": map[string]string{
						"aws:SourceArn": fmt.Sprintf("arn:aws:sns:us-east-1:%s:*", senderAccount),
					},
				},
			},
		},
	}
	b, _ := json.Marshal(policy)
	return string(b)
}
