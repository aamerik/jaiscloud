package multiaccount

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── SQS isolation ────────────────────────────────────────────────────────────

// TestSQS_AccountIsolation verifies that two accounts creating the same queue
// name produce independent resources — each account only sees its own queue.
func TestSQS_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	// Both accounts create a queue with the same name.
	_, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("shared-queue")})
	require.NoError(t, err)
	_, err = sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("shared-queue")})
	require.NoError(t, err)

	// Account A lists queues — must only see its own.
	listA, err := sqsA.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	for _, u := range listA.QueueUrls {
		assert.Contains(t, u, AcctA, "account A should only see its own queue URLs")
		assert.NotContains(t, u, AcctB)
	}

	// Account B lists queues — must only see its own.
	listB, err := sqsB.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	for _, u := range listB.QueueUrls {
		assert.Contains(t, u, AcctB, "account B should only see its own queue URLs")
		assert.NotContains(t, u, AcctA)
	}
}

// TestSQS_MessageIsolation verifies that messages sent to account A's queue
// are not visible when polling account B's queue with the same name.
func TestSQS_MessageIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	outA, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("msg-iso-q")})
	require.NoError(t, err)
	outB, err := sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("msg-iso-q")})
	require.NoError(t, err)

	// Send a message into A's queue.
	_, err = sqsA.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    outA.QueueUrl,
		MessageBody: aws.String("hello from A"),
	})
	require.NoError(t, err)

	// Receive from B's queue — must be empty.
	recv, err := sqsB.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            outB.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages, "B's queue should not contain A's message")
}

// ─── DynamoDB isolation ────────────────────────────────────────────────────────

// TestDynamoDB_TableIsolation verifies that tables created by account A are
// not visible to account B's ListTables.
func TestDynamoDB_TableIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ddA := newDynamoFor(t, AcctA)
	ddB := newDynamoFor(t, AcctB)

	createTable := func(client *dynamodb.Client, name string) {
		t.Helper()
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(name),
			AttributeDefinitions: []dbtypes.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: dbtypes.ScalarAttributeTypeS},
			},
			KeySchema: []dbtypes.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: dbtypes.KeyTypeHash},
			},
			BillingMode: dbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)
	}

	createTable(ddA, "acct-iso-table")
	createTable(ddB, "acct-iso-table")

	listA, err := ddA.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	assert.Contains(t, listA.TableNames, "acct-iso-table")

	// Verify A can PutItem without seeing B's items.
	_, err = ddA.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("acct-iso-table"),
		Item: map[string]dbtypes.AttributeValue{
			"pk":   &dbtypes.AttributeValueMemberS{Value: "key-from-A"},
			"data": &dbtypes.AttributeValueMemberS{Value: "value-A"},
		},
	})
	require.NoError(t, err)

	// B's copy of the table must not have A's item.
	getResp, err := ddB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("acct-iso-table"),
		Key: map[string]dbtypes.AttributeValue{
			"pk": &dbtypes.AttributeValueMemberS{Value: "key-from-A"},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, getResp.Item, "B should not see A's item")
}

// ─── KMS isolation / cross-account blob rejection ─────────────────────────────

// TestKMS_AccountIsolation verifies that a key created in account A is not
// listed by account B and that a ciphertext from A is rejected by B's Decrypt.
func TestKMS_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsA := newKMSFor(t, AcctA)
	kmsB := newKMSFor(t, AcctB)

	// Create a key in account A.
	keyOut, err := kmsA.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyIDa := aws.ToString(keyOut.KeyMetadata.KeyId)

	// Encrypt plaintext using A's key.
	encOut, err := kmsA.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyIDa),
		Plaintext: []byte("secret data"),
	})
	require.NoError(t, err)
	ciphertext := encOut.CiphertextBlob

	// B tries to decrypt A's ciphertext — must fail with IncorrectKeyException
	// (the v2 blob carries A's account ID; B's account differs).
	_, err = kmsB.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: ciphertext,
	})
	require.Error(t, err, "B should not be able to decrypt A's ciphertext")
	assert.Contains(t, err.Error(), "IncorrectKeyException",
		"error should be IncorrectKeyException for cross-account blob")

	// A can still decrypt its own ciphertext.
	decOut, err := kmsA.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: ciphertext,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("secret data"), decOut.Plaintext)
}

// TestKMS_KeysNotShared verifies that key IDs created in A are not listed by B.
func TestKMS_KeysNotShared(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsA := newKMSFor(t, AcctA)
	kmsB := newKMSFor(t, AcctB)

	// Create keys in both accounts.
	outA, err := kmsA.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	_, err = kmsB.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyIDA := aws.ToString(outA.KeyMetadata.KeyId)

	// B's ListKeys must not include A's key.
	listB, err := kmsB.ListKeys(ctx, &kms.ListKeysInput{})
	require.NoError(t, err)
	for _, k := range listB.Keys {
		assert.NotEqual(t, keyIDA, aws.ToString(k.KeyId),
			"B's ListKeys must not include A's key")
	}
}

// TestKMS_GenerateDataKey_CrossAccountBlob verifies that a data key encrypted
// under A's CMK carries A's account in the blob and cannot be decrypted by B.
func TestKMS_GenerateDataKey_CrossAccountBlob(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsA := newKMSFor(t, AcctA)
	kmsB := newKMSFor(t, AcctB)

	keyOut, err := kmsA.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	gdkOut, err := kmsA.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   keyOut.KeyMetadata.KeyId,
		KeySpec: "AES_256",
	})
	require.NoError(t, err)

	// The encrypted data key is A's ciphertext; B must not decrypt it.
	_, err = kmsB.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: gdkOut.CiphertextBlob,
	})
	require.Error(t, err, "B should not decrypt A's data key ciphertext")

	// Verify A can round-trip the data key.
	decOut, err := kmsA.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: gdkOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, base64.StdEncoding.EncodeToString(gdkOut.Plaintext),
		base64.StdEncoding.EncodeToString(decOut.Plaintext))
}
