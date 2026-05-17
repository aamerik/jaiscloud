package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDynamoDBTransactWriteCancellationReasons verifies that when a
// TransactWriteItems call fails due to a condition check failure, the returned
// TransactionCanceledException includes a CancellationReasons array with one
// entry per item in the transaction. Items whose condition passed (or had no
// condition) have Code "None"; items whose condition failed have Code
// "ConditionalCheckFailed".
func TestDynamoDBTransactWriteCancellationReasons(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	// Use a simple hash-key-only table so we can PK-check easily.
	_, err := c.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String("txn-cancel-tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Pre-seed an item that the ConditionCheck will reference.
	// We intentionally do NOT put a "Status" attribute so that the condition
	// "attribute_exists(Status)" will fail.
	_, err = c.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("txn-cancel-tbl"),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "item-exists"},
			"Data": &types.AttributeValueMemberS{Value: "seed"},
		},
	})
	require.NoError(t, err)

	// TransactWriteItems:
	//   Item 0 (Put, no condition) — should succeed → CancellationReason Code "None"
	//   Item 1 (ConditionCheck, condition fails) — should fail → CancellationReason Code "ConditionalCheckFailed"
	_, txnErr := c.TransactWriteItems(ctx, &awsdynamo.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("txn-cancel-tbl"),
					Item: map[string]types.AttributeValue{
						"PK":    &types.AttributeValueMemberS{Value: "new-item"},
						"Value": &types.AttributeValueMemberS{Value: "hello"},
					},
				},
			},
			{
				ConditionCheck: &types.ConditionCheck{
					TableName:           aws.String("txn-cancel-tbl"),
					Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "item-exists"}},
					ConditionExpression: aws.String("attribute_exists(#s)"),
					ExpressionAttributeNames: map[string]string{
						"#s": "Status",
					},
				},
			},
		},
	})

	require.Error(t, txnErr, "TransactWriteItems should fail when a ConditionCheck fails")

	var cancelErr *types.TransactionCanceledException
	require.True(t, errors.As(txnErr, &cancelErr),
		"error must be TransactionCanceledException, got: %T: %v", txnErr, txnErr)

	reasons := cancelErr.CancellationReasons
	require.Len(t, reasons, 2, "CancellationReasons must have one entry per TransactItem")

	// Item 0: Put with no condition — Code must be "None".
	assert.Equal(t, "None", aws.ToString(reasons[0].Code),
		"first item (unconditional Put) should have Code None")

	// Item 1: ConditionCheck that failed — Code must be "ConditionalCheckFailed".
	assert.Equal(t, "ConditionalCheckFailed", aws.ToString(reasons[1].Code),
		"second item (failing ConditionCheck) should have Code ConditionalCheckFailed")
}

// TestDynamoDBBatchWriteItemLimit verifies that BatchWriteItem rejects requests
// that exceed the AWS-mandated maximum of 25 items with a ValidationException.
func TestDynamoDBBatchWriteItemLimit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)

	// Create a table to write into.
	_, err := c.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String("batch-limit-tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Build 26 PutRequests (one over the limit).
	putRequests := make([]types.WriteRequest, 26)
	for i := range putRequests {
		putRequests[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				},
			},
		}
	}

	_, batchErr := c.BatchWriteItem(ctx, &awsdynamo.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"batch-limit-tbl": putRequests,
		},
	})

	require.Error(t, batchErr, "BatchWriteItem with 26 items should fail")
	assertAWSError(t, batchErr, "ValidationException")
}
