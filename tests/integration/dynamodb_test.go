package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTable(t *testing.T, client *awsdynamo.Client, name string) {
	t.Helper()
	_, err := client.CreateTable(context.Background(), &awsdynamo.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

func TestDynamoDB_CreateDescribeDeleteTable(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "test-table")

	out, err := client.DescribeTable(ctx, &awsdynamo.DescribeTableInput{
		TableName: aws.String("test-table"),
	})
	require.NoError(t, err)
	require.Equal(t, "test-table", *out.Table.TableName)

	listOut, err := client.ListTables(ctx, &awsdynamo.ListTablesInput{})
	require.NoError(t, err)
	require.Contains(t, listOut.TableNames, "test-table")

	_, err = client.DeleteTable(ctx, &awsdynamo.DeleteTableInput{TableName: aws.String("test-table")})
	require.NoError(t, err)

	listOut2, err := client.ListTables(ctx, &awsdynamo.ListTablesInput{})
	require.NoError(t, err)
	require.NotContains(t, listOut2.TableNames, "test-table")
}

func TestDynamoDB_PutGetDeleteItem(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "items-table")

	_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("items-table"),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "user#1"},
			"SK":   &types.AttributeValueMemberS{Value: "profile"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
			"Age":  &types.AttributeValueMemberN{Value: "30"},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("items-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "user#1"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)
	name, ok := getOut.Item["Name"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "Alice", name.Value)

	_, err = client.DeleteItem(ctx, &awsdynamo.DeleteItemInput{
		TableName: aws.String("items-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "user#1"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
		},
	})
	require.NoError(t, err)

	getOut2, err := client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("items-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "user#1"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
		},
	})
	require.NoError(t, err)
	require.Nil(t, getOut2.Item)
}

func TestDynamoDB_UpdateItem(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "update-table")

	_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("update-table"),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "item#1"},
			"SK":    &types.AttributeValueMemberS{Value: "v1"},
			"Count": &types.AttributeValueMemberN{Value: "5"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName: aws.String("update-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "item#1"},
			"SK": &types.AttributeValueMemberS{Value: "v1"},
		},
		UpdateExpression: aws.String("SET #n = :val"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "Updated"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.NoError(t, err)

	getOut, err := client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("update-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "item#1"},
			"SK": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)
	n, ok := getOut.Item["Name"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "Updated", n.Value)
}

func TestDynamoDB_Scan(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "scan-table")

	for i, name := range []string{"Alice", "Bob", "Carol"} {
		_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("scan-table"),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "user"},
				"SK":   &types.AttributeValueMemberS{Value: name},
				"Idx":  &types.AttributeValueMemberN{Value: string(rune('0' + i))},
			},
		})
		require.NoError(t, err)
	}

	scanOut, err := client.Scan(ctx, &awsdynamo.ScanInput{
		TableName: aws.String("scan-table"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), scanOut.Count)
}

func TestDynamoDB_Query(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "query-table")

	for _, sk := range []string{"a", "b", "c"} {
		_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("query-table"),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "tenant#1"},
				"SK": &types.AttributeValueMemberS{Value: sk},
			},
		})
		require.NoError(t, err)
	}

	qOut, err := client.Query(ctx, &awsdynamo.QueryInput{
		TableName:              aws.String("query-table"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant#1"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), qOut.Count)
}

func TestDynamoDB_BatchWriteGetItem(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "batch-table")

	_, err := client.BatchWriteItem(ctx, &awsdynamo.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"batch-table": {
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "pk"},
					"SK": &types.AttributeValueMemberS{Value: "sk1"},
				}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "pk"},
					"SK": &types.AttributeValueMemberS{Value: "sk2"},
				}}},
			},
		},
	})
	require.NoError(t, err)

	scanOut, err := client.Scan(ctx, &awsdynamo.ScanInput{TableName: aws.String("batch-table")})
	require.NoError(t, err)
	require.Equal(t, int32(2), scanOut.Count)
}

func TestDynamoDB_TransactWriteGetItems(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "txn-table")

	// Seed an item to delete within the transaction.
	_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("txn-table"),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "del"},
			"SK": &types.AttributeValueMemberS{Value: "v0"},
		},
	})
	require.NoError(t, err)

	// Transact: put two items, delete one.
	_, err = client.TransactWriteItems(ctx, &awsdynamo.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{
				TableName: aws.String("txn-table"),
				Item: map[string]types.AttributeValue{
					"PK":    &types.AttributeValueMemberS{Value: "a"},
					"SK":    &types.AttributeValueMemberS{Value: "v1"},
					"Value": &types.AttributeValueMemberS{Value: "alpha"},
				},
			}},
			{Put: &types.Put{
				TableName: aws.String("txn-table"),
				Item: map[string]types.AttributeValue{
					"PK":    &types.AttributeValueMemberS{Value: "b"},
					"SK":    &types.AttributeValueMemberS{Value: "v1"},
					"Value": &types.AttributeValueMemberS{Value: "beta"},
				},
			}},
			{Delete: &types.Delete{
				TableName: aws.String("txn-table"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "del"},
					"SK": &types.AttributeValueMemberS{Value: "v0"},
				},
			}},
		},
	})
	require.NoError(t, err)

	// TransactGetItems reads both written items.
	getOut, err := client.TransactGetItems(ctx, &awsdynamo.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{
				TableName: aws.String("txn-table"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "a"},
					"SK": &types.AttributeValueMemberS{Value: "v1"},
				},
			}},
			{Get: &types.Get{
				TableName: aws.String("txn-table"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "b"},
					"SK": &types.AttributeValueMemberS{Value: "v1"},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.Len(t, getOut.Responses, 2)
	valA, ok := getOut.Responses[0].Item["Value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "alpha", valA.Value)

	// Deleted item should be gone.
	deletedOut, err := client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("txn-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "del"},
			"SK": &types.AttributeValueMemberS{Value: "v0"},
		},
	})
	require.NoError(t, err)
	assert.Nil(t, deletedOut.Item)
}

func TestDynamoDB_BatchGetItem(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "bget-table")

	// Write 3 items.
	for i := 1; i <= 3; i++ {
		_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("bget-table"),
			Item: map[string]types.AttributeValue{
				"PK":  &types.AttributeValueMemberS{Value: "pk"},
				"SK":  &types.AttributeValueMemberS{Value: fmt.Sprintf("sk%d", i)},
				"Num": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
			},
		})
		require.NoError(t, err)
	}

	// BatchGetItem for sk1 and sk3 (skip sk2).
	bgOut, err := client.BatchGetItem(ctx, &awsdynamo.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"bget-table": {
				Keys: []map[string]types.AttributeValue{
					{"PK": &types.AttributeValueMemberS{Value: "pk"}, "SK": &types.AttributeValueMemberS{Value: "sk1"}},
					{"PK": &types.AttributeValueMemberS{Value: "pk"}, "SK": &types.AttributeValueMemberS{Value: "sk3"}},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, bgOut.Responses["bget-table"], 2)
	assert.Empty(t, bgOut.UnprocessedKeys)
}

func TestDynamoDB_FilterExpression(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "filter-table")

	// Insert items with different Status values.
	for i, status := range []string{"active", "active", "inactive"} {
		_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("filter-table"),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: "pk"},
				"SK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("%s-%d", status, i)},
				"Status": &types.AttributeValueMemberS{Value: status},
			},
		})
		require.NoError(t, err)
	}

	// Scan with FilterExpression to get only "active" items.
	scanOut, err := client.Scan(ctx, &awsdynamo.ScanInput{
		TableName:        aws.String("filter-table"),
		FilterExpression: aws.String("#s = :val"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), scanOut.Count)
}

func TestDynamoDB_LimitReturnsLastEvaluatedKey(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "limit-table")

	// Insert 5 items under the same PK.
	for i := 1; i <= 5; i++ {
		_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("limit-table"),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "partition"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("item%d", i)},
			},
		})
		require.NoError(t, err)
	}

	// Query with Limit=2 — should return 2 items and a LastEvaluatedKey.
	qOut, err := client.Query(ctx, &awsdynamo.QueryInput{
		TableName:              aws.String("limit-table"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "partition"},
		},
		Limit: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), qOut.Count)
	assert.NotNil(t, qOut.LastEvaluatedKey, "expected LastEvaluatedKey when Limit truncates results")
}

func TestDynamoDB_ConditionalPut(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "cond-table")

	// First put — no existing item, condition "attribute_not_exists(PK)" should pass.
	_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("cond-table"),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "k1"},
			"SK":    &types.AttributeValueMemberS{Value: "v1"},
			"Count": &types.AttributeValueMemberN{Value: "1"},
		},
		ConditionExpression: aws.String("PK = :absent"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":absent": &types.AttributeValueMemberS{Value: "no-such-key"},
		},
	})
	// Condition fails because item doesn't exist yet, so PK can't equal anything.
	require.Error(t, err, "condition should fail on nonexistent item with equality check")

	// Put without condition — must succeed.
	_, err = client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("cond-table"),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "k1"},
			"SK":    &types.AttributeValueMemberS{Value: "v1"},
			"Count": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	// Now condition "Count = :one" should pass.
	_, err = client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("cond-table"),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "k1"},
			"SK":    &types.AttributeValueMemberS{Value: "v1"},
			"Count": &types.AttributeValueMemberN{Value: "2"},
		},
		ConditionExpression: aws.String("Count = :one"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	// Condition "Count = :one" now fails (Count is 2).
	_, err = client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("cond-table"),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "k1"},
			"SK":    &types.AttributeValueMemberS{Value: "v1"},
			"Count": &types.AttributeValueMemberN{Value: "99"},
		},
		ConditionExpression: aws.String("Count = :one"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	require.Error(t, err, "condition should fail when Count != 1")
}

func TestDynamoDB_ConditionalDelete(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "cdel-table")

	_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("cdel-table"),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "row1"},
			"SK":     &types.AttributeValueMemberS{Value: "v1"},
			"Status": &types.AttributeValueMemberS{Value: "pending"},
		},
	})
	require.NoError(t, err)

	// Delete with wrong condition — should fail.
	_, err = client.DeleteItem(ctx, &awsdynamo.DeleteItemInput{
		TableName: aws.String("cdel-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "row1"},
			"SK": &types.AttributeValueMemberS{Value: "v1"},
		},
		ConditionExpression: aws.String("Status = :done"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":done": &types.AttributeValueMemberS{Value: "done"},
		},
	})
	require.Error(t, err, "condition should fail: Status is 'pending' not 'done'")

	// Item should still exist.
	getOut, err := client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("cdel-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "row1"},
			"SK": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)

	// Delete with correct condition — should succeed.
	_, err = client.DeleteItem(ctx, &awsdynamo.DeleteItemInput{
		TableName: aws.String("cdel-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "row1"},
			"SK": &types.AttributeValueMemberS{Value: "v1"},
		},
		ConditionExpression: aws.String("Status = :pending"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pending": &types.AttributeValueMemberS{Value: "pending"},
		},
	})
	require.NoError(t, err)
}

func TestDynamoDB_ReturnValuesAllOld(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "rv-table")

	// Seed an item.
	_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("rv-table"),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "item1"},
			"SK":   &types.AttributeValueMemberS{Value: "v1"},
			"Name": &types.AttributeValueMemberS{Value: "original"},
		},
	})
	require.NoError(t, err)

	// Overwrite with ReturnValues=ALL_OLD — should get the original item back.
	putOut, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
		TableName: aws.String("rv-table"),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "item1"},
			"SK":   &types.AttributeValueMemberS{Value: "v1"},
			"Name": &types.AttributeValueMemberS{Value: "updated"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.Attributes)
	nameAttr, ok := putOut.Attributes["Name"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "original", nameAttr.Value)

	// DeleteItem with ReturnValues=ALL_OLD — should return the item just written.
	delOut, err := client.DeleteItem(ctx, &awsdynamo.DeleteItemInput{
		TableName: aws.String("rv-table"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "item1"},
			"SK": &types.AttributeValueMemberS{Value: "v1"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.Attributes)
	deletedName, ok := delOut.Attributes["Name"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "updated", deletedName.Value)
}

func TestDynamoDB_QueryPagination(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "page-table")

	// Insert 5 items under the same partition key.
	for i := 1; i <= 5; i++ {
		_, err := client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("page-table"),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "tenant"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("row%d", i)},
			},
		})
		require.NoError(t, err)
	}

	// First page: limit 2.
	p1, err := client.Query(ctx, &awsdynamo.QueryInput{
		TableName:              aws.String("page-table"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant"},
		},
		Limit: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), p1.Count)
	require.NotNil(t, p1.LastEvaluatedKey, "page 1 must have LastEvaluatedKey")

	// Second page using ExclusiveStartKey.
	p2, err := client.Query(ctx, &awsdynamo.QueryInput{
		TableName:              aws.String("page-table"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant"},
		},
		Limit:             aws.Int32(2),
		ExclusiveStartKey: p1.LastEvaluatedKey,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), p2.Count)

	// Third page — last 1 item, no more pages.
	p3, err := client.Query(ctx, &awsdynamo.QueryInput{
		TableName:              aws.String("page-table"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant"},
		},
		Limit:             aws.Int32(2),
		ExclusiveStartKey: p2.LastEvaluatedKey,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), p3.Count)
	assert.Nil(t, p3.LastEvaluatedKey, "last page must not have LastEvaluatedKey")
}

func TestDynamoDB_TableTags(t *testing.T) {
	resetState(t)
	client := newDynamoClient(t)
	ctx := context.Background()

	makeTable(t, client, "tagged-table")

	// Get the table ARN from DescribeTable.
	desc, err := client.DescribeTable(ctx, &awsdynamo.DescribeTableInput{
		TableName: aws.String("tagged-table"),
	})
	require.NoError(t, err)
	tableArn := aws.ToString(desc.Table.TableArn)
	require.NotEmpty(t, tableArn)

	// Tag the table.
	_, err = client.TagResource(ctx, &awsdynamo.TagResourceInput{
		ResourceArn: aws.String(tableArn),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("owner"), Value: aws.String("team-a")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsOfResource(ctx, &awsdynamo.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableArn),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	// Untag one key.
	_, err = client.UntagResource(ctx, &awsdynamo.UntagResourceInput{
		ResourceArn: aws.String(tableArn),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsOfResource(ctx, &awsdynamo.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableArn),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "owner", aws.ToString(listOut2.Tags[0].Key))
}

// ─── UpdateItem ConditionExpression ───────────────────────────────────────────

func TestDynamoDB_UpdateItem_ConditionExpression(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)
	makeTable(t, client, "update-cond-tbl")

	// Scenario 1: attribute_exists on non-existent item → ConditionalCheckFailedException
	_, err := client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:           aws.String("update-cond-tbl"),
		Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression:    aws.String("SET #n = :v"),
		ConditionExpression: aws.String("attribute_exists(ownerName)"),
		ExpressionAttributeNames:  map[string]string{"#n": "ownerName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "alice"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConditionalCheckFailedException")

	// Scenario 2: attribute_not_exists on non-existent item → succeeds, creates item
	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:           aws.String("update-cond-tbl"),
		Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression:    aws.String("SET ownerName = :v"),
		ConditionExpression: aws.String("attribute_not_exists(ownerName)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "alice"}},
	})
	require.NoError(t, err)

	item, err := client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("update-cond-tbl"),
		Key:       map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "alice", item.Item["ownerName"].(*types.AttributeValueMemberS).Value)

	// Scenario 3: attribute_exists on an item that has the attribute → succeeds
	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:           aws.String("update-cond-tbl"),
		Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression:    aws.String("SET ownerName = :v"),
		ConditionExpression: aws.String("attribute_exists(ownerName)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "bob"}},
	})
	require.NoError(t, err)

	item, err = client.GetItem(ctx, &awsdynamo.GetItemInput{
		TableName: aws.String("update-cond-tbl"),
		Key:       map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "bob", item.Item["ownerName"].(*types.AttributeValueMemberS).Value)

	// Scenario 4: attribute_not_exists on an item that already has the attribute → fails
	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:           aws.String("update-cond-tbl"),
		Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression:    aws.String("SET ownerName = :v"),
		ConditionExpression: aws.String("attribute_not_exists(ownerName)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "carol"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConditionalCheckFailedException")

	// Scenario 5: no ConditionExpression on non-existent item → succeeds (unchanged behaviour)
	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:        aws.String("update-cond-tbl"),
		Key:              map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k2"}},
		UpdateExpression: aws.String("SET score = :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: "42"}},
	})
	require.NoError(t, err)

	// Scenario 6: condition with ExpressionAttributeNames/#n = :v matching → succeeds, non-matching → fails
	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:           aws.String("update-cond-tbl"),
		Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression:    aws.String("SET ownerName = :new"),
		ConditionExpression: aws.String("#n = :expected"),
		ExpressionAttributeNames:  map[string]string{"#n": "ownerName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberS{Value: "bob"},
			":new":      &types.AttributeValueMemberS{Value: "dave"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &awsdynamo.UpdateItemInput{
		TableName:           aws.String("update-cond-tbl"),
		Key:                 map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression:    aws.String("SET ownerName = :new"),
		ConditionExpression: aws.String("#n = :expected"),
		ExpressionAttributeNames:  map[string]string{"#n": "ownerName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberS{Value: "wrong"},
			":new":      &types.AttributeValueMemberS{Value: "eve"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConditionalCheckFailedException")
}

func TestDynamoDB_BatchWriteItem_Delete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:            aws.String("batch-del-tbl"),
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Write 3 items
	_, err = client.BatchWriteItem(ctx, &awsdynamo.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"batch-del-tbl": {
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k2"}}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k3"}}}},
			},
		},
	})
	require.NoError(t, err)

	// Delete k1 and k3 via BatchWriteItem
	_, err = client.BatchWriteItem(ctx, &awsdynamo.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"batch-del-tbl": {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k1"}}}},
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "k3"}}}},
			},
		},
	})
	require.NoError(t, err)

	// Only k2 should remain
	scanOut, err := client.Scan(ctx, &awsdynamo.ScanInput{TableName: aws.String("batch-del-tbl")})
	require.NoError(t, err)
	require.Len(t, scanOut.Items, 1)
	assert.Equal(t, "k2", scanOut.Items[0]["PK"].(*types.AttributeValueMemberS).Value)
}

func TestDynamoDB_ScanPagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName:            aws.String("scan-page-tbl"),
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Write 5 items
	for i := 1; i <= 5; i++ {
		_, err = client.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String("scan-page-tbl"),
			Item:      map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("item%d", i)}},
		})
		require.NoError(t, err)
	}

	// Page 1: limit=2
	page1, err := client.Scan(ctx, &awsdynamo.ScanInput{
		TableName: aws.String("scan-page-tbl"),
		Limit:     aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, page1.Items, 2)
	assert.NotNil(t, page1.LastEvaluatedKey, "expected a pagination cursor")

	// Page 2: continue from cursor
	page2, err := client.Scan(ctx, &awsdynamo.ScanInput{
		TableName:         aws.String("scan-page-tbl"),
		Limit:             aws.Int32(2),
		ExclusiveStartKey: page1.LastEvaluatedKey,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Items, 2)

	// Page 3: last item
	page3, err := client.Scan(ctx, &awsdynamo.ScanInput{
		TableName:         aws.String("scan-page-tbl"),
		ExclusiveStartKey: page2.LastEvaluatedKey,
	})
	require.NoError(t, err)
	assert.Len(t, page3.Items, 1)
	assert.Nil(t, page3.LastEvaluatedKey)

	// All keys across pages are unique
	seen := map[string]bool{}
	for _, page := range [][]map[string]types.AttributeValue{page1.Items, page2.Items, page3.Items} {
		for _, item := range page {
			key := item["PK"].(*types.AttributeValueMemberS).Value
			assert.False(t, seen[key], "duplicate key %s across pages", key)
			seen[key] = true
		}
	}
	assert.Len(t, seen, 5)
}

// ─── P1.3: TTL ────────────────────────────────────────────────────────────────

func TestDynamoDB_DescribeTimeToLive_Default(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)
	makeTable(t, client, "ttl-tbl")

	out, err := client.DescribeTimeToLive(ctx, &awsdynamo.DescribeTimeToLiveInput{
		TableName: aws.String("ttl-tbl"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TimeToLiveStatusDisabled, out.TimeToLiveDescription.TimeToLiveStatus)
}

func TestDynamoDB_UpdateTimeToLive_EnableDisable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)
	makeTable(t, client, "ttl-tbl2")

	_, err := client.UpdateTimeToLive(ctx, &awsdynamo.UpdateTimeToLiveInput{
		TableName: aws.String("ttl-tbl2"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expiresAt"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeTimeToLive(ctx, &awsdynamo.DescribeTimeToLiveInput{
		TableName: aws.String("ttl-tbl2"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TimeToLiveStatusEnabled, desc.TimeToLiveDescription.TimeToLiveStatus)
	assert.Equal(t, "expiresAt", aws.ToString(desc.TimeToLiveDescription.AttributeName))

	// Disable
	_, err = client.UpdateTimeToLive(ctx, &awsdynamo.UpdateTimeToLiveInput{
		TableName: aws.String("ttl-tbl2"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expiresAt"),
			Enabled:       aws.Bool(false),
		},
	})
	require.NoError(t, err)

	desc2, err := client.DescribeTimeToLive(ctx, &awsdynamo.DescribeTimeToLiveInput{
		TableName: aws.String("ttl-tbl2"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TimeToLiveStatusDisabled, desc2.TimeToLiveDescription.TimeToLiveStatus)
}

// ─── P1.4: PITR ───────────────────────────────────────────────────────────────

func TestDynamoDB_DescribeContinuousBackups_Default(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)
	makeTable(t, client, "pitr-tbl")

	out, err := client.DescribeContinuousBackups(ctx, &awsdynamo.DescribeContinuousBackupsInput{
		TableName: aws.String("pitr-tbl"),
	})
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", string(out.ContinuousBackupsDescription.ContinuousBackupsStatus))
	assert.Equal(t, types.PointInTimeRecoveryStatusDisabled,
		out.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)
}

func TestDynamoDB_UpdateContinuousBackups_EnableDisable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)
	makeTable(t, client, "pitr-tbl2")

	_, err := client.UpdateContinuousBackups(ctx, &awsdynamo.UpdateContinuousBackupsInput{
		TableName: aws.String("pitr-tbl2"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeContinuousBackups(ctx, &awsdynamo.DescribeContinuousBackupsInput{
		TableName: aws.String("pitr-tbl2"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PointInTimeRecoveryStatusEnabled,
		desc.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)

	// Disable
	_, err = client.UpdateContinuousBackups(ctx, &awsdynamo.UpdateContinuousBackupsInput{
		TableName: aws.String("pitr-tbl2"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(false),
		},
	})
	require.NoError(t, err)

	desc2, err := client.DescribeContinuousBackups(ctx, &awsdynamo.DescribeContinuousBackupsInput{
		TableName: aws.String("pitr-tbl2"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PointInTimeRecoveryStatusDisabled,
		desc2.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)
}

