//go:build dynamo_fullmode

package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGSI_QueryByIndex verifies that items written to a table with a GSI can be
// retrieved using the GSI's partition key — the canonical GSI use case.
func TestGSI_QueryByIndex(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	// Create table with a GSI: StatusIndex on Status (PK) + CreatedAt (SK).
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Orders"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("OrderId"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Status"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("CreatedAt"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("OrderId"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("StatusIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("Status"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("CreatedAt"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	// Put items with different statuses.
	items := []map[string]types.AttributeValue{
		{"OrderId": strAttr("o1"), "Status": strAttr("pending"), "CreatedAt": strAttr("2024-01-01"), "Amount": numAttr("100")},
		{"OrderId": strAttr("o2"), "Status": strAttr("pending"), "CreatedAt": strAttr("2024-01-02"), "Amount": numAttr("200")},
		{"OrderId": strAttr("o3"), "Status": strAttr("shipped"), "CreatedAt": strAttr("2024-01-03"), "Amount": numAttr("300")},
		{"OrderId": strAttr("o4"), "Status": strAttr("pending"), "CreatedAt": strAttr("2024-01-04"), "Amount": numAttr("50")},
	}
	for _, item := range items {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("Orders"),
			Item:      item,
		})
		require.NoError(t, err)
	}

	// Query by GSI: all pending orders.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Orders"),
		IndexName:                 aws.String("StatusIndex"),
		KeyConditionExpression:    aws.String("#s = :status"),
		ExpressionAttributeNames:  map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":status": strAttr("pending")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), out.Count, "expected 3 pending orders")

	// Results should be sorted by CreatedAt (GSI SK) ascending.
	require.Len(t, out.Items, 3)
	assert.Equal(t, "2024-01-01", out.Items[0]["CreatedAt"].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, "2024-01-02", out.Items[1]["CreatedAt"].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, "2024-01-04", out.Items[2]["CreatedAt"].(*types.AttributeValueMemberS).Value)

	// Query for shipped orders — should return exactly one.
	outShipped, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Orders"),
		IndexName:                 aws.String("StatusIndex"),
		KeyConditionExpression:    aws.String("#s = :status"),
		ExpressionAttributeNames:  map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":status": strAttr("shipped")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), outShipped.Count)
}

// TestGSI_QueryWithSKCondition verifies GSI queries with sort key conditions
// (begins_with and BETWEEN).
func TestGSI_QueryWithSKCondition(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Events"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("EventId"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Category"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Timestamp"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("EventId"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("CategoryIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("Category"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("Timestamp"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	for i, ts := range []string{"2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01", "2025-01-01"} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("Events"),
			Item: map[string]types.AttributeValue{
				"EventId":   strAttr("e" + string(rune('0'+i+1))),
				"Category":  strAttr("click"),
				"Timestamp": strAttr(ts),
			},
		})
		require.NoError(t, err)
	}

	// BETWEEN filter on GSI SK.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String("Events"),
		IndexName:                aws.String("CategoryIndex"),
		KeyConditionExpression:   aws.String("Category = :cat AND #ts BETWEEN :lo AND :hi"),
		ExpressionAttributeNames: map[string]string{"#ts": "Timestamp"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cat": strAttr("click"),
			":lo":  strAttr("2024-01-01"),
			":hi":  strAttr("2024-04-30"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(4), out.Count, "expected 4 events in 2024")

	// begins_with filter on GSI SK.
	outBW, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String("Events"),
		IndexName:                aws.String("CategoryIndex"),
		KeyConditionExpression:   aws.String("Category = :cat AND begins_with(#ts, :prefix)"),
		ExpressionAttributeNames: map[string]string{"#ts": "Timestamp"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cat":    strAttr("click"),
			":prefix": strAttr("2024"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(4), outBW.Count, "expected 4 events beginning with 2024")
}

// TestLSI_QueryBySortKey verifies that items with the same partition key can be
// queried via a Local Secondary Index using a different sort key attribute.
func TestLSI_QueryBySortKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Products"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("Category"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ProductId"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Price"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("Category"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("ProductId"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("PriceIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("Category"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("Price"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	// Put products in the same category with different prices.
	products := []map[string]types.AttributeValue{
		{"Category": strAttr("electronics"), "ProductId": strAttr("p1"), "Price": numAttr("999"), "Name": strAttr("Laptop")},
		{"Category": strAttr("electronics"), "ProductId": strAttr("p2"), "Price": numAttr("299"), "Name": strAttr("Phone")},
		{"Category": strAttr("electronics"), "ProductId": strAttr("p3"), "Price": numAttr("49"), "Name": strAttr("Cable")},
		{"Category": strAttr("books"), "ProductId": strAttr("p4"), "Price": numAttr("25"), "Name": strAttr("Novel")},
	}
	for _, item := range products {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("Products"),
			Item:      item,
		})
		require.NoError(t, err)
	}

	// Query via LSI: electronics ordered by price.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("Products"),
		IndexName:              aws.String("PriceIndex"),
		KeyConditionExpression: aws.String("Category = :cat"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cat": strAttr("electronics"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), out.Count, "expected 3 electronics")

	// Verify numeric sort by Price ascending.
	require.Len(t, out.Items, 3)
	assert.Equal(t, "49", out.Items[0]["Price"].(*types.AttributeValueMemberN).Value)
	assert.Equal(t, "299", out.Items[1]["Price"].(*types.AttributeValueMemberN).Value)
	assert.Equal(t, "999", out.Items[2]["Price"].(*types.AttributeValueMemberN).Value)

	// Query with BETWEEN on LSI sort key (numeric).
	outRange, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("Products"),
		IndexName:              aws.String("PriceIndex"),
		KeyConditionExpression: aws.String("Category = :cat AND Price BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cat": strAttr("electronics"),
			":lo":  numAttr("100"),
			":hi":  numAttr("9999"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), outRange.Count, "expected 2 electronics above $100")
}

// TestGSI_SparseIndex verifies that items without the GSI partition key attribute
// are not included in GSI query results (sparse index behaviour).
func TestGSI_SparseIndex(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Users"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("UserId"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Email"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("UserId"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("EmailIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("Email"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	// User with email (appears in sparse index).
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Users"),
		Item: map[string]types.AttributeValue{
			"UserId": strAttr("u1"),
			"Email":  strAttr("alice@example.com"),
		},
	})
	require.NoError(t, err)

	// User without email (must NOT appear in index).
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Users"),
		Item:      map[string]types.AttributeValue{"UserId": strAttr("u2"), "Name": strAttr("Bob")},
	})
	require.NoError(t, err)

	// Query via EmailIndex — only u1 should appear.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("Users"),
		IndexName:              aws.String("EmailIndex"),
		KeyConditionExpression: aws.String("Email = :email"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":email": strAttr("alice@example.com"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Count)
	assert.Equal(t, "u1", out.Items[0]["UserId"].(*types.AttributeValueMemberS).Value)
}

// TestGSI_UpdateItemReflectsInIndex verifies that updating a GSI attribute on an
// existing item correctly moves the item to the new GSI partition.
func TestGSI_UpdateItemReflectsInIndex(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Tasks"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("TaskId"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("TaskId"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("StatusIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("Status"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	// Put a task with Status=open.
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Tasks"),
		Item: map[string]types.AttributeValue{
			"TaskId": strAttr("t1"),
			"Status": strAttr("open"),
			"Title":  strAttr("Fix the bug"),
		},
	})
	require.NoError(t, err)

	// Query before update.
	outBefore, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Tasks"),
		IndexName:                 aws.String("StatusIndex"),
		KeyConditionExpression:    aws.String("#s = :s"),
		ExpressionAttributeNames:  map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":s": strAttr("open")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), outBefore.Count, "t1 should be in open partition before update")

	// Update Status to done.
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String("Tasks"),
		Key:                       map[string]types.AttributeValue{"TaskId": strAttr("t1")},
		UpdateExpression:          aws.String("SET #s = :done"),
		ExpressionAttributeNames:  map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":done": strAttr("done")},
	})
	require.NoError(t, err)

	// After update, open partition must be empty.
	outOpen, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Tasks"),
		IndexName:                 aws.String("StatusIndex"),
		KeyConditionExpression:    aws.String("#s = :s"),
		ExpressionAttributeNames:  map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":s": strAttr("open")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), outOpen.Count, "open partition should be empty after update")

	// done partition must contain t1.
	outDone, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Tasks"),
		IndexName:                 aws.String("StatusIndex"),
		KeyConditionExpression:    aws.String("#s = :s"),
		ExpressionAttributeNames:  map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":s": strAttr("done")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), outDone.Count, "t1 should be in done partition after update")
}

// TestGSI_DeleteItemRemovesFromIndex verifies that deleting an item removes it
// from all GSI index partitions.
func TestGSI_DeleteItemRemovesFromIndex(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Sessions"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("SessionId"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("UserId"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("SessionId"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("UserIndex"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("UserId"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("Sessions"),
		Item: map[string]types.AttributeValue{
			"SessionId": strAttr("s1"),
			"UserId":    strAttr("user42"),
		},
	})
	require.NoError(t, err)

	// Confirm item is in the GSI.
	outBefore, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Sessions"),
		IndexName:                 aws.String("UserIndex"),
		KeyConditionExpression:    aws.String("UserId = :uid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":uid": strAttr("user42")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), outBefore.Count)

	// Delete the item.
	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("Sessions"),
		Key:       map[string]types.AttributeValue{"SessionId": strAttr("s1")},
	})
	require.NoError(t, err)

	// GSI partition must now be empty.
	outAfter, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("Sessions"),
		IndexName:                 aws.String("UserIndex"),
		KeyConditionExpression:    aws.String("UserId = :uid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":uid": strAttr("user42")},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), outAfter.Count, "item should be removed from GSI after DeleteItem")
}

// TestGSI_UpdateTableAddAndDelete verifies that a GSI can be added to an
// existing table via UpdateTable and then deleted.
func TestGSI_UpdateTableAddAndDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("Catalog"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("Id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Tag"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("Id"), KeyType: types.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	// Add two items before creating the GSI.
	for _, item := range []map[string]types.AttributeValue{
		{"Id": strAttr("item1"), "Tag": strAttr("alpha"), "Data": strAttr("value1")},
		{"Id": strAttr("item2"), "Tag": strAttr("beta"), "Data": strAttr("value2")},
		{"Id": strAttr("item3"), "Tag": strAttr("alpha"), "Data": strAttr("value3")},
	} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("Catalog"),
			Item:      item,
		})
		require.NoError(t, err)
	}

	// Add a GSI via UpdateTable.
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("Catalog"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("Id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Tag"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("TagIndex"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("Tag"), KeyType: types.KeyTypeHash},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
	})
	require.NoError(t, err)

	// Query via new GSI — backfilled items should be visible.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("Catalog"),
		IndexName:              aws.String("TagIndex"),
		KeyConditionExpression: aws.String("Tag = :tag"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":tag": strAttr("alpha"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), out.Count, "expected 2 items with tag=alpha after GSI backfill")

	// Delete the GSI.
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("Catalog"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Delete: &types.DeleteGlobalSecondaryIndexAction{
					IndexName: aws.String("TagIndex"),
				},
			},
		},
	})
	require.NoError(t, err)

	// Confirm DescribeTable no longer reports the GSI.
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String("Catalog"),
	})
	require.NoError(t, err)
	assert.Empty(t, desc.Table.GlobalSecondaryIndexes, "GSI should be removed after deletion")
}

// TestGSI_CountLimitEnforced verifies that creating more than 20 GSIs returns a
// ValidationException.
func TestGSI_CountLimitEnforced(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	attrDefs := []types.AttributeDefinition{
		{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
	}
	gsis := make([]types.GlobalSecondaryIndex, 21)
	for i := range gsis {
		attrName := aws.String("Attr" + string(rune('A'+i)))
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: attrName,
			AttributeType: types.ScalarAttributeTypeS,
		})
		gsis[i] = types.GlobalSecondaryIndex{
			IndexName: aws.String("Idx" + string(rune('A'+i))),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: attrName, KeyType: types.KeyTypeHash},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}
	}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:              aws.String("TooManyGSI"),
		BillingMode:            types.BillingModePayPerRequest,
		AttributeDefinitions:   attrDefs,
		KeySchema:              []types.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash}},
		GlobalSecondaryIndexes: gsis,
	})
	require.Error(t, err, "creating 21 GSIs should fail")
}

// TestLSI_CountLimitEnforced verifies that creating more than 5 LSIs returns a
// ValidationException.
func TestLSI_CountLimitEnforced(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	attrDefs := []types.AttributeDefinition{
		{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
	}
	lsis := make([]types.LocalSecondaryIndex, 6)
	for i := range lsis {
		attrName := aws.String("LSIAttr" + string(rune('A'+i)))
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: attrName,
			AttributeType: types.ScalarAttributeTypeS,
		})
		lsis[i] = types.LocalSecondaryIndex{
			IndexName: aws.String("LSI" + string(rune('A'+i))),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
				{AttributeName: attrName, KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}
	}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:             aws.String("TooManyLSI"),
		BillingMode:           types.BillingModePayPerRequest,
		AttributeDefinitions:  attrDefs,
		KeySchema:             []types.KeySchemaElement{{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange}},
		LocalSecondaryIndexes: lsis,
	})
	require.Error(t, err, "creating 6 LSIs should fail")
}

// TestGSI_DeleteTableRemovesIndexData verifies that deleting a table cleans up
// its GSI data without affecting other tables.
func TestGSI_DeleteTableRemovesIndexData(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	for _, tbl := range []string{"TableA", "TableB"} {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String(tbl),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("Id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("Tag"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("Id"), KeyType: types.KeyTypeHash},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("TagIdx"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("Tag"), KeyType: types.KeyTypeHash},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tbl),
			Item: map[string]types.AttributeValue{
				"Id":  strAttr("item1"),
				"Tag": strAttr("shared"),
			},
		})
		require.NoError(t, err)
	}

	// Delete only TableA.
	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String("TableA"),
	})
	require.NoError(t, err)

	// TableB's GSI must still return its item.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("TableB"),
		IndexName:              aws.String("TagIdx"),
		KeyConditionExpression: aws.String("Tag = :tag"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":tag": strAttr("shared"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.Count, "TableB GSI item must survive deletion of TableA")
}
