package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeExprTable creates a PK(S)+SK(S) table with PAY_PER_REQUEST billing.
func makeExprTable(t *testing.T, c *awsddb.Client, name string) {
	t.Helper()
	_, err := c.CreateTable(context.Background(), &awsddb.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

// ─── G-PENDING-5: DynamoDB Expression Operator Matrix ────────────────────────

// TestDDB_FilterExpression_Numeric_GT verifies that Scan with "age > :val"
// returns only items whose age attribute exceeds the threshold.
func TestDDB_FilterExpression_Numeric_GT(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-gt-tbl")

	for i, age := range []string{"10", "25", "40", "55"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-gt-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":  &ddbtypes.AttributeValueMemberS{Value: "user"},
				"sk":  &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("u%d", i)},
				"age": &ddbtypes.AttributeValueMemberN{Value: age},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-gt-tbl"),
		FilterExpression: aws.String("age > :val"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":val": &ddbtypes.AttributeValueMemberN{Value: "30"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), out.Count, "expected items with age 40 and 55")
}

// TestDDB_FilterExpression_BETWEEN verifies Scan with "age BETWEEN :lo AND :hi".
func TestDDB_FilterExpression_BETWEEN(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-between-tbl")

	for i, age := range []string{"5", "15", "25", "35", "45"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-between-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":  &ddbtypes.AttributeValueMemberS{Value: "user"},
				"sk":  &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("u%d", i)},
				"age": &ddbtypes.AttributeValueMemberN{Value: age},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-between-tbl"),
		FilterExpression: aws.String("age BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":lo": &ddbtypes.AttributeValueMemberN{Value: "10"},
			":hi": &ddbtypes.AttributeValueMemberN{Value: "30"},
		},
	})
	require.NoError(t, err)
	// ages 15 and 25 are in [10,30]
	assert.Equal(t, int32(2), out.Count, "expected items with age 15 and 25")
}

// TestDDB_FilterExpression_IN verifies Scan with "#status IN (:a, :b)".
func TestDDB_FilterExpression_IN(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-in-tbl")

	for i, status := range []string{"active", "inactive", "pending", "active", "archived"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-in-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":     &ddbtypes.AttributeValueMemberS{Value: "items"},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("item%d", i)},
				"status": &ddbtypes.AttributeValueMemberS{Value: status},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-in-tbl"),
		FilterExpression: aws.String("#s IN (:a, :b)"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":a": &ddbtypes.AttributeValueMemberS{Value: "active"},
			":b": &ddbtypes.AttributeValueMemberS{Value: "pending"},
		},
	})
	require.NoError(t, err)
	// "active" appears twice, "pending" once → 3 items
	assert.Equal(t, int32(3), out.Count, "expected items with status active or pending")
}

// TestDDB_FilterExpression_begins_with verifies Scan with "begins_with(#name, :prefix)".
func TestDDB_FilterExpression_begins_with(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-bw-tbl")

	names := []string{"Alice", "Albert", "Bob", "Alicia", "Carol"}
	for i, name := range names {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-bw-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":   &ddbtypes.AttributeValueMemberS{Value: "users"},
				"sk":   &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("u%d", i)},
				"name": &ddbtypes.AttributeValueMemberS{Value: name},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-bw-tbl"),
		FilterExpression: aws.String("begins_with(#n, :prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#n": "name",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "Al"},
		},
	})
	require.NoError(t, err)
	// Alice, Albert, Alicia start with "Al" → 3 items
	assert.Equal(t, int32(3), out.Count, "expected items whose name begins with 'Al'")
}

// TestDDB_FilterExpression_contains verifies Scan with "contains(#tags, :val)".
func TestDDB_FilterExpression_contains(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-contains-tbl")

	// Use string attributes: "contains" on a String checks for substring.
	items := []struct {
		sk  string
		tag string
	}{
		{"i1", "golang"},
		{"i2", "java"},
		{"i3", "golang-advanced"},
		{"i4", "python"},
	}
	for _, item := range items {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-contains-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":  &ddbtypes.AttributeValueMemberS{Value: "topics"},
				"sk":  &ddbtypes.AttributeValueMemberS{Value: item.sk},
				"tag": &ddbtypes.AttributeValueMemberS{Value: item.tag},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-contains-tbl"),
		FilterExpression: aws.String("contains(#t, :val)"),
		ExpressionAttributeNames: map[string]string{
			"#t": "tag",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":val": &ddbtypes.AttributeValueMemberS{Value: "golang"},
		},
	})
	require.NoError(t, err)
	// "golang" and "golang-advanced" both contain "golang" → 2 items
	assert.Equal(t, int32(2), out.Count, "expected items whose tag contains 'golang'")
}

// TestDDB_FilterExpression_attribute_exists verifies Scan with
// "attribute_exists(#opt)" and "attribute_not_exists(#opt)".
func TestDDB_FilterExpression_attribute_exists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-attrex-tbl")

	// Put 3 items: 2 have "optional" field, 1 does not.
	for _, item := range []struct {
		sk      string
		withOpt bool
	}{
		{"i1", true},
		{"i2", false},
		{"i3", true},
	} {
		attrs := map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "base"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: item.sk},
		}
		if item.withOpt {
			attrs["optional"] = &ddbtypes.AttributeValueMemberS{Value: "present"}
		}
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-attrex-tbl"),
			Item:      attrs,
		})
		require.NoError(t, err)
	}

	existsOut, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-attrex-tbl"),
		FilterExpression: aws.String("attribute_exists(#opt)"),
		ExpressionAttributeNames: map[string]string{
			"#opt": "optional",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), existsOut.Count, "expected 2 items with optional field")

	notExistsOut, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-attrex-tbl"),
		FilterExpression: aws.String("attribute_not_exists(#opt)"),
		ExpressionAttributeNames: map[string]string{
			"#opt": "optional",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), notExistsOut.Count, "expected 1 item without optional field")
}

// TestDDB_ConditionExpression_PutItem_IfNotExists verifies that PutItem with
// "attribute_not_exists(pk)" succeeds on a new item but fails on an existing one.
func TestDDB_ConditionExpression_PutItem_IfNotExists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-putcond-tbl")

	// First put — item does not exist yet → condition passes.
	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName:           aws.String("expr-putcond-tbl"),
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: "key1"},
			"sk":   &ddbtypes.AttributeValueMemberS{Value: "v1"},
			"data": &ddbtypes.AttributeValueMemberS{Value: "first"},
		},
	})
	require.NoError(t, err, "first put with attribute_not_exists should succeed")

	// Second put — item now exists → condition fails.
	_, err = c.PutItem(ctx, &awsddb.PutItemInput{
		TableName:           aws.String("expr-putcond-tbl"),
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: "key1"},
			"sk":   &ddbtypes.AttributeValueMemberS{Value: "v1"},
			"data": &ddbtypes.AttributeValueMemberS{Value: "second"},
		},
	})
	require.Error(t, err, "second put with attribute_not_exists should fail")
	var condFailed *ddbtypes.ConditionalCheckFailedException
	assert.ErrorAs(t, err, &condFailed)
}

// TestDDB_ConditionExpression_UpdateItem_CheckVersion verifies optimistic locking:
// UpdateItem with "#v = :expected" succeeds when the version matches and fails otherwise.
func TestDDB_ConditionExpression_UpdateItem_CheckVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-optlock-tbl")

	// Seed item with version=1.
	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-optlock-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":      &ddbtypes.AttributeValueMemberS{Value: "row1"},
			"sk":      &ddbtypes.AttributeValueMemberS{Value: "v"},
			"version": &ddbtypes.AttributeValueMemberN{Value: "1"},
			"data":    &ddbtypes.AttributeValueMemberS{Value: "original"},
		},
	})
	require.NoError(t, err)

	// Update with correct version → succeeds.
	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-optlock-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "row1"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "v"},
		},
		UpdateExpression:    aws.String("SET #d = :newdata, #v = :newver"),
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#d": "data",
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":expected": &ddbtypes.AttributeValueMemberN{Value: "1"},
			":newdata":  &ddbtypes.AttributeValueMemberS{Value: "updated"},
			":newver":   &ddbtypes.AttributeValueMemberN{Value: "2"},
		},
	})
	require.NoError(t, err, "update with correct version should succeed")

	// Update with stale version → fails.
	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-optlock-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "row1"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "v"},
		},
		UpdateExpression:    aws.String("SET #d = :newdata"),
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#d": "data",
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":expected": &ddbtypes.AttributeValueMemberN{Value: "1"}, // stale — version is now 2
			":newdata":  &ddbtypes.AttributeValueMemberS{Value: "stale-write"},
		},
	})
	require.Error(t, err, "update with stale version should fail")
	var condFailed *ddbtypes.ConditionalCheckFailedException
	assert.ErrorAs(t, err, &condFailed)
}

// TestDDB_UpdateExpression_SET_multiple verifies that a single UpdateItem call
// can SET multiple fields simultaneously.
func TestDDB_UpdateExpression_SET_multiple(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-setmulti-tbl")

	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-setmulti-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "obj"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-setmulti-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "obj"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("SET #a = :va, #b = :vb, #c = :vc"),
		ExpressionAttributeNames: map[string]string{
			"#a": "fieldA",
			"#b": "fieldB",
			"#c": "fieldC",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":va": &ddbtypes.AttributeValueMemberS{Value: "alpha"},
			":vb": &ddbtypes.AttributeValueMemberN{Value: "42"},
			":vc": &ddbtypes.AttributeValueMemberBOOL{Value: true},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-setmulti-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "obj"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)

	a, ok := getOut.Item["fieldA"].(*ddbtypes.AttributeValueMemberS)
	require.True(t, ok, "fieldA must be a string")
	assert.Equal(t, "alpha", a.Value)

	b, ok := getOut.Item["fieldB"].(*ddbtypes.AttributeValueMemberN)
	require.True(t, ok, "fieldB must be a number")
	assert.Equal(t, "42", b.Value)

	boolV, ok := getOut.Item["fieldC"].(*ddbtypes.AttributeValueMemberBOOL)
	require.True(t, ok, "fieldC must be a bool")
	assert.True(t, boolV.Value)
}

// TestDDB_UpdateExpression_REMOVE verifies that UpdateItem REMOVE deletes an attribute.
func TestDDB_UpdateExpression_REMOVE(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-remove-tbl")

	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-remove-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":       &ddbtypes.AttributeValueMemberS{Value: "item"},
			"sk":       &ddbtypes.AttributeValueMemberS{Value: "1"},
			"keepMe":   &ddbtypes.AttributeValueMemberS{Value: "stay"},
			"removeMe": &ddbtypes.AttributeValueMemberS{Value: "gone"},
		},
	})
	require.NoError(t, err)

	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-remove-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "item"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("REMOVE #r"),
		ExpressionAttributeNames: map[string]string{
			"#r": "removeMe",
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-remove-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "item"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)
	assert.Contains(t, getOut.Item, "keepMe", "keepMe must still be present")
	assert.NotContains(t, getOut.Item, "removeMe", "removeMe must have been removed")
}

// TestDDB_UpdateExpression_ADD_Number verifies that "ADD #counter :one" increments
// a numeric attribute.
func TestDDB_UpdateExpression_ADD_Number(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-add-tbl")

	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-add-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":      &ddbtypes.AttributeValueMemberS{Value: "counter"},
			"sk":      &ddbtypes.AttributeValueMemberS{Value: "1"},
			"counter": &ddbtypes.AttributeValueMemberN{Value: "10"},
		},
	})
	require.NoError(t, err)

	// Increment by 5.
	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-add-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "counter"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("ADD #cnt :delta"),
		ExpressionAttributeNames: map[string]string{
			"#cnt": "counter",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":delta": &ddbtypes.AttributeValueMemberN{Value: "5"},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-add-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "counter"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)
	num, ok := getOut.Item["counter"].(*ddbtypes.AttributeValueMemberN)
	require.True(t, ok, "counter must be a number")
	assert.Equal(t, "15", num.Value, "counter should be 10+5=15")
}

// TestDDB_UpdateExpression_if_not_exists verifies that
// "SET #field = if_not_exists(#field, :default)" sets the value only on the
// first call and leaves it unchanged on subsequent calls.
func TestDDB_UpdateExpression_if_not_exists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-ine-tbl")

	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-ine-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "rec"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	// First call: field does not exist → set to default.
	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-ine-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "rec"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("SET #f = if_not_exists(#f, :def)"),
		ExpressionAttributeNames: map[string]string{
			"#f": "score",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":def": &ddbtypes.AttributeValueMemberN{Value: "100"},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-ine-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "rec"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	scoreV, ok := getOut.Item["score"].(*ddbtypes.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "100", scoreV.Value, "score should be set to default 100")

	// Second call: field now exists → value unchanged.
	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-ine-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "rec"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("SET #f = if_not_exists(#f, :def)"),
		ExpressionAttributeNames: map[string]string{
			"#f": "score",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":def": &ddbtypes.AttributeValueMemberN{Value: "999"},
		},
	})
	require.NoError(t, err)

	getOut2, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-ine-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "rec"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	scoreV2, ok := getOut2.Item["score"].(*ddbtypes.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "100", scoreV2.Value, "score must remain 100 on second call")
}

// TestDDB_KeyConditionExpression_BETWEEN verifies Query with
// sort key "sk BETWEEN :lo AND :hi".
func TestDDB_KeyConditionExpression_BETWEEN(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-kce-between-tbl")

	for _, sk := range []string{"2020-01-01", "2021-06-15", "2022-03-10", "2023-11-20", "2024-05-05"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-kce-between-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: "events"},
				"sk": &ddbtypes.AttributeValueMemberS{Value: sk},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Query(ctx, &awsddb.QueryInput{
		TableName:              aws.String("expr-kce-between-tbl"),
		KeyConditionExpression: aws.String("pk = :pk AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: "events"},
			":lo": &ddbtypes.AttributeValueMemberS{Value: "2021-01-01"},
			":hi": &ddbtypes.AttributeValueMemberS{Value: "2023-12-31"},
		},
	})
	require.NoError(t, err)
	// 2021-06-15, 2022-03-10, 2023-11-20 fall in range → 3 items
	assert.Equal(t, int32(3), out.Count, "expected 3 events in date range")
}

// TestDDB_KeyConditionExpression_begins_with verifies Query with
// "begins_with(sk, :prefix)".
func TestDDB_KeyConditionExpression_begins_with(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-kce-bw-tbl")

	for _, sk := range []string{"order#001", "order#002", "invoice#001", "order#003", "receipt#001"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-kce-bw-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: "tenant"},
				"sk": &ddbtypes.AttributeValueMemberS{Value: sk},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Query(ctx, &awsddb.QueryInput{
		TableName:              aws.String("expr-kce-bw-tbl"),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk":     &ddbtypes.AttributeValueMemberS{Value: "tenant"},
			":prefix": &ddbtypes.AttributeValueMemberS{Value: "order#"},
		},
	})
	require.NoError(t, err)
	// order#001, order#002, order#003 → 3 items
	assert.Equal(t, int32(3), out.Count, "expected 3 order items")
}

// TestDDB_ProjectionExpression verifies that GetItem with a ProjectionExpression
// returns only the requested fields.
func TestDDB_ProjectionExpression(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-proj-tbl")

	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-proj-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":      &ddbtypes.AttributeValueMemberS{Value: "item1"},
			"sk":      &ddbtypes.AttributeValueMemberS{Value: "v1"},
			"name":    &ddbtypes.AttributeValueMemberS{Value: "Alice"},
			"email":   &ddbtypes.AttributeValueMemberS{Value: "alice@example.com"},
			"secret":  &ddbtypes.AttributeValueMemberS{Value: "hidden-data"},
			"counter": &ddbtypes.AttributeValueMemberN{Value: "42"},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-proj-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "item1"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "v1"},
		},
		ProjectionExpression: aws.String("#n, #e"),
		ExpressionAttributeNames: map[string]string{
			"#n": "name",
			"#e": "email",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)

	assert.Contains(t, getOut.Item, "name", "name must be returned by projection")
	assert.Contains(t, getOut.Item, "email", "email must be returned by projection")
	assert.NotContains(t, getOut.Item, "secret", "secret must be excluded by projection")
	assert.NotContains(t, getOut.Item, "counter", "counter must be excluded by projection")
}

// TestDDB_FilterExpression_NE verifies Scan with "<>" (not-equal) operator.
func TestDDB_FilterExpression_NE(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-ne-tbl")

	for i, status := range []string{"active", "inactive", "active", "deleted"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-ne-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":     &ddbtypes.AttributeValueMemberS{Value: "items"},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("i%d", i)},
				"status": &ddbtypes.AttributeValueMemberS{Value: status},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-ne-tbl"),
		FilterExpression: aws.String("#s <> :val"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":val": &ddbtypes.AttributeValueMemberS{Value: "active"},
		},
	})
	require.NoError(t, err)
	// inactive and deleted → 2 items
	assert.Equal(t, int32(2), out.Count, "expected 2 non-active items")
}

// TestDDB_FilterExpression_LE verifies Scan with "<=" operator.
func TestDDB_FilterExpression_LE(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-le-tbl")

	for i, score := range []string{"10", "20", "30", "40", "50"} {
		_, err := c.PutItem(ctx, &awsddb.PutItemInput{
			TableName: aws.String("expr-le-tbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":    &ddbtypes.AttributeValueMemberS{Value: "scores"},
				"sk":    &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
				"score": &ddbtypes.AttributeValueMemberN{Value: score},
			},
		})
		require.NoError(t, err)
	}

	out, err := c.Scan(ctx, &awsddb.ScanInput{
		TableName:        aws.String("expr-le-tbl"),
		FilterExpression: aws.String("score <= :max"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":max": &ddbtypes.AttributeValueMemberN{Value: "30"},
		},
	})
	require.NoError(t, err)
	// 10, 20, 30 → 3 items
	assert.Equal(t, int32(3), out.Count, "expected 3 items with score <= 30")
}

// TestDDB_UpdateExpression_SET_and_REMOVE_Combined verifies that a single
// UpdateExpression can combine SET and REMOVE clauses.
func TestDDB_UpdateExpression_SET_and_REMOVE_Combined(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newDynamoClient(t)
	makeExprTable(t, c, "expr-combined-tbl")

	_, err := c.PutItem(ctx, &awsddb.PutItemInput{
		TableName: aws.String("expr-combined-tbl"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":     &ddbtypes.AttributeValueMemberS{Value: "obj"},
			"sk":     &ddbtypes.AttributeValueMemberS{Value: "1"},
			"name":   &ddbtypes.AttributeValueMemberS{Value: "old-name"},
			"legacy": &ddbtypes.AttributeValueMemberS{Value: "to-be-removed"},
		},
	})
	require.NoError(t, err)

	_, err = c.UpdateItem(ctx, &awsddb.UpdateItemInput{
		TableName: aws.String("expr-combined-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "obj"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("SET #n = :newname REMOVE #l"),
		ExpressionAttributeNames: map[string]string{
			"#n": "name",
			"#l": "legacy",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":newname": &ddbtypes.AttributeValueMemberS{Value: "new-name"},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetItem(ctx, &awsddb.GetItemInput{
		TableName: aws.String("expr-combined-tbl"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "obj"},
			"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)

	nameV, ok := getOut.Item["name"].(*ddbtypes.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "new-name", nameV.Value)
	assert.NotContains(t, getOut.Item, "legacy", "legacy field must be removed")
}
