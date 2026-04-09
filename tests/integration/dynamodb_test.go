package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
