package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlue_CreateGetDeleteDatabase(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{
			Name:        aws.String("testdb"),
			Description: aws.String("test database"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetDatabase(ctx, &awsglue.GetDatabaseInput{
		Name: aws.String("testdb"),
	})
	require.NoError(t, err)
	assert.Equal(t, "testdb", aws.ToString(out.Database.Name))
	assert.Equal(t, "test database", aws.ToString(out.Database.Description))

	_, err = client.DeleteDatabase(ctx, &awsglue.DeleteDatabaseInput{
		Name: aws.String("testdb"),
	})
	require.NoError(t, err)

	_, err = client.GetDatabase(ctx, &awsglue.GetDatabaseInput{Name: aws.String("testdb")})
	require.Error(t, err)
}

func TestGlue_GetDatabases(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	for _, name := range []string{"db1", "db2", "db3"} {
		_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
			DatabaseInput: &types.DatabaseInput{Name: aws.String(name)},
		})
		require.NoError(t, err)
	}

	out, err := client.GetDatabases(ctx, &awsglue.GetDatabasesInput{})
	require.NoError(t, err)
	assert.Len(t, out.DatabaseList, 3)
}

func TestGlue_CreateGetDeleteTable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("mydb"),
		TableInput: &types.TableInput{
			Name:      aws.String("orders"),
			TableType: aws.String("EXTERNAL_TABLE"),
			StorageDescriptor: &types.StorageDescriptor{
				Location: aws.String("s3://my-bucket/orders/"),
				Columns: []types.Column{
					{Name: aws.String("id"), Type: aws.String("bigint")},
					{Name: aws.String("amount"), Type: aws.String("double")},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String("mydb"),
		Name:         aws.String("orders"),
	})
	require.NoError(t, err)
	assert.Equal(t, "orders", aws.ToString(out.Table.Name))
	assert.Equal(t, "EXTERNAL_TABLE", aws.ToString(out.Table.TableType))

	_, err = client.DeleteTable(ctx, &awsglue.DeleteTableInput{
		DatabaseName: aws.String("mydb"),
		Name:         aws.String("orders"),
	})
	require.NoError(t, err)

	_, err = client.GetTable(ctx, &awsglue.GetTableInput{DatabaseName: aws.String("mydb"), Name: aws.String("orders")})
	require.Error(t, err)
}

func TestGlue_GetTables(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)

	for _, name := range []string{"orders", "customers", "products"} {
		_, err := client.CreateTable(ctx, &awsglue.CreateTableInput{
			DatabaseName: aws.String("mydb"),
			TableInput:   &types.TableInput{Name: aws.String(name)},
		})
		require.NoError(t, err)
	}

	out, err := client.GetTables(ctx, &awsglue.GetTablesInput{DatabaseName: aws.String("mydb")})
	require.NoError(t, err)
	assert.Len(t, out.TableList, 3)
}

func TestGlue_UpdateTable_IcebergCAS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("mydb"),
		TableInput: &types.TableInput{
			Name:      aws.String("iceberg_table"),
			TableType: aws.String("ICEBERG"),
			Parameters: map[string]string{
				"table_type":        "ICEBERG",
				"metadata_location": "s3://bucket/v1.metadata.json",
			},
		},
	})
	require.NoError(t, err)

	// Update metadata_location (Iceberg commit)
	_, err = client.UpdateTable(ctx, &awsglue.UpdateTableInput{
		DatabaseName: aws.String("mydb"),
		TableInput: &types.TableInput{
			Name: aws.String("iceberg_table"),
			Parameters: map[string]string{
				"metadata_location": "s3://bucket/v2.metadata.json",
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String("mydb"),
		Name:         aws.String("iceberg_table"),
	})
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/v2.metadata.json", out.Table.Parameters["metadata_location"])
}

func TestGlue_CreateGetPartitions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("mydb"),
		TableInput: &types.TableInput{
			Name: aws.String("events"),
			PartitionKeys: []types.Column{
				{Name: aws.String("year"), Type: aws.String("string")},
				{Name: aws.String("month"), Type: aws.String("string")},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.CreatePartition(ctx, &awsglue.CreatePartitionInput{
		DatabaseName: aws.String("mydb"),
		TableName:    aws.String("events"),
		PartitionInput: &types.PartitionInput{
			Values: []string{"2026", "01"},
			StorageDescriptor: &types.StorageDescriptor{
				Location: aws.String("s3://bucket/events/year=2026/month=01/"),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetPartition(ctx, &awsglue.GetPartitionInput{
		DatabaseName:    aws.String("mydb"),
		TableName:       aws.String("events"),
		PartitionValues: []string{"2026", "01"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"2026", "01"}, out.Partition.Values)

	listOut, err := client.GetPartitions(ctx, &awsglue.GetPartitionsInput{
		DatabaseName: aws.String("mydb"),
		TableName:    aws.String("events"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Partitions, 1)
}

func TestGlue_BatchCreateDeletePartitions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("mydb"),
		TableInput:   &types.TableInput{Name: aws.String("logs")},
	})
	require.NoError(t, err)

	// Batch create 3 partitions
	_, err = client.BatchCreatePartition(ctx, &awsglue.BatchCreatePartitionInput{
		DatabaseName: aws.String("mydb"),
		TableName:    aws.String("logs"),
		PartitionInputList: []types.PartitionInput{
			{Values: []string{"2026-01-01"}},
			{Values: []string{"2026-01-02"}},
			{Values: []string{"2026-01-03"}},
		},
	})
	require.NoError(t, err)

	listOut, err := client.GetPartitions(ctx, &awsglue.GetPartitionsInput{
		DatabaseName: aws.String("mydb"),
		TableName:    aws.String("logs"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Partitions, 3)

	// Batch delete 2 partitions
	_, err = client.BatchDeletePartition(ctx, &awsglue.BatchDeletePartitionInput{
		DatabaseName: aws.String("mydb"),
		TableName:    aws.String("logs"),
		PartitionsToDelete: []types.PartitionValueList{
			{Values: []string{"2026-01-01"}},
			{Values: []string{"2026-01-02"}},
		},
	})
	require.NoError(t, err)

	listOut, err = client.GetPartitions(ctx, &awsglue.GetPartitionsInput{
		DatabaseName: aws.String("mydb"),
		TableName:    aws.String("logs"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Partitions, 1)
}

func TestGlueDatabaseCasingPreserved(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	// Create database with mixed-case name
	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{
			Name:        aws.String("MyDB"),
			Description: aws.String("casing test"),
		},
	})
	require.NoError(t, err)

	// GetDatabase with lowercase should succeed (case-insensitive lookup)
	out, err := client.GetDatabase(ctx, &awsglue.GetDatabaseInput{
		Name: aws.String("mydb"),
	})
	require.NoError(t, err)
	// Response Name must preserve original casing
	assert.Equal(t, "MyDB", aws.ToString(out.Database.Name))

	// GetDatabases should also preserve casing in list
	listOut, err := client.GetDatabases(ctx, &awsglue.GetDatabasesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.DatabaseList, 1)
	assert.Equal(t, "MyDB", aws.ToString(listOut.DatabaseList[0].Name))
}

func TestGlueTableCasingPreserved(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("MyDB")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("MyDB"),
		TableInput:   &types.TableInput{Name: aws.String("OrderItems")},
	})
	require.NoError(t, err)

	// GetTable with lowercase should succeed and return original casing
	out, err := client.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String("mydb"),
		Name:         aws.String("orderitems"),
	})
	require.NoError(t, err)
	assert.Equal(t, "OrderItems", aws.ToString(out.Table.Name))
	assert.Equal(t, "MyDB", aws.ToString(out.Table.DatabaseName))

	// GetTables should also preserve casing
	listOut, err := client.GetTables(ctx, &awsglue.GetTablesInput{
		DatabaseName: aws.String("MyDB"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.TableList, 1)
	assert.Equal(t, "OrderItems", aws.ToString(listOut.TableList[0].Name))
}
