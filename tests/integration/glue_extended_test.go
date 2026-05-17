package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGlue_UpdateTable_Columns creates a table with initial columns then updates
// it with a new column set, verifying GetTable reflects the change.
func TestGlue_UpdateTable_Columns(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("updatedb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("updatedb"),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("sales"),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Location: aws.String("s3://bucket/sales/"),
				Columns: []gluetypes.Column{
					{Name: aws.String("id"), Type: aws.String("bigint")},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateTable(ctx, &awsglue.UpdateTableInput{
		DatabaseName: aws.String("updatedb"),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("sales"),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Location: aws.String("s3://bucket/sales/"),
				Columns: []gluetypes.Column{
					{Name: aws.String("id"), Type: aws.String("bigint")},
					{Name: aws.String("amount"), Type: aws.String("double")},
					{Name: aws.String("region"), Type: aws.String("string")},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String("updatedb"),
		Name:         aws.String("sales"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Table.StorageDescriptor)
	assert.Len(t, out.Table.StorageDescriptor.Columns, 3, "updated table should have 3 columns")
}

// TestGlue_GetTables_ByDatabase creates two databases each with two tables and
// verifies GetTables returns only the tables for the requested database.
func TestGlue_GetTables_ByDatabase(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	for _, db := range []string{"db_a", "db_b"} {
		_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
			DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(db)},
		})
		require.NoError(t, err)

		for _, tbl := range []string{"table1", "table2"} {
			_, err := client.CreateTable(ctx, &awsglue.CreateTableInput{
				DatabaseName: aws.String(db),
				TableInput:   &gluetypes.TableInput{Name: aws.String(tbl)},
			})
			require.NoError(t, err)
		}
	}

	out, err := client.GetTables(ctx, &awsglue.GetTablesInput{DatabaseName: aws.String("db_a")})
	require.NoError(t, err)
	assert.Len(t, out.TableList, 2, "should only list tables from db_a")
	for _, tbl := range out.TableList {
		assert.Equal(t, "db_a", aws.ToString(tbl.DatabaseName))
	}
}

// TestGlue_BatchCreatePartition creates a table with partition keys then uses
// BatchCreatePartition to add 3 partitions, asserting all are stored.
func TestGlue_BatchCreatePartition(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("partdb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("partdb"),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("events"),
			PartitionKeys: []gluetypes.Column{
				{Name: aws.String("dt"), Type: aws.String("string")},
			},
		},
	})
	require.NoError(t, err)

	batchOut, err := client.BatchCreatePartition(ctx, &awsglue.BatchCreatePartitionInput{
		DatabaseName: aws.String("partdb"),
		TableName:    aws.String("events"),
		PartitionInputList: []gluetypes.PartitionInput{
			{Values: []string{"2026-01-10"}},
			{Values: []string{"2026-01-11"}},
			{Values: []string{"2026-01-12"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, batchOut.Errors, "batch create should succeed with no errors")

	listOut, err := client.GetPartitions(ctx, &awsglue.GetPartitionsInput{
		DatabaseName: aws.String("partdb"),
		TableName:    aws.String("events"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Partitions, 3, "should have 3 partitions")
}

// TestGlue_DeleteTable creates a table, deletes it, and asserts GetTable returns an error.
func TestGlue_DeleteTable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("deldb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("deldb"),
		TableInput:   &gluetypes.TableInput{Name: aws.String("todelete")},
	})
	require.NoError(t, err)

	_, err = client.DeleteTable(ctx, &awsglue.DeleteTableInput{
		DatabaseName: aws.String("deldb"),
		Name:         aws.String("todelete"),
	})
	require.NoError(t, err)

	_, err = client.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String("deldb"),
		Name:         aws.String("todelete"),
	})
	require.Error(t, err, "GetTable should fail after DeleteTable")
}

// TestGlue_GetConnections creates two connections and verifies both appear in GetConnections.
func TestGlue_GetConnections(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	for _, name := range []string{"conn-alpha", "conn-beta"} {
		_, err := client.CreateConnection(ctx, &awsglue.CreateConnectionInput{
			ConnectionInput: &gluetypes.ConnectionInput{
				Name:           aws.String(name),
				ConnectionType: gluetypes.ConnectionTypeJdbc,
				ConnectionProperties: map[string]string{
					"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/mydb",
					"USERNAME":            "admin",
					"PASSWORD":            "secret",
				},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.GetConnections(ctx, &awsglue.GetConnectionsInput{})
	require.NoError(t, err)
	assert.Len(t, out.ConnectionList, 2, "should have 2 connections")

	names := map[string]bool{}
	for _, c := range out.ConnectionList {
		names[aws.ToString(c.Name)] = true
	}
	assert.True(t, names["conn-alpha"])
	assert.True(t, names["conn-beta"])
}

// TestGlue_DeleteConnection creates a connection, deletes it, and asserts
// GetConnection returns an error afterwards.
func TestGlue_DeleteConnection(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateConnection(ctx, &awsglue.CreateConnectionInput{
		ConnectionInput: &gluetypes.ConnectionInput{
			Name:           aws.String("temp-conn"),
			ConnectionType: gluetypes.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost/db",
				"USERNAME":            "u",
				"PASSWORD":            "p",
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteConnection(ctx, &awsglue.DeleteConnectionInput{
		ConnectionName: aws.String("temp-conn"),
	})
	require.NoError(t, err)

	_, err = client.GetConnection(ctx, &awsglue.GetConnectionInput{
		Name: aws.String("temp-conn"),
	})
	require.Error(t, err, "GetConnection should fail after DeleteConnection")
}

// TestGlue_CreateJob_GetJob creates a Glue job and retrieves it, verifying the
// Name and Command fields are correctly stored.
func TestGlue_CreateJob_GetJob(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateJob(ctx, &awsglue.CreateJobInput{
		Name: aws.String("etl-job"),
		Role: aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		Command: &gluetypes.JobCommand{
			Name:           aws.String("glueetl"),
			ScriptLocation: aws.String("s3://scripts/etl.py"),
			PythonVersion:  aws.String("3"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetJob(ctx, &awsglue.GetJobInput{
		JobName: aws.String("etl-job"),
	})
	require.NoError(t, err)
	assert.Equal(t, "etl-job", aws.ToString(out.Job.Name))
	require.NotNil(t, out.Job.Command)
	assert.Equal(t, "glueetl", aws.ToString(out.Job.Command.Name))
	assert.Equal(t, "s3://scripts/etl.py", aws.ToString(out.Job.Command.ScriptLocation))
}

// TestGlue_StartJobRun creates a job then starts a job run, asserting a non-empty
// JobRunId is returned.
func TestGlue_StartJobRun(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateJob(ctx, &awsglue.CreateJobInput{
		Name: aws.String("run-job"),
		Role: aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		Command: &gluetypes.JobCommand{
			Name:           aws.String("glueetl"),
			ScriptLocation: aws.String("s3://scripts/run.py"),
		},
	})
	require.NoError(t, err)

	runOut, err := client.StartJobRun(ctx, &awsglue.StartJobRunInput{
		JobName: aws.String("run-job"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(runOut.JobRunId), "StartJobRun should return a non-empty JobRunId")
}

// TestGlue_GetCrawlers creates two crawlers and asserts both appear in GetCrawlers.
func TestGlue_GetCrawlers(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	// Crawlers require a target database
	for _, db := range []string{"crawldb1", "crawldb2"} {
		_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
			DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(db)},
		})
		require.NoError(t, err)
	}

	for i, name := range []string{"crawler-one", "crawler-two"} {
		db := "crawldb1"
		if i == 1 {
			db = "crawldb2"
		}
		_, err := client.CreateCrawler(ctx, &awsglue.CreateCrawlerInput{
			Name:         aws.String(name),
			Role:         aws.String("arn:aws:iam::000000000000:role/GlueRole"),
			DatabaseName: aws.String(db),
			Targets: &gluetypes.CrawlerTargets{
				S3Targets: []gluetypes.S3Target{
					{Path: aws.String("s3://bucket/data/")},
				},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.GetCrawlers(ctx, &awsglue.GetCrawlersInput{})
	require.NoError(t, err)
	assert.Len(t, out.Crawlers, 2, "should have 2 crawlers")

	names := map[string]bool{}
	for _, c := range out.Crawlers {
		names[aws.ToString(c.Name)] = true
	}
	assert.True(t, names["crawler-one"])
	assert.True(t, names["crawler-two"])
}

// TestGlue_StartCrawler creates a crawler, starts it, then asserts the State
// is no longer READY (i.e. RUNNING or STOPPING).
func TestGlue_StartCrawler(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("startdb")},
	})
	require.NoError(t, err)

	_, err = client.CreateCrawler(ctx, &awsglue.CreateCrawlerInput{
		Name:         aws.String("start-crawler"),
		Role:         aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		DatabaseName: aws.String("startdb"),
		Targets: &gluetypes.CrawlerTargets{
			S3Targets: []gluetypes.S3Target{
				{Path: aws.String("s3://bucket/start/")},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.StartCrawler(ctx, &awsglue.StartCrawlerInput{
		Name: aws.String("start-crawler"),
	})
	require.NoError(t, err)

	getOut, err := client.GetCrawler(ctx, &awsglue.GetCrawlerInput{
		Name: aws.String("start-crawler"),
	})
	require.NoError(t, err)
	state := string(getOut.Crawler.State)
	assert.NotEqual(t, "READY", state, "crawler state should have transitioned from READY after StartCrawler")
}

// TestGlue_GetJobRun creates a job, starts a run, then retrieves the run by ID
// and asserts the JobName and a non-empty state are returned.
func TestGlue_GetJobRun(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateJob(ctx, &awsglue.CreateJobInput{
		Name: aws.String("getrun-job"),
		Role: aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		Command: &gluetypes.JobCommand{
			Name:           aws.String("glueetl"),
			ScriptLocation: aws.String("s3://scripts/getrun.py"),
		},
	})
	require.NoError(t, err)

	runOut, err := client.StartJobRun(ctx, &awsglue.StartJobRunInput{
		JobName: aws.String("getrun-job"),
	})
	require.NoError(t, err)
	runID := aws.ToString(runOut.JobRunId)
	require.NotEmpty(t, runID)

	getOut, err := client.GetJobRun(ctx, &awsglue.GetJobRunInput{
		JobName: aws.String("getrun-job"),
		RunId:   aws.String(runID),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.JobRun)
	assert.Equal(t, "getrun-job", aws.ToString(getOut.JobRun.JobName))
	assert.NotEmpty(t, string(getOut.JobRun.JobRunState), "job run state should be set")
}

// TestGlue_GetDatabase_NotFound asserts that GetDatabase with a non-existent
// database name returns an error.
func TestGlue_GetDatabase_NotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.GetDatabase(ctx, &awsglue.GetDatabaseInput{
		Name: aws.String("does-not-exist"),
	})
	require.Error(t, err, "GetDatabase with non-existent name should return an error")
}
