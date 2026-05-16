// Package integration provides Athena round-trip integration tests.
// NOTE: Athena is not yet implemented in JaisCloud; these tests are skipped
// until the provider is added.
package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatype "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAthena_StartQueryExecution(t *testing.T) {
	t.Skip("Athena not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newAthenaClient(t)

	out, err := c.StartQueryExecution(ctx, &awsathena.StartQueryExecutionInput{
		QueryString: aws.String("SELECT 1"),
		WorkGroup:   aws.String("primary"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.QueryExecutionId))
}

func TestAthena_GetQueryExecution(t *testing.T) {
	t.Skip("Athena not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newAthenaClient(t)

	startOut, err := c.StartQueryExecution(ctx, &awsathena.StartQueryExecutionInput{
		QueryString: aws.String("SELECT 1"),
		WorkGroup:   aws.String("primary"),
	})
	require.NoError(t, err)
	qid := aws.ToString(startOut.QueryExecutionId)

	getOut, err := c.GetQueryExecution(ctx, &awsathena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(qid),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.QueryExecution)
	require.NotNil(t, getOut.QueryExecution.Status)
	assert.NotEmpty(t, string(getOut.QueryExecution.Status.State))
}

func TestAthena_GetQueryResults(t *testing.T) {
	t.Skip("Athena not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newAthenaClient(t)

	startOut, err := c.StartQueryExecution(ctx, &awsathena.StartQueryExecutionInput{
		QueryString: aws.String("SELECT 1"),
		WorkGroup:   aws.String("primary"),
	})
	require.NoError(t, err)
	qid := aws.ToString(startOut.QueryExecutionId)

	// Poll until SUCCEEDED or timeout
	waitFor(t, 10*time.Second, func() bool {
		out, err := c.GetQueryExecution(ctx, &awsathena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(qid),
		})
		if err != nil {
			return false
		}
		return out.QueryExecution.Status.State == athenatype.QueryExecutionStateSucceeded
	})

	resultsOut, err := c.GetQueryResults(ctx, &awsathena.GetQueryResultsInput{
		QueryExecutionId: aws.String(qid),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resultsOut.ResultSet.Rows)
}
