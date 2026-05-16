// Package integration provides Redshift round-trip integration tests.
// NOTE: Redshift is not yet implemented in JaisCloud; these tests are skipped
// until the provider is added.
package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsredshift "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedshift_CreateCluster(t *testing.T) {
	t.Skip("Redshift not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newRedshiftClient(t)

	_, err := c.CreateCluster(ctx, &awsredshift.CreateClusterInput{
		ClusterIdentifier:  aws.String("test"),
		NodeType:           aws.String("dc2.large"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Password1!"),
		DBName:             aws.String("testdb"),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeClusters(ctx, &awsredshift.DescribeClustersInput{
		ClusterIdentifier: aws.String("test"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.Clusters)
	assert.Equal(t, "test", aws.ToString(descOut.Clusters[0].ClusterIdentifier))
	assert.NotEmpty(t, aws.ToString(descOut.Clusters[0].ClusterStatus))
}

func TestRedshift_DescribeClusters(t *testing.T) {
	t.Skip("Redshift not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newRedshiftClient(t)

	_, err := c.CreateCluster(ctx, &awsredshift.CreateClusterInput{
		ClusterIdentifier:  aws.String("desc-test"),
		NodeType:           aws.String("dc2.large"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Password1!"),
		DBName:             aws.String("testdb"),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeClusters(ctx, &awsredshift.DescribeClustersInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.Clusters)

	found := false
	for _, cl := range descOut.Clusters {
		if aws.ToString(cl.ClusterIdentifier) == "desc-test" {
			found = true
			break
		}
	}
	assert.True(t, found, "cluster desc-test should be present")
}
