package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRDS_CreateDescribeDeleteDBInstance(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	out, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("mydb"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
	})
	require.NoError(t, err)
	assert.Equal(t, "mydb", aws.ToString(out.DBInstance.DBInstanceIdentifier))
	assert.Equal(t, "available", aws.ToString(out.DBInstance.DBInstanceStatus))

	descOut, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("mydb"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBInstances, 1)
	assert.Equal(t, "mydb", aws.ToString(descOut.DBInstances[0].DBInstanceIdentifier))
	assert.NotNil(t, descOut.DBInstances[0].Endpoint)

	_, err = client.DeleteDBInstance(ctx, &awsrds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   aws.String("mydb"),
		SkipFinalSnapshot:      aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("mydb"),
	})
	require.Error(t, err)
}

func TestRDS_ListDBInstances(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	for _, id := range []string{"db-1", "db-2", "db-3"} {
		_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String(id),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
			MasterUsername:       aws.String("admin"),
			MasterUserPassword:   aws.String("password123"),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{})
	require.NoError(t, err)
	assert.Len(t, out.DBInstances, 3)
}

func TestRDS_CreateDescribeDeleteDBCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	out, err := client.CreateDBCluster(ctx, &awsrds.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("mycluster"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("password123"),
	})
	require.NoError(t, err)
	assert.Equal(t, "mycluster", aws.ToString(out.DBCluster.DBClusterIdentifier))
	assert.Equal(t, "available", aws.ToString(out.DBCluster.Status))

	descOut, err := client.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("mycluster"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBClusters, 1)
	assert.Equal(t, "mycluster", aws.ToString(descOut.DBClusters[0].DBClusterIdentifier))

	_, err = client.DeleteDBCluster(ctx, &awsrds.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String("mycluster"),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("mycluster"),
	})
	require.Error(t, err)
}

func TestRDS_CreateDescribeDeleteDBSubnetGroup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	out, err := client.CreateDBSubnetGroup(ctx, &awsrds.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("mysubnetgroup"),
		DBSubnetGroupDescription: aws.String("Test subnet group"),
		SubnetIds:                []string{"subnet-abc123"},
	})
	require.NoError(t, err)
	assert.Equal(t, "mysubnetgroup", aws.ToString(out.DBSubnetGroup.DBSubnetGroupName))

	descOut, err := client.DescribeDBSubnetGroups(ctx, &awsrds.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: aws.String("mysubnetgroup"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBSubnetGroups, 1)
	assert.Equal(t, "mysubnetgroup", aws.ToString(descOut.DBSubnetGroups[0].DBSubnetGroupName))

	_, err = client.DeleteDBSubnetGroup(ctx, &awsrds.DeleteDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("mysubnetgroup"),
	})
	require.NoError(t, err)

	_, err = client.DescribeDBSubnetGroups(ctx, &awsrds.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: aws.String("mysubnetgroup"),
	})
	require.Error(t, err)
}

func TestRDS_ModifyDBInstance(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("mydb"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyDBInstance(ctx, &awsrds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String("mydb"),
		DBInstanceClass:      aws.String("db.t3.small"),
		ApplyImmediately:     aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "db.t3.small", aws.ToString(modOut.DBInstance.DBInstanceClass))
}

// ensure types import is used
var _ types.DBInstance

// TestRDSUnroutedReturnsEnvelope verifies that an unrouted RDS action returns a
// valid XML envelope rather than a bare <Response/> (fix 1.1.2).
func TestRDSUnroutedReturnsEnvelope(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)
	// DescribeAccountAttributes is not registered in the provider; the codec must
	// still return a valid XML wrapper so the SDK does not raise a parse error.
	_, err := client.DescribeAccountAttributes(ctx, &awsrds.DescribeAccountAttributesInput{})
	if err != nil {
		assert.NotContains(t, err.Error(), "EOF", "response must be a valid XML envelope, not bare <Response/>")
		assert.NotContains(t, err.Error(), "SerializationError")
	}
}

func TestRDSClusterARNFormat(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRDSClient(t)
	out, err := c.CreateDBCluster(ctx, &awsrds.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("arn-cluster"),
		Engine:              aws.String("aurora"),
	})
	require.NoError(t, err)
	arn := aws.ToString(out.DBCluster.DBClusterArn)
	assert.Regexp(t, `^arn:aws:rds:us-east-1:000000000000:cluster:arn-cluster$`, arn)
}
