package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRDSModifyDBInstanceFullParams verifies that ModifyDBInstance updates all
// supported fields when ApplyImmediately=true.
func TestRDSModifyDBInstanceFullParams(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("modify-test-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("initialpass"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyDBInstance(ctx, &awsrds.ModifyDBInstanceInput{
		DBInstanceIdentifier:  aws.String("modify-test-db"),
		DBInstanceClass:       aws.String("db.t3.small"),
		AllocatedStorage:      aws.Int32(50),
		EngineVersion:         aws.String("8.0.32"),
		MultiAZ:               aws.Bool(true),
		BackupRetentionPeriod: aws.Int32(7),
		MasterUserPassword:    aws.String("newpass123"),
		DBParameterGroupName:  aws.String("my-param-group"),
		ApplyImmediately:      aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.DBInstance)
	assert.Equal(t, "db.t3.small", aws.ToString(modOut.DBInstance.DBInstanceClass))
	assert.Equal(t, int32(50), aws.ToInt32(modOut.DBInstance.AllocatedStorage))
	assert.Equal(t, "8.0.32", aws.ToString(modOut.DBInstance.EngineVersion))
	assert.Equal(t, true, aws.ToBool(modOut.DBInstance.MultiAZ))
	assert.Equal(t, int32(7), aws.ToInt32(modOut.DBInstance.BackupRetentionPeriod))

	// Confirm persistence via Describe
	descOut, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("modify-test-db"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBInstances, 1)
	inst := descOut.DBInstances[0]
	assert.Equal(t, "db.t3.small", aws.ToString(inst.DBInstanceClass))
	assert.Equal(t, int32(50), aws.ToInt32(inst.AllocatedStorage))
	assert.Equal(t, "8.0.32", aws.ToString(inst.EngineVersion))
	assert.Equal(t, true, aws.ToBool(inst.MultiAZ))
	assert.Equal(t, int32(7), aws.ToInt32(inst.BackupRetentionPeriod))
}

// TestRDSModifyDBInstancePendingValues verifies that when ApplyImmediately=false,
// changes are returned as PendingModifiedValues (not applied to the live instance).
func TestRDSModifyDBInstancePendingValues(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("pending-test-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("pass"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyDBInstance(ctx, &awsrds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String("pending-test-db"),
		DBInstanceClass:      aws.String("db.t3.large"),
		ApplyImmediately:     aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.DBInstance)
	// The live class should still be the original
	assert.Equal(t, "db.t3.micro", aws.ToString(modOut.DBInstance.DBInstanceClass))
	// PendingModifiedValues should contain the deferred class
	assert.NotNil(t, modOut.DBInstance.PendingModifiedValues)
	assert.Equal(t, "db.t3.large", aws.ToString(modOut.DBInstance.PendingModifiedValues.DBInstanceClass))
}

// TestRDSCreateDBInstanceParamsHonoured verifies that CreateDBInstance persists
// and returns EngineVersion, AllocatedStorage, Port, and VpcSecurityGroupIds.
func TestRDSCreateDBInstanceParamsHonoured(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	out, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("params-test-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		EngineVersion:        aws.String("14.5"),
		AllocatedStorage:     aws.Int32(100),
		Port:                 aws.Int32(5433),
		MasterUsername:       aws.String("pgadmin"),
		MasterUserPassword:   aws.String("pgpass"),
		DBSubnetGroupName:    aws.String("my-subnet-group"),
		DBParameterGroupName: aws.String("my-pg"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBInstance)
	assert.Equal(t, "14.5", aws.ToString(out.DBInstance.EngineVersion))
	assert.Equal(t, int32(100), aws.ToInt32(out.DBInstance.AllocatedStorage))
	assert.Equal(t, int32(5433), aws.ToInt32(out.DBInstance.Endpoint.Port))
	assert.Equal(t, "my-subnet-group", aws.ToString(out.DBInstance.DBSubnetGroup.DBSubnetGroupName))
	assert.NotNil(t, out.DBInstance.Endpoint)
	assert.NotEmpty(t, aws.ToString(out.DBInstance.Endpoint.Address))

	// Verify Describe returns same values
	descOut, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("params-test-db"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBInstances, 1)
	inst := descOut.DBInstances[0]
	assert.Equal(t, "14.5", aws.ToString(inst.EngineVersion))
	assert.Equal(t, int32(100), aws.ToInt32(inst.AllocatedStorage))
	assert.Equal(t, int32(5433), aws.ToInt32(inst.Endpoint.Port))
}

// TestRDSDefaultEngineVersionAndPort verifies engine-specific defaults for
// EngineVersion and Port when not explicitly set.
func TestRDSDefaultEngineVersionAndPort(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	cases := []struct {
		engine      string
		wantVersion string
		wantPort    int32
	}{
		{"mysql", "8.0", 3306},
		{"postgres", "15", 5432},
		{"mariadb", "10.11", 3306},
		{"oracle-se2", "19c", 1521},
		{"sqlserver-ex", "15.0", 1433},
	}

	for _, tc := range cases {
		t.Run(tc.engine, func(t *testing.T) {
			id := "default-" + strings.ReplaceAll(tc.engine, "-", "")
			_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String(id),
				DBInstanceClass:      aws.String("db.t3.micro"),
				Engine:               aws.String(tc.engine),
				MasterUsername:       aws.String("admin"),
				MasterUserPassword:   aws.String("pass"),
				AllocatedStorage:     aws.Int32(20),
			})
			require.NoError(t, err)

			descOut, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(id),
			})
			require.NoError(t, err)
			require.Len(t, descOut.DBInstances, 1)
			inst := descOut.DBInstances[0]
			assert.Equal(t, tc.wantVersion, aws.ToString(inst.EngineVersion), "engine %s version", tc.engine)
			assert.Equal(t, tc.wantPort, aws.ToInt32(inst.Endpoint.Port), "engine %s port", tc.engine)
		})
	}
}

// TestRDSEndpointFormat verifies the endpoint address follows the
// <id>.<region>.rds.localhost format.
func TestRDSEndpointFormat(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	out, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("endpoint-test-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("pass"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBInstance)
	require.NotNil(t, out.DBInstance.Endpoint)
	addr := aws.ToString(out.DBInstance.Endpoint.Address)
	assert.True(t, strings.HasPrefix(addr, "endpoint-test-db."), "address should start with db id: %s", addr)
	assert.True(t, strings.HasSuffix(addr, ".rds.localhost"), "address should end with .rds.localhost: %s", addr)
}

// TestRDSLifecycleOps verifies Stop, Start, and Reboot operations update status correctly.
func TestRDSLifecycleOps(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("lifecycle-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("pass"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)

	// Stop
	stopOut, err := client.StopDBInstance(ctx, &awsrds.StopDBInstanceInput{
		DBInstanceIdentifier: aws.String("lifecycle-db"),
	})
	require.NoError(t, err)
	require.NotNil(t, stopOut.DBInstance)
	assert.Equal(t, "stopped", aws.ToString(stopOut.DBInstance.DBInstanceStatus))

	// Confirm via Describe
	descOut, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("lifecycle-db"),
	})
	require.NoError(t, err)
	assert.Equal(t, "stopped", aws.ToString(descOut.DBInstances[0].DBInstanceStatus))

	// Start
	startOut, err := client.StartDBInstance(ctx, &awsrds.StartDBInstanceInput{
		DBInstanceIdentifier: aws.String("lifecycle-db"),
	})
	require.NoError(t, err)
	require.NotNil(t, startOut.DBInstance)
	assert.Equal(t, "available", aws.ToString(startOut.DBInstance.DBInstanceStatus))

	// Reboot
	rebootOut, err := client.RebootDBInstance(ctx, &awsrds.RebootDBInstanceInput{
		DBInstanceIdentifier: aws.String("lifecycle-db"),
	})
	require.NoError(t, err)
	require.NotNil(t, rebootOut.DBInstance)
	assert.Equal(t, "available", aws.ToString(rebootOut.DBInstance.DBInstanceStatus))
}

// TestRDSReadReplica verifies CreateDBInstanceReadReplica creates a replica and
// PromoteReadReplica removes the source linkage.
func TestRDSReadReplica(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	// Create source instance
	_, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("source-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		EngineVersion:        aws.String("8.0"),
		AllocatedStorage:     aws.Int32(20),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("pass"),
	})
	require.NoError(t, err)

	// Create read replica
	replicaOut, err := client.CreateDBInstanceReadReplica(ctx, &awsrds.CreateDBInstanceReadReplicaInput{
		DBInstanceIdentifier:       aws.String("replica-db"),
		SourceDBInstanceIdentifier: aws.String("source-db"),
	})
	require.NoError(t, err)
	require.NotNil(t, replicaOut.DBInstance)
	assert.Equal(t, "replica-db", aws.ToString(replicaOut.DBInstance.DBInstanceIdentifier))
	assert.Equal(t, "source-db", aws.ToString(replicaOut.DBInstance.ReadReplicaSourceDBInstanceIdentifier))
	assert.Equal(t, "available", aws.ToString(replicaOut.DBInstance.DBInstanceStatus))
	assert.Equal(t, "8.0", aws.ToString(replicaOut.DBInstance.EngineVersion))

	// Confirm replica exists via Describe
	descOut, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("replica-db"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBInstances, 1)
	assert.Equal(t, "source-db", aws.ToString(descOut.DBInstances[0].ReadReplicaSourceDBInstanceIdentifier))

	// Promote replica
	promoteOut, err := client.PromoteReadReplica(ctx, &awsrds.PromoteReadReplicaInput{
		DBInstanceIdentifier: aws.String("replica-db"),
	})
	require.NoError(t, err)
	require.NotNil(t, promoteOut.DBInstance)
	// After promotion, the source identifier must be empty
	assert.Empty(t, aws.ToString(promoteOut.DBInstance.ReadReplicaSourceDBInstanceIdentifier))

	// Confirm via Describe
	descOut2, err := client.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("replica-db"),
	})
	require.NoError(t, err)
	require.Len(t, descOut2.DBInstances, 1)
	assert.Empty(t, aws.ToString(descOut2.DBInstances[0].ReadReplicaSourceDBInstanceIdentifier))
}

// TestRDSSubnetGroupARN verifies that CreateDBSubnetGroup returns a proper ARN.
func TestRDSSubnetGroupARN(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRDSClient(t)

	out, err := client.CreateDBSubnetGroup(ctx, &awsrds.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("arn-subnet-group"),
		DBSubnetGroupDescription: aws.String("test"),
		SubnetIds:                []string{"subnet-abc123"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBSubnetGroup)
	arn := aws.ToString(out.DBSubnetGroup.DBSubnetGroupArn)
	assert.Regexp(t, `^arn:aws:rds:us-east-1:000000000000:subgrp:arn-subnet-group$`, arn)
}
