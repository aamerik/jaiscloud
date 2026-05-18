package multiaccount

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsapigw "github.com/aws/aws-sdk-go-v2/service/apigateway"
	awscfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	awselasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── CloudFormation ───────────────────────────────────────────────────────────

func TestCloudFormation_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfnA := newCFNFor(t, AcctA)
	cfnB := newCFNFor(t, AcctB)

	// Minimal template: creates an SSM parameter (no global namespace; avoids S3 bucket name conflicts).
	template := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"Param":{"Type":"AWS::SSM::Parameter","Properties":{"Name":"/cfn/test/shared-stack","Type":"String","Value":"hello"}}}}`

	_, err := cfnA.CreateStack(ctx, &awscfn.CreateStackInput{
		StackName:    aws.String("shared-stack"),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)
	_, err = cfnB.CreateStack(ctx, &awscfn.CreateStackInput{
		StackName:    aws.String("shared-stack"),
		TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	listA, err := cfnA.ListStacks(ctx, &awscfn.ListStacksInput{})
	require.NoError(t, err)
	for _, s := range listA.StackSummaries {
		assert.Contains(t, aws.ToString(s.StackId), AcctA, "A's stacks must embed A's account in ARN")
		assert.NotContains(t, aws.ToString(s.StackId), AcctB)
	}

	listB, err := cfnB.ListStacks(ctx, &awscfn.ListStacksInput{})
	require.NoError(t, err)
	for _, s := range listB.StackSummaries {
		assert.Contains(t, aws.ToString(s.StackId), AcctB, "B's stacks must embed B's account in ARN")
		assert.NotContains(t, aws.ToString(s.StackId), AcctA)
	}

	// Delete A's stack — B's must survive.
	_, err = cfnA.DeleteStack(ctx, &awscfn.DeleteStackInput{StackName: aws.String("shared-stack")})
	require.NoError(t, err)
	listBAfter, err := cfnB.ListStacks(ctx, &awscfn.ListStacksInput{})
	require.NoError(t, err)
	found := false
	for _, s := range listBAfter.StackSummaries {
		if aws.ToString(s.StackName) == "shared-stack" {
			found = true
		}
	}
	assert.True(t, found, "B's stack must survive A's delete")
}

// ─── API Gateway ──────────────────────────────────────────────────────────────

func TestAPIGateway_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	apigwA := newAPIGWFor(t, AcctA)
	apigwB := newAPIGWFor(t, AcctB)

	outA, err := apigwA.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("shared-api")})
	require.NoError(t, err)
	outB, err := apigwB.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("shared-api")})
	require.NoError(t, err)

	// IDs must differ.
	assert.NotEqual(t, aws.ToString(outA.Id), aws.ToString(outB.Id),
		"REST API IDs must be distinct across accounts")

	listA, err := apigwA.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	assert.Len(t, listA.Items, 1, "A sees only its own API")

	listB, err := apigwB.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	assert.Len(t, listB.Items, 1, "B sees only its own API")

	// Delete A's API — B's must survive.
	_, err = apigwA.DeleteRestApi(ctx, &awsapigw.DeleteRestApiInput{RestApiId: outA.Id})
	require.NoError(t, err)
	listBAfter, err := apigwB.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Items, 1, "B's API must survive A's delete")
}

// ─── Glue ─────────────────────────────────────────────────────────────────────

func TestGlue_DatabaseIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	glueA := newGlueFor(t, AcctA)
	glueB := newGlueFor(t, AcctB)

	_, err := glueA.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("shared-db")},
	})
	require.NoError(t, err)
	_, err = glueB.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("shared-db")},
	})
	require.NoError(t, err)

	listA, err := glueA.GetDatabases(ctx, &glue.GetDatabasesInput{})
	require.NoError(t, err)
	assert.Len(t, listA.DatabaseList, 1, "A sees only its own Glue database")

	listB, err := glueB.GetDatabases(ctx, &glue.GetDatabasesInput{})
	require.NoError(t, err)
	assert.Len(t, listB.DatabaseList, 1, "B sees only its own Glue database")

	// Delete A's database — B's must survive.
	_, err = glueA.DeleteDatabase(ctx, &glue.DeleteDatabaseInput{Name: aws.String("shared-db")})
	require.NoError(t, err)
	listBAfter, err := glueB.GetDatabases(ctx, &glue.GetDatabasesInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.DatabaseList, 1, "B's database must survive A's delete")
}

func TestGlue_TableIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	glueA := newGlueFor(t, AcctA)
	glueB := newGlueFor(t, AcctB)

	_, err := glueA.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)
	_, err = glueB.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("mydb")},
	})
	require.NoError(t, err)

	tableInput := &gluetypes.TableInput{
		Name: aws.String("shared-table"),
		StorageDescriptor: &gluetypes.StorageDescriptor{
			Columns:  []gluetypes.Column{{Name: aws.String("id"), Type: aws.String("string")}},
			Location: aws.String("s3://bucket/prefix/"),
		},
	}
	_, err = glueA.CreateTable(ctx, &glue.CreateTableInput{DatabaseName: aws.String("mydb"), TableInput: tableInput})
	require.NoError(t, err)
	_, err = glueB.CreateTable(ctx, &glue.CreateTableInput{DatabaseName: aws.String("mydb"), TableInput: tableInput})
	require.NoError(t, err)

	tablesA, err := glueA.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String("mydb")})
	require.NoError(t, err)
	assert.Len(t, tablesA.TableList, 1, "A sees only its own table")

	tablesB, err := glueB.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String("mydb")})
	require.NoError(t, err)
	assert.Len(t, tablesB.TableList, 1, "B sees only its own table")
}

// ─── StepFunctions ────────────────────────────────────────────────────────────

func TestStepFunctions_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sfnA := newSFNFor(t, AcctA)
	sfnB := newSFNFor(t, AcctB)
	iamA := newIAMFor(t, AcctA)
	iamB := newIAMFor(t, AcctB)

	assumeDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	roleA, err := iamA.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("sfn-exec"),
		AssumeRolePolicyDocument: aws.String(assumeDoc),
	})
	require.NoError(t, err)
	roleB, err := iamB.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("sfn-exec"),
		AssumeRolePolicyDocument: aws.String(assumeDoc),
	})
	require.NoError(t, err)

	defn := `{"Comment":"test","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	outA, err := sfnA.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
		Name:       aws.String("shared-sm"),
		RoleArn:    roleA.Role.Arn,
		Definition: aws.String(defn),
	})
	require.NoError(t, err)
	_, err = sfnB.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
		Name:       aws.String("shared-sm"),
		RoleArn:    roleB.Role.Arn,
		Definition: aws.String(defn),
	})
	require.NoError(t, err)

	listA, err := sfnA.ListStateMachines(ctx, &sfn.ListStateMachinesInput{})
	require.NoError(t, err)
	for _, sm := range listA.StateMachines {
		assert.Contains(t, aws.ToString(sm.StateMachineArn), AcctA, "A's state machines must embed A's account")
		assert.NotContains(t, aws.ToString(sm.StateMachineArn), AcctB)
	}

	listB, err := sfnB.ListStateMachines(ctx, &sfn.ListStateMachinesInput{})
	require.NoError(t, err)
	for _, sm := range listB.StateMachines {
		assert.Contains(t, aws.ToString(sm.StateMachineArn), AcctB, "B's state machines must embed B's account")
		assert.NotContains(t, aws.ToString(sm.StateMachineArn), AcctA)
	}

	// Delete A's state machine — B's must survive.
	_, err = sfnA.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: outA.StateMachineArn})
	require.NoError(t, err)
	listBAfter, err := sfnB.ListStateMachines(ctx, &sfn.ListStateMachinesInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.StateMachines, 1, "B's state machine must survive A's delete")
}

// ─── ECR ──────────────────────────────────────────────────────────────────────

func TestECR_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ecrA := newECRFor(t, AcctA)
	ecrB := newECRFor(t, AcctB)

	_, err := ecrA.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("shared-repo")})
	require.NoError(t, err)
	_, err = ecrB.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("shared-repo")})
	require.NoError(t, err)

	listA, err := ecrA.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})
	require.NoError(t, err)
	for _, r := range listA.Repositories {
		assert.Contains(t, aws.ToString(r.RepositoryArn), AcctA, "A's repository ARNs must embed A's account")
		assert.NotContains(t, aws.ToString(r.RepositoryArn), AcctB)
	}

	listB, err := ecrB.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})
	require.NoError(t, err)
	for _, r := range listB.Repositories {
		assert.Contains(t, aws.ToString(r.RepositoryArn), AcctB, "B's repository ARNs must embed B's account")
		assert.NotContains(t, aws.ToString(r.RepositoryArn), AcctA)
	}

	// Delete A's repository — B's must survive.
	_, err = ecrA.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{RepositoryName: aws.String("shared-repo")})
	require.NoError(t, err)
	listBAfter, err := ecrB.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Repositories, 1, "B's repository must survive A's delete")
}

// ─── ElastiCache ──────────────────────────────────────────────────────────────

func TestElastiCache_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ecA := newElastiCacheFor(t, AcctA)
	ecB := newElastiCacheFor(t, AcctB)

	_, err := ecA.CreateCacheCluster(ctx, &awselasticache.CreateCacheClusterInput{
		CacheClusterId: aws.String("shared-cache"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		Engine:         aws.String("redis"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)
	_, err = ecB.CreateCacheCluster(ctx, &awselasticache.CreateCacheClusterInput{
		CacheClusterId: aws.String("shared-cache"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		Engine:         aws.String("redis"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	descA, err := ecA.DescribeCacheClusters(ctx, &awselasticache.DescribeCacheClustersInput{})
	require.NoError(t, err)
	assert.Len(t, descA.CacheClusters, 1, "A sees only its own cache cluster")
	for _, c := range descA.CacheClusters {
		assert.Contains(t, aws.ToString(c.ARN), AcctA, "A's cache cluster ARN must embed A's account")
	}

	descB, err := ecB.DescribeCacheClusters(ctx, &awselasticache.DescribeCacheClustersInput{})
	require.NoError(t, err)
	assert.Len(t, descB.CacheClusters, 1, "B sees only its own cache cluster")
	for _, c := range descB.CacheClusters {
		assert.Contains(t, aws.ToString(c.ARN), AcctB, "B's cache cluster ARN must embed B's account")
	}

	// Delete A's cluster — B's must survive.
	_, err = ecA.DeleteCacheCluster(ctx, &awselasticache.DeleteCacheClusterInput{
		CacheClusterId: aws.String("shared-cache"),
	})
	require.NoError(t, err)
	descBAfter, err := ecB.DescribeCacheClusters(ctx, &awselasticache.DescribeCacheClustersInput{})
	require.NoError(t, err)
	assert.Len(t, descBAfter.CacheClusters, 1, "B's cache cluster must survive A's delete")
}

// ─── RDS ──────────────────────────────────────────────────────────────────────

func TestRDS_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	rdsA := newRDSFor(t, AcctA)
	rdsB := newRDSFor(t, AcctB)

	_, err := rdsA.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("shared-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)
	_, err = rdsB.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("shared-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err)

	descA, err := rdsA.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{})
	require.NoError(t, err)
	assert.Len(t, descA.DBInstances, 1, "A sees only its own DB instance")
	for _, db := range descA.DBInstances {
		assert.Contains(t, aws.ToString(db.DBInstanceArn), AcctA, "A's DB ARN must embed A's account")
	}

	descB, err := rdsB.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{})
	require.NoError(t, err)
	assert.Len(t, descB.DBInstances, 1, "B sees only its own DB instance")
	for _, db := range descB.DBInstances {
		assert.Contains(t, aws.ToString(db.DBInstanceArn), AcctB, "B's DB ARN must embed B's account")
	}

	// Delete A's instance — B's must survive.
	_, err = rdsA.DeleteDBInstance(ctx, &awsrds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String("shared-db"),
		SkipFinalSnapshot:    aws.Bool(true),
	})
	require.NoError(t, err)
	descBAfter, err := rdsB.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{})
	require.NoError(t, err)
	assert.Len(t, descBAfter.DBInstances, 1, "B's DB instance must survive A's delete")
}
