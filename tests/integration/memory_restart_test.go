package integration_test

// TestMemoryModeRestart verifies that all resources created in memory mode
// (no --dsn) survive a process restart when using --data-dir.
//
// The test:
//  1. Starts jaiscloud-aws in memory mode with a temp data-dir.
//  2. Creates resources across every supported stateful service:
//     SQS, SNS, IAM, Lambda, DynamoDB, S3, KMS, SecretsManager, SSM,
//     CloudWatch, EventBridge, Glue, API Gateway, EC2, Route53, ECS.
//  3. Gracefully stops the server (SIGTERM triggers a final state.json save).
//  4. Restarts the server from the same data-dir.
//  5. Asserts every resource is present in the restarted server.
//
// The jaiscloud-aws binary must be pre-built. The test looks for it at:
//   - $JAISCLOUD_AWS_BINARY env var
//   - ../../jaiscloud-aws (project root, relative to tests/integration/)
//
// Build with: go build -o jaiscloud-aws ./cmd/jaiscloud-aws/
// The test is skipped if the binary is not found.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsvc "github.com/aws/aws-sdk-go-v2/service/apigateway"
	cwsvc "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	dynamosvc "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ecssvc "github.com/aws/aws-sdk-go-v2/service/ecs"
	ebsvc "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	gluesvc "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	iamsvc "github.com/aws/aws-sdk-go-v2/service/iam"
	kmssvc "github.com/aws/aws-sdk-go-v2/service/kms"
	lambdasvc "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	smsvc "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	snssvc "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsvc "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restartFindBinary returns the path to the jaiscloud-aws binary, or skips the test.
func restartFindBinary(t *testing.T) string {
	t.Helper()
	if b := os.Getenv("JAISCLOUD_AWS_BINARY"); b != "" {
		if _, err := os.Stat(b); err == nil {
			return b
		}
		t.Skipf("JAISCLOUD_AWS_BINARY=%q not found", b)
	}
	candidates := []string{
		"../../jaiscloud-aws",
		filepath.Join(os.Getenv("GOPATH"), "bin", "jaiscloud-aws"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("jaiscloud-aws binary not found; run: go build -o jaiscloud-aws ./cmd/jaiscloud-aws/")
	return ""
}

// restartFreePort finds an available TCP port on localhost.
func restartFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "find free port")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// restartStartServer starts jaiscloud-aws in memory mode on the given port using dataDir
// for state persistence and instanceID to pin the session blob directory.
func restartStartServer(t *testing.T, binary, dataDir, instanceID string, port int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binary, "start")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("JAISCLOUD_PORT=%d", port),
		fmt.Sprintf("JAISCLOUD_DATA_DIR=%s", dataDir),
		fmt.Sprintf("JAISCLOUD_STATE_DIR=%s", dataDir),
		fmt.Sprintf("JAISCLOUD_INSTANCE_ID=%s", instanceID),
		"JAISCLOUD_LOG_LEVEL=warn",
		"JAISCLOUD_EPHEMERAL=false",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start jaiscloud-aws")
	return cmd
}

// restartStopServer sends SIGTERM to the server and waits for it to exit (which
// triggers a final state.json save via SnapshotLoop.Stop).
func restartStopServer(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		t.Log("warning: server did not stop within 15s after SIGTERM; killed")
	}
}

// restartWaitReady polls the health endpoint until the server responds 200 or times out.
func restartWaitReady(t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(endpoint + "/_jaiscloud/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("server at %s did not become ready within 30s", endpoint)
}

// restartBaseCfg returns an aws.Config with static test credentials and us-east-1.
// The endpoint is set per-client via service options.
func restartBaseCfg() aws.Config {
	cfg, _ := sdkconfig.LoadDefaultConfig(context.Background(),
		sdkconfig.WithRegion("us-east-1"),
		sdkconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	return cfg
}

// TestMemoryModeRestart is the main test. See package doc comment for scenario.
func TestMemoryModeRestart(t *testing.T) {
	binary := restartFindBinary(t)

	dataDir := t.TempDir()
	instanceID := fmt.Sprintf("test-restart-%d", restartFreePort(t))
	port := restartFreePort(t)
	endpoint := fmt.Sprintf("http://localhost:%d", port)

	// Clean up the session blob dir (stored in /tmp/jaiscloud-<instanceID>/) after the test.
	t.Cleanup(func() {
		os.RemoveAll(filepath.Join(os.TempDir(), "jaiscloud-"+instanceID))
	})

	// ─── Phase 1: first server instance — create all resources ───────────────

	cmd1 := restartStartServer(t, binary, dataDir, instanceID, port)
	restartWaitReady(t, endpoint)

	ctx := context.Background()
	baseCfg := restartBaseCfg()

	// ── SQS: queue + message ──────────────────────────────────────────────────

	sqsClient := sqssvc.NewFromConfig(baseCfg, func(o *sqssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const sqsQueueName = "restart-test-queue"
	sqsOut, err := sqsClient.CreateQueue(ctx, &sqssvc.CreateQueueInput{
		QueueName: aws.String(sqsQueueName),
	})
	require.NoError(t, err, "SQS CreateQueue")
	sqsQueueURL := aws.ToString(sqsOut.QueueUrl)

	const sqsMsgBody = "restart-test-message"
	_, err = sqsClient.SendMessage(ctx, &sqssvc.SendMessageInput{
		QueueUrl:    aws.String(sqsQueueURL),
		MessageBody: aws.String(sqsMsgBody),
	})
	require.NoError(t, err, "SQS SendMessage")

	// ── SNS: topic ────────────────────────────────────────────────────────────

	snsClient := snssvc.NewFromConfig(baseCfg, func(o *snssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const snsTopicName = "restart-test-topic"
	snsOut, err := snsClient.CreateTopic(ctx, &snssvc.CreateTopicInput{
		Name: aws.String(snsTopicName),
	})
	require.NoError(t, err, "SNS CreateTopic")
	snsTopicARN := aws.ToString(snsOut.TopicArn)
	require.NotEmpty(t, snsTopicARN)

	// ── IAM: role ─────────────────────────────────────────────────────────────

	iamClient := iamsvc.NewFromConfig(baseCfg, func(o *iamsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const iamRoleName = "restart-test-role"
	const trustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	iamRoleOut, err := iamClient.CreateRole(ctx, &iamsvc.CreateRoleInput{
		RoleName:                 aws.String(iamRoleName),
		AssumeRolePolicyDocument: aws.String(trustPolicy),
	})
	require.NoError(t, err, "IAM CreateRole")
	iamRoleARN := aws.ToString(iamRoleOut.Role.Arn)
	require.NotEmpty(t, iamRoleARN)

	// ── Lambda: function (mock executor — any zip bytes work) ─────────────────

	lambdaClient := lambdasvc.NewFromConfig(baseCfg, func(o *lambdasvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const lambdaFuncName = "restart-test-fn"
	_, err = lambdaClient.CreateFunction(ctx, &lambdasvc.CreateFunctionInput{
		FunctionName: aws.String(lambdaFuncName),
		Runtime:      lambdatypes.RuntimePython312,
		Role:         aws.String(iamRoleARN),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: []byte("fake-zip")},
	})
	require.NoError(t, err, "Lambda CreateFunction")

	// ── DynamoDB: table with stream + items ───────────────────────────────────

	dynaClient := dynamosvc.NewFromConfig(baseCfg, func(o *dynamosvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const ddbTableName = "restart-test-table"
	_, err = dynaClient.CreateTable(ctx, &dynamosvc.CreateTableInput{
		TableName: aws.String(ddbTableName),
		AttributeDefinitions: []dynamotypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamotypes.ScalarAttributeTypeS},
		},
		KeySchema: []dynamotypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamotypes.KeyTypeHash},
		},
		StreamSpecification: &dynamotypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dynamotypes.StreamViewTypeNewAndOldImages,
		},
		BillingMode: dynamotypes.BillingModePayPerRequest,
	})
	require.NoError(t, err, "DynamoDB CreateTable")

	for i := 1; i <= 3; i++ {
		_, err = dynaClient.PutItem(ctx, &dynamosvc.PutItemInput{
			TableName: aws.String(ddbTableName),
			Item: map[string]dynamotypes.AttributeValue{
				"id":  &dynamotypes.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				"val": &dynamotypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", i*10)},
			},
		})
		require.NoError(t, err, "DynamoDB PutItem %d", i)
	}

	// ── S3: bucket + object ───────────────────────────────────────────────────

	s3Client := s3svc.NewFromConfig(baseCfg, func(o *s3svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	const s3BucketName = "restart-test-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3svc.CreateBucketInput{
		Bucket: aws.String(s3BucketName),
	})
	require.NoError(t, err, "S3 CreateBucket")

	const s3Key = "test/restart-object.txt"
	const s3Body = "restart-test-content"
	_, err = s3Client.PutObject(ctx, &s3svc.PutObjectInput{
		Bucket:      aws.String(s3BucketName),
		Key:         aws.String(s3Key),
		Body:        strings.NewReader(s3Body),
		ContentType: aws.String("text/plain"),
	})
	require.NoError(t, err, "S3 PutObject")

	// ── KMS: key + alias ─────────────────────────────────────────────────────

	kmsClient := kmssvc.NewFromConfig(baseCfg, func(o *kmssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	kmsKeyOut, err := kmsClient.CreateKey(ctx, &kmssvc.CreateKeyInput{
		Description: aws.String("restart-test-key"),
	})
	require.NoError(t, err, "KMS CreateKey")
	kmsKeyID := aws.ToString(kmsKeyOut.KeyMetadata.KeyId)

	const kmsAlias = "alias/restart-test-key"
	_, err = kmsClient.CreateAlias(ctx, &kmssvc.CreateAliasInput{
		AliasName:   aws.String(kmsAlias),
		TargetKeyId: aws.String(kmsKeyID),
	})
	require.NoError(t, err, "KMS CreateAlias")

	// ── SecretsManager: secret ────────────────────────────────────────────────

	smClient := smsvc.NewFromConfig(baseCfg, func(o *smsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const secretName = "restart-test-secret"
	const secretValue = "restart-secret-value"
	_, err = smClient.CreateSecret(ctx, &smsvc.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(secretValue),
	})
	require.NoError(t, err, "SecretsManager CreateSecret")

	// ── SSM Parameter Store: parameter ────────────────────────────────────────

	ssmClient := ssmsvc.NewFromConfig(baseCfg, func(o *ssmsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const ssmParamName = "/restart-test/param"
	const ssmParamValue = "restart-param-value"
	_, err = ssmClient.PutParameter(ctx, &ssmsvc.PutParameterInput{
		Name:  aws.String(ssmParamName),
		Value: aws.String(ssmParamValue),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err, "SSM PutParameter")

	// ── CloudWatch: metric alarm ──────────────────────────────────────────────

	cwClient := cwsvc.NewFromConfig(baseCfg, func(o *cwsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const cwAlarmName = "restart-test-alarm"
	_, err = cwClient.PutMetricAlarm(ctx, &cwsvc.PutMetricAlarmInput{
		AlarmName:          aws.String(cwAlarmName),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("RestartTestMetric"),
		Namespace:          aws.String("JaisCloud/RestartTest"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticSum,
		Threshold:          aws.Float64(100),
	})
	require.NoError(t, err, "CloudWatch PutMetricAlarm")

	// ── EventBridge: rule ─────────────────────────────────────────────────────

	ebClient := ebsvc.NewFromConfig(baseCfg, func(o *ebsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const ebRuleName = "restart-test-rule"
	_, err = ebClient.PutRule(ctx, &ebsvc.PutRuleInput{
		Name:               aws.String(ebRuleName),
		ScheduleExpression: aws.String("rate(5 minutes)"),
		State:              ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err, "EventBridge PutRule")

	// ── Glue: database ────────────────────────────────────────────────────────

	glueClient := gluesvc.NewFromConfig(baseCfg, func(o *gluesvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const glueDBName = "restart_test_db"
	_, err = glueClient.CreateDatabase(ctx, &gluesvc.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{
			Name: aws.String(glueDBName),
		},
	})
	require.NoError(t, err, "Glue CreateDatabase")

	// ── API Gateway: REST API ─────────────────────────────────────────────────

	apigwClient := apigwsvc.NewFromConfig(baseCfg, func(o *apigwsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const apiName = "restart-test-api"
	apiOut, err := apigwClient.CreateRestApi(ctx, &apigwsvc.CreateRestApiInput{
		Name: aws.String(apiName),
	})
	require.NoError(t, err, "API Gateway CreateRestApi")
	apiID := aws.ToString(apiOut.Id)
	require.NotEmpty(t, apiID)

	// ── EC2: VPC ──────────────────────────────────────────────────────────────

	ec2Client := ec2svc.NewFromConfig(baseCfg, func(o *ec2svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	vpcOut, err := ec2Client.CreateVpc(ctx, &ec2svc.CreateVpcInput{
		CidrBlock: aws.String("10.99.0.0/16"),
	})
	require.NoError(t, err, "EC2 CreateVpc")
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)
	require.NotEmpty(t, vpcID)

	// ── Route53: hosted zone ──────────────────────────────────────────────────

	r53Client := route53svc.NewFromConfig(baseCfg, func(o *route53svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	r53Out, err := r53Client.CreateHostedZone(ctx, &route53svc.CreateHostedZoneInput{
		Name:            aws.String("restart-test.example.com."),
		CallerReference: aws.String(fmt.Sprintf("restart-test-%d", time.Now().UnixNano())),
		HostedZoneConfig: &route53types.HostedZoneConfig{
			Comment: aws.String("restart test zone"),
		},
	})
	require.NoError(t, err, "Route53 CreateHostedZone")
	hostedZoneID := aws.ToString(r53Out.HostedZone.Id)
	require.NotEmpty(t, hostedZoneID)

	// ── ECS: cluster ─────────────────────────────────────────────────────────

	ecsClient := ecssvc.NewFromConfig(baseCfg, func(o *ecssvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	const ecsClusterName = "restart-test-cluster"
	_, err = ecsClient.CreateCluster(ctx, &ecssvc.CreateClusterInput{
		ClusterName: aws.String(ecsClusterName),
	})
	require.NoError(t, err, "ECS CreateCluster")

	// ─── Stop server: SIGTERM triggers final state.json save via SnapshotLoop ─

	restartStopServer(t, cmd1)

	// ─── Phase 2: second server instance — verify all resources survive ───────

	cmd2 := restartStartServer(t, binary, dataDir, instanceID, port)
	t.Cleanup(func() { restartStopServer(t, cmd2) })
	restartWaitReady(t, endpoint)

	// Recreate clients (same config, same endpoint — just re-binding for clarity).
	sqsClient = sqssvc.NewFromConfig(baseCfg, func(o *sqssvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	snsClient = snssvc.NewFromConfig(baseCfg, func(o *snssvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	iamClient = iamsvc.NewFromConfig(baseCfg, func(o *iamsvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	lambdaClient = lambdasvc.NewFromConfig(baseCfg, func(o *lambdasvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	dynaClient = dynamosvc.NewFromConfig(baseCfg, func(o *dynamosvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	s3Client = s3svc.NewFromConfig(baseCfg, func(o *s3svc.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	kmsClient = kmssvc.NewFromConfig(baseCfg, func(o *kmssvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	smClient = smsvc.NewFromConfig(baseCfg, func(o *smsvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	ssmClient = ssmsvc.NewFromConfig(baseCfg, func(o *ssmsvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	cwClient = cwsvc.NewFromConfig(baseCfg, func(o *cwsvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	ebClient = ebsvc.NewFromConfig(baseCfg, func(o *ebsvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	glueClient = gluesvc.NewFromConfig(baseCfg, func(o *gluesvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	apigwClient = apigwsvc.NewFromConfig(baseCfg, func(o *apigwsvc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	ec2Client = ec2svc.NewFromConfig(baseCfg, func(o *ec2svc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	r53Client = route53svc.NewFromConfig(baseCfg, func(o *route53svc.Options) { o.BaseEndpoint = aws.String(endpoint) })
	ecsClient = ecssvc.NewFromConfig(baseCfg, func(o *ecssvc.Options) { o.BaseEndpoint = aws.String(endpoint) })

	// ── Verify SQS ────────────────────────────────────────────────────────────

	t.Run("SQS_QueueAndMessageSurvive", func(t *testing.T) {
		listOut, err := sqsClient.ListQueues(ctx, &sqssvc.ListQueuesInput{
			QueueNamePrefix: aws.String(sqsQueueName),
		})
		require.NoError(t, err)
		var foundQueue bool
		for _, u := range listOut.QueueUrls {
			if strings.Contains(u, sqsQueueName) {
				foundQueue = true
				break
			}
		}
		assert.True(t, foundQueue, "SQS queue %q must survive restart", sqsQueueName)

		var receivedBody string
		waitFor(t, 5*time.Second, func() bool {
			recvOut, err := sqsClient.ReceiveMessage(ctx, &sqssvc.ReceiveMessageInput{
				QueueUrl:            aws.String(sqsQueueURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			if err != nil || len(recvOut.Messages) == 0 {
				return false
			}
			receivedBody = aws.ToString(recvOut.Messages[0].Body)
			return true
		})
		assert.Equal(t, sqsMsgBody, receivedBody, "SQS message body must survive restart")
	})

	// ── Verify SNS ────────────────────────────────────────────────────────────

	t.Run("SNS_TopicSurvives", func(t *testing.T) {
		var found bool
		var nextToken *string
		for {
			listOut, err := snsClient.ListTopics(ctx, &snssvc.ListTopicsInput{
				NextToken: nextToken,
			})
			require.NoError(t, err)
			for _, topic := range listOut.Topics {
				if strings.Contains(aws.ToString(topic.TopicArn), snsTopicName) {
					found = true
					break
				}
			}
			if found || listOut.NextToken == nil {
				break
			}
			nextToken = listOut.NextToken
		}
		assert.True(t, found, "SNS topic %q must survive restart", snsTopicName)
	})

	// ── Verify IAM ────────────────────────────────────────────────────────────

	t.Run("IAM_RoleSurvives", func(t *testing.T) {
		getRoleOut, err := iamClient.GetRole(ctx, &iamsvc.GetRoleInput{
			RoleName: aws.String(iamRoleName),
		})
		require.NoError(t, err, "IAM GetRole must succeed after restart")
		assert.Equal(t, iamRoleName, aws.ToString(getRoleOut.Role.RoleName))
	})

	// ── Verify Lambda ─────────────────────────────────────────────────────────

	t.Run("Lambda_FunctionSurvives", func(t *testing.T) {
		getFnOut, err := lambdaClient.GetFunction(ctx, &lambdasvc.GetFunctionInput{
			FunctionName: aws.String(lambdaFuncName),
		})
		require.NoError(t, err, "Lambda GetFunction must succeed after restart")
		assert.Equal(t, lambdaFuncName, aws.ToString(getFnOut.Configuration.FunctionName))
	})

	// ── Verify DynamoDB ───────────────────────────────────────────────────────

	t.Run("DynamoDB_TableAndItemsSurvive", func(t *testing.T) {
		descOut, err := dynaClient.DescribeTable(ctx, &dynamosvc.DescribeTableInput{
			TableName: aws.String(ddbTableName),
		})
		require.NoError(t, err, "DynamoDB DescribeTable must succeed after restart")
		assert.Equal(t, ddbTableName, aws.ToString(descOut.Table.TableName))
		assert.True(t, aws.ToBool(descOut.Table.StreamSpecification.StreamEnabled),
			"DynamoDB stream must remain enabled after restart")

		scanOut, err := dynaClient.Scan(ctx, &dynamosvc.ScanInput{
			TableName: aws.String(ddbTableName),
		})
		require.NoError(t, err, "DynamoDB Scan must succeed after restart")
		assert.Equal(t, 3, int(scanOut.Count), "all 3 DynamoDB items must survive restart")
	})

	// ── Verify S3 ─────────────────────────────────────────────────────────────

	t.Run("S3_BucketAndObjectSurvive", func(t *testing.T) {
		_, err := s3Client.HeadBucket(ctx, &s3svc.HeadBucketInput{
			Bucket: aws.String(s3BucketName),
		})
		require.NoError(t, err, "S3 HeadBucket must succeed after restart")

		getOut, err := s3Client.GetObject(ctx, &s3svc.GetObjectInput{
			Bucket: aws.String(s3BucketName),
			Key:    aws.String(s3Key),
		})
		require.NoError(t, err, "S3 GetObject must succeed after restart")
		defer getOut.Body.Close()

		var buf bytes.Buffer
		buf.ReadFrom(getOut.Body)
		assert.Equal(t, s3Body, buf.String(), "S3 object body must survive restart")
		assert.Equal(t, "text/plain", aws.ToString(getOut.ContentType))
	})

	// ── Verify KMS ────────────────────────────────────────────────────────────

	t.Run("KMS_KeyAndAliasSurvive", func(t *testing.T) {
		descOut, err := kmsClient.DescribeKey(ctx, &kmssvc.DescribeKeyInput{
			KeyId: aws.String(kmsKeyID),
		})
		require.NoError(t, err, "KMS DescribeKey must succeed after restart")
		assert.Equal(t, "restart-test-key", aws.ToString(descOut.KeyMetadata.Description))

		listOut, err := kmsClient.ListAliases(ctx, &kmssvc.ListAliasesInput{
			KeyId: aws.String(kmsKeyID),
		})
		require.NoError(t, err, "KMS ListAliases must succeed after restart")
		var foundAlias bool
		for _, a := range listOut.Aliases {
			if aws.ToString(a.AliasName) == kmsAlias {
				foundAlias = true
				break
			}
		}
		assert.True(t, foundAlias, "KMS alias %q must survive restart", kmsAlias)
	})

	// ── Verify SecretsManager ─────────────────────────────────────────────────

	t.Run("SecretsManager_SecretSurvives", func(t *testing.T) {
		getOut, err := smClient.GetSecretValue(ctx, &smsvc.GetSecretValueInput{
			SecretId: aws.String(secretName),
		})
		require.NoError(t, err, "SecretsManager GetSecretValue must succeed after restart")
		assert.Equal(t, secretValue, aws.ToString(getOut.SecretString),
			"secret value must survive restart")
	})

	// ── Verify SSM ────────────────────────────────────────────────────────────

	t.Run("SSM_ParameterSurvives", func(t *testing.T) {
		getOut, err := ssmClient.GetParameter(ctx, &ssmsvc.GetParameterInput{
			Name: aws.String(ssmParamName),
		})
		require.NoError(t, err, "SSM GetParameter must succeed after restart")
		assert.Equal(t, ssmParamValue, aws.ToString(getOut.Parameter.Value),
			"SSM parameter value must survive restart")
	})

	// ── Verify CloudWatch ─────────────────────────────────────────────────────

	t.Run("CloudWatch_AlarmSurvives", func(t *testing.T) {
		descOut, err := cwClient.DescribeAlarms(ctx, &cwsvc.DescribeAlarmsInput{
			AlarmNames: []string{cwAlarmName},
		})
		require.NoError(t, err, "CloudWatch DescribeAlarms must succeed after restart")
		require.NotEmpty(t, descOut.MetricAlarms, "CloudWatch alarm must survive restart")
		assert.Equal(t, cwAlarmName, aws.ToString(descOut.MetricAlarms[0].AlarmName))
	})

	// ── Verify EventBridge ────────────────────────────────────────────────────

	t.Run("EventBridge_RuleSurvives", func(t *testing.T) {
		listOut, err := ebClient.ListRules(ctx, &ebsvc.ListRulesInput{
			NamePrefix: aws.String(ebRuleName),
		})
		require.NoError(t, err, "EventBridge ListRules must succeed after restart")
		var found bool
		for _, r := range listOut.Rules {
			if aws.ToString(r.Name) == ebRuleName {
				found = true
				break
			}
		}
		assert.True(t, found, "EventBridge rule %q must survive restart", ebRuleName)
	})

	// ── Verify Glue ───────────────────────────────────────────────────────────

	t.Run("Glue_DatabaseSurvives", func(t *testing.T) {
		getOut, err := glueClient.GetDatabase(ctx, &gluesvc.GetDatabaseInput{
			Name: aws.String(glueDBName),
		})
		require.NoError(t, err, "Glue GetDatabase must succeed after restart")
		assert.Equal(t, glueDBName, aws.ToString(getOut.Database.Name))
	})

	// ── Verify API Gateway ────────────────────────────────────────────────────

	t.Run("APIGateway_RestAPISurvives", func(t *testing.T) {
		getOut, err := apigwClient.GetRestApi(ctx, &apigwsvc.GetRestApiInput{
			RestApiId: aws.String(apiID),
		})
		require.NoError(t, err, "API Gateway GetRestApi must succeed after restart")
		assert.Equal(t, apiName, aws.ToString(getOut.Name))
	})

	// ── Verify EC2 ────────────────────────────────────────────────────────────

	t.Run("EC2_VPCSurvives", func(t *testing.T) {
		descOut, err := ec2Client.DescribeVpcs(ctx, &ec2svc.DescribeVpcsInput{
			VpcIds: []string{vpcID},
		})
		require.NoError(t, err, "EC2 DescribeVpcs must succeed after restart")
		require.NotEmpty(t, descOut.Vpcs, "EC2 VPC must survive restart")
		assert.Equal(t, vpcID, aws.ToString(descOut.Vpcs[0].VpcId))
	})

	// ── Verify Route53 ────────────────────────────────────────────────────────

	t.Run("Route53_HostedZoneSurvives", func(t *testing.T) {
		listOut, err := r53Client.ListHostedZones(ctx, &route53svc.ListHostedZonesInput{})
		require.NoError(t, err, "Route53 ListHostedZones must succeed after restart")
		var found bool
		for _, z := range listOut.HostedZones {
			if aws.ToString(z.Id) == hostedZoneID {
				found = true
				break
			}
		}
		assert.True(t, found, "Route53 hosted zone %q must survive restart", hostedZoneID)
	})

	// ── Verify ECS ────────────────────────────────────────────────────────────

	t.Run("ECS_ClusterSurvives", func(t *testing.T) {
		descOut, err := ecsClient.DescribeClusters(ctx, &ecssvc.DescribeClustersInput{
			Clusters: []string{ecsClusterName},
		})
		require.NoError(t, err, "ECS DescribeClusters must succeed after restart")
		var found bool
		for _, c := range descOut.Clusters {
			if aws.ToString(c.ClusterName) == ecsClusterName {
				found = true
				break
			}
		}
		assert.True(t, found, "ECS cluster %q must survive restart", ecsClusterName)
	})
}
