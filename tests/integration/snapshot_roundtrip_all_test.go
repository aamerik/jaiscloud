package integration_test

// TestRoundTrip_AllResources is a comprehensive snapshot export/import test that:
//  1. Creates state across SQS, DynamoDB (with GSI, LSI, Streams), S3 (with tags,
//     versioning), KMS, SecretsManager, and SSM Parameter Store.
//  2. Exports the full state via POST /_jaiscloud/export.
//  3. Resets all state via POST /_jaiscloud/reset.
//  4. Imports the snapshot via POST /_jaiscloud/import.
//  5. Verifies every resource — including tags, metadata, GSI/LSI queries, and
//     DynamoDB stream records — is correctly restored.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamo_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsdynamostreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streams_types "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	kms_types "github.com/aws/aws-sdk-go-v2/service/kms/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sm_types "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssm_types "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip_AllResources(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	ctx := context.Background()

	sqsClient := newSQSClient(t)
	dynaClient := newDynamoClient(t)
	streamsClient := newDynamoStreamsClient(t)
	s3Client := newS3Client(t)
	kmsClient := newKMSClient(t)
	smClient := newSMClient(t)
	ssmClient := newSSMClient(t)

	// ── 1. SQS ───────────────────────────────────────────────────────────────

	const sqsQueueName = "rt-all-sqs-queue"
	sqsCreateOut, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String(sqsQueueName),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "86400",
		},
	})
	require.NoError(t, err, "create SQS queue")
	sqsQueueURL := aws.ToString(sqsCreateOut.QueueUrl)

	const sqsMsgBody = "snapshot-round-trip-payload"
	_, err = sqsClient.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    aws.String(sqsQueueURL),
		MessageBody: aws.String(sqsMsgBody),
	})
	require.NoError(t, err, "send SQS message")

	// ── 2. DynamoDB table with GSI ────────────────────────────────────────────

	const ddbGSITable = "rt-all-dynamo-gsi"
	_, err = dynaClient.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String(ddbGSITable),
		AttributeDefinitions: []dynamo_types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamo_types.ScalarAttributeTypeS},
			{AttributeName: aws.String("category"), AttributeType: dynamo_types.ScalarAttributeTypeS},
		},
		KeySchema: []dynamo_types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamo_types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []dynamo_types.GlobalSecondaryIndex{{
			IndexName: aws.String("category-index"),
			KeySchema: []dynamo_types.KeySchemaElement{
				{AttributeName: aws.String("category"), KeyType: dynamo_types.KeyTypeHash},
			},
			Projection: &dynamo_types.Projection{ProjectionType: dynamo_types.ProjectionTypeAll},
		}},
		BillingMode: dynamo_types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "create DynamoDB GSI table")

	gsiItems := []struct{ id, category, name string }{
		{"p1", "electronics", "Laptop"},
		{"p2", "electronics", "Phone"},
		{"p3", "clothing", "Shirt"},
	}
	for _, item := range gsiItems {
		_, err = dynaClient.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String(ddbGSITable),
			Item: map[string]dynamo_types.AttributeValue{
				"id":       &dynamo_types.AttributeValueMemberS{Value: item.id},
				"category": &dynamo_types.AttributeValueMemberS{Value: item.category},
				"name":     &dynamo_types.AttributeValueMemberS{Value: item.name},
			},
		})
		require.NoError(t, err, "PutItem GSI table %s", item.id)
	}

	// ── 3. DynamoDB table with LSI ────────────────────────────────────────────

	const ddbLSITable = "rt-all-dynamo-lsi"
	_, err = dynaClient.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String(ddbLSITable),
		AttributeDefinitions: []dynamo_types.AttributeDefinition{
			{AttributeName: aws.String("game"), AttributeType: dynamo_types.ScalarAttributeTypeS},
			{AttributeName: aws.String("player"), AttributeType: dynamo_types.ScalarAttributeTypeS},
			{AttributeName: aws.String("score"), AttributeType: dynamo_types.ScalarAttributeTypeN},
		},
		KeySchema: []dynamo_types.KeySchemaElement{
			{AttributeName: aws.String("game"), KeyType: dynamo_types.KeyTypeHash},
			{AttributeName: aws.String("player"), KeyType: dynamo_types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []dynamo_types.LocalSecondaryIndex{{
			IndexName: aws.String("score-index"),
			KeySchema: []dynamo_types.KeySchemaElement{
				{AttributeName: aws.String("game"), KeyType: dynamo_types.KeyTypeHash},
				{AttributeName: aws.String("score"), KeyType: dynamo_types.KeyTypeRange},
			},
			Projection: &dynamo_types.Projection{ProjectionType: dynamo_types.ProjectionTypeAll},
		}},
		BillingMode: dynamo_types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "create DynamoDB LSI table")

	lsiRows := []struct {
		game, player string
		score        int
	}{
		{"chess", "alice", 95},
		{"chess", "bob", 72},
		{"chess", "carol", 88},
	}
	for _, r := range lsiRows {
		_, err = dynaClient.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String(ddbLSITable),
			Item: map[string]dynamo_types.AttributeValue{
				"game":   &dynamo_types.AttributeValueMemberS{Value: r.game},
				"player": &dynamo_types.AttributeValueMemberS{Value: r.player},
				"score":  &dynamo_types.AttributeValueMemberN{Value: fmt.Sprintf("%d", r.score)},
			},
		})
		require.NoError(t, err, "PutItem LSI table %s/%s", r.game, r.player)
	}

	// ── 4. DynamoDB table with Streams enabled ────────────────────────────────

	const ddbStreamTable = "rt-all-dynamo-streams"
	_, err = dynaClient.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String(ddbStreamTable),
		AttributeDefinitions: []dynamo_types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamo_types.ScalarAttributeTypeS},
		},
		KeySchema: []dynamo_types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamo_types.KeyTypeHash},
		},
		StreamSpecification: &dynamo_types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dynamo_types.StreamViewTypeNewAndOldImages,
		},
		BillingMode: dynamo_types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "create DynamoDB Streams table")

	// Write items to generate stream records.
	for i := 1; i <= 3; i++ {
		_, err = dynaClient.PutItem(ctx, &awsdynamo.PutItemInput{
			TableName: aws.String(ddbStreamTable),
			Item: map[string]dynamo_types.AttributeValue{
				"id":    &dynamo_types.AttributeValueMemberS{Value: fmt.Sprintf("stream-item-%d", i)},
				"value": &dynamo_types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i*10)},
			},
		})
		require.NoError(t, err, "PutItem streams table %d", i)
	}

	// Capture the stream ARN before export.
	descTableOut, err := dynaClient.DescribeTable(ctx, &awsdynamo.DescribeTableInput{
		TableName: aws.String(ddbStreamTable),
	})
	require.NoError(t, err, "DescribeTable for streams table")
	streamARNBeforeExport := aws.ToString(descTableOut.Table.LatestStreamArn)
	require.NotEmpty(t, streamARNBeforeExport, "stream ARN must be set before export")

	// ── 5. S3 bucket with objects, tags, and versioning ───────────────────────

	const s3Bucket = "rt-all-s3-bucket"
	_, err = s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(s3Bucket),
	})
	require.NoError(t, err, "create S3 bucket")

	// Put an object with custom metadata and tags.
	const s3Key = "data/snapshot-test.txt"
	const s3Body = "snapshot-test-content"
	_, err = s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(s3Bucket),
		Key:         aws.String(s3Key),
		Body:        strings.NewReader(s3Body),
		ContentType: aws.String("text/plain"),
		Metadata:    map[string]string{"x-custom-meta": "meta-value"},
	})
	require.NoError(t, err, "put S3 object")

	_, err = s3Client.PutObjectTagging(ctx, &awss3.PutObjectTaggingInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
		Tagging: &s3_types.Tagging{
			TagSet: []s3_types.Tag{
				{Key: aws.String("env"), Value: aws.String("test")},
				{Key: aws.String("owner"), Value: aws.String("alice")},
			},
		},
	})
	require.NoError(t, err, "tag S3 object")

	// Enable versioning on the bucket.
	_, err = s3Client.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(s3Bucket),
		VersioningConfiguration: &s3_types.VersioningConfiguration{
			Status: s3_types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err, "enable S3 versioning")

	// ── 6. KMS key with alias ─────────────────────────────────────────────────

	kmsCreateOut, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("rt-all-kms-key"),
		Tags: []kms_types.Tag{
			{TagKey: aws.String("purpose"), TagValue: aws.String("snapshot-test")},
		},
	})
	require.NoError(t, err, "create KMS key")
	kmsKeyID := aws.ToString(kmsCreateOut.KeyMetadata.KeyId)

	const kmsAlias = "alias/rt-all-test-key"
	_, err = kmsClient.CreateAlias(ctx, &awskms.CreateAliasInput{
		AliasName:   aws.String(kmsAlias),
		TargetKeyId: aws.String(kmsKeyID),
	})
	require.NoError(t, err, "create KMS alias")

	// ── 7. SecretsManager secret ──────────────────────────────────────────────

	const secretName = "rt-all-secret"
	const secretValue = "super-secret-snapshot-value"
	smCreateOut, err := smClient.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(secretValue),
		Tags: []sm_types.Tag{
			{Key: aws.String("app"), Value: aws.String("snapshot-test")},
		},
	})
	require.NoError(t, err, "create SecretsManager secret")
	secretARN := aws.ToString(smCreateOut.ARN)
	require.NotEmpty(t, secretARN)

	// ── 8. SSM Parameter Store parameter ─────────────────────────────────────

	const ssmParamName = "/rt-all/snapshot-param"
	const ssmParamValue = "snapshot-param-value"
	_, err = ssmClient.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String(ssmParamName),
		Value: aws.String(ssmParamValue),
		Type:  ssm_types.ParameterTypeString,
		Tags: []ssm_types.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err, "create SSM parameter")

	// ══════════════════════════════════════════════════════════════════════════
	// Export, reset, import.
	// ══════════════════════════════════════════════════════════════════════════

	snapshot := exportSnapshot(t)
	resetState(t)

	status := importSnapshot(t, snapshot)
	require.Equal(t, http.StatusOK, status, "import must return 200")

	// ══════════════════════════════════════════════════════════════════════════
	// Verification.
	// ══════════════════════════════════════════════════════════════════════════

	// ── Verify SQS ────────────────────────────────────────────────────────────

	t.Run("SQS_MessageSurvives", func(t *testing.T) {
		var receivedBody string
		waitFor(t, 5*time.Second, func() bool {
			recvOut, err := sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
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
		assert.Equal(t, sqsMsgBody, receivedBody, "SQS message body must match after restore")
	})

	// ── Verify DynamoDB GSI ───────────────────────────────────────────────────

	t.Run("DynamoDB_GSI_Query", func(t *testing.T) {
		scanOut, err := dynaClient.Scan(ctx, &awsdynamo.ScanInput{
			TableName: aws.String(ddbGSITable),
		})
		require.NoError(t, err, "Scan GSI table after restore")
		require.Equal(t, 3, int(scanOut.Count), "all 3 items must be restored in GSI table")

		qOut, err := dynaClient.Query(ctx, &awsdynamo.QueryInput{
			TableName:              aws.String(ddbGSITable),
			IndexName:              aws.String("category-index"),
			KeyConditionExpression: aws.String("category = :cat"),
			ExpressionAttributeValues: map[string]dynamo_types.AttributeValue{
				":cat": &dynamo_types.AttributeValueMemberS{Value: "electronics"},
			},
		})
		require.NoError(t, err, "GSI query after restore")
		assert.Equal(t, 2, int(qOut.Count), "GSI query must return 2 electronics items after restore")
	})

	// ── Verify DynamoDB LSI ───────────────────────────────────────────────────

	t.Run("DynamoDB_LSI_Query", func(t *testing.T) {
		scanOut, err := dynaClient.Scan(ctx, &awsdynamo.ScanInput{
			TableName: aws.String(ddbLSITable),
		})
		require.NoError(t, err, "Scan LSI table after restore")
		require.Equal(t, 3, int(scanOut.Count), "all 3 chess players must be restored")

		qOut, err := dynaClient.Query(ctx, &awsdynamo.QueryInput{
			TableName:              aws.String(ddbLSITable),
			IndexName:              aws.String("score-index"),
			KeyConditionExpression: aws.String("game = :g"),
			ExpressionAttributeValues: map[string]dynamo_types.AttributeValue{
				":g": &dynamo_types.AttributeValueMemberS{Value: "chess"},
			},
		})
		require.NoError(t, err, "LSI query after restore")
		assert.Equal(t, 3, int(qOut.Count), "LSI query must return all chess players after restore")
	})

	// ── Verify DynamoDB Streams ───────────────────────────────────────────────

	t.Run("DynamoDB_Streams_RecordsSurvive", func(t *testing.T) {
		// Verify the table still has streams enabled.
		descOut, err := dynaClient.DescribeTable(ctx, &awsdynamo.DescribeTableInput{
			TableName: aws.String(ddbStreamTable),
		})
		require.NoError(t, err, "DescribeTable streams table after restore")
		require.NotNil(t, descOut.Table.StreamSpecification)
		assert.True(t, aws.ToBool(descOut.Table.StreamSpecification.StreamEnabled),
			"streams must still be enabled after restore")

		streamARNAfterImport := aws.ToString(descOut.Table.LatestStreamArn)
		assert.Equal(t, streamARNBeforeExport, streamARNAfterImport,
			"stream ARN must be preserved across export/import")

		// List streams to confirm the stream is visible.
		listOut, err := streamsClient.ListStreams(ctx, &awsdynamostreams.ListStreamsInput{
			TableName: aws.String(ddbStreamTable),
		})
		require.NoError(t, err, "ListStreams after restore")
		require.NotEmpty(t, listOut.Streams, "stream must be listed after restore")

		// Describe the stream and read records.
		descStreamOut, err := streamsClient.DescribeStream(ctx, &awsdynamostreams.DescribeStreamInput{
			StreamArn: aws.String(streamARNAfterImport),
		})
		require.NoError(t, err, "DescribeStream after restore")
		require.NotEmpty(t, descStreamOut.StreamDescription.Shards, "stream must have shards")

		shardID := aws.ToString(descStreamOut.StreamDescription.Shards[0].ShardId)
		iterOut, err := streamsClient.GetShardIterator(ctx, &awsdynamostreams.GetShardIteratorInput{
			StreamArn:         aws.String(streamARNAfterImport),
			ShardId:           aws.String(shardID),
			ShardIteratorType: streams_types.ShardIteratorTypeTrimHorizon,
		})
		require.NoError(t, err, "GetShardIterator after restore")

		recOut, err := streamsClient.GetRecords(ctx, &awsdynamostreams.GetRecordsInput{
			ShardIterator: iterOut.ShardIterator,
		})
		require.NoError(t, err, "GetRecords after restore")
		assert.Equal(t, 3, len(recOut.Records),
			"all 3 stream records (INSERT events) must survive export/import")
	})

	// ── Verify S3 ─────────────────────────────────────────────────────────────

	t.Run("S3_ObjectAndMetadata", func(t *testing.T) {
		getOut, err := s3Client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(s3Key),
		})
		require.NoError(t, err, "GetObject after restore")
		defer getOut.Body.Close()

		var bodyBuf bytes.Buffer
		bodyBuf.ReadFrom(getOut.Body)
		assert.Equal(t, s3Body, bodyBuf.String(), "S3 object body must match after restore")
		assert.Equal(t, "text/plain", aws.ToString(getOut.ContentType),
			"S3 ContentType must be preserved after restore")
		assert.Equal(t, "meta-value", getOut.Metadata["x-custom-meta"],
			"S3 user metadata must survive restore")
	})

	t.Run("S3_TagsSurvive", func(t *testing.T) {
		taggingOut, err := s3Client.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(s3Key),
		})
		require.NoError(t, err, "GetObjectTagging after restore")
		tags := make(map[string]string, len(taggingOut.TagSet))
		for _, tag := range taggingOut.TagSet {
			tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
		assert.Equal(t, "test", tags["env"], "S3 tag 'env' must survive restore")
		assert.Equal(t, "alice", tags["owner"], "S3 tag 'owner' must survive restore")
	})

	t.Run("S3_VersioningEnabled", func(t *testing.T) {
		versioningOut, err := s3Client.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{
			Bucket: aws.String(s3Bucket),
		})
		require.NoError(t, err, "GetBucketVersioning after restore")
		assert.Equal(t, s3_types.BucketVersioningStatusEnabled, versioningOut.Status,
			"bucket versioning must remain Enabled after restore")
	})

	// ── Verify KMS ────────────────────────────────────────────────────────────

	t.Run("KMS_KeyAndAliasSurvive", func(t *testing.T) {
		descOut, err := kmsClient.DescribeKey(ctx, &awskms.DescribeKeyInput{
			KeyId: aws.String(kmsKeyID),
		})
		require.NoError(t, err, "DescribeKey after restore")
		assert.Equal(t, "rt-all-kms-key",
			aws.ToString(descOut.KeyMetadata.Description),
			"KMS key description must survive restore")

		listAliasOut, err := kmsClient.ListAliases(ctx, &awskms.ListAliasesInput{
			KeyId: aws.String(kmsKeyID),
		})
		require.NoError(t, err, "ListAliases after restore")
		var foundAlias bool
		for _, a := range listAliasOut.Aliases {
			if aws.ToString(a.AliasName) == kmsAlias {
				foundAlias = true
				break
			}
		}
		assert.True(t, foundAlias, "KMS alias %q must survive restore", kmsAlias)
	})

	// ── Verify SecretsManager ─────────────────────────────────────────────────

	t.Run("SecretsManager_SecretValueSurvives", func(t *testing.T) {
		getSecretOut, err := smClient.GetSecretValue(ctx, &awssm.GetSecretValueInput{
			SecretId: aws.String(secretName),
		})
		require.NoError(t, err, "GetSecretValue after restore")
		assert.Equal(t, secretValue, aws.ToString(getSecretOut.SecretString),
			"secret value must be preserved after restore")
	})

	t.Run("SecretsManager_TagsSurvive", func(t *testing.T) {
		descOut, err := smClient.DescribeSecret(ctx, &awssm.DescribeSecretInput{
			SecretId: aws.String(secretName),
		})
		require.NoError(t, err, "DescribeSecret after restore")
		var foundTag bool
		for _, tag := range descOut.Tags {
			if aws.ToString(tag.Key) == "app" && aws.ToString(tag.Value) == "snapshot-test" {
				foundTag = true
				break
			}
		}
		assert.True(t, foundTag, "SM secret tag 'app=snapshot-test' must survive restore")
	})

	// ── Verify SSM ────────────────────────────────────────────────────────────

	t.Run("SSM_ParameterValueSurvives", func(t *testing.T) {
		getParamOut, err := ssmClient.GetParameter(ctx, &awsssm.GetParameterInput{
			Name: aws.String(ssmParamName),
		})
		require.NoError(t, err, "GetParameter after restore")
		assert.Equal(t, ssmParamValue, aws.ToString(getParamOut.Parameter.Value),
			"SSM parameter value must survive restore")
	})

	t.Run("SSM_TagsSurvive", func(t *testing.T) {
		listTagsOut, err := ssmClient.ListTagsForResource(ctx, &awsssm.ListTagsForResourceInput{
			ResourceType: ssm_types.ResourceTypeForTaggingParameter,
			ResourceId:   aws.String(ssmParamName),
		})
		require.NoError(t, err, "ListTagsForResource (SSM) after restore")
		var foundTag bool
		for _, tag := range listTagsOut.TagList {
			if aws.ToString(tag.Key) == "env" && aws.ToString(tag.Value) == "test" {
				foundTag = true
				break
			}
		}
		assert.True(t, foundTag, "SSM parameter tag 'env=test' must survive restore")
	})
}
