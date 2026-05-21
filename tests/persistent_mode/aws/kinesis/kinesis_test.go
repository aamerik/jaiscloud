//go:build kinesis_e2e

package kinesis_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func kinesisClient(t *testing.T) *awskinesis.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	return awskinesis.NewFromConfig(cfg, func(o *awskinesis.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func waitForStreamActive(t *testing.T, client *awskinesis.Client, name string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		desc, err := client.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(name),
		})
		if err == nil && desc.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("stream %q did not become ACTIVE within 30s", name)
}

func streamName(t *testing.T) string {
	return fmt.Sprintf("e2e-%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano()%100000)
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestE2E_StreamLifecycle(t *testing.T) {
	client := kinesisClient(t)
	ctx := context.Background()
	name := streamName(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{StreamName: aws.String(name)})
	}()

	waitForStreamActive(t, client, name)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String(name),
	})
	require.NoError(t, err)
	assert.Equal(t, types.StreamStatusActive, desc.StreamDescription.StreamStatus)
	assert.Len(t, desc.StreamDescription.Shards, 2)
}

func TestE2E_PutAndGetRecord(t *testing.T) {
	client := kinesisClient(t)
	ctx := context.Background()
	name := streamName(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{StreamName: aws.String(name)})
	}()

	waitForStreamActive(t, client, name)

	payload := []byte("hello kinesis persistent mode")
	putOut, err := client.PutRecord(ctx, &awskinesis.PutRecordInput{
		StreamName:   aws.String(name),
		PartitionKey: aws.String("pk1"),
		Data:         payload,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *putOut.SequenceNumber)

	iterOut, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String(name),
		ShardId:           putOut.ShardId,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(ctx, &awskinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 1)
	assert.Equal(t, payload, recOut.Records[0].Data)
}

func TestE2E_SplitShard(t *testing.T) {
	client := kinesisClient(t)
	ctx := context.Background()
	name := streamName(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{StreamName: aws.String(name)})
	}()

	waitForStreamActive(t, client, name)

	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String(name),
	})
	require.NoError(t, err)
	shardID := *desc.StreamDescription.Shards[0].ShardId

	_, err = client.SplitShard(ctx, &awskinesis.SplitShardInput{
		StreamName:         aws.String(name),
		ShardToSplit:       aws.String(shardID),
		NewStartingHashKey: aws.String("170141183460469231731687303715884105728"),
	})
	require.NoError(t, err)

	waitForStreamActive(t, client, name)

	desc2, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{
		StreamName: aws.String(name),
	})
	require.NoError(t, err)
	assert.Len(t, desc2.StreamDescription.Shards, 3)
}

func TestE2E_EnhancedFanOut_ConsumerLifecycle(t *testing.T) {
	client := kinesisClient(t)
	ctx := context.Background()
	name := streamName(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{StreamName: aws.String(name)})
	}()

	waitForStreamActive(t, client, name)

	desc, err := client.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(name),
	})
	require.NoError(t, err)
	streamARN := *desc.StreamDescriptionSummary.StreamARN

	regOut, err := client.RegisterStreamConsumer(ctx, &awskinesis.RegisterStreamConsumerInput{
		StreamARN:    aws.String(streamARN),
		ConsumerName: aws.String("e2e-consumer"),
	})
	require.NoError(t, err)
	consumerARN := *regOut.Consumer.ConsumerARN

	listOut, err := client.ListStreamConsumers(ctx, &awskinesis.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Consumers, 1)

	_, err = client.DeregisterStreamConsumer(ctx, &awskinesis.DeregisterStreamConsumerInput{
		ConsumerARN: aws.String(consumerARN),
	})
	require.NoError(t, err)
}

func TestE2E_PutRecords_Batch(t *testing.T) {
	client := kinesisClient(t)
	ctx := context.Background()
	name := streamName(t)

	_, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteStream(ctx, &awskinesis.DeleteStreamInput{StreamName: aws.String(name)})
	}()

	waitForStreamActive(t, client, name)

	records := make([]types.PutRecordsRequestEntry, 10)
	for i := range records {
		records[i] = types.PutRecordsRequestEntry{
			Data:         []byte(fmt.Sprintf("record-%d", i)),
			PartitionKey: aws.String(fmt.Sprintf("pk-%d", i)),
		}
	}

	out, err := client.PutRecords(ctx, &awskinesis.PutRecordsInput{
		StreamName: aws.String(name),
		Records:    records,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), *out.FailedRecordCount)
	assert.Len(t, out.Records, 10)
}
