//go:build spark_e2e

package emr_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	awsemrc "github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"jaiscloud/internal/clock"
)

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func awsCfg(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return cfg
}

func newEMRClient(t *testing.T) *awsemr.Client {
	t.Helper()
	host := jaiscloudHost()
	return awsemr.NewFromConfig(awsCfg(t), func(o *awsemr.Options) {
		o.BaseEndpoint = aws.String(host)
	})
}

func newEMRContainersClient(t *testing.T) *awsemrc.Client {
	t.Helper()
	host := jaiscloudHost()
	return awsemrc.NewFromConfig(awsCfg(t), func(o *awsemrc.Options) {
		o.BaseEndpoint = aws.String(host)
	})
}

func newEventBridgeClient(t *testing.T) *awseb.Client {
	t.Helper()
	host := jaiscloudHost()
	return awseb.NewFromConfig(awsCfg(t), func(o *awseb.Options) {
		o.BaseEndpoint = aws.String(host)
	})
}

func newSQSClient(t *testing.T) *awssqs.Client {
	t.Helper()
	host := jaiscloudHost()
	return awssqs.NewFromConfig(awsCfg(t), func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(host)
	})
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudHost()+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

func requireDockerEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("SPARK_E2E_DOCKER_IMAGE") == "" {
		t.Skip("SPARK_E2E_DOCKER_IMAGE not set — skipping Docker Spark e2e test")
	}
}

func requireK8sEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("SPARK_E2E_SPARK_IMAGE") == "" {
		t.Skip("SPARK_E2E_SPARK_IMAGE not set — skipping K8s Spark e2e test")
	}
}

func pollInterval() time.Duration {
	if v := os.Getenv("SPARK_E2E_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return 3 * time.Second
}

func jobTimeout() time.Duration {
	if v := os.Getenv("SPARK_E2E_JOB_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return 5 * time.Minute
}

func pollEMRStep(t *testing.T, emrClient *awsemr.Client, clusterID, stepID string) string {
	t.Helper()
	deadline := clock.RealNow().Add(jobTimeout())
	for clock.RealNow().Before(deadline) {
		out, err := emrClient.DescribeStep(context.Background(), &awsemr.DescribeStepInput{
			ClusterId: aws.String(clusterID),
			StepId:    aws.String(stepID),
		})
		if err != nil {
			t.Logf("DescribeStep error (retrying): %v", err)
			time.Sleep(pollInterval())
			continue
		}
		state := string(out.Step.Status.State)
		t.Logf("step %s state: %s", stepID, state)
		if isTerminalStepState(state) {
			return state
		}
		time.Sleep(pollInterval())
	}
	t.Fatalf("step %s did not reach terminal state within %s", stepID, jobTimeout())
	return ""
}

func isTerminalStepState(state string) bool {
	switch state {
	case "COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED":
		return true
	}
	return false
}

func pollSQSMessage(t *testing.T, sqsClient *awssqs.Client, queueURL string, maxWait time.Duration) map[string]any {
	t.Helper()
	deadline := clock.RealNow().Add(maxWait)
	for clock.RealNow().Before(deadline) {
		out, err := sqsClient.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Logf("ReceiveMessage error (retrying): %v", err)
			time.Sleep(time.Second)
			continue
		}
		if len(out.Messages) > 0 {
			var body map[string]any
			if err := json.Unmarshal([]byte(*out.Messages[0].Body), &body); err != nil {
				t.Fatalf("parse SQS message body: %v", err)
			}
			return body
		}
	}
	t.Fatalf("no SQS message received within %s on %s", maxWait, queueURL)
	return nil
}

func createQueue(t *testing.T, sqsClient *awssqs.Client, name string) string {
	t.Helper()
	out, err := sqsClient.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("CreateQueue %s: %v", name, err)
	}
	return *out.QueueUrl
}

func sparkPiArgs(slices int) []string {
	return []string{
		"spark-submit",
		"--master", "local[2]",
		"--class", "org.apache.spark.examples.SparkPi",
		"/opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
		fmt.Sprintf("%d", slices),
	}
}

func badClassArgs() []string {
	return []string{
		"spark-submit",
		"--class", "com.nonexistent.ClassThatDoesNotExist",
		"/opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
	}
}

func createCluster(t *testing.T, emrClient *awsemr.Client, name string) string {
	t.Helper()
	out, err := emrClient.RunJobFlow(context.Background(), &awsemr.RunJobFlowInput{
		Name:         aws.String(name),
		ReleaseLabel: aws.String("emr-6.10.0"),
		ServiceRole:  aws.String("EMR_DefaultRole"),
		JobFlowRole:  aws.String("EMR_EC2_DefaultRole"),
		LogUri:       aws.String("s3://my-bucket/logs/"),
		Instances: &emrtypes.JobFlowInstancesConfig{
			MasterInstanceType:          aws.String("m5.xlarge"),
			SlaveInstanceType:           aws.String("m5.xlarge"),
			InstanceCount:               aws.Int32(1),
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatalf("RunJobFlow %s: %v", name, err)
	}
	return *out.JobFlowId
}
