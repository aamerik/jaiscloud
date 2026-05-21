//go:build spark_e2e

// EventBridge notification end-to-end tests.
// These tests use the mock Spark executor (no real Docker/K8s needed) to validate
// the full EventBridge rule → SQS target delivery path.
//
// Run: go test -v -tags spark_e2e ./tests/persistent_mode/eventbridge/ -run TestSparkJob_EventBridge -timeout 5m

package eventbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	awsemrc "github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	emrctypes "github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// TestSparkJob_EventBridge_StepCompletion_Docker verifies COMPLETED event is delivered to SQS.
func TestSparkJob_EventBridge_StepCompletion_Docker(t *testing.T) {
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrClient := newEMRClient(t)

	// 1. Create SQS queue
	queueURL := createQueue(t, sqsClient, "emr-events-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:emr-events-queue"

	// 2. Create EventBridge rule for EMR step COMPLETED
	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("emr-step-completed"),
		EventPattern: aws.String(`{"source":["aws.emr"],"detail":{"state":["COMPLETED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}

	// 3. Add SQS target
	targetsOut, err := ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule: aws.String("emr-step-completed"),
		Targets: []ebtypes.Target{
			{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)},
		},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}
	if targetsOut.FailedEntryCount > 0 {
		t.Fatalf("PutTargets failed for some entries: %v", targetsOut.FailedEntries)
	}

	// 4. Run a cluster + step (mock executor: completes immediately)
	clusterID := createCluster(t, emrClient, "eb-test-cluster")
	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("SparkPi-EventBridge"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: sparkPiArgs(5),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	stepID := stepOut.StepIds[0]

	// 5. Poll until step terminal
	pollEMRStep(t, emrClient, clusterID, stepID)

	// 6. Poll SQS for the EventBridge notification
	msg := pollSQSMessage(t, sqsClient, queueURL, 10*time.Second)

	// 7. Assert message structure
	if msg["source"] != "aws.emr" {
		t.Errorf("expected source=aws.emr, got %v", msg["source"])
	}
	if msg["detail-type"] != "EMR Step Status Change" {
		t.Errorf("expected detail-type='EMR Step Status Change', got %v", msg["detail-type"])
	}
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail")
	}
	if detail["state"] != "COMPLETED" {
		t.Errorf("expected detail.state=COMPLETED, got %v", detail["state"])
	}
	if detail["jobFlowId"] != clusterID {
		t.Errorf("expected detail.jobFlowId=%s, got %v", clusterID, detail["jobFlowId"])
	}
	if detail["stepId"] != stepID {
		t.Errorf("expected detail.stepId=%s, got %v", stepID, detail["stepId"])
	}
	if msg["version"] != "0" {
		t.Errorf("expected version=0, got %v", msg["version"])
	}
}

// TestSparkJob_EventBridge_StepFailed_Docker verifies FAILED event triggers a FAILED-filter rule.
func TestSparkJob_EventBridge_StepFailed_Docker(t *testing.T) {
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrClient := newEMRClient(t)

	queueURL := createQueue(t, sqsClient, "emr-failures-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:emr-failures-queue"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("emr-step-failed"),
		EventPattern: aws.String(`{"source":["aws.emr"],"detail":{"state":["FAILED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule:    aws.String("emr-step-failed"),
		Targets: []ebtypes.Target{{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)}},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	clusterID := createCluster(t, emrClient, "eb-fail-cluster")
	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("BadClass"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: badClassArgs(),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	stepID := stepOut.StepIds[0]
	pollEMRStep(t, emrClient, clusterID, stepID)

	msg := pollSQSMessage(t, sqsClient, queueURL, 10*time.Second)
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail")
	}
	if detail["state"] != "FAILED" {
		t.Errorf("expected detail.state=FAILED, got %v", detail["state"])
	}
}

// TestSparkJob_EventBridge_JobRunCompletion_K8s verifies EMR-Containers job run COMPLETED event.
func TestSparkJob_EventBridge_JobRunCompletion_K8s(t *testing.T) {
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrcClient := newEMRContainersClient(t)

	queueURL := createQueue(t, sqsClient, "emrc-events-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:emrc-events-queue"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("emrc-jobrun-completed"),
		EventPattern: aws.String(`{"source":["aws.emr-containers"],"detail":{"state":["COMPLETED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule:    aws.String("emrc-jobrun-completed"),
		Targets: []ebtypes.Target{{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)}},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	vcID := createVirtualCluster(t, emrcClient, "eb-k8s-cluster")
	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("SparkPi-EB"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint:          aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments: k8sSparkPiArgs(5),
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id
	pollJobRun(t, emrcClient, vcID, jobRunID)

	msg := pollSQSMessage(t, sqsClient, queueURL, 10*time.Second)
	if msg["source"] != "aws.emr-containers" {
		t.Errorf("expected source=aws.emr-containers, got %v", msg["source"])
	}
	if msg["detail-type"] != "EMR Job Run State Change" {
		t.Errorf("unexpected detail-type: %v", msg["detail-type"])
	}
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail")
	}
	if detail["state"] != "COMPLETED" {
		t.Errorf("expected detail.state=COMPLETED, got %v", detail["state"])
	}
	if detail["virtualClusterId"] != vcID {
		t.Errorf("expected detail.virtualClusterId=%s, got %v", vcID, detail["virtualClusterId"])
	}
	if detail["id"] != jobRunID {
		t.Errorf("expected detail.id=%s, got %v", jobRunID, detail["id"])
	}
}

// TestSparkJob_EventBridge_NoRule_SilentDrop verifies events with no matching rule cause no error.
func TestSparkJob_EventBridge_NoRule_SilentDrop(t *testing.T) {
	resetState(t)

	emrClient := newEMRClient(t)

	// No EventBridge rules configured
	clusterID := createCluster(t, emrClient, "no-rule-cluster")
	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("SparkPi-NoRule"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: sparkPiArgs(5),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	stepID := stepOut.StepIds[0]
	pollEMRStep(t, emrClient, clusterID, stepID)
	time.Sleep(2 * time.Second)
}

// TestSparkJob_EventBridge_MultipleTargets verifies one rule delivers to two SQS queues.
func TestSparkJob_EventBridge_MultipleTargets(t *testing.T) {
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrClient := newEMRClient(t)

	q1URL := createQueue(t, sqsClient, "target-queue-1")
	q2URL := createQueue(t, sqsClient, "target-queue-2")
	q1ARN := "arn:aws:sqs:us-east-1:000000000000:target-queue-1"
	q2ARN := "arn:aws:sqs:us-east-1:000000000000:target-queue-2"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("emr-all"),
		EventPattern: aws.String(`{"source":["aws.emr"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule: aws.String("emr-all"),
		Targets: []ebtypes.Target{
			{Id: aws.String("target-1"), Arn: aws.String(q1ARN)},
			{Id: aws.String("target-2"), Arn: aws.String(q2ARN)},
		},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	clusterID := createCluster(t, emrClient, "multi-target-cluster")
	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("SparkPi-MultiTarget"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: sparkPiArgs(5),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	pollEMRStep(t, emrClient, clusterID, stepOut.StepIds[0])

	msg1 := pollSQSMessage(t, sqsClient, q1URL, 10*time.Second)
	msg2 := pollSQSMessage(t, sqsClient, q2URL, 10*time.Second)

	if msg1["source"] != "aws.emr" {
		t.Errorf("queue-1: expected source=aws.emr, got %v", msg1["source"])
	}
	if msg2["source"] != "aws.emr" {
		t.Errorf("queue-2: expected source=aws.emr, got %v", msg2["source"])
	}
	if msg1["detail-type"] != msg2["detail-type"] {
		t.Errorf("expected identical detail-type across targets, got %v vs %v", msg1["detail-type"], msg2["detail-type"])
	}
}
