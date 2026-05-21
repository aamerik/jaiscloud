//go:build spark_e2e

package dpc_test

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

// TestDPC_EMR_EC2_SparkPi exercises the command-runner.jar (Pattern 2) submission path
// via EMR on EC2: RunJobFlow → AddJobFlowSteps → poll step → verify EventBridge COMPLETED event.
func TestDPC_EMR_EC2_SparkPi(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrClient := newEMRClient(t)

	// Set up EventBridge rule + SQS target before submitting.
	queueURL := createQueue(t, sqsClient, "dpc-ec2-completed-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:dpc-ec2-completed-queue"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("dpc-ec2-step-completed"),
		EventPattern: aws.String(`{"source":["aws.emr"],"detail":{"state":["COMPLETED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule:    aws.String("dpc-ec2-step-completed"),
		Targets: []ebtypes.Target{{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)}},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	// Create EMR cluster.
	runOut, err := emrClient.RunJobFlow(context.Background(), &awsemr.RunJobFlowInput{
		Name:         aws.String("DPC-EC2-SparkPi"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		ServiceRole:  aws.String("EMR_DefaultRole"),
		JobFlowRole:  aws.String("EMR_EC2_DefaultRole"),
		LogUri:       aws.String("s3://dpc-logs/"),
		Instances: &emrtypes.JobFlowInstancesConfig{
			MasterInstanceType:          aws.String("m5.xlarge"),
			SlaveInstanceType:           aws.String("m5.xlarge"),
			InstanceCount:               aws.Int32(1),
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatalf("RunJobFlow: %v", err)
	}
	clusterID := *runOut.JobFlowId
	t.Logf("created cluster %s", clusterID)

	// Submit SparkPi step via command-runner.jar (DPC/EMR classic pattern).
	stepsOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("SparkPi-DPC"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar: aws.String("command-runner.jar"),
					Args: []string{
						"spark-submit",
						"--master", "yarn",
						"--deploy-mode", "cluster",
						"--class", "org.apache.spark.examples.SparkPi",
						"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
						"10",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	stepID := stepsOut.StepIds[0]
	t.Logf("submitted step %s on cluster %s", stepID, clusterID)

	// Poll until terminal state.
	finalState := pollEMRStep(t, emrClient, clusterID, stepID)
	if finalState != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", finalState)
	}

	// Verify EventBridge → SQS notification arrives.
	msg := pollSQSMessageWithState(t, sqsClient, queueURL, "COMPLETED", 15*time.Second)
	if msg["source"] != "aws.emr" {
		t.Errorf("expected source=aws.emr, got %v", msg["source"])
	}
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail in SQS message")
	}
	if detail["clusterId"] != clusterID {
		t.Errorf("expected detail.clusterId=%s, got %v", clusterID, detail["clusterId"])
	}
	if detail["stepId"] != stepID {
		t.Errorf("expected detail.stepId=%s, got %v", stepID, detail["stepId"])
	}
}

// TestDPC_EMR_EKS_SparkPi exercises the EMR on EKS (Pattern 1) submission path with
// configurationOverrides and sparkSubmitParameters:
// CreateVirtualCluster → StartJobRun → poll job run → verify EventBridge COMPLETED event.
func TestDPC_EMR_EKS_SparkPi(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrcClient := newEMRContainersClient(t)

	// Set up EventBridge rule + SQS target before submitting.
	queueURL := createQueue(t, sqsClient, "dpc-eks-completed-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:dpc-eks-completed-queue"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("dpc-eks-jobrun-completed"),
		EventPattern: aws.String(`{"source":["aws.emr-containers"],"detail":{"state":["COMPLETED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule:    aws.String("dpc-eks-jobrun-completed"),
		Targets: []ebtypes.Target{{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)}},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	vcID := createVirtualCluster(t, emrcClient, "dpc-eks-cluster")

	// Submit using configurationOverrides + sparkSubmitParameters (DPC pattern).
	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("SparkPi-DPC-EKS"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint: aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments: []string{
					"--master", "local[2]",
					"--class", "org.apache.spark.examples.SparkPi",
					"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
					"10",
				},
				SparkSubmitParameters: aws.String("--conf spark.executor.memory=1g --conf spark.executor.cores=1"),
			},
		},
		ConfigurationOverrides: &emrctypes.ConfigurationOverrides{
			ApplicationConfiguration: []emrctypes.Configuration{
				{
					Classification: aws.String("spark-defaults"),
					Properties: map[string]string{
						"spark.executor.instances": "1",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id
	t.Logf("submitted job run %s on virtual cluster %s", jobRunID, vcID)

	// Poll until terminal state.
	finalState := pollJobRun(t, emrcClient, vcID, jobRunID)
	if finalState != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", finalState)
	}

	// Verify EventBridge → SQS notification.
	msg := pollSQSMessageWithState(t, sqsClient, queueURL, "COMPLETED", 15*time.Second)
	if msg["source"] != "aws.emr-containers" {
		t.Errorf("expected source=aws.emr-containers, got %v", msg["source"])
	}
	if msg["detail-type"] != "EMR Job Run State Change" {
		t.Errorf("unexpected detail-type: %v", msg["detail-type"])
	}
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail in SQS message")
	}
	if detail["virtualClusterId"] != vcID {
		t.Errorf("expected detail.virtualClusterId=%s, got %v", vcID, detail["virtualClusterId"])
	}
	if detail["id"] != jobRunID {
		t.Errorf("expected detail.id=%s, got %v", jobRunID, detail["id"])
	}
}
