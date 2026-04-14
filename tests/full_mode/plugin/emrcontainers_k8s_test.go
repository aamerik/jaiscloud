//go:build spark_e2e

package plugin_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemrc "github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	emrctypes "github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

func k8sNamespace() string {
	if ns := os.Getenv("SPARK_E2E_K8S_NAMESPACE"); ns != "" {
		return ns
	}
	return "default"
}

// TestSparkJob_K8s_StartJobRun_And_Complete verifies the full virtual cluster + job run lifecycle
// and that a COMPLETED EventBridge notification is delivered to SQS.
func TestSparkJob_K8s_StartJobRun_And_Complete(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrcClient := newEMRContainersClient(t)

	// 1. Create SQS queue + EventBridge rule before submitting the job so no events are missed.
	queueURL := createQueue(t, sqsClient, "k8s-completed-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:k8s-completed-queue"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("k8s-jobrun-completed"),
		EventPattern: aws.String(`{"source":["aws.emr-containers"],"detail":{"state":["COMPLETED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule:    aws.String("k8s-jobrun-completed"),
		Targets: []ebtypes.Target{{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)}},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	// 2. Create virtual cluster and submit job run.
	vcID := createVirtualCluster(t, emrcClient, "k8s-test-cluster")
	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("SparkPi-K8s"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint:          aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments: k8sSparkPiArgs(10),
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id
	t.Logf("submitted job run %s on virtual cluster %s", jobRunID, vcID)

	// 3. Poll DescribeJobRun until terminal state.
	finalState := pollJobRun(t, emrcClient, vcID, jobRunID)
	if finalState != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", finalState)
	}

	// 4. Verify EventBridge → SQS notification.
	msg := pollSQSMessage(t, sqsClient, queueURL, 15*time.Second)
	if msg["source"] != "aws.emr-containers" {
		t.Errorf("expected source=aws.emr-containers, got %v", msg["source"])
	}
	if msg["detail-type"] != "EMR Containers Job Run State Change" {
		t.Errorf("unexpected detail-type: %v", msg["detail-type"])
	}
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail in SQS message")
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

// TestSparkJob_K8s_CancelJobRun verifies cancelling a running job transitions to CANCELLED.
func TestSparkJob_K8s_CancelJobRun(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	emrcClient := newEMRContainersClient(t)
	vcID := createVirtualCluster(t, emrcClient, "k8s-cancel-cluster")

	// Use high slice count to keep the job running long enough to cancel.
	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("SparkPi-Cancel"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint:          aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments: k8sSparkPiArgs(100000),
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id

	// Wait briefly for the K8s Job to reach RUNNING before cancelling.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := emrcClient.DescribeJobRun(context.Background(), &awsemrc.DescribeJobRunInput{
			VirtualClusterId: aws.String(vcID),
			Id:               aws.String(jobRunID),
		})
		if err == nil && (string(out.JobRun.State) == "RUNNING" || string(out.JobRun.State) == "PENDING") {
			break
		}
		time.Sleep(pollInterval())
	}

	_, err = emrcClient.CancelJobRun(context.Background(), &awsemrc.CancelJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Id:               aws.String(jobRunID),
	})
	if err != nil {
		t.Fatalf("CancelJobRun: %v", err)
	}

	finalState := pollJobRun(t, emrcClient, vcID, jobRunID)
	if finalState != "CANCELLED" {
		t.Errorf("expected CANCELLED, got %s", finalState)
	}
}

// TestSparkJob_K8s_MultipleJobRuns_Concurrent verifies independent concurrent job runs.
func TestSparkJob_K8s_MultipleJobRuns_Concurrent(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	emrcClient := newEMRContainersClient(t)
	vcID := createVirtualCluster(t, emrcClient, "k8s-concurrent-cluster")

	const numJobs = 3
	jobRunIDs := make([]string, numJobs)

	for i := 0; i < numJobs; i++ {
		jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
			VirtualClusterId: aws.String(vcID),
			Name:             aws.String(fmt.Sprintf("SparkPi-Concurrent-%d", i+1)),
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
			t.Fatalf("StartJobRun %d: %v", i+1, err)
		}
		jobRunIDs[i] = *jobOut.Id
	}

	// Verify all IDs are unique.
	seen := map[string]bool{}
	for _, id := range jobRunIDs {
		if seen[id] {
			t.Errorf("duplicate job run ID: %s", id)
		}
		seen[id] = true
	}

	// Poll all concurrently.
	var wg sync.WaitGroup
	for i, jobRunID := range jobRunIDs {
		wg.Add(1)
		go func(idx int, id string) {
			defer wg.Done()
			finalState := pollJobRun(t, emrcClient, vcID, id)
			if finalState != "COMPLETED" {
				t.Errorf("job run %d (%s): expected COMPLETED, got %s", idx+1, id, finalState)
			}
		}(i, jobRunID)
	}
	wg.Wait()
}

// TestSparkJob_K8s_FailedJobRun_ReportsFailure verifies a bad entry class leads to FAILED state
// and that a FAILED EventBridge notification is delivered to SQS with a non-empty FailureReason.
func TestSparkJob_K8s_FailedJobRun_ReportsFailure(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	sqsClient := newSQSClient(t)
	ebClient := newEventBridgeClient(t)
	emrcClient := newEMRContainersClient(t)

	// Set up EventBridge rule for FAILED events before submitting.
	queueURL := createQueue(t, sqsClient, "k8s-failed-queue")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:k8s-failed-queue"

	_, err := ebClient.PutRule(context.Background(), &awseb.PutRuleInput{
		Name:         aws.String("k8s-jobrun-failed"),
		EventPattern: aws.String(`{"source":["aws.emr-containers"],"detail":{"state":["FAILED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	if err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	_, err = ebClient.PutTargets(context.Background(), &awseb.PutTargetsInput{
		Rule:    aws.String("k8s-jobrun-failed"),
		Targets: []ebtypes.Target{{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)}},
	})
	if err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	vcID := createVirtualCluster(t, emrcClient, "k8s-fail-cluster")
	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("BadClass-K8s"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint: aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments: []string{
					"--master", "local[2]",
					"--class", "com.nonexistent.Main",
					"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id

	finalState := pollJobRun(t, emrcClient, vcID, jobRunID)
	if finalState != "FAILED" {
		t.Errorf("expected FAILED, got %s", finalState)
	}

	// Verify failure info is populated in DescribeJobRun.
	descOut, err := emrcClient.DescribeJobRun(context.Background(), &awsemrc.DescribeJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Id:               aws.String(jobRunID),
	})
	if err != nil {
		t.Fatalf("DescribeJobRun: %v", err)
	}
	if descOut.JobRun.FailureReason == "" {
		t.Error("expected non-empty FailureReason")
	}

	// Verify FAILED EventBridge notification arrives in SQS.
	msg := pollSQSMessage(t, sqsClient, queueURL, 15*time.Second)
	detail, _ := msg["detail"].(map[string]any)
	if detail == nil {
		t.Fatal("expected non-nil detail in SQS message")
	}
	if detail["state"] != "FAILED" {
		t.Errorf("expected detail.state=FAILED, got %v", detail["state"])
	}
	if detail["id"] != jobRunID {
		t.Errorf("expected detail.id=%s, got %v", jobRunID, detail["id"])
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func createVirtualCluster(t *testing.T, emrcClient *awsemrc.Client, name string) string {
	t.Helper()
	out, err := emrcClient.CreateVirtualCluster(context.Background(), &awsemrc.CreateVirtualClusterInput{
		Name: aws.String(name),
		ContainerProvider: &emrctypes.ContainerProvider{
			Id:   aws.String(k8sNamespace()),
			Type: emrctypes.ContainerProviderTypeEks,
			Info: &emrctypes.ContainerInfoMemberEksInfo{
				Value: emrctypes.EksInfo{
					Namespace: aws.String(k8sNamespace()),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateVirtualCluster %s: %v", name, err)
	}
	return *out.Id
}

func k8sSparkPiArgs(slices int) []string {
	// Run SparkPi in local mode inside the K8s Job container (apache/spark:3.5.0).
	// This avoids cluster deploy-mode which requires driver pod RBAC and a reachable
	// K8s API from within the container — not needed for local testing.
	return []string{
		"--master", "local[2]",
		"--class", "org.apache.spark.examples.SparkPi",
		"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
		fmt.Sprintf("%d", slices),
	}
}
