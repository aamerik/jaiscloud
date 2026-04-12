//go:build spark_e2e

package plugin_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemrc "github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	emrctypes "github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
)

func k8sNamespace() string {
	if ns := os.Getenv("SPARK_E2E_K8S_NAMESPACE"); ns != "" {
		return ns
	}
	return "default"
}

func sparkImage() string {
	return os.Getenv("SPARK_E2E_SPARK_IMAGE")
}

// TestSparkJob_K8s_StartJobRun_And_Complete verifies the full virtual cluster + job run lifecycle.
func TestSparkJob_K8s_StartJobRun_And_Complete(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	emrcClient := newEMRContainersClient(t)
	vcID := createVirtualCluster(t, emrcClient, "k8s-test-cluster")

	args := k8sSparkPiArgs(10)

	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("SparkPi-K8s"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint:            aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments:   args,
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id
	t.Logf("submitted job run %s on virtual cluster %s", jobRunID, vcID)

	finalState := pollJobRun(t, emrcClient, vcID, jobRunID)
	if finalState != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", finalState)
	}
}

// TestSparkJob_K8s_CancelJobRun verifies cancelling a running job transitions to CANCELLED.
func TestSparkJob_K8s_CancelJobRun(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	emrcClient := newEMRContainersClient(t)
	vcID := createVirtualCluster(t, emrcClient, "k8s-cancel-cluster")

	// Use high slice count to keep the job running long enough to cancel.
	args := k8sSparkPiArgs(1000)

	jobOut, err := emrcClient.StartJobRun(context.Background(), &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("SparkPi-Cancel"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		JobDriver: &emrctypes.JobDriver{
			SparkSubmitJobDriver: &emrctypes.SparkSubmitJobDriver{
				EntryPoint:          aws.String("/opt/spark/bin/spark-submit"),
				EntryPointArguments: args,
			},
		},
	})
	if err != nil {
		t.Fatalf("StartJobRun: %v", err)
	}
	jobRunID := *jobOut.Id

	// Cancel immediately
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

	// Verify all IDs are unique
	seen := map[string]bool{}
	for _, id := range jobRunIDs {
		if seen[id] {
			t.Errorf("duplicate job run ID: %s", id)
		}
		seen[id] = true
	}

	// Poll all concurrently
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

// TestSparkJob_K8s_FailedJobRun_ReportsFailure verifies a bad entry class leads to FAILED.
func TestSparkJob_K8s_FailedJobRun_ReportsFailure(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	emrcClient := newEMRContainersClient(t)
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

	// Verify failure info is populated
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
	return []string{
		"--master", fmt.Sprintf("k8s://https://kubernetes.default.svc"),
		"--deploy-mode", "cluster",
		"--class", "org.apache.spark.examples.SparkPi",
		"--conf", fmt.Sprintf("spark.kubernetes.container.image=%s", sparkImage()),
		"--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", k8sNamespace()),
		fmt.Sprintf("local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar"),
		fmt.Sprintf("%d", slices),
	}
}
