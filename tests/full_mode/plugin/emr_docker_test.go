//go:build spark_e2e

package plugin_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
)

// TestSparkJob_Docker_Submit_And_Complete verifies the full cluster + step lifecycle
// using a real Spark Docker container running SparkPi.
func TestSparkJob_Docker_Submit_And_Complete(t *testing.T) {
	requireDockerEnv(t)
	resetState(t)

	emrClient := newEMRClient(t)

	// 1. Create cluster
	clusterID := createCluster(t, emrClient, "docker-test-cluster")

	// 2. Verify initial cluster state
	descOut, err := emrClient.DescribeCluster(context.Background(), &awsemr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}
	state := string(descOut.Cluster.Status.State)
	if state != "WAITING" && state != "BOOTSTRAPPING" && state != "RUNNING" {
		t.Fatalf("expected cluster in initial state, got %s", state)
	}

	// 3. Submit SparkPi step
	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("SparkPi"),
				ActionOnFailure: types.ActionOnFailureTerminateCluster,
				HadoopJarStep: &types.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: sparkPiArgs(10),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	if len(stepOut.StepIds) == 0 {
		t.Fatal("expected at least one step ID")
	}
	stepID := stepOut.StepIds[0]
	t.Logf("submitted step %s on cluster %s", stepID, clusterID)

	// 4. Poll until terminal state
	finalState := pollEMRStep(t, emrClient, clusterID, stepID)
	if finalState != "COMPLETED" {
		t.Errorf("expected step COMPLETED, got %s", finalState)
	}

	// 5. Terminate cluster
	_, err = emrClient.TerminateJobFlows(context.Background(), &awsemr.TerminateJobFlowsInput{
		JobFlowIds: []string{clusterID},
	})
	if err != nil {
		t.Fatalf("TerminateJobFlows: %v", err)
	}
	termOut, err := emrClient.DescribeCluster(context.Background(), &awsemr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	if err != nil {
		t.Fatalf("DescribeCluster after terminate: %v", err)
	}
	termState := string(termOut.Cluster.Status.State)
	if termState != "TERMINATED" && termState != "TERMINATING" {
		t.Errorf("expected cluster TERMINATED/TERMINATING, got %s", termState)
	}
}

// TestSparkJob_Docker_SparkConf_Passed verifies SparkConf keys are preserved.
func TestSparkJob_Docker_SparkConf_Passed(t *testing.T) {
	requireDockerEnv(t)
	resetState(t)

	emrClient := newEMRClient(t)
	clusterID := createCluster(t, emrClient, "docker-conf-cluster")

	args := append(sparkPiArgs(5),
		"--conf", "spark.driver.memory=512m",
		"--conf", "spark.executor.memory=512m",
	)

	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("SparkPi-WithConf"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep: &types.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: args,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	stepID := stepOut.StepIds[0]

	// Verify args are preserved in DescribeStep
	descStep, err := emrClient.DescribeStep(context.Background(), &awsemr.DescribeStepInput{
		ClusterId: aws.String(clusterID),
		StepId:    aws.String(stepID),
	})
	if err != nil {
		t.Fatalf("DescribeStep: %v", err)
	}
	foundConf := false
	for _, arg := range descStep.Step.Config.Args {
		if arg == "spark.driver.memory=512m" {
			foundConf = true
		}
	}
	if !foundConf {
		t.Error("expected spark.driver.memory=512m in step args")
	}

	finalState := pollEMRStep(t, emrClient, clusterID, stepID)
	if finalState != "COMPLETED" {
		t.Errorf("expected COMPLETED, got %s", finalState)
	}
}

// TestSparkJob_Docker_FailedStep_ReportsFailure verifies a bad class causes FAILED state.
func TestSparkJob_Docker_FailedStep_ReportsFailure(t *testing.T) {
	requireDockerEnv(t)
	resetState(t)

	emrClient := newEMRClient(t)
	clusterID := createCluster(t, emrClient, "docker-fail-cluster")

	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("BadClass"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep: &types.HadoopJarStepConfig{
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

	finalState := pollEMRStep(t, emrClient, clusterID, stepID)
	if finalState != "FAILED" {
		t.Errorf("expected FAILED, got %s", finalState)
	}

	// Verify failure details are populated
	descStep, err := emrClient.DescribeStep(context.Background(), &awsemr.DescribeStepInput{
		ClusterId: aws.String(clusterID),
		StepId:    aws.String(stepID),
	})
	if err != nil {
		t.Fatalf("DescribeStep: %v", err)
	}
	if descStep.Step.Status.FailureDetails == nil || descStep.Step.Status.FailureDetails.Message == nil {
		t.Error("expected FailureDetails.Message to be non-empty")
	}
}

// TestSparkJob_Docker_MultipleStepsSequential verifies multiple steps run in order.
func TestSparkJob_Docker_MultipleStepsSequential(t *testing.T) {
	requireDockerEnv(t)
	resetState(t)

	emrClient := newEMRClient(t)
	clusterID := createCluster(t, emrClient, "docker-multi-cluster")

	// Submit 3 SparkPi steps
	stepOut, err := emrClient.AddJobFlowSteps(context.Background(), &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("SparkPi-1"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep:   &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar"), Args: sparkPiArgs(5)},
			},
			{
				Name:            aws.String("SparkPi-2"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep:   &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar"), Args: sparkPiArgs(5)},
			},
			{
				Name:            aws.String("SparkPi-3"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep:   &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar"), Args: sparkPiArgs(5)},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddJobFlowSteps: %v", err)
	}
	if len(stepOut.StepIds) != 3 {
		t.Fatalf("expected 3 step IDs, got %d", len(stepOut.StepIds))
	}

	// Poll each step sequentially
	for i, stepID := range stepOut.StepIds {
		finalState := pollEMRStep(t, emrClient, clusterID, stepID)
		if finalState != "COMPLETED" {
			t.Errorf("step %d (%s): expected COMPLETED, got %s", i+1, stepID, finalState)
		}
	}
}

// ─── helpers ──────────────────────────────────────────────────��──────────────

func createCluster(t *testing.T, emrClient *awsemr.Client, name string) string {
	t.Helper()
	out, err := emrClient.RunJobFlow(context.Background(), &awsemr.RunJobFlowInput{
		Name:         aws.String(name),
		ReleaseLabel: aws.String("emr-6.10.0"),
		ServiceRole:  aws.String("EMR_DefaultRole"),
		JobFlowRole:  aws.String("EMR_EC2_DefaultRole"),
		LogUri:       aws.String("s3://my-bucket/logs/"),
		Instances: &types.JobFlowInstancesConfig{
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
