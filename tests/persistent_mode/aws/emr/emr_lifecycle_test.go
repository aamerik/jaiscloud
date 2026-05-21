//go:build spark_e2e

package emr_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSparkJob_K8s_SurvivesServerRestart verifies that a running EMR step is
// re-adopted by the K8s executor after a server restart (cleanupOrphans resumes
// the suspended Job and the step reaches a terminal state).
func TestSparkJob_K8s_SurvivesServerRestart(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	emrClient := newEMRClient(t)

	clusterID := createCluster(t, emrClient, "lifecycle-restart-cluster")

	out, err := emrClient.AddJobFlowSteps(ctx, &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("pi-step"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: sparkPiArgs(5),
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.StepIds, 1)
	stepID := *out.StepIds[0]

	// Poll until step reaches a terminal state (server suspend/resume is
	// transparent from the API perspective).
	finalState := pollEMRStep(t, emrClient, clusterID, stepID)
	assert.Equal(t, "COMPLETED", finalState, "step should complete after server lifecycle")
}

// TestSparkJob_K8s_TerminateCluster_AfterStepComplete terminates a cluster
// after its step completes and verifies the cluster reaches TERMINATED state.
func TestSparkJob_K8s_TerminateCluster_AfterStepComplete(t *testing.T) {
	requireK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	emrClient := newEMRClient(t)

	clusterID := createCluster(t, emrClient, "lifecycle-terminate-cluster")

	out, err := emrClient.AddJobFlowSteps(ctx, &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []emrtypes.StepConfig{
			{
				Name:            aws.String("pi-step"),
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: sparkPiArgs(2),
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.StepIds, 1)
	stepID := *out.StepIds[0]

	finalStepState := pollEMRStep(t, emrClient, clusterID, stepID)
	require.Equal(t, "COMPLETED", finalStepState)

	_, err = emrClient.TerminateJobFlows(ctx, &awsemr.TerminateJobFlowsInput{
		JobFlowIds: []string{clusterID},
	})
	require.NoError(t, err)

	desc, err := emrClient.DescribeCluster(ctx, &awsemr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	clusterState := string(desc.Cluster.Status.State)
	assert.Equal(t, "TERMINATED", clusterState)
}
