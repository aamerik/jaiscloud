package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemrc "github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Virtual Clusters ─────────────────────────────────────────────────────────

func TestEMRC_CreateDescribeDeleteVirtualCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	out, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
		Name: aws.String("my-vc"),
		ContainerProvider: &types.ContainerProvider{
			Type: types.ContainerProviderTypeEks,
			Id:   aws.String("my-eks-cluster"),
			Info: &types.ContainerInfoMemberEksInfo{
				Value: types.EksInfo{Namespace: aws.String("emr")},
			},
		},
	})
	require.NoError(t, err)
	vcID := aws.ToString(out.Id)
	assert.NotEmpty(t, vcID)
	assert.Equal(t, "my-vc", aws.ToString(out.Name))
	assert.Contains(t, aws.ToString(out.Arn), "virtualclusters/")

	desc, err := c.DescribeVirtualCluster(ctx, &awsemrc.DescribeVirtualClusterInput{
		Id: aws.String(vcID),
	})
	require.NoError(t, err)
	assert.Equal(t, vcID, aws.ToString(desc.VirtualCluster.Id))
	assert.Equal(t, "my-vc", aws.ToString(desc.VirtualCluster.Name))
	assert.Equal(t, types.VirtualClusterStateRunning, desc.VirtualCluster.State)

	_, err = c.DeleteVirtualCluster(ctx, &awsemrc.DeleteVirtualClusterInput{
		Id: aws.String(vcID),
	})
	require.NoError(t, err)
}

func TestEMRC_ListVirtualClusters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	for _, name := range []string{"vc-1", "vc-2", "vc-3"} {
		_, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
			Name: aws.String(name),
			ContainerProvider: &types.ContainerProvider{
				Type: types.ContainerProviderTypeEks,
				Id:   aws.String("eks-cluster"),
			},
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListVirtualClusters(ctx, &awsemrc.ListVirtualClustersInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.VirtualClusters, 3)
}

// ─── Job Runs ─────────────────────────────────────────────────────────────────

func TestEMRC_StartDescribeJobRun(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	vcOut, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
		Name: aws.String("job-vc"),
		ContainerProvider: &types.ContainerProvider{
			Type: types.ContainerProviderTypeEks,
			Id:   aws.String("eks-cluster"),
		},
	})
	require.NoError(t, err)
	vcID := aws.ToString(vcOut.Id)

	runOut, err := c.StartJobRun(ctx, &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("my-job"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		JobDriver: &types.JobDriver{
			SparkSubmitJobDriver: &types.SparkSubmitJobDriver{
				EntryPoint: aws.String("s3://my-bucket/app.py"),
			},
		},
	})
	require.NoError(t, err)
	jobID := aws.ToString(runOut.Id)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, vcID, aws.ToString(runOut.VirtualClusterId))
	assert.Contains(t, aws.ToString(runOut.Arn), "jobruns/")

	descOut, err := c.DescribeJobRun(ctx, &awsemrc.DescribeJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Id:               aws.String(jobID),
	})
	require.NoError(t, err)
	assert.Equal(t, jobID, aws.ToString(descOut.JobRun.Id))
	assert.Equal(t, "my-job", aws.ToString(descOut.JobRun.Name))
}

// TestEMRC_CancelJobRun_TerminalStateRejected verifies that attempting to cancel
// an already-terminal job returns ValidationException (real-AWS parity).
// In mock mode (no k8s) StartJobRun immediately completes the job.
func TestEMRC_CancelJobRun_TerminalStateRejected(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	vcOut, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
		Name: aws.String("cancel-vc"),
		ContainerProvider: &types.ContainerProvider{
			Type: types.ContainerProviderTypeEks,
			Id:   aws.String("eks-cluster"),
		},
	})
	require.NoError(t, err)
	vcID := aws.ToString(vcOut.Id)

	runOut, err := c.StartJobRun(ctx, &awsemrc.StartJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("instant-complete-job"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRContainersRole"),
		JobDriver: &types.JobDriver{
			SparkSubmitJobDriver: &types.SparkSubmitJobDriver{
				EntryPoint: aws.String("s3://my-bucket/app.py"),
			},
		},
	})
	require.NoError(t, err)
	jobID := aws.ToString(runOut.Id)

	// In mock mode the job immediately reaches COMPLETED — cancelling it must
	// return ValidationException, matching real EMR on EKS behavior.
	_, cancelErr := c.CancelJobRun(ctx, &awsemrc.CancelJobRunInput{
		VirtualClusterId: aws.String(vcID),
		Id:               aws.String(jobID),
	})
	require.Error(t, cancelErr)
	assert.Contains(t, cancelErr.Error(), "ValidationException")
}

func TestEMRC_ListJobRuns(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	vcOut, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
		Name: aws.String("list-jobs-vc"),
		ContainerProvider: &types.ContainerProvider{
			Type: types.ContainerProviderTypeEks,
			Id:   aws.String("eks-cluster"),
		},
	})
	require.NoError(t, err)
	vcID := aws.ToString(vcOut.Id)

	for i := 0; i < 3; i++ {
		_, err := c.StartJobRun(ctx, &awsemrc.StartJobRunInput{
			VirtualClusterId: aws.String(vcID),
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRRole"),
			ReleaseLabel:     aws.String("emr-6.10.0-latest"),
			JobDriver: &types.JobDriver{
				SparkSubmitJobDriver: &types.SparkSubmitJobDriver{
					EntryPoint: aws.String("s3://bucket/app.py"),
				},
			},
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListJobRuns(ctx, &awsemrc.ListJobRunsInput{
		VirtualClusterId: aws.String(vcID),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.JobRuns, 3)
}

// ─── Managed Endpoints ────────────────────────────────────────────────────────

func TestEMRC_CreateDescribeDeleteManagedEndpoint(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	vcOut, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
		Name: aws.String("ep-vc"),
		ContainerProvider: &types.ContainerProvider{
			Type: types.ContainerProviderTypeEks,
			Id:   aws.String("eks-cluster"),
		},
	})
	require.NoError(t, err)
	vcID := aws.ToString(vcOut.Id)

	epOut, err := c.CreateManagedEndpoint(ctx, &awsemrc.CreateManagedEndpointInput{
		VirtualClusterId: aws.String(vcID),
		Name:             aws.String("my-endpoint"),
		Type:             aws.String("JUPYTER_ENTERPRISE_GATEWAY"),
		ReleaseLabel:     aws.String("emr-6.10.0-latest"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRRole"),
	})
	require.NoError(t, err)
	epID := aws.ToString(epOut.Id)
	assert.NotEmpty(t, epID)
	assert.Contains(t, aws.ToString(epOut.Arn), "endpoints/")

	descOut, err := c.DescribeManagedEndpoint(ctx, &awsemrc.DescribeManagedEndpointInput{
		VirtualClusterId: aws.String(vcID),
		Id:               aws.String(epID),
	})
	require.NoError(t, err)
	assert.Equal(t, epID, aws.ToString(descOut.Endpoint.Id))
	assert.Equal(t, "my-endpoint", aws.ToString(descOut.Endpoint.Name))
	assert.Equal(t, types.EndpointStateActive, descOut.Endpoint.State)

	_, err = c.DeleteManagedEndpoint(ctx, &awsemrc.DeleteManagedEndpointInput{
		VirtualClusterId: aws.String(vcID),
		Id:               aws.String(epID),
	})
	require.NoError(t, err)
}

func TestEMRC_ListManagedEndpoints(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRContainersClient(t)

	vcOut, err := c.CreateVirtualCluster(ctx, &awsemrc.CreateVirtualClusterInput{
		Name: aws.String("list-ep-vc"),
		ContainerProvider: &types.ContainerProvider{
			Type: types.ContainerProviderTypeEks,
			Id:   aws.String("eks-cluster"),
		},
	})
	require.NoError(t, err)
	vcID := aws.ToString(vcOut.Id)

	for _, name := range []string{"ep-a", "ep-b"} {
		_, err := c.CreateManagedEndpoint(ctx, &awsemrc.CreateManagedEndpointInput{
			VirtualClusterId: aws.String(vcID),
			Name:             aws.String(name),
			Type:             aws.String("JUPYTER_ENTERPRISE_GATEWAY"),
			ReleaseLabel:     aws.String("emr-6.10.0-latest"),
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/EMRRole"),
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListManagedEndpoints(ctx, &awsemrc.ListManagedEndpointsInput{
		VirtualClusterId: aws.String(vcID),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Endpoints, 2)
}
