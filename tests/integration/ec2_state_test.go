package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForInstanceState polls DescribeInstances until the given instance reaches
// wantState or the timeout expires.
func waitForInstanceState(t *testing.T, client *awsec2.Client, instanceID, wantState string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.DescribeInstances(context.Background(), &awsec2.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		require.NoError(t, err)
		if len(out.Reservations) > 0 && len(out.Reservations[0].Instances) > 0 {
			state := string(out.Reservations[0].Instances[0].State.Name)
			if state == wantState {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("instance %s did not reach state %s within %s", instanceID, wantState, timeout)
}

// TestEC2_InstanceStartsRunning runs a single instance and waits for it to reach
// the running state within 10 seconds.
func TestEC2_InstanceStartsRunning(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)
	require.NotEmpty(t, instanceID)

	// Immediately after RunInstances the instance should be in pending state.
	assert.Equal(t, types.InstanceStateNamePending, runOut.Instances[0].State.Name,
		"instance should start in pending state")

	// Wait up to 10s for the transition to running.
	waitForInstanceState(t, client, instanceID, "running", 10*time.Second)

	// Confirm the state is running via DescribeInstances.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)
	assert.Equal(t, types.InstanceStateNameRunning, descOut.Reservations[0].Instances[0].State.Name,
		"instance should be running after transition")
}

// TestEC2_StopInstance_Running runs an instance, waits for it to be running, then
// stops it and asserts the state is no longer running.
func TestEC2_StopInstance_Running(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	waitForInstanceState(t, client, instanceID, "running", 10*time.Second)

	_, err = client.StopInstances(ctx, &awsec2.StopInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)

	// The instance must leave the running state (stopping or stopped are both acceptable).
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)
	state := string(descOut.Reservations[0].Instances[0].State.Name)
	assert.NotEqual(t, "running", state,
		"instance should not be running immediately after StopInstances")
}

// TestEC2_StartStoppedInstance runs an instance, stops it, waits for stopped,
// then starts it again and asserts it returns to running.
func TestEC2_StartStoppedInstance(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	// Wait for running then stop.
	waitForInstanceState(t, client, instanceID, "running", 10*time.Second)

	_, err = client.StopInstances(ctx, &awsec2.StopInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)

	// Wait for stopped.
	waitForInstanceState(t, client, instanceID, "stopped", 15*time.Second)

	// Start the stopped instance.
	_, err = client.StartInstances(ctx, &awsec2.StartInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)

	// Instance must reach running again.
	waitForInstanceState(t, client, instanceID, "running", 15*time.Second)

	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)
	assert.Equal(t, types.InstanceStateNameRunning, descOut.Reservations[0].Instances[0].State.Name,
		"instance should be running after StartInstances")
}

// TestEC2_TerminateRunningInstance runs an instance, waits for running, then
// terminates it and asserts the state is shutting-down or terminated.
func TestEC2_TerminateRunningInstance(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	waitForInstanceState(t, client, instanceID, "running", 10*time.Second)

	termOut, err := client.TerminateInstances(ctx, &awsec2.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, termOut.TerminatingInstances, 1)

	// The state immediately after TerminateInstances must be shutting-down or terminated.
	state := string(termOut.TerminatingInstances[0].CurrentState.Name)
	validStates := map[string]bool{"shutting-down": true, "terminated": true}
	assert.True(t, validStates[state],
		"state after TerminateInstances must be shutting-down or terminated, got: %s", state)
}

// TestEC2_DescribeInstances_FilterByState runs two instances, waits for both to be
// running, terminates one, and then filters by instance-state-name=running to assert
// only one instance is returned.
func TestEC2_DescribeInstances_FilterByState(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	run1, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, run1.Instances, 1)
	id1 := aws.ToString(run1.Instances[0].InstanceId)

	run2, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, run2.Instances, 1)
	id2 := aws.ToString(run2.Instances[0].InstanceId)

	// Wait for both instances to be running.
	waitForInstanceState(t, client, id1, "running", 10*time.Second)
	waitForInstanceState(t, client, id2, "running", 10*time.Second)

	// Terminate the first instance.
	_, err = client.TerminateInstances(ctx, &awsec2.TerminateInstancesInput{
		InstanceIds: []string{id1},
	})
	require.NoError(t, err)

	// Filter by running — only id2 should appear.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("instance-state-name"), Values: []string{"running"}},
		},
	})
	require.NoError(t, err)

	runningIDs := make([]string, 0)
	for _, r := range descOut.Reservations {
		for _, inst := range r.Instances {
			runningIDs = append(runningIDs, aws.ToString(inst.InstanceId))
		}
	}
	assert.Equal(t, 1, len(runningIDs),
		"only the non-terminated instance should appear when filtering by running")
	if len(runningIDs) == 1 {
		assert.Equal(t, id2, runningIDs[0], "the running instance should be id2")
	}
}

// TestEC2_DescribeInstances_FilterByInstanceId runs two instances and uses the
// InstanceIds parameter to fetch only the first one.
func TestEC2_DescribeInstances_FilterByInstanceId(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	run1, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, run1.Instances, 1)
	id1 := aws.ToString(run1.Instances[0].InstanceId)

	_, err = client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)

	// Describe using InstanceIds — must return only id1.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{id1},
	})
	require.NoError(t, err)

	total := 0
	for _, r := range descOut.Reservations {
		total += len(r.Instances)
	}
	assert.Equal(t, 1, total, "only the requested instance should be returned")
	if total == 1 {
		assert.Equal(t, id1, aws.ToString(descOut.Reservations[0].Instances[0].InstanceId))
	}
}

// TestEC2_RebootInstance_StateStaysRunning runs an instance, waits for running,
// reboots it, and confirms it is still running.
func TestEC2_RebootInstance_StateStaysRunning(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	waitForInstanceState(t, client, instanceID, "running", 10*time.Second)

	_, err = client.RebootInstances(ctx, &awsec2.RebootInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)

	// After reboot the instance must remain running.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)
	assert.Equal(t, types.InstanceStateNameRunning, descOut.Reservations[0].Instances[0].State.Name,
		"instance must stay running after reboot")
}

// TestEC2_TerminateMultipleInstances runs 3 instances then terminates all 3 at once
// and asserts each is in shutting-down or terminated state.
func TestEC2_TerminateMultipleInstances(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(3),
		MaxCount:     aws.Int32(3),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 3, "RunInstances with MaxCount=3 must return 3 instances")

	ids := make([]string, 0, 3)
	for _, inst := range runOut.Instances {
		ids = append(ids, aws.ToString(inst.InstanceId))
	}

	// Wait for all three to reach running state before terminating.
	for _, id := range ids {
		waitForInstanceState(t, client, id, "running", 10*time.Second)
	}

	termOut, err := client.TerminateInstances(ctx, &awsec2.TerminateInstancesInput{
		InstanceIds: ids,
	})
	require.NoError(t, err)
	require.Len(t, termOut.TerminatingInstances, 3,
		"TerminateInstances must report all 3 instances")

	validStates := map[string]bool{"shutting-down": true, "terminated": true}
	for _, ti := range termOut.TerminatingInstances {
		state := string(ti.CurrentState.Name)
		assert.True(t, validStates[state],
			"instance %s: expected shutting-down or terminated, got %s",
			aws.ToString(ti.InstanceId), state)
	}
}
