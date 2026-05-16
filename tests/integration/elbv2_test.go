package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	awselbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestELBv2CreateAndDescribeLoadBalancer(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newELBv2Client(t)

	// Create a load balancer
	createOut, err := client.CreateLoadBalancer(ctx, &awselbv2.CreateLoadBalancerInput{
		Name:   aws.String("my-test-alb"),
		Scheme: elbv2types.LoadBalancerSchemeEnumInternetFacing,
		Type:   elbv2types.LoadBalancerTypeEnumApplication,
	})
	require.NoError(t, err)
	require.Len(t, createOut.LoadBalancers, 1)
	lb := createOut.LoadBalancers[0]
	assert.Equal(t, "my-test-alb", aws.ToString(lb.LoadBalancerName))
	assert.Equal(t, elbv2types.LoadBalancerStateEnumActive, lb.State.Code)
	assert.NotEmpty(t, aws.ToString(lb.LoadBalancerArn))
	assert.Contains(t, aws.ToString(lb.DNSName), "my-test-alb")

	lbArn := lb.LoadBalancerArn

	// Describe all load balancers
	descOut, err := client.DescribeLoadBalancers(ctx, &awselbv2.DescribeLoadBalancersInput{})
	require.NoError(t, err)
	require.Len(t, descOut.LoadBalancers, 1)
	assert.Equal(t, "my-test-alb", aws.ToString(descOut.LoadBalancers[0].LoadBalancerName))

	// Describe by ARN
	descByArn, err := client.DescribeLoadBalancers(ctx, &awselbv2.DescribeLoadBalancersInput{
		LoadBalancerArns: []string{aws.ToString(lbArn)},
	})
	require.NoError(t, err)
	require.Len(t, descByArn.LoadBalancers, 1)
	assert.Equal(t, "my-test-alb", aws.ToString(descByArn.LoadBalancers[0].LoadBalancerName))

	// Describe by name
	descByName, err := client.DescribeLoadBalancers(ctx, &awselbv2.DescribeLoadBalancersInput{
		Names: []string{"my-test-alb"},
	})
	require.NoError(t, err)
	require.Len(t, descByName.LoadBalancers, 1)

	// Delete the load balancer
	_, err = client.DeleteLoadBalancer(ctx, &awselbv2.DeleteLoadBalancerInput{
		LoadBalancerArn: lbArn,
	})
	require.NoError(t, err)

	// Confirm deleted
	descAfterDelete, err := client.DescribeLoadBalancers(ctx, &awselbv2.DescribeLoadBalancersInput{})
	require.NoError(t, err)
	assert.Len(t, descAfterDelete.LoadBalancers, 0)
}

func TestELBv2CreateTargetGroupAndRegisterTargets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newELBv2Client(t)

	// Create a target group
	tgOut, err := client.CreateTargetGroup(ctx, &awselbv2.CreateTargetGroupInput{
		Name:       aws.String("my-target-group"),
		Protocol:   elbv2types.ProtocolEnumHttp,
		Port:       aws.Int32(80),
		VpcId:      aws.String("vpc-12345678"),
		TargetType: elbv2types.TargetTypeEnumInstance,
	})
	require.NoError(t, err)
	require.Len(t, tgOut.TargetGroups, 1)
	tg := tgOut.TargetGroups[0]
	assert.Equal(t, "my-target-group", aws.ToString(tg.TargetGroupName))
	assert.NotEmpty(t, aws.ToString(tg.TargetGroupArn))

	tgArn := tg.TargetGroupArn

	// Register targets
	_, err = client.RegisterTargets(ctx, &awselbv2.RegisterTargetsInput{
		TargetGroupArn: tgArn,
		Targets: []elbv2types.TargetDescription{
			{Id: aws.String("i-1234567890abcdef0")},
			{Id: aws.String("i-abcdef1234567890")},
		},
	})
	require.NoError(t, err)

	// Describe target health — should be healthy
	healthOut, err := client.DescribeTargetHealth(ctx, &awselbv2.DescribeTargetHealthInput{
		TargetGroupArn: tgArn,
	})
	require.NoError(t, err)
	assert.Len(t, healthOut.TargetHealthDescriptions, 2)
	for _, th := range healthOut.TargetHealthDescriptions {
		assert.Equal(t, elbv2types.TargetHealthStateEnumHealthy, th.TargetHealth.State)
	}

	// Deregister one target
	_, err = client.DeregisterTargets(ctx, &awselbv2.DeregisterTargetsInput{
		TargetGroupArn: tgArn,
		Targets: []elbv2types.TargetDescription{
			{Id: aws.String("i-1234567890abcdef0")},
		},
	})
	require.NoError(t, err)

	// Now only 1 target
	healthOut2, err := client.DescribeTargetHealth(ctx, &awselbv2.DescribeTargetHealthInput{
		TargetGroupArn: tgArn,
	})
	require.NoError(t, err)
	assert.Len(t, healthOut2.TargetHealthDescriptions, 1)

	// Describe target groups
	tgDescOut, err := client.DescribeTargetGroups(ctx, &awselbv2.DescribeTargetGroupsInput{})
	require.NoError(t, err)
	require.Len(t, tgDescOut.TargetGroups, 1)
}

func TestELBv2CreateListener(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newELBv2Client(t)

	// Create a load balancer
	lbOut, err := client.CreateLoadBalancer(ctx, &awselbv2.CreateLoadBalancerInput{
		Name:   aws.String("listener-test-alb"),
		Scheme: elbv2types.LoadBalancerSchemeEnumInternetFacing,
		Type:   elbv2types.LoadBalancerTypeEnumApplication,
	})
	require.NoError(t, err)
	lbArn := lbOut.LoadBalancers[0].LoadBalancerArn

	// Create a target group
	tgOut, err := client.CreateTargetGroup(ctx, &awselbv2.CreateTargetGroupInput{
		Name:       aws.String("listener-tg"),
		Protocol:   elbv2types.ProtocolEnumHttp,
		Port:       aws.Int32(80),
		VpcId:      aws.String("vpc-12345678"),
		TargetType: elbv2types.TargetTypeEnumInstance,
	})
	require.NoError(t, err)
	tgArn := tgOut.TargetGroups[0].TargetGroupArn

	// Create a listener
	listenerOut, err := client.CreateListener(ctx, &awselbv2.CreateListenerInput{
		LoadBalancerArn: lbArn,
		Protocol:        elbv2types.ProtocolEnumHttp,
		Port:            aws.Int32(80),
		DefaultActions: []elbv2types.Action{
			{
				Type:           elbv2types.ActionTypeEnumForward,
				TargetGroupArn: tgArn,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, listenerOut.Listeners, 1)
	listener := listenerOut.Listeners[0]
	assert.Equal(t, aws.ToString(lbArn), aws.ToString(listener.LoadBalancerArn))
	assert.NotEmpty(t, aws.ToString(listener.ListenerArn))

	listenerArn := listener.ListenerArn

	// Describe listeners for this LB
	descOut, err := client.DescribeListeners(ctx, &awselbv2.DescribeListenersInput{
		LoadBalancerArn: lbArn,
	})
	require.NoError(t, err)
	require.Len(t, descOut.Listeners, 1)
	assert.Equal(t, aws.ToString(listenerArn), aws.ToString(descOut.Listeners[0].ListenerArn))

	// Delete listener
	_, err = client.DeleteListener(ctx, &awselbv2.DeleteListenerInput{
		ListenerArn: listenerArn,
	})
	require.NoError(t, err)

	// Confirm deleted
	descAfter, err := client.DescribeListeners(ctx, &awselbv2.DescribeListenersInput{
		LoadBalancerArn: lbArn,
	})
	require.NoError(t, err)
	assert.Len(t, descAfter.Listeners, 0)
}
