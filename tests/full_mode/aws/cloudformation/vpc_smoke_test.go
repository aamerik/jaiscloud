//go:build cfn_fullmode

package cloudformation_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const vpcTemplate = `
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  MyVPC:
    Type: AWS::EC2::VPC
    Properties:
      CidrBlock: 10.0.0.0/16
      EnableDnsSupport: true
  PublicSubnet1:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref MyVPC
      CidrBlock: 10.0.1.0/24
      AvailabilityZone: us-east-1a
  PublicSubnet2:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref MyVPC
      CidrBlock: 10.0.2.0/24
      AvailabilityZone: us-east-1b
  IGW:
    Type: AWS::EC2::InternetGateway
  RouteTable:
    Type: AWS::EC2::RouteTable
    Properties:
      VpcId: !Ref MyVPC
  Route:
    Type: AWS::EC2::Route
    Properties:
      RouteTableId: !Ref RouteTable
      DestinationCidrBlock: 0.0.0.0/0
      GatewayId: !Ref IGW
  SubnetAssoc1:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties:
      SubnetId: !Ref PublicSubnet1
      RouteTableId: !Ref RouteTable
Outputs:
  VpcId:
    Value: !Ref MyVPC
  Subnet1:
    Value: !Ref PublicSubnet1
`

// TestCFNVPCSmokeTest deploys a VPC stack with 2 public subnets (172.31.0.0/24,
// 172.31.1.0/24), an InternetGateway, RouteTable, Route, and
// SubnetRouteTableAssociation. Polls until CREATE_COMPLETE, asserts VPC and
// subnet outputs are present, then deletes the stack and polls until
// DELETE_COMPLETE.
func TestCFNVPCSmokeTest(t *testing.T) {
	TestCFN_VPCSmoke(t)
}

// TestCFN_VPCSmoke deploys a VPC with subnets, IGW, route table and route via
// CloudFormation and verifies the stack reaches CREATE_COMPLETE and exposes the
// expected outputs. Then it deletes the stack and polls until DELETE_COMPLETE.
func TestCFN_VPCSmoke(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cfClient := newCFClient(t)

	stackName := "vpc-smoke-test"

	// CreateStack
	_, err := cfClient.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(vpcTemplate),
	})
	require.NoError(t, err, "CreateStack should not return an error")

	// Poll until terminal
	status := pollStackStatus(t, cfClient, stackName)
	require.Equal(t, "CREATE_COMPLETE", status, "expected stack to reach CREATE_COMPLETE, got %s", status)

	// DescribeStacks — verify outputs contain VpcId
	descOut, err := cfClient.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)

	outputs := descOut.Stacks[0].Outputs
	vpcIDOutput := ""
	subnet1Output := ""
	for _, o := range outputs {
		switch aws.ToString(o.OutputKey) {
		case "VpcId":
			vpcIDOutput = aws.ToString(o.OutputValue)
		case "Subnet1":
			subnet1Output = aws.ToString(o.OutputValue)
		}
	}
	assert.NotEmpty(t, vpcIDOutput, "VpcId output must be present")
	assert.NotEmpty(t, subnet1Output, "Subnet1 output must be present")
	t.Logf("VpcId=%s Subnet1=%s", vpcIDOutput, subnet1Output)

	// DescribeStackResources — verify at least the VPC resource is listed
	resOut, err := cfClient.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	resourceTypes := make([]string, 0, len(resOut.StackResources))
	for _, r := range resOut.StackResources {
		resourceTypes = append(resourceTypes, aws.ToString(r.ResourceType))
	}
	t.Logf("stack resources: %s", strings.Join(resourceTypes, ", "))

	hasVPC := false
	for _, rt := range resourceTypes {
		if rt == "AWS::EC2::VPC" {
			hasVPC = true
			break
		}
	}
	assert.True(t, hasVPC, fmt.Sprintf("expected AWS::EC2::VPC in stack resources, got: %v", resourceTypes))

	// DeleteStack
	_, err = cfClient.DeleteStack(ctx, &awscf.DeleteStackInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	deleteStatus := pollStackStatus(t, cfClient, stackName)
	assert.Equal(t, "DELETE_COMPLETE", deleteStatus,
		"expected stack to reach DELETE_COMPLETE, got %s", deleteStatus)
}
