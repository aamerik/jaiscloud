package integration_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// cfnCreateStack creates a stack and asserts it reaches CREATE_COMPLETE.
func cfnCreateStack(t *testing.T, client *awscf.Client, name, tpl string, params ...cftypes.Parameter) {
	t.Helper()
	ctx := context.Background()
	_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String(name),
		TemplateBody: aws.String(tpl),
		Parameters:   params,
		Capabilities: []cftypes.Capability{
			cftypes.CapabilityCapabilityIam,
			cftypes.CapabilityCapabilityNamedIam,
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String(name),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	assert.Equal(t, cftypes.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)
}

// cfnStackOutputs returns a map of OutputKey → OutputValue for a named stack.
func cfnStackOutputs(t *testing.T, client *awscf.Client, name string) map[string]string {
	t.Helper()
	ctx := context.Background()
	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String(name),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	out := make(map[string]string)
	for _, o := range descOut.Stacks[0].Outputs {
		out[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}
	return out
}

// cfnResourceTypes returns a set of ResourceType strings for all resources in a stack.
func cfnResourceTypes(t *testing.T, client *awscf.Client, name string) map[string]bool {
	t.Helper()
	ctx := context.Background()
	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String(name),
	})
	require.NoError(t, err)
	m := make(map[string]bool)
	for _, r := range resOut.StackResources {
		m[aws.ToString(r.ResourceType)] = true
	}
	return m
}

// cfnResourcePhysicalIDs returns a map of LogicalId → PhysicalId for all resources.
func cfnResourcePhysicalIDs(t *testing.T, client *awscf.Client, name string) map[string]string {
	t.Helper()
	ctx := context.Background()
	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String(name),
	})
	require.NoError(t, err)
	m := make(map[string]string)
	for _, r := range resOut.StackResources {
		m[aws.ToString(r.LogicalResourceId)] = aws.ToString(r.PhysicalResourceId)
	}
	return m
}

// ─── Intrinsic function tests ─────────────────────────────────────────────────

func TestCFN_FnJoin(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Joined": {
      "Value": {"Fn::Join": ["-", ["a", "b", "c"]]}
    }
  }
}`
	cfnCreateStack(t, client, "join-stack", tpl)
	outputs := cfnStackOutputs(t, client, "join-stack")
	assert.Equal(t, "a-b-c", outputs["Joined"])
}

func TestCFN_FnSplit(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "SecondElement": {
      "Value": {"Fn::Select": [1, {"Fn::Split": [",", "a,b,c"]}]}
    }
  }
}`
	cfnCreateStack(t, client, "split-stack", tpl)
	outputs := cfnStackOutputs(t, client, "split-stack")
	assert.Equal(t, "b", outputs["SecondElement"])
}

func TestCFN_FnSelect(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "ThirdElement": {
      "Value": {"Fn::Select": [2, ["x", "y", "z"]]}
    }
  }
}`
	cfnCreateStack(t, client, "select-stack", tpl)
	outputs := cfnStackOutputs(t, client, "select-stack")
	assert.Equal(t, "z", outputs["ThirdElement"])
}

func TestCFN_FnBase64(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Encoded": {
      "Value": {"Fn::Base64": "hello"}
    }
  }
}`
	cfnCreateStack(t, client, "b64-stack", tpl)
	outputs := cfnStackOutputs(t, client, "b64-stack")
	expected := base64.StdEncoding.EncodeToString([]byte("hello"))
	assert.Equal(t, expected, outputs["Encoded"])
}

func TestCFN_FnFindInMap(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Mappings": {
    "RegionMap": {
      "us-east-1": {
        "AMI": "ami-12345678"
      },
      "us-west-2": {
        "AMI": "ami-87654321"
      }
    }
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "AMIValue": {
      "Value": {"Fn::FindInMap": ["RegionMap", "us-east-1", "AMI"]}
    }
  }
}`
	cfnCreateStack(t, client, "findinmap-stack", tpl)
	outputs := cfnStackOutputs(t, client, "findinmap-stack")
	assert.Equal(t, "ami-12345678", outputs["AMIValue"])
}

func TestCFN_FnIf_ConditionTrue(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "Env": {"Type": "String"}
  },
  "Conditions": {
    "Prod": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "EnvLabel": {
      "Value": {"Fn::If": ["Prod", "production", "development"]}
    }
  }
}`
	cfnCreateStack(t, client, "if-true-stack", tpl,
		cftypes.Parameter{ParameterKey: aws.String("Env"), ParameterValue: aws.String("prod")},
	)
	outputs := cfnStackOutputs(t, client, "if-true-stack")
	assert.Equal(t, "production", outputs["EnvLabel"])
}

func TestCFN_FnIf_ConditionFalse(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "Env": {"Type": "String"}
  },
  "Conditions": {
    "Prod": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "EnvLabel": {
      "Value": {"Fn::If": ["Prod", "production", "development"]}
    }
  }
}`
	cfnCreateStack(t, client, "if-false-stack", tpl,
		cftypes.Parameter{ParameterKey: aws.String("Env"), ParameterValue: aws.String("dev")},
	)
	outputs := cfnStackOutputs(t, client, "if-false-stack")
	assert.Equal(t, "development", outputs["EnvLabel"])
}

func TestCFN_FnGetAtt_SQSQueue(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "getatt-test-queue"}
    }
  },
  "Outputs": {
    "QueueArn": {
      "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]}
    }
  }
}`
	cfnCreateStack(t, client, "getatt-sqs-stack", tpl)
	outputs := cfnStackOutputs(t, client, "getatt-sqs-stack")
	assert.NotEmpty(t, outputs["QueueArn"])
	assert.Contains(t, outputs["QueueArn"], "arn:aws:sqs")
	assert.Contains(t, outputs["QueueArn"], "getatt-test-queue")
}

func TestCFN_FnGetAtt_S3Bucket(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {"BucketName": "getatt-test-bucket"}
    }
  },
  "Outputs": {
    "BucketArn": {
      "Value": {"Fn::GetAtt": ["MyBucket", "Arn"]}
    }
  }
}`
	cfnCreateStack(t, client, "getatt-s3-stack", tpl)
	outputs := cfnStackOutputs(t, client, "getatt-s3-stack")
	assert.NotEmpty(t, outputs["BucketArn"])
	assert.Contains(t, outputs["BucketArn"], "arn:aws:s3")
}

func TestCFN_FnGetAtt_DynamoDBTable(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyTable": {
      "Type": "AWS::DynamoDB::Table",
      "Properties": {
        "TableName": "getatt-test-table",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST"
      }
    }
  },
  "Outputs": {
    "TableArn": {
      "Value": {"Fn::GetAtt": ["MyTable", "Arn"]}
    }
  }
}`
	cfnCreateStack(t, client, "getatt-ddb-stack", tpl)
	outputs := cfnStackOutputs(t, client, "getatt-ddb-stack")
	assert.NotEmpty(t, outputs["TableArn"])
	assert.Contains(t, outputs["TableArn"], "arn:aws:dynamodb")
}

func TestCFN_FnSub_ResourceRef(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "sub-ref-queue"}
    }
  },
  "Outputs": {
    "ComposedArn": {
      "Value": {"Fn::Sub": "arn:aws:sqs:${AWS::Region}:${AWS::AccountId}:${MyQueue}"}
    }
  }
}`
	cfnCreateStack(t, client, "sub-ref-stack", tpl)
	outputs := cfnStackOutputs(t, client, "sub-ref-stack")
	assert.NotEmpty(t, outputs["ComposedArn"])
	assert.Contains(t, outputs["ComposedArn"], "us-east-1")
	assert.Contains(t, outputs["ComposedArn"], "000000000000")
}

func TestCFN_FnLength(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	// CommaDelimitedList parameter is split into a list by CFN
	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "MyList": {"Type": "CommaDelimitedList"}
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "ListLength": {
      "Value": {"Fn::Length": [{"Ref": "MyList"}]}
    }
  }
}`
	cfnCreateStack(t, client, "length-stack", tpl,
		cftypes.Parameter{ParameterKey: aws.String("MyList"), ParameterValue: aws.String("x,y,z")},
	)
	outputs := cfnStackOutputs(t, client, "length-stack")
	// The output should be some non-empty numeric representation of the list length
	assert.NotEmpty(t, outputs["ListLength"])
}

// ─── Multi-resource dependency tests ─────────────────────────────────────────

func TestCFN_SQS_SNS_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyTopic": {
      "Type": "AWS::SNS::Topic",
      "Properties": {"TopicName": "multi-topic"}
    },
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "multi-queue"}
    },
    "MySubscription": {
      "Type": "AWS::SNS::Subscription",
      "Properties": {
        "TopicArn": {"Ref": "MyTopic"},
        "Protocol": "sqs",
        "Endpoint": {"Fn::GetAtt": ["MyQueue", "Arn"]}
      }
    }
  }
}`
	cfnCreateStack(t, client, "sns-sqs-stack", tpl)
	rtypes := cfnResourceTypes(t, client, "sns-sqs-stack")
	assert.True(t, rtypes["AWS::SNS::Topic"])
	assert.True(t, rtypes["AWS::SQS::Queue"])
	assert.True(t, rtypes["AWS::SNS::Subscription"])
}

func TestCFN_DynamoDB_Lambda_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyTable": {
      "Type": "AWS::DynamoDB::Table",
      "Properties": {
        "TableName": "ddb-lambda-table",
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST"
      }
    },
    "MyRole": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "ddb-lambda-role",
        "AssumeRolePolicyDocument": {
          "Version": "2012-10-17",
          "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
        }
      }
    },
    "MyFunction": {
      "Type": "AWS::Lambda::Function",
      "DependsOn": "MyRole",
      "Properties": {
        "FunctionName": "ddb-lambda-fn",
        "Runtime": "python3.12",
        "Handler": "index.handler",
        "Role": {"Fn::GetAtt": ["MyRole", "Arn"]},
        "Code": {
          "ZipFile": "def handler(event, context):\n    return event\n"
        }
      }
    }
  }
}`
	cfnCreateStack(t, client, "ddb-lambda-stack", tpl)
	rtypes := cfnResourceTypes(t, client, "ddb-lambda-stack")
	assert.True(t, rtypes["AWS::DynamoDB::Table"])
	assert.True(t, rtypes["AWS::Lambda::Function"])
}

func TestCFN_IAM_Role_Policy_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyRole": {
      "Type": "AWS::IAM::Role",
      "Properties": {
        "RoleName": "cfn-test-role",
        "AssumeRolePolicyDocument": {
          "Version": "2012-10-17",
          "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]
        }
      }
    },
    "MyPolicy": {
      "Type": "AWS::IAM::Policy",
      "Properties": {
        "PolicyName": "cfn-test-policy",
        "PolicyDocument": {
          "Version": "2012-10-17",
          "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
        },
        "Roles": [{"Ref": "MyRole"}]
      }
    }
  },
  "Outputs": {
    "RoleArn": {"Value": {"Fn::GetAtt": ["MyRole", "Arn"]}},
    "PolicyArn": {"Value": {"Ref": "MyPolicy"}}
  }
}`
	cfnCreateStack(t, client, "iam-stack", tpl)
	outputs := cfnStackOutputs(t, client, "iam-stack")
	assert.NotEmpty(t, outputs["RoleArn"])
	assert.Contains(t, outputs["RoleArn"], "arn:aws:iam")
	assert.NotEmpty(t, outputs["PolicyArn"])
}

func TestCFN_KMS_Key_Alias_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyKey": {
      "Type": "AWS::KMS::Key",
      "Properties": {
        "Description": "CFN test key",
        "KeyPolicy": {
          "Version": "2012-10-17",
          "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "kms:*", "Resource": "*"}]
        }
      }
    },
    "MyAlias": {
      "Type": "AWS::KMS::Alias",
      "Properties": {
        "AliasName": "alias/cfn-test-key",
        "TargetKeyId": {"Ref": "MyKey"}
      }
    }
  },
  "Outputs": {
    "KeyArn": {"Value": {"Fn::GetAtt": ["MyKey", "Arn"]}}
  }
}`
	cfnCreateStack(t, client, "kms-stack", tpl)
	outputs := cfnStackOutputs(t, client, "kms-stack")
	assert.NotEmpty(t, outputs["KeyArn"])
	assert.Contains(t, outputs["KeyArn"], "arn:aws:kms")
}

func TestCFN_SSM_Parameter_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "ParamValue": {"Type": "String", "Default": "hello-from-cfn"}
  },
  "Resources": {
    "MyParam": {
      "Type": "AWS::SSM::Parameter",
      "Properties": {
        "Name": "/cfn/test/param",
        "Type": "String",
        "Value": {"Ref": "ParamValue"}
      }
    }
  },
  "Outputs": {
    "ParamName": {"Value": {"Ref": "MyParam"}}
  }
}`
	cfnCreateStack(t, client, "ssm-stack", tpl)
	outputs := cfnStackOutputs(t, client, "ssm-stack")
	assert.NotEmpty(t, outputs["ParamName"])

	// Verify the parameter actually exists with the correct value via SSM client
	ssmClient := newSSMClient(t)
	ctx := context.Background()
	gpOut, err := ssmClient.GetParameter(ctx, &awsssm.GetParameterInput{
		Name: aws.String("/cfn/test/param"),
	})
	require.NoError(t, err)
	assert.Equal(t, "hello-from-cfn", aws.ToString(gpOut.Parameter.Value))
}

func TestCFN_SecretsManager_Secret_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MySecret": {
      "Type": "AWS::SecretsManager::Secret",
      "Properties": {
        "Name": "cfn-test-secret",
        "SecretString": "{\"username\": \"admin\", \"password\": \"s3cr3t\"}"
      }
    }
  },
  "Outputs": {
    "SecretArn": {"Value": {"Ref": "MySecret"}}
  }
}`
	cfnCreateStack(t, client, "sm-stack", tpl)
	outputs := cfnStackOutputs(t, client, "sm-stack")
	assert.NotEmpty(t, outputs["SecretArn"])
	assert.Contains(t, outputs["SecretArn"], "arn:aws:secretsmanager")
}

func TestCFN_EC2_VPC_Subnet_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyVPC": {
      "Type": "AWS::EC2::VPC",
      "Properties": {
        "CidrBlock": "10.0.0.0/16"
      }
    },
    "MySubnet": {
      "Type": "AWS::EC2::Subnet",
      "Properties": {
        "VpcId": {"Ref": "MyVPC"},
        "CidrBlock": "10.0.1.0/24"
      }
    }
  },
  "Outputs": {
    "VpcId": {"Value": {"Ref": "MyVPC"}},
    "SubnetId": {"Value": {"Ref": "MySubnet"}}
  }
}`
	cfnCreateStack(t, client, "vpc-subnet-stack", tpl)
	rtypes := cfnResourceTypes(t, client, "vpc-subnet-stack")
	assert.True(t, rtypes["AWS::EC2::VPC"])
	assert.True(t, rtypes["AWS::EC2::Subnet"])

	outputs := cfnStackOutputs(t, client, "vpc-subnet-stack")
	assert.NotEmpty(t, outputs["VpcId"])
	assert.NotEmpty(t, outputs["SubnetId"])
}

func TestCFN_EventBridge_Rule_Stack(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyBus": {
      "Type": "AWS::Events::EventBus",
      "Properties": {
        "Name": "cfn-test-bus"
      }
    },
    "MyRule": {
      "Type": "AWS::Events::Rule",
      "Properties": {
        "Name": "cfn-test-rule",
        "EventBusName": {"Ref": "MyBus"},
        "EventPattern": {"source": ["cfn.test"]},
        "State": "ENABLED"
      }
    }
  },
  "Outputs": {
    "BusArn": {"Value": {"Fn::GetAtt": ["MyBus", "Arn"]}}
  }
}`
	cfnCreateStack(t, client, "eventbridge-stack", tpl)
	rtypes := cfnResourceTypes(t, client, "eventbridge-stack")
	assert.True(t, rtypes["AWS::Events::EventBus"])
	assert.True(t, rtypes["AWS::Events::Rule"])

	outputs := cfnStackOutputs(t, client, "eventbridge-stack")
	assert.NotEmpty(t, outputs["BusArn"])
}

// ─── Update / replace / delete tests ─────────────────────────────────────────

func TestCFN_UpdateStack_AddResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl1 := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "QueueA": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "update-add-queue-a"}
    }
  }
}`
	cfnCreateStack(t, client, "update-add-stack", tpl1)

	tpl2 := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "QueueA": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "update-add-queue-a"}
    },
    "QueueB": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "update-add-queue-b"}
    }
  }
}`
	_, err := client.UpdateStack(ctx, &awscf.UpdateStackInput{
		StackName:    aws.String("update-add-stack"),
		TemplateBody: aws.String(tpl2),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("update-add-stack"),
	})
	require.NoError(t, err)
	assert.Equal(t, cftypes.StackStatusUpdateComplete, descOut.Stacks[0].StackStatus)

	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String("update-add-stack"),
	})
	require.NoError(t, err)
	assert.Len(t, resOut.StackResources, 2)
}

func TestCFN_UpdateStack_RemoveResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl1 := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "QueueA": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "update-rm-queue-a"}
    },
    "QueueB": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "update-rm-queue-b"}
    }
  }
}`
	cfnCreateStack(t, client, "update-rm-stack", tpl1)

	tpl2 := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "QueueA": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "update-rm-queue-a"}
    }
  }
}`
	_, err := client.UpdateStack(ctx, &awscf.UpdateStackInput{
		StackName:    aws.String("update-rm-stack"),
		TemplateBody: aws.String(tpl2),
	})
	require.NoError(t, err)

	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String("update-rm-stack"),
	})
	require.NoError(t, err)
	assert.Len(t, resOut.StackResources, 1)
	assert.Equal(t, "QueueA", aws.ToString(resOut.StackResources[0].LogicalResourceId))
}

func TestCFN_UpdateStack_ModifyParam(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "QName": {"Type": "String"}
  },
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": {"Ref": "QName"}}
    }
  }
}`
	_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("param-update-stack"),
		TemplateBody: aws.String(tpl),
		Parameters: []cftypes.Parameter{
			{ParameterKey: aws.String("QName"), ParameterValue: aws.String("param-update-v1")},
		},
	})
	require.NoError(t, err)

	pids1 := cfnResourcePhysicalIDs(t, client, "param-update-stack")
	assert.Contains(t, pids1["MyQueue"], "param-update-v1")

	_, err = client.UpdateStack(ctx, &awscf.UpdateStackInput{
		StackName:    aws.String("param-update-stack"),
		TemplateBody: aws.String(tpl),
		Parameters: []cftypes.Parameter{
			{ParameterKey: aws.String("QName"), ParameterValue: aws.String("param-update-v2")},
		},
	})
	require.NoError(t, err)

	pids2 := cfnResourcePhysicalIDs(t, client, "param-update-stack")
	assert.Contains(t, pids2["MyQueue"], "param-update-v2")
}

func TestCFN_StackStatus_Outputs(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "status-output-queue"}
    }
  },
  "Outputs": {
    "QueueUrl": {
      "Value": {"Fn::GetAtt": ["MyQueue", "QueueUrl"]},
      "Description": "The URL of the queue"
    },
    "QueueArn": {
      "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]}
    }
  }
}`
	cfnCreateStack(t, client, "status-outputs-stack", tpl)

	ctx := context.Background()
	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("status-outputs-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	stack := descOut.Stacks[0]
	assert.Equal(t, cftypes.StackStatusCreateComplete, stack.StackStatus)

	outputMap := map[string]string{}
	for _, o := range stack.Outputs {
		outputMap[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}
	assert.NotEmpty(t, outputMap["QueueUrl"])
	assert.NotEmpty(t, outputMap["QueueArn"])
	assert.Contains(t, outputMap["QueueUrl"], "status-output-queue")
}

func TestCFN_DescribeStackResources_Types(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    },
    "MyTopic": {
      "Type": "AWS::SNS::Topic",
      "Properties": {}
    },
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {}
    }
  }
}`
	cfnCreateStack(t, client, "res-types-stack", tpl)

	ctx := context.Background()
	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String("res-types-stack"),
	})
	require.NoError(t, err)
	assert.Len(t, resOut.StackResources, 3)

	byLogical := make(map[string]string)
	for _, r := range resOut.StackResources {
		byLogical[aws.ToString(r.LogicalResourceId)] = aws.ToString(r.ResourceType)
	}
	assert.Equal(t, "AWS::SQS::Queue", byLogical["MyQueue"])
	assert.Equal(t, "AWS::SNS::Topic", byLogical["MyTopic"])
	assert.Equal(t, "AWS::S3::Bucket", byLogical["MyBucket"])
}

// ─── Conditions + Pseudo params ───────────────────────────────────────────────

func TestCFN_Conditions_ResourceConditional(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	// When CreateExtraQueue=true, two queues; when false, only one
	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "CreateExtraQueue": {"Type": "String", "AllowedValues": ["true", "false"]}
  },
  "Conditions": {
    "ShouldCreateExtra": {"Fn::Equals": [{"Ref": "CreateExtraQueue"}, "true"]}
  },
  "Resources": {
    "BaseQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "conditional-base-queue"}
    },
    "ExtraQueue": {
      "Type": "AWS::SQS::Queue",
      "Condition": "ShouldCreateExtra",
      "Properties": {"QueueName": "conditional-extra-queue"}
    }
  }
}`

	// Create with condition false — only one resource
	_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("cond-false-stack"),
		TemplateBody: aws.String(tpl),
		Parameters: []cftypes.Parameter{
			{ParameterKey: aws.String("CreateExtraQueue"), ParameterValue: aws.String("false")},
		},
	})
	require.NoError(t, err)

	resOut, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String("cond-false-stack"),
	})
	require.NoError(t, err)
	assert.Len(t, resOut.StackResources, 1)
	assert.Equal(t, "BaseQueue", aws.ToString(resOut.StackResources[0].LogicalResourceId))

	// Create a second stack with condition true — both resources
	_, err = client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("cond-true-stack"),
		TemplateBody: aws.String(tpl),
		Parameters: []cftypes.Parameter{
			{ParameterKey: aws.String("CreateExtraQueue"), ParameterValue: aws.String("true")},
		},
	})
	require.NoError(t, err)

	resOut2, err := client.DescribeStackResources(ctx, &awscf.DescribeStackResourcesInput{
		StackName: aws.String("cond-true-stack"),
	})
	require.NoError(t, err)
	assert.Len(t, resOut2.StackResources, 2)
}

func TestCFN_PseudoParam_Region(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Region": {
      "Value": {"Ref": "AWS::Region"}
    }
  }
}`
	cfnCreateStack(t, client, "pseudo-region-stack", tpl)
	outputs := cfnStackOutputs(t, client, "pseudo-region-stack")
	assert.Equal(t, "us-east-1", outputs["Region"])
}

func TestCFN_PseudoParam_AccountId(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "AccountId": {
      "Value": {"Ref": "AWS::AccountId"}
    }
  }
}`
	cfnCreateStack(t, client, "pseudo-account-stack", tpl)
	outputs := cfnStackOutputs(t, client, "pseudo-account-stack")
	assert.Equal(t, "000000000000", outputs["AccountId"])
}

func TestCFN_PseudoParam_StackName(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "StackNameOutput": {
      "Value": {"Ref": "AWS::StackName"}
    }
  }
}`
	cfnCreateStack(t, client, "pseudo-stackname-stack", tpl)
	outputs := cfnStackOutputs(t, client, "pseudo-stackname-stack")
	assert.Equal(t, "pseudo-stackname-stack", outputs["StackNameOutput"])
}

func TestCFN_PseudoParam_NoValue(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	// AWS::NoValue used inside Fn::If — when condition is false the false
	// branch resolves to empty string (NoValue). Stack should create cleanly.
	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "IncludeExtra": {"Type": "String", "Default": "false"}
  },
  "Conditions": {
    "Extra": {"Fn::Equals": [{"Ref": "IncludeExtra"}, "true"]}
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "MaybeValue": {
      "Value": {"Fn::If": ["Extra", "included", {"Ref": "AWS::NoValue"}]}
    }
  }
}`
	cfnCreateStack(t, client, "novalue-stack", tpl)

	ctx := context.Background()
	descOut, err := client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("novalue-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Stacks, 1)
	assert.Equal(t, cftypes.StackStatusCreateComplete, descOut.Stacks[0].StackStatus)
}

// ─── Additional coverage tests ────────────────────────────────────────────────

func TestCFN_FnJoin_WithRefs(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Joined": {
      "Value": {"Fn::Join": [":", [{"Ref": "AWS::Region"}, {"Ref": "AWS::AccountId"}]]}
    }
  }
}`
	cfnCreateStack(t, client, "join-refs-stack", tpl)
	outputs := cfnStackOutputs(t, client, "join-refs-stack")
	assert.Equal(t, "us-east-1:000000000000", outputs["Joined"])
}

func TestCFN_FnSelect_FromSplit(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "FirstPart": {
      "Value": {"Fn::Select": [0, {"Fn::Split": ["/", "us-east-1/prod/app"]}]}
    },
    "LastPart": {
      "Value": {"Fn::Select": [2, {"Fn::Split": ["/", "us-east-1/prod/app"]}]}
    }
  }
}`
	cfnCreateStack(t, client, "select-split-stack", tpl)
	outputs := cfnStackOutputs(t, client, "select-split-stack")
	assert.Equal(t, "us-east-1", outputs["FirstPart"])
	assert.Equal(t, "app", outputs["LastPart"])
}

func TestCFN_FnSub_WithLocalVars(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Result": {
      "Value": {"Fn::Sub": ["Hello ${Name}!", {"Name": "World"}]}
    }
  }
}`
	cfnCreateStack(t, client, "sub-localvars-stack", tpl)
	outputs := cfnStackOutputs(t, client, "sub-localvars-stack")
	assert.Equal(t, "Hello World!", outputs["Result"])
}

func TestCFN_FnFindInMap_NestedRef(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "Env": {"Type": "String", "Default": "prod"}
  },
  "Mappings": {
    "EnvConfig": {
      "prod":    {"Size": "large"},
      "staging": {"Size": "medium"},
      "dev":     {"Size": "small"}
    }
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "InstanceSize": {
      "Value": {"Fn::FindInMap": ["EnvConfig", {"Ref": "Env"}, "Size"]}
    }
  }
}`
	cfnCreateStack(t, client, "findinmap-ref-stack", tpl)
	outputs := cfnStackOutputs(t, client, "findinmap-ref-stack")
	assert.Equal(t, "large", outputs["InstanceSize"])
}

func TestCFN_MultipleOutputs_AllPresent(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "QueueOne": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "multi-out-queue-1"}
    },
    "QueueTwo": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "multi-out-queue-2"}
    }
  },
  "Outputs": {
    "Queue1Arn": {"Value": {"Fn::GetAtt": ["QueueOne", "Arn"]}},
    "Queue2Arn": {"Value": {"Fn::GetAtt": ["QueueTwo", "Arn"]}},
    "Queue1Url": {"Value": {"Fn::GetAtt": ["QueueOne", "QueueUrl"]}},
    "Queue2Url": {"Value": {"Fn::GetAtt": ["QueueTwo", "QueueUrl"]}}
  }
}`
	cfnCreateStack(t, client, "multi-outputs-stack", tpl)
	outputs := cfnStackOutputs(t, client, "multi-outputs-stack")
	assert.NotEmpty(t, outputs["Queue1Arn"])
	assert.NotEmpty(t, outputs["Queue2Arn"])
	assert.NotEmpty(t, outputs["Queue1Url"])
	assert.NotEmpty(t, outputs["Queue2Url"])
	assert.Contains(t, outputs["Queue1Url"], "multi-out-queue-1")
	assert.Contains(t, outputs["Queue2Url"], "multi-out-queue-2")
}

func TestCFN_DeleteStack_ResourcesRemoved(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "delete-test-queue"}
    }
  }
}`
	cfnCreateStack(t, client, "delete-test-stack", tpl)

	_, err := client.DeleteStack(ctx, &awscf.DeleteStackInput{
		StackName: aws.String("delete-test-stack"),
	})
	require.NoError(t, err)

	_, err = client.DescribeStacks(ctx, &awscf.DescribeStacksInput{
		StackName: aws.String("delete-test-stack"),
	})
	require.Error(t, err)
}

func TestCFN_ListStacks_FilterByStatus(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  }
}`
	for i := 0; i < 3; i++ {
		_, err := client.CreateStack(ctx, &awscf.CreateStackInput{
			StackName:    aws.String(fmt.Sprintf("list-filter-stack-%d", i)),
			TemplateBody: aws.String(tpl),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListStacks(ctx, &awscf.ListStacksInput{
		StackStatusFilter: []cftypes.StackStatus{cftypes.StackStatusCreateComplete},
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.StackSummaries), 3)
	for _, s := range listOut.StackSummaries {
		assert.Equal(t, cftypes.StackStatusCreateComplete, s.StackStatus)
	}
}

func TestCFN_StackId_NotEmpty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  }
}`
	createOut, err := client.CreateStack(ctx, &awscf.CreateStackInput{
		StackName:    aws.String("stackid-test-stack"),
		TemplateBody: aws.String(tpl),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createOut.StackId))
	assert.Contains(t, aws.ToString(createOut.StackId), "stackid-test-stack")
}

func TestCFN_PseudoParam_StackId(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "StackId": {
      "Value": {"Ref": "AWS::StackId"}
    }
  }
}`
	cfnCreateStack(t, client, "pseudo-stackid-stack", tpl)
	outputs := cfnStackOutputs(t, client, "pseudo-stackid-stack")
	assert.NotEmpty(t, outputs["StackId"])
}

func TestCFN_FnGetAtt_QueueURL(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "getatt-url-queue"}
    }
  },
  "Outputs": {
    "QueueUrl": {
      "Value": {"Fn::GetAtt": ["MyQueue", "QueueUrl"]}
    }
  }
}`
	cfnCreateStack(t, client, "getatt-url-stack", tpl)
	outputs := cfnStackOutputs(t, client, "getatt-url-stack")
	assert.NotEmpty(t, outputs["QueueUrl"])
	assert.Contains(t, outputs["QueueUrl"], "getatt-url-queue")
}

func TestCFN_Conditions_FnNot(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "IsProd": {"Type": "String", "Default": "false"}
  },
  "Conditions": {
    "NotProd": {"Fn::Not": [{"Fn::Equals": [{"Ref": "IsProd"}, "true"]}]}
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Label": {
      "Value": {"Fn::If": ["NotProd", "non-prod", "production"]}
    }
  }
}`
	cfnCreateStack(t, client, "not-condition-stack", tpl)
	outputs := cfnStackOutputs(t, client, "not-condition-stack")
	assert.Equal(t, "non-prod", outputs["Label"])
}

func TestCFN_Conditions_FnAnd(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "A": {"Type": "String", "Default": "true"},
    "B": {"Type": "String", "Default": "true"}
  },
  "Conditions": {
    "BothTrue": {
      "Fn::And": [
        {"Fn::Equals": [{"Ref": "A"}, "true"]},
        {"Fn::Equals": [{"Ref": "B"}, "true"]}
      ]
    }
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Result": {
      "Value": {"Fn::If": ["BothTrue", "yes", "no"]}
    }
  }
}`
	cfnCreateStack(t, client, "and-condition-stack", tpl)
	outputs := cfnStackOutputs(t, client, "and-condition-stack")
	assert.Equal(t, "yes", outputs["Result"])
}

func TestCFN_Conditions_FnOr(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "A": {"Type": "String", "Default": "false"},
    "B": {"Type": "String", "Default": "true"}
  },
  "Conditions": {
    "AnyTrue": {
      "Fn::Or": [
        {"Fn::Equals": [{"Ref": "A"}, "true"]},
        {"Fn::Equals": [{"Ref": "B"}, "true"]}
      ]
    }
  },
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "Result": {
      "Value": {"Fn::If": ["AnyTrue", "yes", "no"]}
    }
  }
}`
	cfnCreateStack(t, client, "or-condition-stack", tpl)
	outputs := cfnStackOutputs(t, client, "or-condition-stack")
	assert.Equal(t, "yes", outputs["Result"])
}

func TestCFN_Ref_QueueInOutput(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "ref-queue-output"}
    }
  },
  "Outputs": {
    "QueueRef": {
      "Value": {"Ref": "MyQueue"}
    }
  }
}`
	cfnCreateStack(t, client, "ref-output-stack", tpl)
	outputs := cfnStackOutputs(t, client, "ref-output-stack")
	assert.NotEmpty(t, outputs["QueueRef"])
	assert.Contains(t, outputs["QueueRef"], "ref-queue-output")
}

func TestCFN_FnSub_ArnComposition(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "BucketName": {"Type": "String", "Default": "my-sub-bucket"}
  },
  "Resources": {
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "Properties": {"BucketName": {"Ref": "BucketName"}}
    }
  },
  "Outputs": {
    "BucketArnSub": {
      "Value": {"Fn::Sub": "arn:aws:s3:::${BucketName}"}
    }
  }
}`
	cfnCreateStack(t, client, "sub-arn-stack", tpl)
	outputs := cfnStackOutputs(t, client, "sub-arn-stack")
	assert.Equal(t, "arn:aws:s3:::my-sub-bucket", outputs["BucketArnSub"])
}

func TestCFN_ValidateTemplate_WithParameters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Parameters": {
    "QueueName": {"Type": "String", "Default": "default-queue"},
    "MessageRetentionPeriod": {"Type": "Number", "Default": "345600"}
  },
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {
        "QueueName": {"Ref": "QueueName"},
        "MessageRetentionPeriod": {"Ref": "MessageRetentionPeriod"}
      }
    }
  }
}`
	out, err := client.ValidateTemplate(ctx, &awscf.ValidateTemplateInput{
		TemplateBody: aws.String(tpl),
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}

func TestCFN_DependsOn_OrderGuarantee(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	// Queue must be created before bucket (DependsOn), all three should exist
	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyQueue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "deporder-queue"}
    },
    "MyBucket": {
      "Type": "AWS::S3::Bucket",
      "DependsOn": "MyQueue",
      "Properties": {"BucketName": "deporder-bucket"}
    },
    "MyTopic": {
      "Type": "AWS::SNS::Topic",
      "DependsOn": ["MyQueue", "MyBucket"],
      "Properties": {"TopicName": "deporder-topic"}
    }
  }
}`
	cfnCreateStack(t, client, "deporder-stack", tpl)
	rtypes := cfnResourceTypes(t, client, "deporder-stack")
	assert.True(t, rtypes["AWS::SQS::Queue"])
	assert.True(t, rtypes["AWS::S3::Bucket"])
	assert.True(t, rtypes["AWS::SNS::Topic"])
}

func TestCFN_FnGetAtt_SNSTopic(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "MyTopic": {
      "Type": "AWS::SNS::Topic",
      "Properties": {"TopicName": "getatt-sns-topic"}
    }
  },
  "Outputs": {
    "TopicArn": {
      "Value": {"Fn::GetAtt": ["MyTopic", "TopicArn"]}
    }
  }
}`
	cfnCreateStack(t, client, "getatt-sns-stack", tpl)
	outputs := cfnStackOutputs(t, client, "getatt-sns-stack")
	assert.NotEmpty(t, outputs["TopicArn"])
	assert.Contains(t, outputs["TopicArn"], "arn:aws:sns")
}

func TestCFN_FnSub_StackNameAndRegion(t *testing.T) {
	resetState(t)
	client := newCFClient(t)

	tpl := `{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Placeholder": {
      "Type": "AWS::SQS::Queue",
      "Properties": {}
    }
  },
  "Outputs": {
    "LogicalName": {
      "Value": {"Fn::Sub": "${AWS::StackName}-in-${AWS::Region}"}
    }
  }
}`
	cfnCreateStack(t, client, "sub-combo-stack", tpl)
	outputs := cfnStackOutputs(t, client, "sub-combo-stack")
	assert.Equal(t, "sub-combo-stack-in-us-east-1", outputs["LogicalName"])
}
