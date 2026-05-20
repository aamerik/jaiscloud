// Package model — resourcetypes.go defines typed constants for all resource
// types used in NormalizedRequest.ResourceID calls. Using constants prevents
// typo bugs and makes it easy to grep for all usages of a given resource type.
package model

// Resource type constants for use with NormalizedRequest.ResourceID.
// These correspond to the keys in config.awsARNFormatters (AWS)
// and must be kept in sync when adding new resource types.
const (
	// DynamoDB
	RTDynamoDBTable  = "dynamodb-table"
	RTDynamoDBStream = "dynamodb-stream"

	// Lambda
	RTLambdaFunction = "lambda-function"

	// SNS
	RTSNSTopic        = "sns-topic"
	RTSNSSubscription = "sns-subscription"

	// SQS
	RTSQSQueue = "sqs-queue"

	// IAM
	RTIAMRole   = "iam-role"
	RTIAMPolicy = "iam-policy"
	RTIAMUser   = "iam-user"
	RTIAMRoot   = "iam-root"

	// S3
	RTS3Bucket = "s3-bucket"

	// EventBridge
	RTEventsRule = "events-rule"

	// EMR
	RTEMRCluster         = "emr-cluster"
	RTEMRVirtualCluster  = "emr-virtual-cluster"
	RTEMRJobRun          = "emr-job-run"
	RTEMRManagedEndpoint = "emr-managed-endpoint"

	// KMS
	RTKMSKey   = "kms-key"
	RTKMSAlias = "kms-alias"
	RTKMSGrant = "kms-grant"

	// SecretsManager
	RTSecretsManagerSecret = "secretsmanager-secret"

	// SSM
	RTSSMParameter = "ssm-parameter"

	// API Gateway
	RTAPIGatewayRestAPI    = "apigateway-restapi"
	RTAPIGatewayStage      = "apigateway-stage"
	RTAPIGatewayResource   = "apigateway-resource"
	RTAPIGatewayMethod     = "apigateway-method"
	RTAPIGatewayDeployment = "apigateway-deployment"

	// CloudFormation
	RTCFNStack = "cloudformation-stack"
)
