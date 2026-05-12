// Package aws — services.go is the single source of truth for AWS service metadata.
//
// To add a new AWS service, add one ServiceDescriptor entry to awsServices.
// All detection logic (X-Amz-Target, SigV4 allow-list, Action= lookup) and the
// gateway's service→provider mapping are derived automatically from that entry.
package aws

import (
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/aws/adapter/services"
)

// ServiceDescriptor captures every piece of per-service metadata needed by the
// router and the gateway routing layer.
type ServiceDescriptor struct {
	// SigV4Name is the service token from the SigV4 Authorization header
	// (e.g. "sqs") and the canonical value set on NormalizedRequest.Service.
	SigV4Name string

	// TargetPrefix is the X-Amz-Target prefix used by JSON/Target-protocol services
	// (e.g. "AmazonSQS."). Empty for REST and Query-protocol services.
	TargetPrefix string

	// QueryActions lists every Action= value that unambiguously identifies this
	// service in the URL-query / POST-form-body Query protocol.
	// Only Query-protocol services need this field.
	QueryActions []string

	// ProviderPrefix is the prefix used in the provider Registry dispatch key
	// (e.g. "Queue" → "Queue.CreateQueue"). It appears in Routes() maps as the
	// first segment of every handler key.
	ProviderPrefix string

	// Codec is a factory function that returns a new Codec for this service.
	// Nil for services whose codec is registered via the existing per-file mechanism.
	// When non-nil, AWSAdapter.DetectAndDecode will use this factory to create the codec.
	Codec func() adapter.Codec
}

// awsServices is the authoritative list of AWS services known to JaisCloud.
// Order is not significant; all lookup maps are built at init time.
// Add one entry here when wiring in a new service — nowhere else needs changing.
var awsServices = []ServiceDescriptor{
	{
		SigV4Name:      "sqs",
		TargetPrefix:   "AmazonSQS.",
		ProviderPrefix: "Queue",
		QueryActions: []string{
			"CreateQueue", "DeleteQueue", "ListQueues", "GetQueueUrl",
			"GetQueueAttributes", "SetQueueAttributes",
			"SendMessage", "ReceiveMessage", "DeleteMessage",
			"ChangeMessageVisibility", "PurgeQueue",
			"SendMessageBatch", "DeleteMessageBatch", "ChangeMessageVisibilityBatch",
			"TagQueue", "UntagQueue", "ListQueueTags",
		},
	},
	{
		SigV4Name:      "iam",
		ProviderPrefix: "IAM",
		QueryActions: []string{
			"CreateRole", "GetRole", "DeleteRole", "ListRoles", "UpdateAssumeRolePolicy",
			"CreatePolicy", "GetPolicy", "DeletePolicy", "ListPolicies",
			"AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies",
			"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
			"CreateUser", "GetUser", "DeleteUser", "ListUsers",
			"CreateAccessKey", "DeleteAccessKey", "ListAccessKeys",
			"TagRole", "UntagRole", "ListRoleTags",
		},
	},
	{
		SigV4Name:      "sts",
		ProviderPrefix: "STS",
		QueryActions: []string{
			"AssumeRole", "AssumeRoleWithSAML", "AssumeRoleWithWebIdentity",
			"GetCallerIdentity", "GetSessionToken", "GetFederationToken",
			"DecodeAuthorizationMessage",
		},
	},
	{
		SigV4Name:      "sns",
		ProviderPrefix: "Notification",
		QueryActions: []string{
			"CreateTopic", "DeleteTopic", "GetTopicAttributes", "SetTopicAttributes", "ListTopics",
			"Subscribe", "Unsubscribe", "ConfirmSubscription",
			"ListSubscriptions", "ListSubscriptionsByTopic",
			"GetSubscriptionAttributes", "SetSubscriptionAttributes",
			"Publish", "PublishBatch",
			"TagResource", "UntagResource", "ListTagsForResource",
		},
	},
	{SigV4Name: "dynamodb", TargetPrefix: "DynamoDB_20120810.", ProviderPrefix: "Table"},
	{SigV4Name: "dynamodbstreams", TargetPrefix: "DynamoDBStreams_20120810.", ProviderPrefix: "Streams"},
	{SigV4Name: "s3", ProviderPrefix: "Object"},
	{SigV4Name: "lambda", ProviderPrefix: "Function"},
	{SigV4Name: "glue", TargetPrefix: "AWSGlue.", ProviderPrefix: "Glue"},
	{SigV4Name: "ec2", ProviderPrefix: "Compute"},
	{SigV4Name: "route53", ProviderPrefix: "DNS"},
	{SigV4Name: "rds", ProviderPrefix: "RDS"},
	{SigV4Name: "elasticache", ProviderPrefix: "ElastiCache"},
	{SigV4Name: "ecs", TargetPrefix: "AmazonEC2ContainerServiceV20141113.", ProviderPrefix: "ECS"},
	{SigV4Name: "cloudformation", ProviderPrefix: "CloudFormation"},
	{SigV4Name: "emr", TargetPrefix: "ElasticMapReduce.", ProviderPrefix: "EMR"},
	{SigV4Name: "emr-containers", ProviderPrefix: "EMRContainers"},
	{SigV4Name: "events", TargetPrefix: "AWSEvents.", ProviderPrefix: "EventBridge"},
	{SigV4Name: "eks", ProviderPrefix: "EKS"},
	{
		SigV4Name:      "kinesis",
		TargetPrefix:   "Kinesis_20131202.",
		ProviderPrefix: "Kinesis",
		Codec:          func() adapter.Codec { return &services.KinesisCodec{} },
	},
	{
		SigV4Name:      "ecr",
		TargetPrefix:   "AmazonEC2ContainerRegistry_V20150921.",
		ProviderPrefix: "ECR",
		Codec:          func() adapter.Codec { return &services.ECRCodec{} },
	},
	{
		SigV4Name:      "states",
		TargetPrefix:   "AWSStepFunctions.",
		ProviderPrefix: "StepFunctions",
		Codec:          func() adapter.Codec { return &services.StepFunctionsCodec{} },
	},
	// P0 expansion services — Codec factories set to nil until Phase 2–6 wire them.
	{
		SigV4Name:      "kms",
		TargetPrefix:   "TrentService.",
		ProviderPrefix: "Key",
	},
	{
		SigV4Name:      "secretsmanager",
		TargetPrefix:   "secretsmanager.",
		ProviderPrefix: "Secret",
	},
	{
		SigV4Name:      "ssm",
		TargetPrefix:   "AmazonSSM.",
		ProviderPrefix: "Parameter",
	},
	{
		SigV4Name:      "apigateway",
		ProviderPrefix: "Gateway",
	},
	{
		SigV4Name:      "execute-api",
		ProviderPrefix: "Gateway",
	},
	{
		SigV4Name:      "logs",
		TargetPrefix:   "Logs_20140328.",
		ProviderPrefix: "CloudWatchLogs",
		Codec:          func() adapter.Codec { return &services.LogsCodec{} },
	},
	{
		SigV4Name:      "monitoring",
		ProviderPrefix: "CloudWatch",
		Codec:          func() adapter.Codec { return &services.CloudWatchCodec{} },
		QueryActions: []string{
			"PutMetricData",
			"GetMetricStatistics",
			"GetMetricData",
			"ListMetrics",
			"PutMetricAlarm",
			"DescribeAlarms",
			"DescribeAlarmsForMetric",
			"DeleteAlarms",
			"SetAlarmState",
			"EnableAlarmActions",
			"DisableAlarmActions",
			"GetDashboard",
			"ListDashboards",
			"PutDashboard",
			"DeleteDashboards",
			"TagResource",
			"UntagResource",
			"ListTagsForResource",
		},
	},
	// ── Phase 15 services ────────────────────────────────────────────────────
	{
		SigV4Name:      "cognito-idp",
		TargetPrefix:   "AWSCognitoIdentityProviderService.",
		ProviderPrefix: "Cognito",
		Codec:          func() adapter.Codec { return &services.GenericJSONTargetCodec{Service: "cognito-idp", TargetPrefix: "AWSCognitoIdentityProviderService."} },
	},
	{
		SigV4Name:      "cognito-identity",
		TargetPrefix:   "AWSCognitoIdentityService.",
		ProviderPrefix: "CognitoIdentity",
		Codec:          func() adapter.Codec { return &services.GenericJSONTargetCodec{Service: "cognito-identity", TargetPrefix: "AWSCognitoIdentityService."} },
	},
	{
		SigV4Name:      "acm",
		TargetPrefix:   "CertificateManager.",
		ProviderPrefix: "ACM",
		Codec:          func() adapter.Codec { return &services.GenericJSONTargetCodec{Service: "acm", TargetPrefix: "CertificateManager."} },
	},
	{
		SigV4Name:      "email",
		ProviderPrefix: "SES",
		QueryActions: []string{
			"SendEmail", "SendRawEmail", "SendBulkTemplatedEmail",
			"VerifyEmailIdentity", "VerifyDomainIdentity",
			"ListIdentities", "DeleteIdentity",
			"GetIdentityVerificationAttributes",
			"GetSendQuota", "GetSendStatistics",
		},
	},
	{
		SigV4Name:      "firehose",
		TargetPrefix:   "Firehose_20150804.",
		ProviderPrefix: "Firehose",
		Codec:          func() adapter.Codec { return &services.GenericJSONTargetCodec{Service: "firehose", TargetPrefix: "Firehose_20150804."} },
	},
	{
		SigV4Name:      "cloudfront",
		ProviderPrefix: "CloudFront",
		Codec:          func() adapter.Codec { return &services.CloudFrontCodec{} },
	},
	{
		SigV4Name:      "athena",
		TargetPrefix:   "AmazonAthena.",
		ProviderPrefix: "Athena",
		Codec:          func() adapter.Codec { return &services.GenericJSONTargetCodec{Service: "athena", TargetPrefix: "AmazonAthena."} },
	},
	{
		SigV4Name:      "redshift",
		ProviderPrefix: "Redshift",
		QueryActions: []string{
			"CreateCluster", "DescribeClusters", "DeleteCluster", "ModifyCluster", "RebootCluster",
			"ResumeCluster", "PauseCluster",
			"CreateClusterSubnetGroup", "DescribeClusterSubnetGroups", "DeleteClusterSubnetGroup", "ModifyClusterSubnetGroup",
			"CreateTags", "DeleteTags", "DescribeTags",
		},
	},
}

// ─── Derived lookup tables ────────────────────────────────────────────────────
// Built once at init time from awsServices. Do not modify these directly.

var (
	// targetPrefixToService maps X-Amz-Target prefix → SigV4 service name.
	// Used by DetectService (Priority 1).
	targetPrefixToService map[string]string

	// knownServices is the allow-list of valid AWS service names. Used by
	// DetectService for SigV4 Authorization, presigned SigV4, and SigV2 checks.
	knownServices map[string]bool

	// actionToService maps Action= query-param values → SigV4 service name.
	// Used by DetectService (Priority 3).
	actionToService map[string]string

	// serviceProviderMap maps SigV4 service name → provider registry prefix.
	// Used by AWSAdapter.ServiceToProvider (consumed by the gateway routing layer).
	serviceProviderMap map[string]string
)

func init() {
	targetPrefixToService = make(map[string]string, len(awsServices))
	knownServices = make(map[string]bool, len(awsServices))
	actionToService = make(map[string]string)
	serviceProviderMap = make(map[string]string, len(awsServices))

	for _, svc := range awsServices {
		knownServices[svc.SigV4Name] = true
		serviceProviderMap[svc.SigV4Name] = svc.ProviderPrefix
		if svc.TargetPrefix != "" {
			targetPrefixToService[svc.TargetPrefix] = svc.SigV4Name
		}
		for _, action := range svc.QueryActions {
			actionToService[action] = svc.SigV4Name
		}
	}
}

// detectServiceFromTarget looks up the service for an X-Amz-Target header value
// by iterating the known target prefixes (there are ≤20 entries).
func detectServiceFromTarget(target string) string {
	for prefix, svc := range targetPrefixToService {
		if strings.HasPrefix(target, prefix) {
			return svc
		}
	}
	return ""
}
