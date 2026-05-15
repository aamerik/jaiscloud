package iam

// slrEntry maps a service principal to its SLR metadata.
type slrEntry struct {
	RoleName    string
	PolicyARN   string
	Description string
}

var slrCatalog = map[string]slrEntry{
	"elasticloadbalancing.amazonaws.com": {
		RoleName:    "AWSServiceRoleForElasticLoadBalancing",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSElasticLoadBalancingServiceRolePolicy",
		Description: "Allows ELB to call AWS services on your behalf.",
	},
	"ecs.amazonaws.com": {
		RoleName:    "AWSServiceRoleForECS",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonECSServiceRolePolicy",
		Description: "Allows ECS to manage cluster resources.",
	},
	"emr.amazonaws.com": {
		RoleName:    "AWSServiceRoleForEMRCleanup",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonEMRCleanupPolicy",
		Description: "Allows EMR to clean up resources.",
	},
	"elasticmapreduce.amazonaws.com": {
		RoleName:    "AWSServiceRoleForEMRCleanup",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonEMRCleanupPolicy",
		Description: "Allows EMR to clean up resources.",
	},
	"lambda.amazonaws.com": {
		RoleName:    "AWSServiceRoleForLambda",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSLambdaServiceRolePolicy",
		Description: "Allows Lambda to manage ENIs.",
	},
	"events.amazonaws.com": {
		RoleName:    "AWSServiceRoleForCloudWatchEvents",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/CloudWatchEventsServiceRolePolicy",
		Description: "Allows CloudWatch Events to invoke targets.",
	},
	"apigateway.amazonaws.com": {
		RoleName:    "AWSServiceRoleForAPIGateway",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/APIGatewayServiceRolePolicy",
		Description: "Allows API Gateway to manage VPC links.",
	},
	"cloudwatch.amazonaws.com": {
		RoleName:    "AWSServiceRoleForCloudWatchCrossAccount",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/CloudWatch-CrossAccountSharingServiceRolePolicy",
		Description: "Allows CloudWatch cross-account sharing.",
	},
	"eks.amazonaws.com": {
		RoleName:    "AWSServiceRoleForAmazonEKS",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonEKSServiceRolePolicy",
		Description: "Allows EKS to manage cluster resources.",
	},
	"autoscaling.amazonaws.com": {
		RoleName:    "AWSServiceRoleForAutoScaling",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AutoScalingServiceRolePolicy",
		Description: "Allows Auto Scaling to manage EC2 resources.",
	},
	"rds.amazonaws.com": {
		RoleName:    "AWSServiceRoleForRDS",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonRDSServiceRolePolicy",
		Description: "Allows RDS to manage cluster resources.",
	},
	"ssm.amazonaws.com": {
		RoleName:    "AWSServiceRoleForAmazonSSM",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonSSMServiceRolePolicy",
		Description: "Allows SSM to manage resources.",
	},
	"ec2.amazonaws.com": {
		RoleName:    "AWSServiceRoleForEC2SpotFleet",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonEC2SpotFleetServiceRolePolicy",
		Description: "Allows EC2 Spot Fleet to request and manage spot instances.",
	},
	"support.amazonaws.com": {
		RoleName:    "AWSServiceRoleForSupport",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSSupportServiceRolePolicy",
		Description: "Allows AWS Support to access resources.",
	},
	"cloudtrail.amazonaws.com": {
		RoleName:    "AWSServiceRoleForCloudTrail",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSCloudTrailServiceRolePolicy",
		Description: "Allows CloudTrail to write logs.",
	},
	"firehose.amazonaws.com": {
		RoleName:    "AWSServiceRoleForKinesisFirehose",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonKinesisFirehoseServiceRolePolicy",
		Description: "Allows Kinesis Firehose to deliver data.",
	},
	"glue.amazonaws.com": {
		RoleName:    "AWSServiceRoleForGlue",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSGlueServiceNotebookRolePolicy",
		Description: "Allows Glue to manage ETL resources.",
	},
	"trustedadvisor.amazonaws.com": {
		RoleName:    "AWSServiceRoleForTrustedAdvisor",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSTrustedAdvisorServiceRolePolicy",
		Description: "Allows Trusted Advisor to access resources.",
	},
	"application-autoscaling.amazonaws.com": {
		RoleName:    "AWSServiceRoleForApplicationAutoScaling_ECSService",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingECSServicePolicy",
		Description: "Allows Application Auto Scaling to manage ECS.",
	},
	"s3.amazonaws.com": {
		RoleName:    "AWSServiceRoleForAmazonS3",
		PolicyARN:   "arn:aws:iam::aws:policy/aws-service-role/AmazonS3ServiceRolePolicy",
		Description: "Allows S3 to manage multi-region access points.",
	},
}
