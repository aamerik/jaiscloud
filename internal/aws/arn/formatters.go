// Package arn contains AWS ARN formatting helpers for all resource types.
package arn

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
)

func hashName(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}

// formatters maps abstract resource types to their AWS ARN format function.
// To add a new resource type, add one entry here — no switch statement to update.
var formatters = map[string]func(region, accountID, name string) string{
	// DynamoDB
	"dynamodb-table":  func(r, a, n string) string { return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", r, a, n) },
	"dynamodb-stream": func(r, a, n string) string { return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", r, a, n) },
	// Lambda
	"lambda-function": func(r, a, n string) string { return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", r, a, n) },
	// SNS
	"sns-topic":        func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:%s", r, a, n) },
	"sns-subscription": func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:%s", r, a, n) },
	// SQS
	"sqs-queue": func(r, a, n string) string { return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", r, a, n) },
	// IAM — no region in ARN
	"iam-role":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:role/%s", a, n) },
	"iam-policy": func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:policy/%s", a, n) },
	"iam-user":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:user/%s", a, n) },
	// S3 — no region or account in ARN
	"s3-bucket": func(_, _, n string) string { return fmt.Sprintf("arn:aws:s3:::%s", n) },
	// EventBridge
	"events-rule":            func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", r, a, n) },
	"events-bus":             func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:event-bus/%s", r, a, n) },
	"events-archive":         func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:archive/%s", r, a, n) },
	"events-replay":          func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:replay/%s", r, a, n) },
	"events-connection":      func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:connection/%s", r, a, n) },
	"events-api-destination": func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:api-destination/%s", r, a, n) },
	// EMR
	"emr-cluster": func(r, a, n string) string { return fmt.Sprintf("arn:aws:elasticmapreduce:%s:%s:cluster/%s", r, a, n) },
	// EMR Containers
	"emr-virtual-cluster": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/%s", r, a, n)
	},
	"emr-job-run": func(r, a, n string) string {
		if vcID, runID, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/%s/jobruns/%s", r, a, vcID, runID)
		}
		return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/-/jobruns/%s", r, a, n)
	},
	"emr-managed-endpoint": func(r, a, n string) string {
		if vcID, epID, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/%s/endpoints/%s", r, a, vcID, epID)
		}
		return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/-/endpoints/%s", r, a, n)
	},
	// IAM root
	"iam-root": func(_, a, _ string) string { return fmt.Sprintf("arn:aws:iam::%s:root", a) },
	// KMS
	"kms-key":   func(r, a, n string) string { return fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", r, a, n) },
	"kms-alias": func(r, a, n string) string { return fmt.Sprintf("arn:aws:kms:%s:%s:alias/%s", r, a, n) },
	"kms-grant": func(r, a, n string) string { return fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", r, a, n) },
	// SecretsManager
	"secretsmanager-secret": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s", r, a, n)
	},
	// SSM
	"ssm-parameter": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ssm:%s:%s:parameter/%s", r, a, n) },
	// API Gateway
	"apigateway-restapi":    func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	"apigateway-stage":      func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	"apigateway-resource":   func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	"apigateway-deployment": func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	// CloudFormation
	"cloudformation-stack": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cloudformation:%s:%s:stack/%s", r, a, n)
	},
	// CloudWatch Logs
	"logs-group":  func(r, a, n string) string { return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s:*", r, a, n) },
	"logs-stream": func(r, a, n string) string { return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s", r, a, n) },
	// Kinesis
	"kinesis-stream":   func(r, a, n string) string { return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", r, a, n) },
	"kinesis-consumer": func(r, a, n string) string { return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", r, a, n) },
	// ECR
	"ecr-repository": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", r, a, n) },
	// Step Functions
	"sfn-state-machine": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", r, a, n)
	},
	"sfn-activity": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:states:%s:%s:activity:%s", r, a, n)
	},
	"sfn-execution": func(r, a, n string) string {
		if sm, exec, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", r, a, sm, exec)
		}
		return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s", r, a, n)
	},
	"sfn-express-execution": func(r, a, n string) string {
		if sm, exec, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:states:%s:%s:express:%s:%s", r, a, sm, exec)
		}
		return fmt.Sprintf("arn:aws:states:%s:%s:express:%s", r, a, n)
	},
	// EKS
	"eks-cluster":   func(r, a, n string) string { return fmt.Sprintf("arn:aws:eks:%s:%s:cluster/%s", r, a, n) },
	"eks-nodegroup": func(r, a, n string) string { return fmt.Sprintf("arn:aws:eks:%s:%s:nodegroup/%s", r, a, n) },
	"eks-addon":     func(r, a, n string) string { return fmt.Sprintf("arn:aws:eks:%s:%s:addon/%s", r, a, n) },
	// ECS
	"ecs-cluster":            func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", r, a, n) },
	"ecs-task":               func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:task/%s", r, a, n) },
	"ecs-task-definition":    func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:task-definition/%s", r, a, n) },
	"ecs-service":            func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:service/%s", r, a, n) },
	"ecs-container-instance": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:container-instance/%s", r, a, n) },
	"ecs-task-set":           func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:task-set/%s", r, a, n) },
	"ecs-capacity-provider":  func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:capacity-provider/%s", r, a, n) },
	// RDS
	"rds-cluster":     func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:cluster:%s", r, a, n) },
	"rds-instance":    func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", r, a, n) },
	"rds-subnetgroup": func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:subgrp:%s", r, a, n) },
	"rds-snapshot":    func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:snapshot:%s", r, a, n) },
	"rds-pg":          func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:pg:%s", r, a, n) },
	// STS / IAM additional types
	"sts-assumed-role":     func(_, a, n string) string { return fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s", a, n) },
	"sts-federated-user":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", a, n) },
	"iam-group":            func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:group/%s", a, n) },
	"iam-instance-profile": func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:instance-profile/%s", a, n) },
	"iam-managed-policy":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:policy/%s", a, n) },
	"iam-oidc-provider":    func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", a, n) },
	// CloudFormation changeset
	"cfn-changeset": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cloudformation:%s:%s:changeSet/%s", r, a, n)
	},
	// ElastiCache
	"elasticache-cluster": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:cluster:%s", r, a, n)
	},
	"elasticache-replication-group": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:replicationgroup:%s", r, a, n)
	},
	"elasticache-subnetgroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:subnetgroup:%s", r, a, n)
	},
	"elasticache-parametergroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:parametergroup:%s", r, a, n)
	},
	// Cognito
	"cognito-userpool": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cognito-idp:%s:%s:userpool/%s", r, a, n)
	},
	"cognito-identitypool": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cognito-identity:%s:%s:identitypool/%s", r, a, n)
	},
	// ACM
	"acm-certificate": func(r, a, n string) string { return fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", r, a, n) },
	// Firehose
	"firehose-stream": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/%s", r, a, n)
	},
	// CloudFront
	"cloudfront-distribution": func(_, a, n string) string {
		return fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", a, n)
	},
	// Athena
	"athena-workgroup":        func(r, a, n string) string { return fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", r, a, n) },
	"athena-query-execution":  func(r, a, n string) string { return fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", r, a, n) },
	// Redshift
	"redshift-cluster":    func(r, a, n string) string { return fmt.Sprintf("arn:aws:redshift:%s:%s:cluster:%s", r, a, n) },
	"redshift-subnetgroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:redshift:%s:%s:subnetgroup:%s", r, a, n)
	},
	// S3 access points
	"s3-accesspoint": func(r, a, n string) string { return fmt.Sprintf("arn:aws:s3:%s:%s:accesspoint/%s", r, a, n) },
	// CloudWatch
	"cloudwatch-alarm":     func(r, a, n string) string { return fmt.Sprintf("arn:aws:cloudwatch:%s:%s:alarm:%s", r, a, n) },
	"cloudwatch-dashboard": func(_, a, n string) string { return fmt.Sprintf("arn:aws:cloudwatch::%s:dashboard/%s", a, n) },
	// Lambda code signing
	"lambda-code-signing-config": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:lambda:%s:%s:code-signing-config/%s", r, a, n)
	},
	// SES
	"ses-identity": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ses:%s:%s:identity/%s", r, a, n) },
	// ELBv2
	"elb-loadbalancer": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/app/%s/%x", r, a, n, hashName(n))
	},
	"elb-targetgroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:targetgroup/%s/%x", r, a, n, hashName(n))
	},
	"elb-listener": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:listener/app/%s/%x", r, a, n, hashName(n))
	},
	// AWS Config
	"config-rule": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:config:%s:%s:config-rule/config-rule-%s", r, a, n)
	},
	// Resource Groups
	"resourcegroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:resource-groups:%s:%s:group/%s", r, a, n)
	},
	// SNS platform
	"sns-platform-app":      func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:app/%s", r, a, n) },
	"sns-platform-endpoint": func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:endpoint/%s", r, a, n) },
}

// ResourceID returns a function that formats AWS ARNs for a given region and account.
// Inject the result into NormalizedRequest.ResourceID at the gateway layer.
func ResourceID(region, accountID string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		if f, ok := formatters[resourceType]; ok {
			return f(region, accountID, name)
		}
		slog.Warn("arn.ResourceID: unknown resource type, returning name as-is", "resourceType", resourceType, "name", name)
		return name
	}
}
