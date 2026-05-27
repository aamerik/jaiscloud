// Package jaiscloud is a free, open-source AWS emulator written in Go.
//
// JaisCloud emulates the AWS wire protocols — Query/XML, JSON/Target, REST/XML,
// REST/JSON — so any AWS SDK (Go, Python, Java, Node.js, .NET) points at a
// local JaisCloud instance without code changes.
//
// # Supported services
//
// Full implementations: S3, SQS, DynamoDB + Streams, SNS, Lambda, KMS,
// Secrets Manager, SSM Parameter Store, IAM, STS, EventBridge, CloudFormation,
// CloudWatch + Logs, Glue Data Catalog, EMR on EC2, EMR on EKS, API Gateway,
// Step Functions, Kinesis.
//
// Metadata/CRUD: EC2, Route53, RDS, ElastiCache, ECS, EKS, ELBv2, ECR,
// Redshift, Athena, Config, Kinesis Firehose.
//
// # Key features
//
//   - Single static binary — no Python, no Docker daemon required
//   - Exact AWS wire protocol — no SDK shims or proxy rewrites
//   - PostgreSQL persistence (free — no subscription required)
//   - State export/import and named snapshots
//   - Real Spark/EMR execution via Kubernetes or Docker executor
//   - Multi-account isolation with full ARN scoping
//   - Prometheus metrics at /metrics
//
// # Quick start
//
//	jaiscloud-aws start
//	export AWS_ENDPOINT_URL=http://localhost:4566
//	aws s3 mb s3://my-bucket
//
// See https://jaiscloud.com for full documentation.
// See https://github.com/jaisrajms/jaiscloud for source and releases.
package jaiscloud
