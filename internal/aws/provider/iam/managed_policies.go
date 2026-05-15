package iam

import (
	"context"
	"time"
)

type managedPolicyEntry struct {
	ARN         string
	PolicyName  string
	Description string
}

var awsManagedPolicies = []managedPolicyEntry{
	{ARN: "arn:aws:iam::aws:policy/AdministratorAccess", PolicyName: "AdministratorAccess", Description: "Provides full access to AWS services and resources."},
	{ARN: "arn:aws:iam::aws:policy/ReadOnlyAccess", PolicyName: "ReadOnlyAccess"},
	{ARN: "arn:aws:iam::aws:policy/PowerUserAccess", PolicyName: "PowerUserAccess"},
	{ARN: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole", PolicyName: "AWSLambdaBasicExecutionRole"},
	{ARN: "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole", PolicyName: "AWSLambdaVPCAccessExecutionRole"},
	{ARN: "arn:aws:iam::aws:policy/AmazonS3FullAccess", PolicyName: "AmazonS3FullAccess"},
	{ARN: "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess", PolicyName: "AmazonS3ReadOnlyAccess"},
	{ARN: "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess", PolicyName: "AmazonDynamoDBFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/AmazonSQSFullAccess", PolicyName: "AmazonSQSFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/AmazonSNSFullAccess", PolicyName: "AmazonSNSFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess", PolicyName: "CloudWatchLogsFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/AmazonEC2FullAccess", PolicyName: "AmazonEC2FullAccess"},
	{ARN: "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy", PolicyName: "AmazonECSTaskExecutionRolePolicy"},
	{ARN: "arn:aws:iam::aws:policy/AmazonRDSFullAccess", PolicyName: "AmazonRDSFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/IAMFullAccess", PolicyName: "IAMFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/IAMReadOnlyAccess", PolicyName: "IAMReadOnlyAccess"},
	{ARN: "arn:aws:iam::aws:policy/AmazonAthenaFullAccess", PolicyName: "AmazonAthenaFullAccess"},
	{ARN: "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role", PolicyName: "AmazonElasticMapReduceforEC2Role"},
	{ARN: "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole", PolicyName: "AmazonElasticMapReduceRole"},
	{ARN: "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole", PolicyName: "AWSGlueServiceRole"},
}

// seedManagedPolicies is called from New() to pre-populate the top AWS managed policies.
func (p *IAMProvider) seedManagedPolicies(ctx context.Context) {
	now := time.Now().UTC()
	for _, mp := range awsManagedPolicies {
		// Only seed if not already present
		if _, err := p.resources.Get(ctx, "iam_policies", mp.ARN); err == nil {
			continue
		}
		pd := policyData{
			PolicyName:  mp.PolicyName,
			PolicyID:    "ANPA" + randID(16),
			Arn:         mp.ARN,
			Path:        "/",
			Description: mp.Description,
			Document:    `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			CreateDate:  now,
			UpdateDate:  now,
		}
		_ = saveEntry(ctx, p.resources, "iam_policies", mp.ARN, pd)
	}
}
