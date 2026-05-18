package arn_test

import (
	"testing"

	"jaiscloud/internal/aws/arn"
)

func TestParse_Valid(t *testing.T) {
	cases := []struct {
		input     string
		partition string
		service   string
		region    string
		accountID string
		resource  string
		resType   string
	}{
		{
			"arn:aws:sqs:us-east-1:000000000000:my-queue",
			"aws", "sqs", "us-east-1", "000000000000", "my-queue", "",
		},
		{
			"arn:aws:dynamodb:us-east-1:111111111111:table/MyTable",
			"aws", "dynamodb", "us-east-1", "111111111111", "table/MyTable", "table",
		},
		{
			"arn:aws:iam::000000000000:role/MyRole",
			"aws", "iam", "", "000000000000", "role/MyRole", "role",
		},
		{
			"arn:aws:s3:::my-bucket",
			"aws", "s3", "", "", "my-bucket", "",
		},
		{
			"arn:aws:lambda:us-west-2:222222222222:function:my-fn",
			"aws", "lambda", "us-west-2", "222222222222", "function:my-fn", "function",
		},
		{
			"arn:aws:kms:eu-west-1:333333333333:key/12345678-1234-1234-1234-123456789012",
			"aws", "kms", "eu-west-1", "333333333333", "key/12345678-1234-1234-1234-123456789012", "key",
		},
	}
	for _, c := range cases {
		t.Run(c.input, func(t *testing.T) {
			p, err := arn.Parse(c.input)
			if err != nil {
				t.Fatalf("Parse(%q) = error %v", c.input, err)
			}
			if p.Partition != c.partition {
				t.Errorf("Partition: got %q want %q", p.Partition, c.partition)
			}
			if p.Service != c.service {
				t.Errorf("Service: got %q want %q", p.Service, c.service)
			}
			if p.Region != c.region {
				t.Errorf("Region: got %q want %q", p.Region, c.region)
			}
			if p.AccountID != c.accountID {
				t.Errorf("AccountID: got %q want %q", p.AccountID, c.accountID)
			}
			if p.Resource != c.resource {
				t.Errorf("Resource: got %q want %q", p.Resource, c.resource)
			}
			if p.ResourceType != c.resType {
				t.Errorf("ResourceType: got %q want %q", p.ResourceType, c.resType)
			}
		})
	}
}

func TestParse_Invalid(t *testing.T) {
	cases := []string{
		"",
		"not-an-arn",
		"arn:",
		"arn:aws",
		"arn:aws:sqs",
		"arn:aws:sqs:us-east-1",
		"arn:aws:sqs:us-east-1:000000000000", // 5 fields, needs 6
	}
	for _, c := range cases {
		if _, err := arn.Parse(c); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", c)
		}
	}
}

func TestParse_NoPanic(t *testing.T) {
	inputs := []string{"", "arn", "arn::::::::", "arn::::::", "not-arn-at-all"}
	for _, s := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Parse(%q) panicked: %v", s, r)
				}
			}()
			_, _ = arn.Parse(s)
		}()
	}
}

func TestResolveAccountRegion(t *testing.T) {
	cases := []struct {
		arnStr        string
		callerAcct    string
		callerRegion  string
		wantAccount   string
		wantRegion    string
	}{
		// ARN provides both
		{
			"arn:aws:sqs:us-east-1:222222222222:q",
			"111111111111", "us-west-2",
			"222222222222", "us-east-1",
		},
		// IAM ARN: no region → caller region
		{
			"arn:aws:iam::222222222222:role/R",
			"111111111111", "us-east-1",
			"222222222222", "us-east-1",
		},
		// S3 ARN: no account, no region → both from caller
		{
			"arn:aws:s3:::bucket",
			"111111111111", "us-east-1",
			"111111111111", "us-east-1",
		},
		// Invalid ARN → caller fallback
		{
			"not-an-arn",
			"111111111111", "us-east-1",
			"111111111111", "us-east-1",
		},
	}
	for _, c := range cases {
		acct, region := arn.ResolveAccountRegion(c.arnStr, c.callerAcct, c.callerRegion)
		if acct != c.wantAccount || region != c.wantRegion {
			t.Errorf("ResolveAccountRegion(%q, %q, %q) = (%q, %q), want (%q, %q)",
				c.arnStr, c.callerAcct, c.callerRegion, acct, region, c.wantAccount, c.wantRegion)
		}
	}
}

func TestServiceFromARN(t *testing.T) {
	if s := arn.ServiceFromARN("arn:aws:sqs:us-east-1:123456789012:q"); s != "sqs" {
		t.Errorf("got %q, want sqs", s)
	}
	if s := arn.ServiceFromARN("not-an-arn"); s != "" {
		t.Errorf("got %q, want empty", s)
	}
}
