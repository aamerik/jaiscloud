package multiaccount

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsapigw "github.com/aws/aws-sdk-go-v2/service/apigateway"
	awscfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	awscw "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awsec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	awsecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	awselasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	awsevents "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	awsroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"

	"jaiscloud/internal/aws/identity"
)

const (
	AcctA = "111111111111"
	AcctB = "222222222222"
	AcctC = "333333333333"

	endpoint = "http://localhost:4566"
	region   = "us-east-1"
)

// accessKeyFor returns an LSIA-encoded access key that encodes the given account ID.
func accessKeyFor(t *testing.T, account string) string {
	t.Helper()
	key, err := identity.EncodeLSIA(account)
	if err != nil {
		t.Fatalf("EncodeLSIA(%q): %v", account, err)
	}
	return key
}

// awsCfgFor returns an aws.Config pointed at the local emulator using account-scoped credentials.
func awsCfgFor(t *testing.T, account string) aws.Config {
	t.Helper()
	ak := accessKeyFor(t, account)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, "test", "")),
	)
	if err != nil {
		t.Fatalf("load config for account %s: %v", account, err)
	}
	return cfg
}

func ep(o interface{ SetBaseEndpoint(*string) }) { o.SetBaseEndpoint(aws.String(endpoint)) }

func newSQSFor(t *testing.T, account string) *sqs.Client {
	cfg := awsCfgFor(t, account)
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newDynamoFor(t *testing.T, account string) *awsdynamo.Client {
	cfg := awsCfgFor(t, account)
	return awsdynamo.NewFromConfig(cfg, func(o *awsdynamo.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newKMSFor(t *testing.T, account string) *awskms.Client {
	cfg := awsCfgFor(t, account)
	return awskms.NewFromConfig(cfg, func(o *awskms.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newSTSFor(t *testing.T, account string) *awssts.Client {
	cfg := awsCfgFor(t, account)
	return awssts.NewFromConfig(cfg, func(o *awssts.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newSNSFor(t *testing.T, account string) *awssns.Client {
	cfg := awsCfgFor(t, account)
	return awssns.NewFromConfig(cfg, func(o *awssns.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newS3For(t *testing.T, account string) *awss3.Client {
	cfg := awsCfgFor(t, account)
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func newSMFor(t *testing.T, account string) *awssm.Client {
	cfg := awsCfgFor(t, account)
	return awssm.NewFromConfig(cfg, func(o *awssm.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newLambdaFor(t *testing.T, account string) *awslambda.Client {
	cfg := awsCfgFor(t, account)
	return awslambda.NewFromConfig(cfg, func(o *awslambda.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newSSMFor(t *testing.T, account string) *awsssm.Client {
	cfg := awsCfgFor(t, account)
	return awsssm.NewFromConfig(cfg, func(o *awsssm.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newIAMFor(t *testing.T, account string) *awsiam.Client {
	cfg := awsCfgFor(t, account)
	return awsiam.NewFromConfig(cfg, func(o *awsiam.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newAPIGWFor(t *testing.T, account string) *awsapigw.Client {
	cfg := awsCfgFor(t, account)
	return awsapigw.NewFromConfig(cfg, func(o *awsapigw.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newCFNFor(t *testing.T, account string) *awscfn.Client {
	cfg := awsCfgFor(t, account)
	return awscfn.NewFromConfig(cfg, func(o *awscfn.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newEventsFor(t *testing.T, account string) *awsevents.Client {
	cfg := awsCfgFor(t, account)
	return awsevents.NewFromConfig(cfg, func(o *awsevents.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newCWFor(t *testing.T, account string) *awscw.Client {
	cfg := awsCfgFor(t, account)
	return awscw.NewFromConfig(cfg, func(o *awscw.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newKinesisFor(t *testing.T, account string) *awskinesis.Client {
	cfg := awsCfgFor(t, account)
	return awskinesis.NewFromConfig(cfg, func(o *awskinesis.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newFirehoseFor(t *testing.T, account string) *awsfirehose.Client {
	cfg := awsCfgFor(t, account)
	return awsfirehose.NewFromConfig(cfg, func(o *awsfirehose.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newECSFor(t *testing.T, account string) *awsecs.Client {
	cfg := awsCfgFor(t, account)
	return awsecs.NewFromConfig(cfg, func(o *awsecs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newEC2For(t *testing.T, account string) *awsec2.Client {
	cfg := awsCfgFor(t, account)
	return awsec2.NewFromConfig(cfg, func(o *awsec2.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newRoute53For(t *testing.T, account string) *awsroute53.Client {
	cfg := awsCfgFor(t, account)
	return awsroute53.NewFromConfig(cfg, func(o *awsroute53.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newGlueFor(t *testing.T, account string) *awsglue.Client {
	cfg := awsCfgFor(t, account)
	return awsglue.NewFromConfig(cfg, func(o *awsglue.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newSFNFor(t *testing.T, account string) *awssfn.Client {
	cfg := awsCfgFor(t, account)
	return awssfn.NewFromConfig(cfg, func(o *awssfn.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newECRFor(t *testing.T, account string) *awsecr.Client {
	cfg := awsCfgFor(t, account)
	return awsecr.NewFromConfig(cfg, func(o *awsecr.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newElastiCacheFor(t *testing.T, account string) *awselasticache.Client {
	cfg := awsCfgFor(t, account)
	return awselasticache.NewFromConfig(cfg, func(o *awselasticache.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newRDSFor(t *testing.T, account string) *awsrds.Client {
	cfg := awsCfgFor(t, account)
	return awsrds.NewFromConfig(cfg, func(o *awsrds.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

// resetState wipes all emulator state between tests.
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(endpoint+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

// resetScope wipes state for one (account, region) pair.
func resetScope(t *testing.T, account, reg string) {
	t.Helper()
	u := fmt.Sprintf("%s/_jaiscloud/reset?account=%s&region=%s", endpoint, account, reg)
	resp, err := http.Post(u, "", nil)
	if err != nil {
		t.Fatalf("resetScope: %v", err)
	}
	resp.Body.Close()
}

// resetAccount wipes all regions for one account.
func resetAccount(t *testing.T, account string) {
	t.Helper()
	u := fmt.Sprintf("%s/_jaiscloud/reset?account=%s", endpoint, account)
	resp, err := http.Post(u, "", nil)
	if err != nil {
		t.Fatalf("resetAccount: %v", err)
	}
	resp.Body.Close()
}
