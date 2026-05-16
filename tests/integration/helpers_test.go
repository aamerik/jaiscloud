package integration_test

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	smithy "github.com/aws/smithy-go"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	awsacm "github.com/aws/aws-sdk-go-v2/service/acm"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	awscloudfront "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awscognitoidp "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	awscognitoidentity "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awsdynamostreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	awsec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	awselasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	awsemrc "github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awsredshift "github.com/aws/aws-sdk-go-v2/service/redshift"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	awsconfigservice "github.com/aws/aws-sdk-go-v2/service/configservice"
	awselbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	awsresourcegroups "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	awstagging "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	awsroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3control "github.com/aws/aws-sdk-go-v2/service/s3control"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awsapigw "github.com/aws/aws-sdk-go-v2/service/apigateway"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
)

const jaiscloudEndpoint = "http://localhost:4566"

func newSQSClient(t *testing.T) *sqs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newIAMClient(t *testing.T) *awsiam.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsiam.NewFromConfig(cfg, func(o *awsiam.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newDynamoClient(t *testing.T) *awsdynamo.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsdynamo.NewFromConfig(cfg, func(o *awsdynamo.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newSNSClient(t *testing.T) *awssns.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awssns.NewFromConfig(cfg, func(o *awssns.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newS3Client(t *testing.T) *awss3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
		o.UsePathStyle = true
	})
}

func newSTSClient(t *testing.T) *awssts.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awssts.NewFromConfig(cfg, func(o *awssts.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newEC2Client(t *testing.T) *awsec2.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsec2.NewFromConfig(cfg, func(o *awsec2.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newRoute53Client(t *testing.T) *awsroute53.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsroute53.NewFromConfig(cfg, func(o *awsroute53.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newCFClient(t *testing.T) *awscf.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awscf.NewFromConfig(cfg, func(o *awscf.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newDynamoStreamsClient(t *testing.T) *awsdynamostreams.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsdynamostreams.NewFromConfig(cfg, func(o *awsdynamostreams.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newECSClient(t *testing.T) *awsecs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsecs.NewFromConfig(cfg, func(o *awsecs.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newElastiCacheClient(t *testing.T) *awselasticache.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awselasticache.NewFromConfig(cfg, func(o *awselasticache.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newRDSClient(t *testing.T) *awsrds.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsrds.NewFromConfig(cfg, func(o *awsrds.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newGlueClient(t *testing.T) *awsglue.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsglue.NewFromConfig(cfg, func(o *awsglue.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newEMRClient(t *testing.T) *awsemr.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsemr.NewFromConfig(cfg, func(o *awsemr.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newEMRContainersClient(t *testing.T) *awsemrc.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsemrc.NewFromConfig(cfg, func(o *awsemrc.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newKMSClient(t *testing.T) *awskms.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awskms.NewFromConfig(cfg, func(o *awskms.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newSMClient(t *testing.T) *awssm.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awssm.NewFromConfig(cfg, func(o *awssm.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newSSMClient(t *testing.T) *awsssm.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsssm.NewFromConfig(cfg, func(o *awsssm.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newAPIGWClient(t *testing.T) *awsapigw.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsapigw.NewFromConfig(cfg, func(o *awsapigw.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newCWLClient(t *testing.T) *awscwl.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awscwl.NewFromConfig(cfg, func(o *awscwl.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudEndpoint+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

// assertAWSError asserts that err is an AWS API error with the given code.
func assertAWSError(t *testing.T, err error, expectedCode string) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, expectedCode, apiErr.ErrorCode())
}

// waitFor polls until check() returns true or timeout is reached.
func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// newAWSConfig returns a base AWS config pointing at the local emulator.
func newAWSConfig(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	return cfg
}

// host returns the base URL JaisCloud uses in queue URLs (e.g. http://localhost:4566).
func host() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func newCognitoIDPClient(t *testing.T) *awscognitoidp.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awscognitoidp.NewFromConfig(cfg, func(o *awscognitoidp.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newCognitoIdentityClient(t *testing.T) *awscognitoidentity.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awscognitoidentity.NewFromConfig(cfg, func(o *awscognitoidentity.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newACMClient(t *testing.T) *awsacm.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsacm.NewFromConfig(cfg, func(o *awsacm.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newSESClient(t *testing.T) *awsses.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsses.NewFromConfig(cfg, func(o *awsses.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newFirehoseClient(t *testing.T) *awsfirehose.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsfirehose.NewFromConfig(cfg, func(o *awsfirehose.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newCloudfrontClient(t *testing.T) *awscloudfront.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awscloudfront.NewFromConfig(cfg, func(o *awscloudfront.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newAthenaClient(t *testing.T) *awsathena.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsathena.NewFromConfig(cfg, func(o *awsathena.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newRedshiftClient(t *testing.T) *awsredshift.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsredshift.NewFromConfig(cfg, func(o *awsredshift.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

// s3ControlFlatEndpoint bypasses the S3 Control endpoint-rules engine, which
// would otherwise prepend the account-ID as a host prefix. It always returns
// the emulator's base URL unchanged so requests reach localhost:4566.
type s3ControlFlatEndpoint struct{ base string }

func (r *s3ControlFlatEndpoint) ResolveEndpoint(ctx context.Context, params awss3control.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(r.base)
	if err != nil {
		return smithyendpoints.Endpoint{}, err
	}
	return smithyendpoints.Endpoint{URI: *u}, nil
}

func newS3ControlClient(t *testing.T) *awss3control.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awss3control.NewFromConfig(cfg, func(o *awss3control.Options) {
		o.EndpointResolverV2 = &s3ControlFlatEndpoint{base: jaiscloudEndpoint}
	})
}

func newELBv2Client(t *testing.T) *awselbv2.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awselbv2.NewFromConfig(cfg, func(o *awselbv2.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newConfigServiceClient(t *testing.T) *awsconfigservice.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsconfigservice.NewFromConfig(cfg, func(o *awsconfigservice.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newResourceGroupsClient(t *testing.T) *awsresourcegroups.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsresourcegroups.NewFromConfig(cfg, func(o *awsresourcegroups.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newTaggingClient(t *testing.T) *awstagging.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awstagging.NewFromConfig(cfg, func(o *awstagging.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}
