package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoute53_CreateGetDeleteHostedZone(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	out, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("example.com"),
		CallerReference: aws.String("ref-1"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.HostedZone.Name), "example.com")
	zoneId := aws.ToString(out.HostedZone.Id)
	assert.NotEmpty(t, zoneId)

	getOut, err := client.GetHostedZone(ctx, &awsroute53.GetHostedZoneInput{
		Id: aws.String(zoneId),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(getOut.HostedZone.Name), "example.com")

	_, err = client.DeleteHostedZone(ctx, &awsroute53.DeleteHostedZoneInput{
		Id: aws.String(zoneId),
	})
	require.NoError(t, err)

	_, err = client.GetHostedZone(ctx, &awsroute53.GetHostedZoneInput{Id: aws.String(zoneId)})
	require.Error(t, err)
}

func TestRoute53_ListHostedZones(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	for _, name := range []string{"alpha.com", "beta.com", "gamma.com"} {
		_, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
			Name:            aws.String(name),
			CallerReference: aws.String("ref-" + name),
		})
		require.NoError(t, err)
	}

	out, err := client.ListHostedZones(ctx, &awsroute53.ListHostedZonesInput{})
	require.NoError(t, err)
	assert.Len(t, out.HostedZones, 3)
}

func TestRoute53_ChangeResourceRecordSets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("example.com"),
		CallerReference: aws.String("ref-1"),
	})
	require.NoError(t, err)
	zoneId := aws.ToString(zoneOut.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneId),
		ChangeBatch: &types.ChangeBatch{
			Changes: []types.Change{
				{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String("www.example.com"),
						Type: types.RRTypeA,
						TTL:  aws.Int64(300),
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String("1.2.3.4")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestRoute53_CreateGetDeleteHealthCheck(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	out, err := client.CreateHealthCheck(ctx, &awsroute53.CreateHealthCheckInput{
		CallerReference: aws.String("hc-ref-1"),
		HealthCheckConfig: &types.HealthCheckConfig{
			Type:                     types.HealthCheckTypeHttps,
			FullyQualifiedDomainName: aws.String("api.example.com"),
			Port:                     aws.Int32(443),
		},
	})
	require.NoError(t, err)
	hcId := aws.ToString(out.HealthCheck.Id)
	assert.NotEmpty(t, hcId)

	getOut, err := client.GetHealthCheck(ctx, &awsroute53.GetHealthCheckInput{
		HealthCheckId: aws.String(hcId),
	})
	require.NoError(t, err)
	assert.Equal(t, hcId, aws.ToString(getOut.HealthCheck.Id))

	listOut, err := client.ListHealthChecks(ctx, &awsroute53.ListHealthChecksInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.HealthChecks, 1)

	_, err = client.DeleteHealthCheck(ctx, &awsroute53.DeleteHealthCheckInput{
		HealthCheckId: aws.String(hcId),
	})
	require.NoError(t, err)

	listOut, err = client.ListHealthChecks(ctx, &awsroute53.ListHealthChecksInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.HealthChecks, 0)
}
