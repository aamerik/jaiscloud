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

// TestRoute53MultiChangeBatch verifies that a batch with multiple Change entries
// persists all changes with correct TTLs (fix 1.1.3).
func TestRoute53MultiChangeBatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	// Create hosted zone.
	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("example.com"),
		CallerReference: aws.String("ref-batch"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	ttl60 := int64(60)
	ttl600 := int64(600)

	// Submit 2 changes in a single batch.
	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &types.ChangeBatch{
			Changes: []types.Change{
				{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String("a.example.com"),
						Type: types.RRTypeA,
						TTL:  &ttl60,
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String("1.2.3.4")},
						},
					},
				},
				{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String("mx.example.com"),
						Type: types.RRTypeMx,
						TTL:  &ttl600,
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String("10 mail.example.com")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Both changes must appear in ListResourceRecordSets.
	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	names := map[string]int64{}
	for _, rr := range listOut.ResourceRecordSets {
		names[aws.ToString(rr.Name)] = aws.ToInt64(rr.TTL)
	}
	assert.Equal(t, ttl60, names["a.example.com"], "A record TTL must be 60")
	assert.Equal(t, ttl600, names["mx.example.com"], "MX record TTL must be 600")
}
