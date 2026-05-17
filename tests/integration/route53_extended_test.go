package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRoute53_PrivateHostedZone creates a private hosted zone and verifies
// the Config.PrivateZone field is reflected in GetHostedZone.
func TestRoute53_PrivateHostedZone(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	out, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("private.example.com"),
		CallerReference: aws.String("private-ref-1"),
		HostedZoneConfig: &r53types.HostedZoneConfig{
			PrivateZone: true,
			Comment:     aws.String("private zone"),
		},
		VPC: &r53types.VPC{
			VPCId:     aws.String("vpc-12345"),
			VPCRegion: r53types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
	zoneID := aws.ToString(out.HostedZone.Id)
	assert.NotEmpty(t, zoneID)

	getOut, err := client.GetHostedZone(ctx, &awsroute53.GetHostedZoneInput{
		Id: aws.String(zoneID),
	})
	require.NoError(t, err)
	assert.NotNil(t, getOut.HostedZone.Config)
	assert.True(t, getOut.HostedZone.Config.PrivateZone, "hosted zone should be private")
}

// TestRoute53_ChangeRecordSets_Multiple creates an A record and a CNAME in a
// single ChangeResourceRecordSets call and asserts both are listed.
func TestRoute53_ChangeRecordSets_Multiple(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("multi.example.com"),
		CallerReference: aws.String("multi-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionCreate,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name: aws.String("www.multi.example.com"),
						Type: r53types.RRTypeA,
						TTL:  aws.Int64(300),
						ResourceRecords: []r53types.ResourceRecord{
							{Value: aws.String("10.0.0.1")},
						},
					},
				},
				{
					Action: r53types.ChangeActionCreate,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name: aws.String("api.multi.example.com"),
						Type: r53types.RRTypeCname,
						TTL:  aws.Int64(60),
						ResourceRecords: []r53types.ResourceRecord{
							{Value: aws.String("www.multi.example.com")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	names := map[string]bool{}
	for _, rr := range listOut.ResourceRecordSets {
		names[aws.ToString(rr.Name)] = true
	}
	assert.True(t, names["www.multi.example.com"] || names["www.multi.example.com."], "A record should be present")
	assert.True(t, names["api.multi.example.com"] || names["api.multi.example.com."], "CNAME record should be present")
}

// TestRoute53_ChangeRecordSets_UPSERT creates a record then UPSERTs the same name
// with a new value, verifying only one record exists with the updated value.
func TestRoute53_ChangeRecordSets_UPSERT(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("upsert.example.com"),
		CallerReference: aws.String("upsert-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	createAndChange := func(action r53types.ChangeAction, ip string) {
		t.Helper()
		_, err := client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
			HostedZoneId: aws.String(zoneID),
			ChangeBatch: &r53types.ChangeBatch{
				Changes: []r53types.Change{
					{
						Action: action,
						ResourceRecordSet: &r53types.ResourceRecordSet{
							Name: aws.String("host.upsert.example.com"),
							Type: r53types.RRTypeA,
							TTL:  aws.Int64(300),
							ResourceRecords: []r53types.ResourceRecord{
								{Value: aws.String(ip)},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}

	// CREATE with original IP
	createAndChange(r53types.ChangeActionCreate, "1.1.1.1")
	// UPSERT with new IP
	createAndChange(r53types.ChangeActionUpsert, "2.2.2.2")

	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	var aRecords []r53types.ResourceRecordSet
	for _, rr := range listOut.ResourceRecordSets {
		name := aws.ToString(rr.Name)
		if rr.Type == r53types.RRTypeA && (name == "host.upsert.example.com" || name == "host.upsert.example.com.") {
			aRecords = append(aRecords, rr)
		}
	}
	require.Len(t, aRecords, 1, "UPSERT should result in exactly one A record")
	require.Len(t, aRecords[0].ResourceRecords, 1)
	assert.Equal(t, "2.2.2.2", aws.ToString(aRecords[0].ResourceRecords[0].Value))
}

// TestRoute53_ChangeRecordSets_DELETE creates an A record then deletes it,
// verifying it is absent from the subsequent list.
func TestRoute53_ChangeRecordSets_DELETE(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("delete.example.com"),
		CallerReference: aws.String("delete-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	rrInput := func(action r53types.ChangeAction) *awsroute53.ChangeResourceRecordSetsInput {
		return &awsroute53.ChangeResourceRecordSetsInput{
			HostedZoneId: aws.String(zoneID),
			ChangeBatch: &r53types.ChangeBatch{
				Changes: []r53types.Change{
					{
						Action: action,
						ResourceRecordSet: &r53types.ResourceRecordSet{
							Name: aws.String("temp.delete.example.com"),
							Type: r53types.RRTypeA,
							TTL:  aws.Int64(60),
							ResourceRecords: []r53types.ResourceRecord{
								{Value: aws.String("3.3.3.3")},
							},
						},
					},
				},
			},
		}
	}

	_, err = client.ChangeResourceRecordSets(ctx, rrInput(r53types.ChangeActionCreate))
	require.NoError(t, err)

	_, err = client.ChangeResourceRecordSets(ctx, rrInput(r53types.ChangeActionDelete))
	require.NoError(t, err)

	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	for _, rr := range listOut.ResourceRecordSets {
		name := aws.ToString(rr.Name)
		assert.False(t, name == "temp.delete.example.com" || name == "temp.delete.example.com.",
			"deleted record should not be present, got: %s", name)
	}
}

// TestRoute53_ListResourceRecordSets_Pagination creates 5 records then paginates
// with MaxItems=2, asserting NextRecordName is set and all 5 records are returned.
func TestRoute53_ListResourceRecordSets_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("paginate.example.com"),
		CallerReference: aws.String("paginate-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	// Create 5 A records
	var changes []r53types.Change
	for i := 1; i <= 5; i++ {
		changes = append(changes, r53types.Change{
			Action: r53types.ChangeActionCreate,
			ResourceRecordSet: &r53types.ResourceRecordSet{
				Name: aws.String(fmt.Sprintf("host%d.paginate.example.com", i)),
				Type: r53types.RRTypeA,
				TTL:  aws.Int64(300),
				ResourceRecords: []r53types.ResourceRecord{
					{Value: aws.String(fmt.Sprintf("10.0.0.%d", i))},
				},
			},
		})
	}
	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch:  &r53types.ChangeBatch{Changes: changes},
	})
	require.NoError(t, err)

	// Paginate with MaxItems=2
	var allRecords []r53types.ResourceRecordSet
	var nextName *string
	var nextType r53types.RRType
	for {
		in := &awsroute53.ListResourceRecordSetsInput{
			HostedZoneId: aws.String(zoneID),
			MaxItems:     aws.Int32(2),
		}
		if nextName != nil {
			in.StartRecordName = nextName
			in.StartRecordType = nextType
		}
		page, err := client.ListResourceRecordSets(ctx, in)
		require.NoError(t, err)
		allRecords = append(allRecords, page.ResourceRecordSets...)
		if !page.IsTruncated {
			break
		}
		nextName = page.NextRecordName
		nextType = page.NextRecordType
	}

	// We created 5 records; there may be default NS/SOA in the list too.
	assert.GreaterOrEqual(t, len(allRecords), 5, "should have at least 5 records across pages")
}

// TestRoute53_RecordType_TXT creates a TXT record and verifies it is listed.
func TestRoute53_RecordType_TXT(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("txt.example.com"),
		CallerReference: aws.String("txt-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionCreate,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name: aws.String("_verify.txt.example.com"),
						Type: r53types.RRTypeTxt,
						TTL:  aws.Int64(300),
						ResourceRecords: []r53types.ResourceRecord{
							{Value: aws.String(`"v=spf1 include:example.com ~all"`)},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	found := false
	for _, rr := range listOut.ResourceRecordSets {
		if rr.Type == r53types.RRTypeTxt {
			found = true
			assert.NotEmpty(t, rr.ResourceRecords)
		}
	}
	assert.True(t, found, "TXT record should be present in list")
}

// TestRoute53_RecordType_MX creates an MX record and verifies it appears in the list.
func TestRoute53_RecordType_MX(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("mx.example.com"),
		CallerReference: aws.String("mx-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionCreate,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name: aws.String("mx.example.com"),
						Type: r53types.RRTypeMx,
						TTL:  aws.Int64(300),
						ResourceRecords: []r53types.ResourceRecord{
							{Value: aws.String("10 mail.mx.example.com")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	found := false
	for _, rr := range listOut.ResourceRecordSets {
		if rr.Type == r53types.RRTypeMx {
			found = true
			require.NotEmpty(t, rr.ResourceRecords)
			assert.Contains(t, aws.ToString(rr.ResourceRecords[0].Value), "mail.mx.example.com")
		}
	}
	assert.True(t, found, "MX record should be present")
}

// TestRoute53_RecordType_CNAME creates a CNAME record and verifies the value is correct.
func TestRoute53_RecordType_CNAME(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	zoneOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("cname.example.com"),
		CallerReference: aws.String("cname-ref-1"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionCreate,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name: aws.String("alias.cname.example.com"),
						Type: r53types.RRTypeCname,
						TTL:  aws.Int64(300),
						ResourceRecords: []r53types.ResourceRecord{
							{Value: aws.String("target.cname.example.com")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListResourceRecordSets(ctx, &awsroute53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	found := false
	for _, rr := range listOut.ResourceRecordSets {
		if rr.Type == r53types.RRTypeCname {
			found = true
			require.NotEmpty(t, rr.ResourceRecords)
			assert.Equal(t, "target.cname.example.com", aws.ToString(rr.ResourceRecords[0].Value))
		}
	}
	assert.True(t, found, "CNAME record should be present")
}

// TestRoute53_HealthCheck_CRUD exercises the full health check lifecycle:
// create, get, list, delete, verify gone.
func TestRoute53_HealthCheck_CRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	out, err := client.CreateHealthCheck(ctx, &awsroute53.CreateHealthCheckInput{
		CallerReference: aws.String("hc-crud-ref-1"),
		HealthCheckConfig: &r53types.HealthCheckConfig{
			Type:                     r53types.HealthCheckTypeHttp,
			FullyQualifiedDomainName: aws.String("health.example.com"),
			Port:                     aws.Int32(80),
			ResourcePath:             aws.String("/health"),
		},
	})
	require.NoError(t, err)
	hcID := aws.ToString(out.HealthCheck.Id)
	assert.NotEmpty(t, hcID)

	getOut, err := client.GetHealthCheck(ctx, &awsroute53.GetHealthCheckInput{
		HealthCheckId: aws.String(hcID),
	})
	require.NoError(t, err)
	assert.Equal(t, hcID, aws.ToString(getOut.HealthCheck.Id))
	assert.Equal(t, r53types.HealthCheckTypeHttp, getOut.HealthCheck.HealthCheckConfig.Type)

	listOut, err := client.ListHealthChecks(ctx, &awsroute53.ListHealthChecksInput{})
	require.NoError(t, err)
	found := false
	for _, hc := range listOut.HealthChecks {
		if aws.ToString(hc.Id) == hcID {
			found = true
		}
	}
	assert.True(t, found, "health check should appear in list")

	_, err = client.DeleteHealthCheck(ctx, &awsroute53.DeleteHealthCheckInput{
		HealthCheckId: aws.String(hcID),
	})
	require.NoError(t, err)

	listOut2, err := client.ListHealthChecks(ctx, &awsroute53.ListHealthChecksInput{})
	require.NoError(t, err)
	for _, hc := range listOut2.HealthChecks {
		assert.NotEqual(t, hcID, aws.ToString(hc.Id), "deleted health check should not appear in list")
	}
}

// TestRoute53_ListHostedZones_ThreeZonesPagination creates 3 zones then paginates with
// MaxItems=2, verifying all 3 zones are returned across pages.
func TestRoute53_ListHostedZones_ThreeZonesPagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	for i := 1; i <= 3; i++ {
		_, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
			Name:            aws.String(fmt.Sprintf("zone%d.example.com", i)),
			CallerReference: aws.String(fmt.Sprintf("page-ref-%d", i)),
		})
		require.NoError(t, err)
	}

	var allZones []r53types.HostedZone
	var marker *string
	for {
		in := &awsroute53.ListHostedZonesInput{MaxItems: aws.Int32(2)}
		if marker != nil {
			in.Marker = marker
		}
		page, err := client.ListHostedZones(ctx, in)
		require.NoError(t, err)
		allZones = append(allZones, page.HostedZones...)
		if !page.IsTruncated {
			break
		}
		marker = page.NextMarker
	}

	assert.Len(t, allZones, 3, "should list all 3 hosted zones across pages")
}
