// Package integration provides ACM (Certificate Manager) round-trip integration tests.
package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsacm "github.com/aws/aws-sdk-go-v2/service/acm"
	awsacmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACM_RequestAndListCertificates(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName:              aws.String("example.com"),
		SubjectAlternativeNames: []string{"www.example.com", "api.example.com"},
		ValidationMethod:        awsacmtypes.ValidationMethodDns,
	})
	require.NoError(t, err)
	certARN := aws.ToString(reqOut.CertificateArn)
	assert.NotEmpty(t, certARN)

	listOut, err := c.ListCertificates(ctx, &awsacm.ListCertificatesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.CertificateSummaryList)

	found := false
	for _, cert := range listOut.CertificateSummaryList {
		if aws.ToString(cert.CertificateArn) == certARN {
			found = true
			break
		}
	}
	assert.True(t, found, "certificate should appear in ListCertificates")
}

func TestACM_DescribeCertificate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("test.example.com"),
	})
	require.NoError(t, err)
	certARN := aws.ToString(reqOut.CertificateArn)

	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)
	assert.Equal(t, "test.example.com", aws.ToString(descOut.Certificate.DomainName))
}

func TestACM_DeleteCertificate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("delete.example.com"),
	})
	require.NoError(t, err)
	certARN := aws.ToString(reqOut.CertificateArn)

	_, err = c.DeleteCertificate(ctx, &awsacm.DeleteCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)

	_, err = c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

// TestACMRequestAndDescribe verifies that DescribeCertificate returns full metadata
// including Status=="ISSUED", populated DomainValidationOptions with ResourceRecord.
func TestACMRequestAndDescribe(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName:       aws.String("example.com"),
		ValidationMethod: awsacmtypes.ValidationMethodDns,
	})
	require.NoError(t, err)
	certARN := aws.ToString(reqOut.CertificateArn)
	assert.NotEmpty(t, certARN)

	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)

	cert := descOut.Certificate
	assert.Equal(t, awsacmtypes.CertificateStatusIssued, cert.Status,
		"emulator should auto-issue certificates as ISSUED")
	assert.NotEmpty(t, cert.DomainValidationOptions,
		"DomainValidationOptions must be non-empty")
	assert.Equal(t, awsacmtypes.DomainStatusSuccess, cert.DomainValidationOptions[0].ValidationStatus,
		"ValidationStatus should be SUCCESS")
	require.NotNil(t, cert.DomainValidationOptions[0].ResourceRecord,
		"ResourceRecord must be present for DNS validation")
	assert.Equal(t, awsacmtypes.RecordTypeCname, cert.DomainValidationOptions[0].ResourceRecord.Type,
		"ResourceRecord.Type should be CNAME")
	assert.NotEmpty(t, aws.ToString(cert.DomainValidationOptions[0].ResourceRecord.Name))
	assert.NotEmpty(t, aws.ToString(cert.DomainValidationOptions[0].ResourceRecord.Value))

	// Timestamps must be non-nil (they would be nil if wire format is wrong int vs float).
	assert.NotNil(t, cert.CreatedAt, "CreatedAt should be non-nil")
	assert.NotNil(t, cert.IssuedAt, "IssuedAt should be non-nil")
	assert.NotNil(t, cert.NotBefore, "NotBefore should be non-nil")
	assert.NotNil(t, cert.NotAfter, "NotAfter should be non-nil")
}

// TestACMDomainValidationOptionsIncludesSANs verifies that all SANs get their own
// DomainValidationOption entry, each with a ResourceRecord.
func TestACMDomainValidationOptionsIncludesSANs(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName:              aws.String("example.com"),
		SubjectAlternativeNames: []string{"www.example.com", "api.example.com"},
		ValidationMethod:        awsacmtypes.ValidationMethodDns,
	})
	require.NoError(t, err)
	certARN := aws.ToString(reqOut.CertificateArn)

	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)

	// Expect 3 DomainValidationOptions: main domain + 2 SANs.
	dvos := descOut.Certificate.DomainValidationOptions
	assert.Len(t, dvos, 3, "should have one DomainValidationOption per domain+SAN")

	domainsSeen := map[string]bool{}
	for _, dvo := range dvos {
		domainsSeen[aws.ToString(dvo.DomainName)] = true
		assert.Equal(t, awsacmtypes.DomainStatusSuccess, dvo.ValidationStatus)
		require.NotNil(t, dvo.ResourceRecord)
		assert.Equal(t, awsacmtypes.RecordTypeCname, dvo.ResourceRecord.Type)
	}
	assert.True(t, domainsSeen["example.com"])
	assert.True(t, domainsSeen["www.example.com"])
	assert.True(t, domainsSeen["api.example.com"])
}

// TestACMListCertificates verifies that multiple certificates are all listed.
func TestACMListCertificates(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	for _, domain := range []string{"alpha.example.com", "beta.example.com", "gamma.example.com"} {
		_, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
			DomainName:       aws.String(domain),
			ValidationMethod: awsacmtypes.ValidationMethodDns,
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListCertificates(ctx, &awsacm.ListCertificatesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.CertificateSummaryList, 3, "should list all 3 certificates")
}
