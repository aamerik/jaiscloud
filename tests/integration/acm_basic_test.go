// Package integration provides ACM (Certificate Manager) round-trip integration tests.
// NOTE: ACM is not yet implemented in JaisCloud; these tests are skipped
// until the provider is added.
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
	t.Skip("ACM not yet implemented")
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
	t.Skip("ACM not yet implemented")
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
	t.Skip("ACM not yet implemented")
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
