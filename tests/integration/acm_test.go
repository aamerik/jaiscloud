package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsacm "github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── RequestCertificate / DescribeCertificate / DeleteCertificate ─────────────

func TestACM_RequestDescribeDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	// Request a certificate.
	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("example.com"),
	})
	require.NoError(t, err)
	arn := aws.ToString(reqOut.CertificateArn)
	require.NotEmpty(t, arn)

	// Describe it and check key fields.
	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)
	cert := descOut.Certificate
	assert.Equal(t, "example.com", aws.ToString(cert.DomainName))
	assert.Equal(t, acmtypes.CertificateStatusIssued, cert.Status)
	assert.Equal(t, acmtypes.KeyAlgorithmRsa2048, cert.KeyAlgorithm)
	require.NotEmpty(t, cert.DomainValidationOptions)
	dvo := cert.DomainValidationOptions[0]
	assert.Equal(t, "example.com", aws.ToString(dvo.DomainName))
	require.NotNil(t, dvo.ResourceRecord)
	assert.Equal(t, acmtypes.RecordTypeCname, dvo.ResourceRecord.Type)
	assert.NotEmpty(t, aws.ToString(dvo.ResourceRecord.Name))
	assert.NotEmpty(t, aws.ToString(dvo.ResourceRecord.Value))

	// Delete it — should succeed.
	_, err = c.DeleteCertificate(ctx, &awsacm.DeleteCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)

	// Describe after delete should return an error.
	_, err = c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.Error(t, err)
}

// ─── SubjectAlternativeNames ──────────────────────────────────────────────────

func TestACM_SubjectAlternativeNames(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName:              aws.String("primary.example.com"),
		SubjectAlternativeNames: []string{"san1.example.com", "san2.example.com"},
	})
	require.NoError(t, err)
	arn := aws.ToString(reqOut.CertificateArn)

	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)
	cert := descOut.Certificate

	// SubjectAlternativeNames must contain the two SANs.
	assert.Len(t, cert.SubjectAlternativeNames, 2)
	assert.Contains(t, cert.SubjectAlternativeNames, "san1.example.com")
	assert.Contains(t, cert.SubjectAlternativeNames, "san2.example.com")

	// DomainValidationOptions = 1 (primary) + 2 (SANs) = 3 entries.
	assert.Len(t, cert.DomainValidationOptions, 3)
}

// ─── ListCertificates ─────────────────────────────────────────────────────────

func TestACM_ListCertificates(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	// Create two certificates.
	r1, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("first.example.com"),
	})
	require.NoError(t, err)
	arn1 := aws.ToString(r1.CertificateArn)

	r2, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("second.example.com"),
	})
	require.NoError(t, err)
	arn2 := aws.ToString(r2.CertificateArn)

	// List and assert both ARNs are present.
	listOut, err := c.ListCertificates(ctx, &awsacm.ListCertificatesInput{})
	require.NoError(t, err)

	arns := make([]string, 0, len(listOut.CertificateSummaryList))
	for _, s := range listOut.CertificateSummaryList {
		arns = append(arns, aws.ToString(s.CertificateArn))
	}
	assert.Contains(t, arns, arn1)
	assert.Contains(t, arns, arn2)
}

// ─── DeleteCertificate — not found ───────────────────────────────────────────

func TestACM_DeleteNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	fakeARN := "arn:aws:acm:us-east-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"
	_, err := c.DeleteCertificate(ctx, &awsacm.DeleteCertificateInput{
		CertificateArn: aws.String(fakeARN),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// ─── ImportCertificate ────────────────────────────────────────────────────────

func TestACM_ImportCertificate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	// Import a certificate (provider accepts placeholder bytes).
	impOut, err := c.ImportCertificate(ctx, &awsacm.ImportCertificateInput{
		Certificate: []byte("-----BEGIN CERTIFICATE-----\nMIIFake\n-----END CERTIFICATE-----\n"),
		PrivateKey:  []byte("-----BEGIN RSA PRIVATE KEY-----\nMIIFake\n-----END RSA PRIVATE KEY-----\n"),
	})
	require.NoError(t, err)
	arn := aws.ToString(impOut.CertificateArn)
	require.NotEmpty(t, arn)

	// Describe should report Type=IMPORTED.
	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)
	assert.Equal(t, acmtypes.CertificateTypeImported, descOut.Certificate.Type)
}

// ─── Tags round-trip ──────────────────────────────────────────────────────────

func TestACM_TagsRoundtrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("tagged.example.com"),
	})
	require.NoError(t, err)
	arn := aws.ToString(reqOut.CertificateArn)

	// Add a tag.
	_, err = c.AddTagsToCertificate(ctx, &awsacm.AddTagsToCertificateInput{
		CertificateArn: aws.String(arn),
		Tags: []acmtypes.Tag{
			{Key: aws.String("Env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)

	// List tags — the tag must be present.
	listOut, err := c.ListTagsForCertificate(ctx, &awsacm.ListTagsForCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 1)
	assert.Equal(t, "Env", aws.ToString(listOut.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(listOut.Tags[0].Value))

	// Remove the tag.
	_, err = c.RemoveTagsFromCertificate(ctx, &awsacm.RemoveTagsFromCertificateInput{
		CertificateArn: aws.String(arn),
		Tags: []acmtypes.Tag{
			{Key: aws.String("Env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)

	// List tags again — must be empty.
	listOut2, err := c.ListTagsForCertificate(ctx, &awsacm.ListTagsForCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Tags)
}

// ─── ValidationMethod=EMAIL ───────────────────────────────────────────────────

func TestACM_ValidationMethod_Email(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName:       aws.String("emailval.example.com"),
		ValidationMethod: acmtypes.ValidationMethodEmail,
	})
	require.NoError(t, err)
	arn := aws.ToString(reqOut.CertificateArn)

	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)
	require.NotEmpty(t, descOut.Certificate.DomainValidationOptions)
	assert.Equal(t, acmtypes.ValidationMethodEmail, descOut.Certificate.DomainValidationOptions[0].ValidationMethod)
}
