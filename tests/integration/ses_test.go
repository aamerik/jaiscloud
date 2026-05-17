package integration_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── SES Basic ────────────────────────────────────────────────────────────────

func TestSES_SendEmail_Basic(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Verify the sender first.
	_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
		EmailAddress: aws.String("sender@example.com"),
	})
	require.NoError(t, err)

	// Send email — should succeed.
	out, err := c.SendEmail(ctx, &awsses.SendEmailInput{
		Source: aws.String("sender@example.com"),
		Destination: &sestypes.Destination{
			ToAddresses: []string{"recipient@example.com"},
		},
		Message: &sestypes.Message{
			Subject: &sestypes.Content{Data: aws.String("Hello")},
			Body: &sestypes.Body{
				Text: &sestypes.Content{Data: aws.String("Hello World")},
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.MessageId))
}

func TestSES_VerifyEmailIdentity(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
		EmailAddress: aws.String("verify@example.com"),
	})
	require.NoError(t, err)
}

func TestSES_ListIdentities(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Verify a couple of identities.
	for _, email := range []string{"a@example.com", "b@example.com"} {
		_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
			EmailAddress: aws.String(email),
		})
		require.NoError(t, err)
	}

	out, err := c.ListIdentities(ctx, &awsses.ListIdentitiesInput{})
	require.NoError(t, err)
	assert.Len(t, out.Identities, 2)
}

// ─── SendRawEmail ─────────────────────────────────────────────────────────────

func TestSES_SendRawEmail(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Build a minimal raw MIME message and base64-encode it.
	rawMsg := "From: raw@example.com\r\nTo: dest@example.com\r\nSubject: Raw Test\r\n\r\nHello raw world.\r\n"
	encoded := base64.StdEncoding.EncodeToString([]byte(rawMsg))

	out, err := c.SendRawEmail(ctx, &awsses.SendRawEmailInput{
		RawMessage: &sestypes.RawMessage{
			Data: []byte(encoded),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.MessageId))
}

// ─── GetIdentityVerificationAttributes ────────────────────────────────────────

func TestSES_GetIdentityVerificationAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	emails := []string{"alice@example.com", "bob@example.com"}
	for _, e := range emails {
		_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
			EmailAddress: aws.String(e),
		})
		require.NoError(t, err)
	}

	out, err := c.GetIdentityVerificationAttributes(ctx, &awsses.GetIdentityVerificationAttributesInput{
		Identities: emails,
	})
	require.NoError(t, err)
	require.Len(t, out.VerificationAttributes, 2)
	for _, e := range emails {
		attr, ok := out.VerificationAttributes[e]
		require.True(t, ok, "expected attribute for %s", e)
		assert.Equal(t, sestypes.VerificationStatusSuccess, attr.VerificationStatus)
	}
}

// ─── DeleteIdentity ───────────────────────────────────────────────────────────

func TestSES_DeleteIdentity(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Verify two identities.
	for _, e := range []string{"keep@example.com", "gone@example.com"} {
		_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
			EmailAddress: aws.String(e),
		})
		require.NoError(t, err)
	}

	// Confirm both are listed.
	listOut, err := c.ListIdentities(ctx, &awsses.ListIdentitiesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Identities, 2)

	// Delete one.
	_, err = c.DeleteIdentity(ctx, &awsses.DeleteIdentityInput{
		Identity: aws.String("gone@example.com"),
	})
	require.NoError(t, err)

	// Only one should remain.
	listOut2, err := c.ListIdentities(ctx, &awsses.ListIdentitiesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut2.Identities, 1)
	assert.Equal(t, "keep@example.com", listOut2.Identities[0])
}

// ─── GetSendQuota ─────────────────────────────────────────────────────────────

func TestSES_GetSendQuota(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	out, err := c.GetSendQuota(ctx, &awsses.GetSendQuotaInput{})
	require.NoError(t, err)
	assert.Greater(t, out.Max24HourSend, float64(0))
	assert.Greater(t, out.MaxSendRate, float64(0))
}

// ─── GetSendStatistics ────────────────────────────────────────────────────────

func TestSES_GetSendStatistics(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Verify sender and send an email so there is activity.
	_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
		EmailAddress: aws.String("stats@example.com"),
	})
	require.NoError(t, err)

	_, err = c.SendEmail(ctx, &awsses.SendEmailInput{
		Source: aws.String("stats@example.com"),
		Destination: &sestypes.Destination{
			ToAddresses: []string{"dest@example.com"},
		},
		Message: &sestypes.Message{
			Subject: &sestypes.Content{Data: aws.String("Stats test")},
			Body: &sestypes.Body{
				Text: &sestypes.Content{Data: aws.String("body")},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.GetSendStatistics(ctx, &awsses.GetSendStatisticsInput{})
	require.NoError(t, err)
	// SendDataPoints may be empty (provider returns an empty slice), but the
	// response itself must be non-nil and return without error.
	assert.NotNil(t, out)
}

// ─── ListIdentities with IdentityType filter ──────────────────────────────────

func TestSES_ListIdentities_ByType(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Verify one email address and one domain.
	_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
		EmailAddress: aws.String("user@filter.com"),
	})
	require.NoError(t, err)

	_, err = c.VerifyDomainIdentity(ctx, &awsses.VerifyDomainIdentityInput{
		Domain: aws.String("filterdomain.com"),
	})
	require.NoError(t, err)

	// Filter by EmailAddress — only the email identity should be returned.
	emailOut, err := c.ListIdentities(ctx, &awsses.ListIdentitiesInput{
		IdentityType: sestypes.IdentityTypeEmailAddress,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"user@filter.com"}, emailOut.Identities)

	// Filter by Domain — only the domain identity should be returned.
	domainOut, err := c.ListIdentities(ctx, &awsses.ListIdentitiesInput{
		IdentityType: sestypes.IdentityTypeDomain,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"filterdomain.com"}, domainOut.Identities)
}

// ─── H-PENDING-24: Verified-sender enforcement ───────────────────────────────

func TestSES_VerifiedSenderEnforcement(t *testing.T) {
	t.Run("unverified sender rejected", func(t *testing.T) {
		resetState(t)
		ctx := context.Background()
		c := newSESClient(t)

		// Sending from an unverified address should fail.
		_, err := c.SendEmail(ctx, &awsses.SendEmailInput{
			Source: aws.String("nobody@notverified.com"),
			Destination: &sestypes.Destination{
				ToAddresses: []string{"recipient@example.com"},
			},
			Message: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String("Test")},
				Body: &sestypes.Body{
					Text: &sestypes.Content{Data: aws.String("body")},
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not verified")
	})

	t.Run("verified email sender allowed", func(t *testing.T) {
		resetState(t)
		ctx := context.Background()
		c := newSESClient(t)

		// Verify the sender.
		_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
			EmailAddress: aws.String("verified@example.com"),
		})
		require.NoError(t, err)

		// Now sending should succeed.
		out, err := c.SendEmail(ctx, &awsses.SendEmailInput{
			Source: aws.String("verified@example.com"),
			Destination: &sestypes.Destination{
				ToAddresses: []string{"dest@example.com"},
			},
			Message: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String("Hello")},
				Body: &sestypes.Body{
					Text: &sestypes.Content{Data: aws.String("Body text")},
				},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(out.MessageId))
	})

	t.Run("verified domain allows any email from that domain", func(t *testing.T) {
		resetState(t)
		ctx := context.Background()
		c := newSESClient(t)

		// Verify the domain (not a specific address).
		_, err := c.VerifyDomainIdentity(ctx, &awsses.VerifyDomainIdentityInput{
			Domain: aws.String("mydomain.com"),
		})
		require.NoError(t, err)

		// Any address @mydomain.com should now be allowed.
		out, err := c.SendEmail(ctx, &awsses.SendEmailInput{
			Source: aws.String("any@mydomain.com"),
			Destination: &sestypes.Destination{
				ToAddresses: []string{"dest@example.com"},
			},
			Message: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String("Test")},
				Body: &sestypes.Body{
					Text: &sestypes.Content{Data: aws.String("Body")},
				},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(out.MessageId))
	})
}
