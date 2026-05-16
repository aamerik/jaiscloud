package integration_test

import (
	"context"
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
