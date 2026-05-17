package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscognitoidentity "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCognitoIdentity_CreateDescribeDelete creates a pool, describes it, deletes it,
// then verifies that a second describe returns an error.
func TestCognitoIdentity_CreateDescribeDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("my-fed-pool"),
		AllowUnauthenticatedIdentities: false,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut)
	poolID := aws.ToString(createOut.IdentityPoolId)
	require.NotEmpty(t, poolID)
	assert.Equal(t, "my-fed-pool", aws.ToString(createOut.IdentityPoolName))

	// Describe the pool — name must match.
	descOut, err := c.DescribeIdentityPool(ctx, &awscognitoidentity.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-fed-pool", aws.ToString(descOut.IdentityPoolName))
	assert.Equal(t, poolID, aws.ToString(descOut.IdentityPoolId))

	// Delete the pool.
	_, err = c.DeleteIdentityPool(ctx, &awscognitoidentity.DeleteIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)

	// Describe after delete must return an error.
	_, err = c.DescribeIdentityPool(ctx, &awscognitoidentity.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.Error(t, err, "DescribeIdentityPool should fail after deletion")
}

// TestCognitoIdentity_AllowUnauthenticated verifies that AllowUnauthenticatedIdentities
// is preserved on the pool.
func TestCognitoIdentity_AllowUnauthenticated(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("unauth-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)
	poolID := aws.ToString(createOut.IdentityPoolId)
	require.NotEmpty(t, poolID)

	descOut, err := c.DescribeIdentityPool(ctx, &awscognitoidentity.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.True(t, descOut.AllowUnauthenticatedIdentities,
		"AllowUnauthenticatedIdentities should be true after creation with true")
}

// TestCognitoIdentity_ListPools creates two pools and asserts both appear in ListIdentityPools.
func TestCognitoIdentity_ListPools(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	out1, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("list-pool-a"),
		AllowUnauthenticatedIdentities: false,
	})
	require.NoError(t, err)
	poolID1 := aws.ToString(out1.IdentityPoolId)

	out2, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("list-pool-b"),
		AllowUnauthenticatedIdentities: false,
	})
	require.NoError(t, err)
	poolID2 := aws.ToString(out2.IdentityPoolId)

	listOut, err := c.ListIdentityPools(ctx, &awscognitoidentity.ListIdentityPoolsInput{
		MaxResults: aws.Int32(60),
	})
	require.NoError(t, err)

	ids := make(map[string]bool)
	for _, p := range listOut.IdentityPools {
		ids[aws.ToString(p.IdentityPoolId)] = true
	}
	assert.True(t, ids[poolID1], "pool-a must be present in list")
	assert.True(t, ids[poolID2], "pool-b must be present in list")
}

// TestCognitoIdentity_UpdatePool creates a pool, renames it, and verifies the new name.
func TestCognitoIdentity_UpdatePool(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("original-pool"),
		AllowUnauthenticatedIdentities: false,
	})
	require.NoError(t, err)
	poolID := aws.ToString(createOut.IdentityPoolId)

	_, err = c.UpdateIdentityPool(ctx, &awscognitoidentity.UpdateIdentityPoolInput{
		IdentityPoolId:                 aws.String(poolID),
		IdentityPoolName:               aws.String("renamed-pool"),
		AllowUnauthenticatedIdentities: false,
	})
	require.NoError(t, err)

	descOut, err := c.DescribeIdentityPool(ctx, &awscognitoidentity.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed-pool", aws.ToString(descOut.IdentityPoolName),
		"pool name should reflect the update")
}

// TestCognitoIdentity_GetId creates a pool and calls GetId; asserts the returned
// IdentityId is non-empty and contains a colon (format: region:uuid).
func TestCognitoIdentity_GetId(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("getid-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)
	poolID := aws.ToString(createOut.IdentityPoolId)

	getIDOut, err := c.GetId(ctx, &awscognitoidentity.GetIdInput{
		AccountId:      aws.String("000000000000"),
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	identityID := aws.ToString(getIDOut.IdentityId)
	assert.NotEmpty(t, identityID, "IdentityId must be non-empty")
	assert.True(t, strings.Contains(identityID, ":"),
		"IdentityId must contain ':' separator (format: region:uuid), got: %s", identityID)
}

// TestCognitoIdentity_GetCredentials creates a pool, obtains an identity, then calls
// GetCredentialsForIdentity and asserts all credential fields are populated.
func TestCognitoIdentity_GetCredentials(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("creds-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)
	poolID := aws.ToString(createOut.IdentityPoolId)

	getIDOut, err := c.GetId(ctx, &awscognitoidentity.GetIdInput{
		AccountId:      aws.String("000000000000"),
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	identityID := aws.ToString(getIDOut.IdentityId)
	require.NotEmpty(t, identityID)

	credsOut, err := c.GetCredentialsForIdentity(ctx, &awscognitoidentity.GetCredentialsForIdentityInput{
		IdentityId: aws.String(identityID),
	})
	require.NoError(t, err)
	require.NotNil(t, credsOut.Credentials, "Credentials must not be nil")
	assert.NotEmpty(t, aws.ToString(credsOut.Credentials.AccessKeyId), "AccessKeyId must be set")
	assert.NotEmpty(t, aws.ToString(credsOut.Credentials.SecretKey), "SecretKey must be set")
	assert.NotEmpty(t, aws.ToString(credsOut.Credentials.SessionToken), "SessionToken must be set")
	require.NotNil(t, credsOut.Credentials.Expiration, "Expiration must not be nil")
	assert.True(t, credsOut.Credentials.Expiration.Unix() > 0, "Expiration must be a positive timestamp")
}

// TestCognitoIdentity_GetOpenIdToken creates a pool, obtains an identity, then calls
// GetOpenIdToken and asserts the Token is non-empty.
func TestCognitoIdentity_GetOpenIdToken(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("oidtoken-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)
	poolID := aws.ToString(createOut.IdentityPoolId)

	getIDOut, err := c.GetId(ctx, &awscognitoidentity.GetIdInput{
		AccountId:      aws.String("000000000000"),
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	identityID := aws.ToString(getIDOut.IdentityId)
	require.NotEmpty(t, identityID)

	tokenOut, err := c.GetOpenIdToken(ctx, &awscognitoidentity.GetOpenIdTokenInput{
		IdentityId: aws.String(identityID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(tokenOut.Token), "Token must be non-empty")
	assert.Equal(t, identityID, aws.ToString(tokenOut.IdentityId),
		"IdentityId in response must match the requested one")
}

// TestCognitoIdentity_DeleteNotFound verifies that deleting a pool with a fake ID
// returns an error.
func TestCognitoIdentity_DeleteNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	_, err := c.DeleteIdentityPool(ctx, &awscognitoidentity.DeleteIdentityPoolInput{
		IdentityPoolId: aws.String("us-east-1:00000000-0000-0000-0000-000000000000"),
	})
	require.Error(t, err, "DeleteIdentityPool with a non-existent pool must return an error")
}
