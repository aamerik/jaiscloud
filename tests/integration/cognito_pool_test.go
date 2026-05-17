// Package integration provides Cognito User Pool round-trip integration tests.
package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscognitoidp "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCognito_CreateUserPool(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIDPClient(t)

	out, err := c.CreateUserPool(ctx, &awscognitoidp.CreateUserPoolInput{
		PoolName: aws.String("test-pool"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.UserPool)
	assert.NotEmpty(t, aws.ToString(out.UserPool.Id))
	assert.Equal(t, "test-pool", aws.ToString(out.UserPool.Name))
}

func TestCognito_CreateUserPoolClient(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIDPClient(t)

	poolOut, err := c.CreateUserPool(ctx, &awscognitoidp.CreateUserPoolInput{
		PoolName: aws.String("client-test-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(poolOut.UserPool.Id)

	clientOut, err := c.CreateUserPoolClient(ctx, &awscognitoidp.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("my-app-client"),
	})
	require.NoError(t, err)
	require.NotNil(t, clientOut.UserPoolClient)
	assert.NotEmpty(t, aws.ToString(clientOut.UserPoolClient.ClientId))
	assert.Equal(t, "my-app-client", aws.ToString(clientOut.UserPoolClient.ClientName))
}

func TestCognito_AdminCreateUser(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCognitoIDPClient(t)

	poolOut, err := c.CreateUserPool(ctx, &awscognitoidp.CreateUserPoolInput{
		PoolName: aws.String("user-test-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(poolOut.UserPool.Id)

	_, err = c.AdminCreateUser(ctx, &awscognitoidp.AdminCreateUserInput{
		UserPoolId:        aws.String(poolID),
		Username:          aws.String("alice"),
		TemporaryPassword: aws.String("TempPass1!"),
	})
	require.NoError(t, err)

	listOut, err := c.ListUsers(ctx, &awscognitoidp.ListUsersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.Users)

	found := false
	for _, u := range listOut.Users {
		if aws.ToString(u.Username) == "alice" {
			found = true
			break
		}
	}
	assert.True(t, found, "user alice should be present in list")
}
