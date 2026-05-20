package services_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"jaiscloud/internal/aws/adapter/services"
)

func TestSNSAddPermissionWireParse(t *testing.T) {
	codec := &services.SNSCodec{}
	body := []byte(
		"Action=AddPermission" +
			"&TopicArn=arn:aws:sns:us-east-1:000000000000:test-topic" +
			"&Label=test" +
			"&AWSAccountId.member.1=111111111111" +
			"&AWSAccountId.member.2=222222222222" +
			"&ActionName.member.1=Publish" +
			"&ActionName.member.2=Subscribe",
	)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	nr, err := codec.Decode(req, body)
	require.NoError(t, err)
	assert.Equal(t, "sns", nr.Service)
	assert.Equal(t, "AddPermission", nr.Action)

	accts, ok := nr.Params["AWSAccountId"].([]string)
	require.True(t, ok, "AWSAccountId should be []string")
	assert.Equal(t, []string{"111111111111", "222222222222"}, accts)

	actions, ok := nr.Params["ActionName"].([]string)
	require.True(t, ok, "ActionName should be []string")
	assert.Equal(t, []string{"Publish", "Subscribe"}, actions)

	assert.Equal(t, "test", nr.Params["Label"])
}

func TestSNSAddPermissionWireParse_Single(t *testing.T) {
	codec := &services.SNSCodec{}
	body := []byte(
		"Action=AddPermission" +
			"&TopicArn=arn:aws:sns:us-east-1:000000000000:test-topic" +
			"&Label=single" +
			"&AWSAccountId.member.1=111111111111" +
			"&ActionName.member.1=Publish",
	)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	nr, err := codec.Decode(req, body)
	require.NoError(t, err)

	accts, ok := nr.Params["AWSAccountId"].([]string)
	require.True(t, ok)
	assert.Equal(t, []string{"111111111111"}, accts)

	actions, ok := nr.Params["ActionName"].([]string)
	require.True(t, ok)
	assert.Equal(t, []string{"Publish"}, actions)
}
