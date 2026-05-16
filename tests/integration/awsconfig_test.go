package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfigservice "github.com/aws/aws-sdk-go-v2/service/configservice"
	configtypes "github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_PutAndDescribeRecorder(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newConfigServiceClient(t)

	// Put a configuration recorder
	_, err := client.PutConfigurationRecorder(ctx, &awsconfigservice.PutConfigurationRecorderInput{
		ConfigurationRecorder: &configtypes.ConfigurationRecorder{
			Name:    aws.String("my-recorder"),
			RoleARN: aws.String("arn:aws:iam::000000000000:role/config-role"),
			RecordingGroup: &configtypes.RecordingGroup{
				AllSupported: true,
			},
		},
	})
	require.NoError(t, err)

	// Describe recorders
	descOut, err := client.DescribeConfigurationRecorders(ctx, &awsconfigservice.DescribeConfigurationRecordersInput{})
	require.NoError(t, err)
	require.Len(t, descOut.ConfigurationRecorders, 1)
	assert.Equal(t, "my-recorder", aws.ToString(descOut.ConfigurationRecorders[0].Name))
	assert.Equal(t, "arn:aws:iam::000000000000:role/config-role", aws.ToString(descOut.ConfigurationRecorders[0].RoleARN))

	// Start recorder
	_, err = client.StartConfigurationRecorder(ctx, &awsconfigservice.StartConfigurationRecorderInput{
		ConfigurationRecorderName: aws.String("my-recorder"),
	})
	require.NoError(t, err)

	// Describe recorder status — should be recording
	statusOut, err := client.DescribeConfigurationRecorderStatus(ctx, &awsconfigservice.DescribeConfigurationRecorderStatusInput{})
	require.NoError(t, err)
	require.Len(t, statusOut.ConfigurationRecordersStatus, 1)
	assert.True(t, statusOut.ConfigurationRecordersStatus[0].Recording)

	// Stop recorder
	_, err = client.StopConfigurationRecorder(ctx, &awsconfigservice.StopConfigurationRecorderInput{
		ConfigurationRecorderName: aws.String("my-recorder"),
	})
	require.NoError(t, err)

	// Describe status again — should not be recording
	statusOut2, err := client.DescribeConfigurationRecorderStatus(ctx, &awsconfigservice.DescribeConfigurationRecorderStatusInput{})
	require.NoError(t, err)
	require.Len(t, statusOut2.ConfigurationRecordersStatus, 1)
	assert.False(t, statusOut2.ConfigurationRecordersStatus[0].Recording)
}

func TestConfig_PutConfigRule(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newConfigServiceClient(t)

	// Put a config rule
	_, err := client.PutConfigRule(ctx, &awsconfigservice.PutConfigRuleInput{
		ConfigRule: &configtypes.ConfigRule{
			ConfigRuleName: aws.String("my-config-rule"),
			Source: &configtypes.Source{
				Owner: configtypes.OwnerAws,
				SourceIdentifier: aws.String("RESTRICTED_SSH"),
			},
		},
	})
	require.NoError(t, err)

	// Describe config rules
	descOut, err := client.DescribeConfigRules(ctx, &awsconfigservice.DescribeConfigRulesInput{})
	require.NoError(t, err)
	require.Len(t, descOut.ConfigRules, 1)
	assert.Equal(t, "my-config-rule", aws.ToString(descOut.ConfigRules[0].ConfigRuleName))
	assert.NotEmpty(t, aws.ToString(descOut.ConfigRules[0].ConfigRuleArn))

	// Put another rule
	_, err = client.PutConfigRule(ctx, &awsconfigservice.PutConfigRuleInput{
		ConfigRule: &configtypes.ConfigRule{
			ConfigRuleName: aws.String("another-rule"),
			Source: &configtypes.Source{
				Owner: configtypes.OwnerAws,
				SourceIdentifier: aws.String("CLOUD_TRAIL_ENABLED"),
			},
		},
	})
	require.NoError(t, err)

	// Should now have 2 rules
	descOut2, err := client.DescribeConfigRules(ctx, &awsconfigservice.DescribeConfigRulesInput{})
	require.NoError(t, err)
	assert.Len(t, descOut2.ConfigRules, 2)

	// GetComplianceDetailsByConfigRule — should return empty results without error
	compOut, err := client.GetComplianceDetailsByConfigRule(ctx, &awsconfigservice.GetComplianceDetailsByConfigRuleInput{
		ConfigRuleName: aws.String("my-config-rule"),
	})
	require.NoError(t, err)
	assert.Empty(t, compOut.EvaluationResults)

	// DescribeConfigRuleEvaluationStatus — should return empty
	evalOut, err := client.DescribeConfigRuleEvaluationStatus(ctx, &awsconfigservice.DescribeConfigRuleEvaluationStatusInput{})
	require.NoError(t, err)
	assert.Empty(t, evalOut.ConfigRulesEvaluationStatus)
}

func TestConfig_PutDeliveryChannel(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newConfigServiceClient(t)

	// First create a recorder
	_, err := client.PutConfigurationRecorder(ctx, &awsconfigservice.PutConfigurationRecorderInput{
		ConfigurationRecorder: &configtypes.ConfigurationRecorder{
			Name:    aws.String("default"),
			RoleARN: aws.String("arn:aws:iam::000000000000:role/config-role"),
		},
	})
	require.NoError(t, err)

	// Put delivery channel
	_, err = client.PutDeliveryChannel(ctx, &awsconfigservice.PutDeliveryChannelInput{
		DeliveryChannel: &configtypes.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("my-config-bucket"),
		},
	})
	require.NoError(t, err)

	// Describe delivery channels
	descOut, err := client.DescribeDeliveryChannels(ctx, &awsconfigservice.DescribeDeliveryChannelsInput{})
	require.NoError(t, err)
	require.Len(t, descOut.DeliveryChannels, 1)
	assert.Equal(t, "default", aws.ToString(descOut.DeliveryChannels[0].Name))
	assert.Equal(t, "my-config-bucket", aws.ToString(descOut.DeliveryChannels[0].S3BucketName))
}
