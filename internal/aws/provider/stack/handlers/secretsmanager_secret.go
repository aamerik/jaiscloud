package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	secretprovider "jaiscloud/internal/aws/secret"
	"jaiscloud/internal/model"
)

// NewSecretsManagerSecretHandler returns a ResourceHandler for AWS::SecretsManager::Secret.
func NewSecretsManagerSecretHandler(secretP *secretprovider.SecretProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["Name"] = name
			resp, err := secretP.CreateSecret(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["ARN"].(string)
			return arn, map[string]any{"Id": name}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := secretP.DeleteSecret(ctx, &model.NormalizedRequest{
				ResourceID: func(_, _ string) string { return physicalID },
				Params:     map[string]any{"SecretId": physicalID, "ForceDeleteWithoutRecovery": true},
			})
			return err
		},
	}
}
