package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	keyprovider "jaiscloud/internal/aws/key"
	"jaiscloud/internal/model"
)

// NewKMSKeyHandler returns a ResourceHandler for AWS::KMS::Key.
func NewKMSKeyHandler(keyP *keyprovider.KeyProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := keyP.CreateKey(ctx, child(nr, params))
			if err != nil {
				return "", nil, err
			}
			keyID, arn := "", ""
			if km, ok := resp.Data["KeyMetadata"].(map[string]any); ok {
				keyID, _ = km["KeyId"].(string)
				arn, _ = km["Arn"].(string)
			}
			return keyID, map[string]any{"Arn": arn, "KeyId": keyID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := keyP.ScheduleKeyDeletion(ctx, &model.NormalizedRequest{
				Params: map[string]any{"KeyId": physicalID, "PendingWindowInDays": float64(7)},
			})
			return err
		},
	}
}
