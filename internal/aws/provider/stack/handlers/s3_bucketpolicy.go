package handlers

import (
	"context"
	"encoding/json"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	objectprovider "jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/model"
)

// NewS3BucketPolicyHandler returns a ResourceHandler for AWS::S3::BucketPolicy.
func NewS3BucketPolicyHandler(objectP *objectprovider.ObjectProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			bucket := propStr(props, "Bucket", "")
			// PolicyDocument may be a string or a map; normalise to JSON bytes.
			var policyBytes []byte
			switch p := props["PolicyDocument"].(type) {
			case string:
				policyBytes = []byte(p)
			default:
				policyBytes, _ = json.Marshal(p)
			}
			if _, err := objectP.PutBucketPolicy(ctx, child(nr, map[string]any{
				"_bucket": bucket,
				"_body":   policyBytes,
			})); err != nil {
				return "", nil, err
			}
			return bucket + "/policy", map[string]any{}, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			bucket := propStr(props, "Bucket", "")
			_, err := objectP.DeleteBucketPolicy(ctx, &model.NormalizedRequest{Params: map[string]any{"_bucket": bucket}})
			return err
		},
	}
}
