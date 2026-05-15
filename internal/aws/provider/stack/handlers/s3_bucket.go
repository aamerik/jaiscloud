package handlers

import (
	"context"
	"strings"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	objectprovider "jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/model"
)

// NewS3BucketHandler returns a ResourceHandler for AWS::S3::Bucket.
func NewS3BucketHandler(objectP *objectprovider.ObjectProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			stackName, _ := nr.Params["StackName"].(string)
			defaultName := logicalID
			if stackName != "" {
				defaultName = strings.ToLower(stackName + "-" + logicalID)
			}
			name := propStr(props, "BucketName", defaultName)
			if _, err := objectP.CreateBucket(ctx, child(nr, map[string]any{"_bucket": name})); err != nil {
				return "", nil, err
			}
			arn := "arn:aws:s3:::" + name
			return name, map[string]any{"Arn": arn, "DomainName": name + ".s3.amazonaws.com"}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := objectP.DeleteBucket(ctx, &model.NormalizedRequest{Params: map[string]any{"_bucket": physicalID}})
			return err
		},
	}
}
