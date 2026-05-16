package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/model"
)

// NewSQSQueueHandler returns a ResourceHandler for AWS::SQS::Queue.
func NewSQSQueueHandler(queueP *queue.QueueProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "QueueName", logicalID)
			resp, err := queueP.CreateQueue(ctx, child(nr, map[string]any{"QueueName": name}))
			if err != nil {
				return "", nil, err
			}
			url := resp.Data["QueueUrl"].(string)
			arn := nr.ResourceID("sqs-queue", name)
			return url, map[string]any{"QueueUrl": url, "Arn": arn}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := queueP.DeleteQueue(ctx, &model.NormalizedRequest{Params: map[string]any{"QueueUrl": physicalID}})
			return err
		},
	}
}
