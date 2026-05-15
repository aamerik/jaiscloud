package handlers

import (
	"context"
	"fmt"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/table"
	"jaiscloud/internal/model"
)

// NewDynamoDBTableHandler returns a ResourceHandler for AWS::DynamoDB::Table.
func NewDynamoDBTableHandler(tableP *table.TableProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "TableName", logicalID)
			params := copyProps(props)
			params["TableName"] = name
			if _, err := tableP.CreateTable(ctx, child(nr, params)); err != nil {
				return "", nil, err
			}
			arn := fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", nr.Region, nr.AccountID, name)
			return name, map[string]any{"Arn": arn, "StreamArn": ""}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := tableP.DeleteTable(ctx, &model.NormalizedRequest{Params: map[string]any{"TableName": physicalID}})
			return err
		},
	}
}
