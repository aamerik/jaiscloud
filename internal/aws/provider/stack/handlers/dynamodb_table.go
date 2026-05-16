package handlers

import (
	"context"

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
			arn := nr.ResourceID("dynamodb-table", name)
			return name, map[string]any{"Arn": arn, "StreamArn": ""}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "TableName", logicalID) != propStr(newProps, "TableName", logicalID) {
				return "", nil, true, nil
			}
			// KeySchema changes require replacement
			// For in-place update forward to UpdateTable
			params := map[string]any{"TableName": physicalID}
			if v, ok := newProps["ProvisionedThroughput"]; ok {
				params["ProvisionedThroughput"] = v
			}
			if v, ok := newProps["GlobalSecondaryIndexes"]; ok {
				params["GlobalSecondaryIndexUpdates"] = v
			}
			if v, ok := newProps["StreamSpecification"]; ok {
				params["StreamSpecification"] = v
			}
			if _, err := tableP.UpdateTable(ctx, child(nr, params)); err != nil {
				return "", nil, false, err
			}
			arn := nr.ResourceID("dynamodb-table", physicalID)
			return physicalID, map[string]any{"Arn": arn, "StreamArn": ""}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := tableP.DeleteTable(ctx, &model.NormalizedRequest{Params: map[string]any{"TableName": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn", "StreamArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"TableName", "KeySchema", "BillingMode"},
			RequireUpdate:      []string{"ProvisionedThroughput", "GlobalSecondaryIndexes", "Tags", "StreamSpecification"},
		},
	}
}
