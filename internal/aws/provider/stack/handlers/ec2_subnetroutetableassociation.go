package handlers

import (
	"context"

	"jaiscloud/internal/aws/provider/compute"
	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2SubnetRouteTableAssociationHandler returns a ResourceHandler for AWS::EC2::SubnetRouteTableAssociation.
func NewEC2SubnetRouteTableAssociationHandler(computeP *compute.ComputeProvider) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			subnetID := propStr(props, "SubnetId", "")
			rtID := propStr(props, "RouteTableId", "")
			resp, err := computeP.AssociateRouteTable(ctx, child(nr, map[string]any{
				"SubnetId":     subnetID,
				"RouteTableId": rtID,
			}))
			if err != nil {
				return "", nil, err
			}
			assocID, _ := resp.Data["AssociationId"].(string)
			if assocID == "" {
				assocID = subnetID + "/" + rtID
			}
			return assocID, map[string]any{"AssociationId": assocID}, nil
		},
		// Associations are immutable — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		// AssociateRouteTable has no corresponding Disassociate in compute.go; no-op on delete.
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			return nil
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"SubnetId", "RouteTableId"},
		},
	}
}
