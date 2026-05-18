package rds

import (
	"context"
	"encoding/json"
	"net/http"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func (p *RelationalProvider) RestoreDBInstanceFromDBSnapshot(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	newID := strParam(nr.Params, "DBInstanceIdentifier")
	snapID := strParam(nr.Params, "DBSnapshotIdentifier")
	if newID == "" || snapID == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "DBInstanceIdentifier and DBSnapshotIdentifier are required", HTTPStatus: http.StatusBadRequest}
	}

	// Load snapshot
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtDBSnapshot, snapID)
	if err != nil {
		return nil, &model.ProviderError{Code: "DBSnapshotNotFound", Message: "Snapshot not found: " + snapID, HTTPStatus: http.StatusNotFound}
	}
	var snap dbSnapshot
	json.Unmarshal(e.Data, &snap)

	// Create new DB instance from snapshot data
	inst := dbInstance{
		DBInstanceIdentifier: newID,
		DBInstanceClass:      strParam(nr.Params, "DBInstanceClass"),
		Engine:               snap.Engine,
		DBInstanceStatus:     "available",
		EngineVersion:        snap.EngineVersion,
		AllocatedStorage:     snap.AllocatedStorage,
		DBInstanceArn:        nr.ResourceID("rds-instance", newID),
	}
	if inst.DBInstanceClass == "" {
		inst.DBInstanceClass = "db.t3.micro"
	}

	data, _ := json.Marshal(inst)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtDBInstance, ID: newID, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DBInstanceAlreadyExists", Message: "DB instance " + newID + " already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"DBInstance": inst.toWire()}), nil
}
