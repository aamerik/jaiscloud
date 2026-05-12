package rds

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtDBSnapshot = "rds_db_snapshot"
	rtRDSTags    = "rds_tags"
)

type dbSnapshot struct {
	DBSnapshotIdentifier string    `json:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string    `json:"DBInstanceIdentifier"`
	SnapshotType         string    `json:"SnapshotType"`
	Status               string    `json:"Status"`
	Engine               string    `json:"Engine"`
	EngineVersion        string    `json:"EngineVersion"`
	AllocatedStorage     int       `json:"AllocatedStorage"`
	ARN                  string    `json:"ARN"`
	SnapshotCreateTime   time.Time `json:"SnapshotCreateTime"`
}

func (s dbSnapshot) toWire() map[string]any {
	return map[string]any{
		"DBSnapshotIdentifier": s.DBSnapshotIdentifier,
		"DBInstanceIdentifier": s.DBInstanceIdentifier,
		"SnapshotType":         s.SnapshotType,
		"Status":               s.Status,
		"Engine":               s.Engine,
		"EngineVersion":        s.EngineVersion,
		"AllocatedStorage":     fmt.Sprintf("%d", s.AllocatedStorage),
		"DBSnapshotArn":        s.ARN,
		"SnapshotCreateTime":   s.SnapshotCreateTime.Format(time.RFC3339),
	}
}

func (p *RelationalProvider) CreateDBSnapshot(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	snapID := strParam(nr.Params, "DBSnapshotIdentifier")
	instanceID := strParam(nr.Params, "DBInstanceIdentifier")
	if snapID == "" || instanceID == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "DBSnapshotIdentifier and DBInstanceIdentifier are required", HTTPStatus: http.StatusBadRequest}
	}
	// Source instance must exist
	e, err := p.resources.Get(ctx, rtDBInstance, instanceID)
	if err != nil {
		return nil, &model.ProviderError{Code: "DBInstanceNotFound", Message: "DB instance " + instanceID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var inst dbInstance
	_ = json.Unmarshal(e.Data, &inst)

	snap := dbSnapshot{
		DBSnapshotIdentifier: snapID,
		DBInstanceIdentifier: instanceID,
		SnapshotType:         "manual",
		Status:               "available",
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		AllocatedStorage:     inst.AllocatedStorage,
		ARN:                  nr.ResourceID("rds-snapshot", snapID),
		SnapshotCreateTime:   time.Now().UTC(),
	}
	data, _ := json.Marshal(snap)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDBSnapshot, ID: snapID, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DBSnapshotAlreadyExists", Message: "snapshot " + snapID + " already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"DBSnapshot": snap.toWire()}), nil
}

func (p *RelationalProvider) DescribeDBSnapshots(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	snapID := strParam(nr.Params, "DBSnapshotIdentifier")
	instanceID := strParam(nr.Params, "DBInstanceIdentifier")
	entries, _ := p.resources.List(ctx, rtDBSnapshot, "")
	var snaps []map[string]any
	for _, e := range entries {
		var s dbSnapshot
		if json.Unmarshal(e.Data, &s) != nil {
			continue
		}
		if snapID != "" && s.DBSnapshotIdentifier != snapID {
			continue
		}
		if instanceID != "" && s.DBInstanceIdentifier != instanceID {
			continue
		}
		snaps = append(snaps, s.toWire())
	}
	if snaps == nil {
		snaps = []map[string]any{}
	}
	return provider.OK(map[string]any{"DBSnapshots": snaps}), nil
}

func (p *RelationalProvider) DeleteDBSnapshot(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	snapID := strParam(nr.Params, "DBSnapshotIdentifier")
	e, err := p.resources.Get(ctx, rtDBSnapshot, snapID)
	if err != nil {
		return nil, &model.ProviderError{Code: "DBSnapshotNotFound", Message: "snapshot " + snapID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var s dbSnapshot
	_ = json.Unmarshal(e.Data, &s)
	_ = p.resources.Delete(ctx, rtDBSnapshot, snapID)
	return provider.OK(map[string]any{"DBSnapshot": s.toWire()}), nil
}

func (p *RelationalProvider) CopyDBSnapshot(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	srcID := strParam(nr.Params, "SourceDBSnapshotIdentifier")
	tgtID := strParam(nr.Params, "TargetDBSnapshotIdentifier")
	if srcID == "" || tgtID == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "SourceDBSnapshotIdentifier and TargetDBSnapshotIdentifier are required", HTTPStatus: http.StatusBadRequest}
	}
	e, err := p.resources.Get(ctx, rtDBSnapshot, srcID)
	if err != nil {
		return nil, &model.ProviderError{Code: "DBSnapshotNotFound", Message: "source snapshot " + srcID + " not found", HTTPStatus: http.StatusNotFound}
	}
	var src dbSnapshot
	_ = json.Unmarshal(e.Data, &src)

	snap := dbSnapshot{
		DBSnapshotIdentifier: tgtID,
		DBInstanceIdentifier: src.DBInstanceIdentifier,
		SnapshotType:         "manual",
		Status:               "available",
		Engine:               src.Engine,
		EngineVersion:        src.EngineVersion,
		AllocatedStorage:     src.AllocatedStorage,
		ARN:                  nr.ResourceID("rds-snapshot", tgtID),
		SnapshotCreateTime:   time.Now().UTC(),
	}
	data, _ := json.Marshal(snap)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDBSnapshot, ID: tgtID, Data: data}); err == store.ErrAlreadyExists {
		return nil, &model.ProviderError{Code: "DBSnapshotAlreadyExists", Message: "snapshot " + tgtID + " already exists", HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(map[string]any{"DBSnapshot": snap.toWire()}), nil
}

// ─── Tagging ──────────────────────────────────────────────────────────────────

// extractRDSTags reads Tags.Tag.N.Key / Tags.Tag.N.Value from Query-protocol params.
func extractRDSTags(params map[string]any) map[string]string {
	tags := map[string]string{}
	for i := 1; ; i++ {
		k := strParam(params, fmt.Sprintf("Tags.Tag.%d.Key", i))
		if k == "" {
			break
		}
		tags[k] = strParam(params, fmt.Sprintf("Tags.Tag.%d.Value", i))
	}
	return tags
}

func extractRDSTagKeys(params map[string]any) []string {
	var keys []string
	for i := 1; ; i++ {
		k := strParam(params, fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	return keys
}

func loadRDSTags(ctx context.Context, res store.ResourceStore, arn string) map[string]string {
	e, err := res.Get(ctx, rtRDSTags, arn)
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	_ = json.Unmarshal(e.Data, &m)
	return m
}

func saveRDSTags(ctx context.Context, res store.ResourceStore, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtRDSTags, ID: arn, Data: data}
	if err := res.Create(ctx, entry); err == store.ErrAlreadyExists {
		res.Update(ctx, entry)
	}
}

func (p *RelationalProvider) AddTagsToResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceName")
	newTags := extractRDSTags(nr.Params)
	existing := loadRDSTags(ctx, p.resources, arn)
	for k, v := range newTags {
		existing[k] = v
	}
	saveRDSTags(ctx, p.resources, arn, existing)
	return provider.OK(map[string]any{}), nil
}

func (p *RelationalProvider) RemoveTagsFromResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceName")
	keys := extractRDSTagKeys(nr.Params)
	existing := loadRDSTags(ctx, p.resources, arn)
	for _, k := range keys {
		delete(existing, k)
	}
	saveRDSTags(ctx, p.resources, arn, existing)
	return provider.OK(map[string]any{}), nil
}

func (p *RelationalProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceName")
	tags := loadRDSTags(ctx, p.resources, arn)
	list := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"TagList": list}), nil
}

// ─── DB Parameter Groups ──────────────────────────────────────────────────────

type dbParameterGroup struct {
	DBParameterGroupName   string `json:"DBParameterGroupName"`
	DBParameterGroupFamily string `json:"DBParameterGroupFamily"`
	Description            string `json:"Description"`
	DBParameterGroupArn    string `json:"DBParameterGroupArn"`
}

func (g dbParameterGroup) toWire() map[string]any {
	return map[string]any{
		"DBParameterGroupName":   g.DBParameterGroupName,
		"DBParameterGroupFamily": g.DBParameterGroupFamily,
		"Description":            g.Description,
		"DBParameterGroupArn":    g.DBParameterGroupArn,
	}
}

func (p *RelationalProvider) CreateDBParameterGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "DBParameterGroupName")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "DBParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	grp := dbParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: strParam(nr.Params, "DBParameterGroupFamily"),
		Description:            strParam(nr.Params, "Description"),
		DBParameterGroupArn:    nr.ResourceID("pg", name),
	}
	data, _ := json.Marshal(grp)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDBParameterGroup, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DBParameterGroupAlreadyExists", Message: "DB parameter group already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"DBParameterGroup": grp.toWire()}), nil
}

func (p *RelationalProvider) DescribeDBParameterGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "DBParameterGroupName")
	if name != "" {
		e, err := p.resources.Get(ctx, rtDBParameterGroup, name)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "DBParameterGroupNotFound", Message: "DB parameter group not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var grp dbParameterGroup
		json.Unmarshal(e.Data, &grp)
		return provider.OK(map[string]any{"DBParameterGroups": []any{grp.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtDBParameterGroup, "")
	if err != nil {
		return nil, err
	}
	groups := make([]any, 0, len(entries))
	for _, e := range entries {
		var grp dbParameterGroup
		json.Unmarshal(e.Data, &grp)
		groups = append(groups, grp.toWire())
	}
	return provider.OK(map[string]any{"DBParameterGroups": groups}), nil
}

func (p *RelationalProvider) DeleteDBParameterGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "DBParameterGroupName")
	if err := p.resources.Delete(ctx, rtDBParameterGroup, name); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "DBParameterGroupNotFound", Message: "DB parameter group not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *RelationalProvider) DeleteDBParameterGroupIgnoreNotFound(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.DeleteDBParameterGroup(ctx, nr)
}
