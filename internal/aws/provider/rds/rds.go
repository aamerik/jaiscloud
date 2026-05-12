// Package rds implements the RDS provider (RelationalProvider).
package rds

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// RelationalProvider handles RDS DB instances, clusters, and subnet groups.
type RelationalProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *RelationalProvider {
	return &RelationalProvider{resources: resources}
}

func (p *RelationalProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"RDS.CreateDBInstance":      p.CreateDBInstance,
		"RDS.DescribeDBInstances":   p.DescribeDBInstances,
		"RDS.ModifyDBInstance":      p.ModifyDBInstance,
		"RDS.DeleteDBInstance":      p.DeleteDBInstance,
		"RDS.CreateDBCluster":       p.CreateDBCluster,
		"RDS.DescribeDBClusters":    p.DescribeDBClusters,
		"RDS.ModifyDBCluster":       p.ModifyDBCluster,
		"RDS.DeleteDBCluster":       p.DeleteDBCluster,
		"RDS.CreateDBSubnetGroup":    p.CreateDBSubnetGroup,
		"RDS.DescribeDBSubnetGroups": p.DescribeDBSubnetGroups,
		"RDS.DeleteDBSubnetGroup":    p.DeleteDBSubnetGroup,
		// Snapshots (14.7)
		"RDS.CreateDBSnapshot":   p.CreateDBSnapshot,
		"RDS.DescribeDBSnapshots": p.DescribeDBSnapshots,
		"RDS.DeleteDBSnapshot":   p.DeleteDBSnapshot,
		"RDS.CopyDBSnapshot":     p.CopyDBSnapshot,
		// Tagging (14.7)
		"RDS.AddTagsToResource":      p.AddTagsToResource,
		"RDS.RemoveTagsFromResource": p.RemoveTagsFromResource,
		"RDS.ListTagsForResource":    p.ListTagsForResource,
		// Parameter Groups
		"RDS.CreateDBParameterGroup":   p.CreateDBParameterGroup,
		"RDS.DescribeDBParameterGroups": p.DescribeDBParameterGroups,
		"RDS.DeleteDBParameterGroup":   p.DeleteDBParameterGroup,
	}
}

const (
	rtDBInstance        = "rds_db_instance"
	rtDBCluster         = "rds_db_cluster"
	rtDBSubnetGroup     = "rds_db_subnet_group"
	rtDBParameterGroup  = "rds_db_parameter_group"
)

func newID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%X", b)
}

// ─── DB Instances ─────────────────────────────────────────────────────────────

type dbInstance struct {
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	DBInstanceClass      string `json:"DBInstanceClass"`
	Engine               string `json:"Engine"`
	DBInstanceStatus     string `json:"DBInstanceStatus"`
	MasterUsername       string `json:"MasterUsername"`
	DBName               string `json:"DBName"`
	AllocatedStorage     int    `json:"AllocatedStorage"`
	MultiAZ              bool   `json:"MultiAZ"`
	EngineVersion        string `json:"EngineVersion"`
	PubliclyAccessible   bool   `json:"PubliclyAccessible"`
	Port                 int    `json:"Port"`
	DBInstanceArn        string `json:"DBInstanceArn"`
}

func (d dbInstance) toWire() map[string]any {
	port := d.Port
	if port == 0 {
		port = defaultPort(d.Engine)
	}
	return map[string]any{
		"DBInstanceIdentifier": d.DBInstanceIdentifier,
		"DBInstanceClass":      d.DBInstanceClass,
		"Engine":               d.Engine,
		"DBInstanceStatus":     d.DBInstanceStatus,
		"MasterUsername":       d.MasterUsername,
		"DBName":               d.DBName,
		"AllocatedStorage":     fmt.Sprintf("%d", d.AllocatedStorage),
		"MultiAZ":              fmt.Sprintf("%v", d.MultiAZ),
		"EngineVersion":        d.EngineVersion,
		"PubliclyAccessible":   fmt.Sprintf("%v", d.PubliclyAccessible),
		"DBInstanceArn":        d.DBInstanceArn,
		"Endpoint": map[string]any{
			"Address": d.DBInstanceIdentifier + ".jaiscloud.local",
			"Port":    fmt.Sprintf("%d", port),
		},
	}
}

func defaultPort(engine string) int {
	switch strings.ToLower(engine) {
	case "mysql", "mariadb", "aurora", "aurora-mysql":
		return 3306
	case "postgres", "aurora-postgresql":
		return 5432
	case "sqlserver-se", "sqlserver-ee", "sqlserver-ex", "sqlserver-web":
		return 1433
	case "oracle-ee", "oracle-se2":
		return 1521
	}
	return 3306
}

func (p *RelationalProvider) CreateDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBInstanceIdentifier")
	if id == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "DBInstanceIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	inst := dbInstance{
		DBInstanceIdentifier: id,
		DBInstanceClass:      strParam(nr.Params, "DBInstanceClass"),
		Engine:               strParam(nr.Params, "Engine"),
		DBInstanceStatus:     "available",
		MasterUsername:       strParam(nr.Params, "MasterUsername"),
		DBName:               strParam(nr.Params, "DBName"),
		AllocatedStorage:     20,
		EngineVersion:        strParam(nr.Params, "EngineVersion"),
		DBInstanceArn:        nr.ResourceID("db", id),
	}
	if inst.EngineVersion == "" {
		inst.EngineVersion = "8.0"
	}
	data, _ := json.Marshal(inst)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDBInstance, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DBInstanceAlreadyExists", Message: "DB instance already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return &model.ProviderResponse{
		HTTPStatus: http.StatusOK,
		Data:       map[string]any{"DBInstance": inst.toWire()},
	}, nil
}

func (p *RelationalProvider) DescribeDBInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBInstanceIdentifier")
	if id != "" {
		e, err := p.resources.Get(ctx, rtDBInstance, id)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "DBInstanceNotFound", Message: "DB instance not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var inst dbInstance
		json.Unmarshal(e.Data, &inst)
		return provider.OK(map[string]any{"DBInstances": []map[string]any{inst.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtDBInstance, "")
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range entries {
		var inst dbInstance
		json.Unmarshal(e.Data, &inst)
		list = append(list, inst.toWire())
	}
	return provider.OK(map[string]any{"DBInstances": list}), nil
}

func (p *RelationalProvider) ModifyDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBInstanceIdentifier")
	e, err := p.resources.Get(ctx, rtDBInstance, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "DBInstanceNotFound", Message: "DB instance not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var inst dbInstance
	json.Unmarshal(e.Data, &inst)
	if cls := strParam(nr.Params, "DBInstanceClass"); cls != "" {
		inst.DBInstanceClass = cls
	}
	data, _ := json.Marshal(inst)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtDBInstance, ID: id, Data: data})
	return provider.OK(map[string]any{"DBInstanceModified": inst.toWire()}), nil
}

func (p *RelationalProvider) DeleteDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBInstanceIdentifier")
	e, err := p.resources.Get(ctx, rtDBInstance, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "DBInstanceNotFound", Message: "DB instance not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var inst dbInstance
	json.Unmarshal(e.Data, &inst)
	inst.DBInstanceStatus = "deleting"
	p.resources.Delete(ctx, rtDBInstance, id)
	return provider.OK(map[string]any{"DBInstanceDeleted": inst.toWire()}), nil
}

// ─── DB Clusters ──────────────────────────────────────────────────────────────

type dbCluster struct {
	DBClusterIdentifier string `json:"DBClusterIdentifier"`
	DBClusterArn        string `json:"DBClusterArn"`
	Status              string `json:"Status"`
	Engine              string `json:"Engine"`
	EngineVersion       string `json:"EngineVersion"`
	MasterUsername      string `json:"MasterUsername"`
	DatabaseName        string `json:"DatabaseName"`
	Port                int    `json:"Port"`
}

func (c dbCluster) toWire() map[string]any {
	return map[string]any{
		"DBClusterIdentifier": c.DBClusterIdentifier,
		"DBClusterArn":        c.DBClusterArn,
		"Status":              c.Status,
		"Engine":              c.Engine,
		"EngineVersion":       c.EngineVersion,
		"MasterUsername":      c.MasterUsername,
		"DatabaseName":        c.DatabaseName,
		"Endpoint":            c.DBClusterIdentifier + ".cluster.jaiscloud.local",
		"ReaderEndpoint":      c.DBClusterIdentifier + ".cluster-ro.jaiscloud.local",
		"Port":                fmt.Sprintf("%d", c.Port),
	}
}

func (p *RelationalProvider) CreateDBCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBClusterIdentifier")
	if id == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "DBClusterIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	engine := strParam(nr.Params, "Engine")
	c := dbCluster{
		DBClusterIdentifier: id,
		DBClusterArn:        fmt.Sprintf("arn:aws:rds:us-east-1:000000000000:cluster:%s", id),
		Status:              "available",
		Engine:              engine,
		EngineVersion:       strParam(nr.Params, "EngineVersion"),
		MasterUsername:      strParam(nr.Params, "MasterUsername"),
		DatabaseName:        strParam(nr.Params, "DatabaseName"),
		Port:                defaultPort(engine),
	}
	if c.EngineVersion == "" {
		c.EngineVersion = "8.0"
	}
	data, _ := json.Marshal(c)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDBCluster, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DBClusterAlreadyExistsFault", Message: "DB cluster already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return &model.ProviderResponse{
		HTTPStatus: http.StatusOK,
		Data:       map[string]any{"DBCluster": c.toWire()},
	}, nil
}

func (p *RelationalProvider) DescribeDBClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBClusterIdentifier")
	if id != "" {
		e, err := p.resources.Get(ctx, rtDBCluster, id)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "DBClusterNotFoundFault", Message: "DB cluster not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var c dbCluster
		json.Unmarshal(e.Data, &c)
		return provider.OK(map[string]any{"DBClusters": []map[string]any{c.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtDBCluster, "")
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range entries {
		var c dbCluster
		json.Unmarshal(e.Data, &c)
		list = append(list, c.toWire())
	}
	return provider.OK(map[string]any{"DBClusters": list}), nil
}

func (p *RelationalProvider) ModifyDBCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBClusterIdentifier")
	e, err := p.resources.Get(ctx, rtDBCluster, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "DBClusterNotFoundFault", Message: "DB cluster not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var c dbCluster
	json.Unmarshal(e.Data, &c)
	if v := strParam(nr.Params, "MasterUserPassword"); v != "" {
		// accepted but not stored
	}
	data, _ := json.Marshal(c)
	p.resources.Update(ctx, store.ResourceEntry{Type: rtDBCluster, ID: id, Data: data})
	return provider.OK(map[string]any{"DBClusterModified": c.toWire()}), nil
}

func (p *RelationalProvider) DeleteDBCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "DBClusterIdentifier")
	e, err := p.resources.Get(ctx, rtDBCluster, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "DBClusterNotFoundFault", Message: "DB cluster not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var c dbCluster
	json.Unmarshal(e.Data, &c)
	c.Status = "deleting"
	p.resources.Delete(ctx, rtDBCluster, id)
	return provider.OK(map[string]any{"DBClusterDeleted": c.toWire()}), nil
}

// ─── DB Subnet Groups ─────────────────────────────────────────────────────────

type dbSubnetGroup struct {
	DBSubnetGroupName        string `json:"DBSubnetGroupName"`
	DBSubnetGroupDescription string `json:"DBSubnetGroupDescription"`
	VpcId                    string `json:"VpcId"`
	SubnetGroupStatus        string `json:"SubnetGroupStatus"`
}

func (s dbSubnetGroup) toWire() map[string]any {
	return map[string]any{
		"DBSubnetGroupName":        s.DBSubnetGroupName,
		"DBSubnetGroupDescription": s.DBSubnetGroupDescription,
		"VpcId":                    s.VpcId,
		"SubnetGroupStatus":        s.SubnetGroupStatus,
	}
}

func (p *RelationalProvider) CreateDBSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "DBSubnetGroupName")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "DBSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	sg := dbSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: strParam(nr.Params, "DBSubnetGroupDescription"),
		VpcId:                    strParam(nr.Params, "VpcId"),
		SubnetGroupStatus:        "Complete",
	}
	data, _ := json.Marshal(sg)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDBSubnetGroup, ID: name, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DBSubnetGroupAlreadyExists", Message: "DB subnet group already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return &model.ProviderResponse{
		HTTPStatus: http.StatusOK,
		Data:       map[string]any{"DBSubnetGroup": sg.toWire()},
	}, nil
}

func (p *RelationalProvider) DescribeDBSubnetGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "DBSubnetGroupName")
	if name != "" {
		e, err := p.resources.Get(ctx, rtDBSubnetGroup, name)
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "DBSubnetGroupNotFoundFault", Message: "DB subnet group not found", HTTPStatus: http.StatusNotFound}
		}
		if err != nil {
			return nil, err
		}
		var sg dbSubnetGroup
		json.Unmarshal(e.Data, &sg)
		return provider.OK(map[string]any{"DBSubnetGroups": []map[string]any{sg.toWire()}}), nil
	}
	entries, err := p.resources.List(ctx, rtDBSubnetGroup, "")
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range entries {
		var sg dbSubnetGroup
		json.Unmarshal(e.Data, &sg)
		list = append(list, sg.toWire())
	}
	return provider.OK(map[string]any{"DBSubnetGroups": list}), nil
}

func (p *RelationalProvider) DeleteDBSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "DBSubnetGroupName")
	if err := p.resources.Delete(ctx, rtDBSubnetGroup, name); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "DBSubnetGroupNotFoundFault", Message: "DB subnet group not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(nil), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

var _ = newID // suppress unused warning
