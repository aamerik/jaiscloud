// Package redshift implements the Redshift cluster provider.
package redshift

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtCluster     = "redshift_cluster"
	rtSubnetGroup = "redshift_subnet_group"
	rtRSTags      = "redshift_tags"
)

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Clusters
		"Redshift.CreateCluster":    p.CreateCluster,
		"Redshift.DescribeClusters": p.DescribeClusters,
		"Redshift.DeleteCluster":    p.DeleteCluster,
		"Redshift.ModifyCluster":    p.ModifyCluster,
		"Redshift.RebootCluster":    p.RebootCluster,
		"Redshift.ResumeCluster":    p.ResumeCluster,
		"Redshift.PauseCluster":     p.PauseCluster,
		// Subnet groups
		"Redshift.CreateClusterSubnetGroup":    p.CreateClusterSubnetGroup,
		"Redshift.DescribeClusterSubnetGroups": p.DescribeClusterSubnetGroups,
		"Redshift.DeleteClusterSubnetGroup":    p.DeleteClusterSubnetGroup,
		"Redshift.ModifyClusterSubnetGroup":    p.ModifyClusterSubnetGroup,
		// Tags
		"Redshift.CreateTags":   p.CreateTags,
		"Redshift.DeleteTags":   p.DeleteTags,
		"Redshift.DescribeTags": p.DescribeTags,
	}
}

type redshiftCluster struct {
	ClusterIdentifier string            `json:"ClusterIdentifier"`
	NodeType          string            `json:"NodeType"`
	ClusterStatus     string            `json:"ClusterStatus"`
	MasterUsername    string            `json:"MasterUsername"`
	DBName            string            `json:"DBName"`
	NumberOfNodes     int               `json:"NumberOfNodes"`
	Endpoint          endpoint          `json:"Endpoint"`
	ClusterCreateTime time.Time         `json:"ClusterCreateTime"`
	Tags              map[string]string `json:"Tags"`
	ARN               string            `json:"ClusterNamespaceArn"`
}

type endpoint struct {
	Address string `json:"Address"`
	Port    int    `json:"Port"`
}

type clusterSubnetGroup struct {
	Name        string   `json:"ClusterSubnetGroupName"`
	Description string   `json:"Description"`
	VpcID       string   `json:"VpcId"`
	Status      string   `json:"SubnetGroupStatus"`
	SubnetIDs   []string `json:"SubnetIds"`
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func intParam(params map[string]any, key string, def int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case string:
		var n int
		fmt.Sscanf(v, "%d", &n)
		return n
	}
	return def
}

func rsErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func clusterToWire(c redshiftCluster) map[string]any {
	return map[string]any{
		"ClusterIdentifier": c.ClusterIdentifier,
		"NodeType":          c.NodeType,
		"ClusterStatus":     c.ClusterStatus,
		"MasterUsername":    c.MasterUsername,
		"DBName":            c.DBName,
		"NumberOfNodes":     fmt.Sprintf("%d", c.NumberOfNodes),
		"Endpoint": map[string]any{
			"Address": c.Endpoint.Address,
			"Port":    fmt.Sprintf("%d", c.Endpoint.Port),
		},
		"ClusterCreateTime":   c.ClusterCreateTime.UTC().Format(time.RFC3339),
		"ClusterNamespaceArn": c.ARN,
	}
}

func (p *Provider) loadCluster(ctx context.Context, account, region, id string) (redshiftCluster, error) {
	e, err := p.resources.Get(ctx, account, region, rtCluster, id)
	if err != nil {
		return redshiftCluster{}, rsErr("ClusterNotFound", "Cluster not found: "+id, http.StatusNotFound)
	}
	var c redshiftCluster
	_ = json.Unmarshal(e.Data, &c)
	return c, nil
}

func (p *Provider) saveCluster(ctx context.Context, account, region string, c redshiftCluster) {
	data, _ := json.Marshal(c)
	entry := store.ResourceEntry{Type: rtCluster, ID: c.ClusterIdentifier, Data: data}
	_ = p.resources.Upsert(ctx, account, region, entry)
}

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "ClusterIdentifier")
	if id == "" {
		return nil, rsErr("InvalidParameterValue", "ClusterIdentifier is required", http.StatusBadRequest)
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCluster, id); err == nil {
		return nil, rsErr("ClusterAlreadyExists", "Cluster "+id+" already exists", http.StatusBadRequest)
	}
	nodeType := str(nr.Params, "NodeType")
	if nodeType == "" {
		nodeType = "dc2.large"
	}
	numNodes := intParam(nr.Params, "NumberOfNodes", 1)
	region := nr.Region
	if region == "" {
		region = "us-east-1"
	}
	c := redshiftCluster{
		ClusterIdentifier: id,
		NodeType:          nodeType,
		ClusterStatus:     "available",
		MasterUsername:    str(nr.Params, "MasterUsername"),
		DBName:            str(nr.Params, "DBName"),
		NumberOfNodes:     numNodes,
		Endpoint: endpoint{
			Address: fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", id, randHex(8), region),
			Port:    5439,
		},
		ClusterCreateTime: clock.Now(),
		Tags:              map[string]string{},
		ARN:               nr.ResourceID("redshift-cluster", id),
	}
	p.saveCluster(ctx, nr.AccountID, nr.Region, c)
	return provider.OK(map[string]any{"Cluster": clusterToWire(c)}), nil
}

func (p *Provider) DescribeClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterID := str(nr.Params, "ClusterIdentifier")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtCluster, "")
	var clusters []map[string]any
	for _, e := range entries {
		var c redshiftCluster
		if json.Unmarshal(e.Data, &c) != nil {
			continue
		}
		if filterID != "" && c.ClusterIdentifier != filterID {
			continue
		}
		clusters = append(clusters, clusterToWire(c))
	}
	if clusters == nil {
		clusters = []map[string]any{}
	}
	return provider.OK(map[string]any{"Clusters": clusters}), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "ClusterIdentifier")
	c, err := p.loadCluster(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtCluster, id)
	return provider.OK(map[string]any{"Cluster": clusterToWire(c)}), nil
}

func (p *Provider) ModifyCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "ClusterIdentifier")
	c, err := p.loadCluster(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	if v := str(nr.Params, "NodeType"); v != "" {
		c.NodeType = v
	}
	if v := intParam(nr.Params, "NumberOfNodes", 0); v > 0 {
		c.NumberOfNodes = v
	}
	c.ClusterStatus = "available"
	p.saveCluster(ctx, nr.AccountID, nr.Region, c)
	return provider.OK(map[string]any{"Cluster": clusterToWire(c)}), nil
}

func (p *Provider) RebootCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "ClusterIdentifier")
	c, err := p.loadCluster(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	c.ClusterStatus = "available"
	p.saveCluster(ctx, nr.AccountID, nr.Region, c)
	return provider.OK(map[string]any{"Cluster": clusterToWire(c)}), nil
}

func (p *Provider) ResumeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "ClusterIdentifier")
	c, err := p.loadCluster(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	c.ClusterStatus = "available"
	p.saveCluster(ctx, nr.AccountID, nr.Region, c)
	return provider.OK(map[string]any{"Cluster": clusterToWire(c)}), nil
}

func (p *Provider) PauseCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "ClusterIdentifier")
	c, err := p.loadCluster(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	c.ClusterStatus = "paused"
	p.saveCluster(ctx, nr.AccountID, nr.Region, c)
	return provider.OK(map[string]any{"Cluster": clusterToWire(c)}), nil
}

// ─── Subnet Groups ────────────────────────────────────────────────────────────

func extractSubnetIDs(params map[string]any) []string {
	var ids []string
	for i := 1; ; i++ {
		v := str(params, fmt.Sprintf("SubnetIds.SubnetIdentifier.%d", i))
		if v == "" {
			break
		}
		ids = append(ids, v)
	}
	return ids
}

func (p *Provider) CreateClusterSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "ClusterSubnetGroupName")
	if name == "" {
		return nil, rsErr("InvalidParameterValue", "ClusterSubnetGroupName is required", http.StatusBadRequest)
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtSubnetGroup, name); err == nil {
		return nil, rsErr("ClusterSubnetGroupAlreadyExists", "Subnet group "+name+" already exists", http.StatusBadRequest)
	}
	g := clusterSubnetGroup{
		Name:        name,
		Description: str(nr.Params, "Description"),
		VpcID:       str(nr.Params, "VpcId"),
		Status:      "Complete",
		SubnetIDs:   extractSubnetIDs(nr.Params),
	}
	data, _ := json.Marshal(g)
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtSubnetGroup, ID: name, Data: data})
	return provider.OK(map[string]any{"ClusterSubnetGroup": subnetGroupToWire(g)}), nil
}

func (p *Provider) DescribeClusterSubnetGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "ClusterSubnetGroupName")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtSubnetGroup, "")
	var groups []map[string]any
	for _, e := range entries {
		var g clusterSubnetGroup
		if json.Unmarshal(e.Data, &g) != nil {
			continue
		}
		if name != "" && g.Name != name {
			continue
		}
		groups = append(groups, subnetGroupToWire(g))
	}
	if groups == nil {
		groups = []map[string]any{}
	}
	return provider.OK(map[string]any{"ClusterSubnetGroups": groups}), nil
}

func (p *Provider) DeleteClusterSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "ClusterSubnetGroupName")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtSubnetGroup, name); err == store.ErrNotFound {
		return nil, rsErr("ClusterSubnetGroupNotFoundFault", "Subnet group not found: "+name, http.StatusNotFound)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ModifyClusterSubnetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "ClusterSubnetGroupName")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtSubnetGroup, name)
	if err != nil {
		return nil, rsErr("ClusterSubnetGroupNotFoundFault", "Subnet group not found: "+name, http.StatusNotFound)
	}
	var g clusterSubnetGroup
	_ = json.Unmarshal(e.Data, &g)
	if v := str(nr.Params, "Description"); v != "" {
		g.Description = v
	}
	if ids := extractSubnetIDs(nr.Params); len(ids) > 0 {
		g.SubnetIDs = ids
	}
	data, _ := json.Marshal(g)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtSubnetGroup, ID: name, Data: data})
	return provider.OK(map[string]any{"ClusterSubnetGroup": subnetGroupToWire(g)}), nil
}

func subnetGroupToWire(g clusterSubnetGroup) map[string]any {
	subnets := make([]map[string]any, 0, len(g.SubnetIDs))
	for _, id := range g.SubnetIDs {
		subnets = append(subnets, map[string]any{"SubnetIdentifier": id, "SubnetStatus": "Active"})
	}
	return map[string]any{
		"ClusterSubnetGroupName": g.Name,
		"Description":            g.Description,
		"VpcId":                  g.VpcID,
		"SubnetGroupStatus":      g.Status,
		"Subnets":                subnets,
	}
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

// extractEC2Tags reads Tag.N.Key / Tag.N.Value from Query-protocol params.
func extractEC2Tags(params map[string]any) map[string]string {
	tags := map[string]string{}
	for i := 1; ; i++ {
		k := str(params, fmt.Sprintf("Tag.%d.Key", i))
		if k == "" {
			break
		}
		tags[k] = str(params, fmt.Sprintf("Tag.%d.Value", i))
	}
	return tags
}

func (p *Provider) CreateTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "ResourceName")
	tags := loadRSTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	for k, v := range extractEC2Tags(nr.Params) {
		tags[k] = v
	}
	saveRSTags(ctx, p.resources, nr.AccountID, nr.Region, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DeleteTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "ResourceName")
	tags := loadRSTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	for i := 1; ; i++ {
		k := str(nr.Params, fmt.Sprintf("Tag.%d.Key", i))
		if k == "" {
			break
		}
		delete(tags, k)
	}
	saveRSTags(ctx, p.resources, nr.AccountID, nr.Region, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "ResourceName")
	tags := loadRSTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	list := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]any{
			"ResourceName": arn,
			"ResourceType": "cluster",
			"TagKey":       k,
			"TagValue":     v,
		})
	}
	return provider.OK(map[string]any{"TaggedResources": list}), nil
}

func loadRSTags(ctx context.Context, res store.ResourceStore, account, region, arn string) map[string]string {
	e, err := res.Get(ctx, account, region, rtRSTags, arn)
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	_ = json.Unmarshal(e.Data, &m)
	return m
}

func saveRSTags(ctx context.Context, res store.ResourceStore, account, region, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtRSTags, ID: arn, Data: data}
	_ = res.Upsert(ctx, account, region, entry)
}

// Silence unused import
var _ = strings.HasPrefix
