package eks

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

type eksNodegroup struct {
	ClusterName    string            `json:"clusterName"`
	NodegroupName  string            `json:"nodegroupName"`
	Arn            string            `json:"nodegroupArn"`
	Status         string            `json:"status"`
	NodeRole       string            `json:"nodeRole"`
	Subnets        []string          `json:"subnets"`
	InstanceTypes  []string          `json:"instanceTypes"`
	AmiType        string            `json:"amiType"`
	ReleaseVersion string            `json:"releaseVersion"`
	Labels         map[string]string `json:"labels"`
	ScalingConfig  map[string]int    `json:"scalingConfig"`
	CreatedAt      time.Time         `json:"createdAt"`
	ModifiedAt     time.Time         `json:"modifiedAt"`
}

func nodegroupKey(clusterName, nodegroupName string) string {
	return clusterName + "/" + nodegroupName
}

func nodegroupToWire(ng eksNodegroup) map[string]any {
	return map[string]any{
		"clusterName":    ng.ClusterName,
		"nodegroupName":  ng.NodegroupName,
		"nodegroupArn":   ng.Arn,
		"status":         ng.Status,
		"nodeRole":       ng.NodeRole,
		"subnets":        ng.Subnets,
		"instanceTypes":  ng.InstanceTypes,
		"amiType":        ng.AmiType,
		"releaseVersion": ng.ReleaseVersion,
		"labels":         ng.Labels,
		"scalingConfig":  ng.ScalingConfig,
		"createdAt":      ng.CreatedAt.UTC().Format(time.RFC3339),
		"modifiedAt":     ng.ModifiedAt.UTC().Format(time.RFC3339),
	}
}

func (p *EKSProvider) CreateNodegroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	nodegroupName, _ := nr.Params["nodegroupName"].(string)
	if clusterName == "" || nodegroupName == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "clusterName and nodegroupName are required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCluster, clusterName); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "cluster " + clusterName + " not found", HTTPStatus: http.StatusNotFound}
	}

	subnets := strSliceParam(nr.Params, "subnets")
	instanceTypes := strSliceParam(nr.Params, "instanceTypes")
	labels := strMapParam(nr.Params, "labels")
	scaling := scalingParam(nr.Params)

	now := time.Now().UTC()
	ng := eksNodegroup{
		ClusterName:    clusterName,
		NodegroupName:  nodegroupName,
		Arn:            nr.ResourceID("eks-nodegroup", clusterName+"/"+nodegroupName),
		Status:         "ACTIVE",
		NodeRole:       strParam(nr.Params, "nodeRole"),
		Subnets:        subnets,
		InstanceTypes:  instanceTypes,
		AmiType:        strParam(nr.Params, "amiType"),
		ReleaseVersion: strParam(nr.Params, "releaseVersion"),
		Labels:         labels,
		ScalingConfig:  scaling,
		CreatedAt:      now,
		ModifiedAt:     now,
	}
	data, _ := json.Marshal(ng)
	key := nodegroupKey(clusterName, nodegroupName)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtNodegroup, ID: key, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "ResourceInUseException", Message: "nodegroup " + nodegroupName + " already exists in cluster " + clusterName, HTTPStatus: http.StatusConflict}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"nodegroup": nodegroupToWire(ng)}), nil
}

func (p *EKSProvider) DescribeNodegroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	nodegroupName, _ := nr.Params["nodegroupName"].(string)
	key := nodegroupKey(clusterName, nodegroupName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtNodegroup, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "nodegroup " + nodegroupName + " not found in cluster " + clusterName, HTTPStatus: http.StatusNotFound}
	}
	var ng eksNodegroup
	_ = json.Unmarshal(e.Data, &ng)
	return provider.OK(map[string]any{"nodegroup": nodegroupToWire(ng)}), nil
}

func (p *EKSProvider) ListNodegroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtNodegroup, "")
	prefix := clusterName + "/"
	names := make([]string, 0)
	for _, e := range entries {
		var ng eksNodegroup
		if json.Unmarshal(e.Data, &ng) == nil && strings.HasPrefix(e.ID, prefix) {
			names = append(names, ng.NodegroupName)
		}
	}
	maxResults := 100
	if v, ok := nr.Params["maxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["nextToken"].(string)
	page, next, pgErr := pagination.Paginate(names, maxResults, token, "ListNodegroups")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterException", pgErr.Error(), 400)
	}
	data := map[string]any{"nodegroups": page}
	if next != "" {
		data["nextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *EKSProvider) DeleteNodegroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	nodegroupName, _ := nr.Params["nodegroupName"].(string)
	key := nodegroupKey(clusterName, nodegroupName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtNodegroup, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "nodegroup " + nodegroupName + " not found in cluster " + clusterName, HTTPStatus: http.StatusNotFound}
	}
	var ng eksNodegroup
	_ = json.Unmarshal(e.Data, &ng)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtNodegroup, key)
	return provider.OK(map[string]any{"nodegroup": nodegroupToWire(ng)}), nil
}

func (p *EKSProvider) UpdateNodegroupConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	nodegroupName, _ := nr.Params["nodegroupName"].(string)
	key := nodegroupKey(clusterName, nodegroupName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtNodegroup, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "nodegroup " + nodegroupName + " not found", HTTPStatus: http.StatusNotFound}
	}
	var ng eksNodegroup
	_ = json.Unmarshal(e.Data, &ng)

	if sc := scalingParam(nr.Params); len(sc) > 0 {
		ng.ScalingConfig = sc
	}
	if labels, ok := nr.Params["labels"].(map[string]any); ok {
		if add, ok := labels["addOrUpdateLabels"].(map[string]any); ok {
			for k, v := range add {
				if s, ok := v.(string); ok {
					ng.Labels[k] = s
				}
			}
		}
		if remove, ok := labels["removeLabels"].([]any); ok {
			for _, k := range remove {
				if s, ok := k.(string); ok {
					delete(ng.Labels, s)
				}
			}
		}
	}
	ng.ModifiedAt = time.Now().UTC()

	data, _ := json.Marshal(ng)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtNodegroup, ID: key, Data: data})
	return provider.OK(map[string]any{"update": map[string]any{
		"clusterName":   clusterName,
		"nodegroupName": nodegroupName,
		"updateId":      "update-" + nodegroupName,
		"status":        "Successful",
		"type":          "ConfigUpdate",
	}}), nil
}

func (p *EKSProvider) UpdateNodegroupVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	nodegroupName, _ := nr.Params["nodegroupName"].(string)
	key := nodegroupKey(clusterName, nodegroupName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtNodegroup, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "nodegroup " + nodegroupName + " not found", HTTPStatus: http.StatusNotFound}
	}
	var ng eksNodegroup
	_ = json.Unmarshal(e.Data, &ng)

	if v := strParam(nr.Params, "releaseVersion"); v != "" {
		ng.ReleaseVersion = v
	}
	ng.ModifiedAt = time.Now().UTC()

	data, _ := json.Marshal(ng)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtNodegroup, ID: key, Data: data})
	return provider.OK(map[string]any{"update": map[string]any{
		"clusterName":   clusterName,
		"nodegroupName": nodegroupName,
		"updateId":      "update-" + nodegroupName,
		"status":        "Successful",
		"type":          "VersionUpdate",
	}}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func strSliceParam(params map[string]any, key string) []string {
	raw, ok := params[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func strMapParam(params map[string]any, key string) map[string]string {
	raw, ok := params[key].(map[string]any)
	if !ok {
		return map[string]string{}
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func scalingParam(params map[string]any) map[string]int {
	raw, ok := params["scalingConfig"].(map[string]any)
	if !ok {
		return map[string]int{}
	}
	out := map[string]int{}
	for _, k := range []string{"minSize", "maxSize", "desiredSize"} {
		if v, ok := raw[k].(float64); ok {
			out[k] = int(v)
		}
	}
	return out
}
