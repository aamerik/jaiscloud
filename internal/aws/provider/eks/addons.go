package eks

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

type eksAddon struct {
	ClusterName           string    `json:"clusterName"`
	AddonName             string    `json:"addonName"`
	AddonVersion          string    `json:"addonVersion"`
	Arn                   string    `json:"addonArn"`
	Status                string    `json:"status"`
	ServiceAccountRoleArn string    `json:"serviceAccountRoleArn"`
	ConfigurationValues   string    `json:"configurationValues"`
	CreatedAt             time.Time `json:"createdAt"`
	ModifiedAt            time.Time `json:"modifiedAt"`
}

func addonKey(clusterName, addonName string) string {
	return clusterName + "/" + addonName
}

func addonToWire(a eksAddon) map[string]any {
	return map[string]any{
		"clusterName":           a.ClusterName,
		"addonName":             a.AddonName,
		"addonVersion":          a.AddonVersion,
		"addonArn":              a.Arn,
		"status":                a.Status,
		"serviceAccountRoleArn": a.ServiceAccountRoleArn,
		"configurationValues":   a.ConfigurationValues,
		"createdAt":             a.CreatedAt.UTC().Format(time.RFC3339),
		"modifiedAt":            a.ModifiedAt.UTC().Format(time.RFC3339),
	}
}

func (p *EKSProvider) CreateAddon(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	addonName, _ := nr.Params["addonName"].(string)
	if clusterName == "" || addonName == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "clusterName and addonName are required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCluster, clusterName); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "cluster " + clusterName + " not found", HTTPStatus: http.StatusNotFound}
	}

	now := clock.Now()
	a := eksAddon{
		ClusterName:           clusterName,
		AddonName:             addonName,
		AddonVersion:          strParam(nr.Params, "addonVersion"),
		Arn:                   nr.ResourceID("eks-addon", clusterName+"/"+addonName),
		Status:                "ACTIVE",
		ServiceAccountRoleArn: strParam(nr.Params, "serviceAccountRoleArn"),
		ConfigurationValues:   strParam(nr.Params, "configurationValues"),
		CreatedAt:             now,
		ModifiedAt:            now,
	}
	data, _ := json.Marshal(a)
	key := addonKey(clusterName, addonName)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtAddon, ID: key, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "ResourceInUseException", Message: "addon " + addonName + " already exists in cluster " + clusterName, HTTPStatus: http.StatusConflict}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"addon": addonToWire(a)}), nil
}

func (p *EKSProvider) DescribeAddon(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	addonName, _ := nr.Params["addonName"].(string)
	key := addonKey(clusterName, addonName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtAddon, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "addon " + addonName + " not found in cluster " + clusterName, HTTPStatus: http.StatusNotFound}
	}
	var a eksAddon
	_ = json.Unmarshal(e.Data, &a)
	return provider.OK(map[string]any{"addon": addonToWire(a)}), nil
}

func (p *EKSProvider) ListAddons(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtAddon, "")
	prefix := clusterName + "/"
	names := make([]string, 0)
	for _, e := range entries {
		var a eksAddon
		if json.Unmarshal(e.Data, &a) == nil && strings.HasPrefix(e.ID, prefix) {
			names = append(names, a.AddonName)
		}
	}
	maxResults := 100
	if v, ok := nr.Params["maxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["nextToken"].(string)
	page, next, pgErr := pagination.Paginate(names, maxResults, token, "ListAddons")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterException", pgErr.Error(), 400)
	}
	data := map[string]any{"addons": page}
	if next != "" {
		data["nextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *EKSProvider) DeleteAddon(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	addonName, _ := nr.Params["addonName"].(string)
	key := addonKey(clusterName, addonName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtAddon, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "addon " + addonName + " not found in cluster " + clusterName, HTTPStatus: http.StatusNotFound}
	}
	var a eksAddon
	_ = json.Unmarshal(e.Data, &a)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtAddon, key)
	return provider.OK(map[string]any{"addon": addonToWire(a)}), nil
}

func (p *EKSProvider) UpdateAddon(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterName, _ := nr.Params["clusterName"].(string)
	addonName, _ := nr.Params["addonName"].(string)
	key := addonKey(clusterName, addonName)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtAddon, key)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "addon " + addonName + " not found", HTTPStatus: http.StatusNotFound}
	}
	var a eksAddon
	_ = json.Unmarshal(e.Data, &a)

	if v := strParam(nr.Params, "addonVersion"); v != "" {
		a.AddonVersion = v
	}
	if v := strParam(nr.Params, "serviceAccountRoleArn"); v != "" {
		a.ServiceAccountRoleArn = v
	}
	if v := strParam(nr.Params, "configurationValues"); v != "" {
		a.ConfigurationValues = v
	}
	a.ModifiedAt = clock.Now()

	data, _ := json.Marshal(a)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtAddon, ID: key, Data: data})
	return provider.OK(map[string]any{"update": map[string]any{
		"clusterName": clusterName,
		"addonName":   addonName,
		"updateId":    "update-" + addonName,
		"status":      "Successful",
		"type":        "AddonUpdate",
	}}), nil
}
