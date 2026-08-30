// Package iam implements the Google Cloud IAM provider (service accounts).
package iam

import (
	"context"
	"encoding/json"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtServiceAccount = "gcp_service_account"

// Provider handles IAM service accounts and their IAM policies.
type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"IAM.ServiceAccountCreate":       p.Create,
		"IAM.ServiceAccountList":         p.List,
		"IAM.ServiceAccountGet":          p.Get,
		"IAM.ServiceAccountDelete":       p.Delete,
		"IAM.ServiceAccountGetIamPolicy": p.GetIamPolicy,
		"IAM.ServiceAccountSetIamPolicy": p.SetIamPolicy,
	}
}

type serviceAccountMeta struct {
	Name        string `json:"name"`
	Email       string `json:"email"`
	DisplayName string `json:"displayName"`
	ProjectID   string `json:"projectId"`
}

// emailFromName strips the "serviceAccounts/" prefix from a relative name.
func emailFromName(name string) string {
	return strings.TrimPrefix(name, "serviceAccounts/")
}

func (p *Provider) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	accountID, _ := nr.Params["accountId"].(string)
	if accountID == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			accountID, _ = body["accountId"].(string)
		}
	}
	if accountID == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing accountId", 400)
	}
	displayName := accountID
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if sa, ok := body["serviceAccount"].(map[string]any); ok {
			if dn, _ := sa["displayName"].(string); dn != "" {
				displayName = dn
			}
		}
	}
	email := accountID + "@" + nr.AccountID + ".iam.gserviceaccount.com"
	m := serviceAccountMeta{
		Name:        "projects/" + nr.AccountID + "/serviceAccounts/" + email,
		Email:       email,
		DisplayName: displayName,
		ProjectID:   nr.AccountID,
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtServiceAccount, ID: email, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("AlreadyExists", "service account already exists", 409)
		}
		return nil, err
	}
	return provider.OK(saToMap(m)), nil
}

func (p *Provider) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(entries))
	for _, e := range entries {
		var m serviceAccountMeta
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, saToMap(m))
		}
	}
	return provider.OK(map[string]any{"accounts": items}), nil
}

func (p *Provider) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	email := emailFromName(nr.Params["name"].(string))
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	var m serviceAccountMeta
	json.Unmarshal(e.Data, &m)
	return provider.OK(saToMap(m)), nil
}

func (p *Provider) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	email := emailFromName(nr.Params["name"].(string))
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"version":  1,
		"bindings": []any{},
		"etag":     "ACAB",
	}), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	bindings := []any{}
	if bs, ok := body["bindings"].([]any); ok {
		bindings = bs
	}
	etag, _ := body["etag"].(string)
	return provider.OK(map[string]any{
		"version":  1,
		"bindings": bindings,
		"etag":     etag,
	}), nil
}

func saToMap(m serviceAccountMeta) map[string]any {
	return map[string]any{
		"name":        m.Name,
		"email":       m.Email,
		"displayName": m.DisplayName,
		"projectId":   m.ProjectID,
		"uniqueId":    m.Email,
	}
}
