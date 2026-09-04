// Package iam implements the Google Cloud IAM provider (service accounts).
package iam

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtServiceAccount       = "gcp_service_account"
	rtServiceAccountPolicy = "gcp_service_account_policy"
)

// Provider handles IAM service accounts and their IAM policies.
type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"IAM.ServiceAccountCreate":             p.Create,
		"IAM.ServiceAccountList":               p.List,
		"IAM.ServiceAccountGet":                p.Get,
		"IAM.ServiceAccountDelete":             p.Delete,
		"IAM.ServiceAccountGetIamPolicy":       p.GetIamPolicy,
		"IAM.ServiceAccountSetIamPolicy":       p.SetIamPolicy,
		"IAM.ServiceAccountTestIamPermissions": p.TestIamPermissions,
		"IAM.ServiceAccountKeyCreate":          p.ServiceAccountKeyCreate,
		"IAM.ServiceAccountKeyList":            p.ServiceAccountKeyList,
		"IAM.ServiceAccountKeyGet":             p.ServiceAccountKeyGet,
		"IAM.ServiceAccountKeyDelete":          p.ServiceAccountKeyDelete,
		"IAM.ServiceAccountSignBlob":           p.ServiceAccountSignBlob,
		"IAM.ServiceAccountSignJwt":            p.ServiceAccountSignJwt,
	}
}

type serviceAccountMeta struct {
	Name           string `json:"name"`
	Email          string `json:"email"`
	DisplayName    string `json:"displayName"`
	ProjectID      string `json:"projectId"`
	Description    string `json:"description"`
	Disabled       bool   `json:"disabled"`
	Oauth2ClientID string `json:"oauth2ClientId"`
	Etag           string `json:"etag"`
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// emailFromName extracts the service-account email from a full or relative
// resource name (last path segment).
func emailFromName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
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
	description := ""
	disabled := false
	oauthClientID := ""
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if sa, ok := body["serviceAccount"].(map[string]any); ok {
			if dn, _ := sa["displayName"].(string); dn != "" {
				displayName = dn
			}
			description, _ = sa["description"].(string)
			disabled, _ = sa["disabled"].(bool)
			oauthClientID, _ = sa["oauth2ClientId"].(string)
		}
	}
	email := accountID + "@" + nr.AccountID + ".iam.gserviceaccount.com"
	m := serviceAccountMeta{
		Name:           nr.ResourceID("service-account", email),
		Email:          email,
		DisplayName:    displayName,
		ProjectID:      nr.AccountID,
		Description:    description,
		Disabled:       disabled,
		Oauth2ClientID: oauthClientID,
		Etag:           policy.Etag(displayName),
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtServiceAccount, ID: email, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
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
	page, nextToken := paging.Apply(entries, nr.Params)
	items := make([]any, 0, len(page))
	for _, e := range page {
		var m serviceAccountMeta
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, saToMap(m))
		}
	}
	resp := map[string]any{"accounts": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	var m serviceAccountMeta
	json.Unmarshal(e.Data, &m)
	return provider.OK(saToMap(m)), nil
}

func (p *Provider) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	pol := policy.Load(ctx, p.resources, nr.AccountID, rtServiceAccountPolicy, email)
	// Honor options.requestedPolicyVersion: report at least the requested
	// version (IAM supports versions 1 and 3; the emulator stores bindings
	// without conditions, so 1 is authoritative but the version is surfaced).
	if rpv, ok := nr.Params["options.requestedPolicyVersion"].(string); ok {
		if v, err := strconv.Atoi(rpv); err == nil && v > pol.Version {
			pol.Version = v
		}
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtServiceAccountPolicy, email, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func (p *Provider) requireServiceAccount(ctx context.Context, account, email string) error {
	if _, err := p.resources.Get(ctx, account, store.GlobalRegion, rtServiceAccount, email); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.NewProviderError("NotFound", "service account not found", 404)
		}
		return err
	}
	return nil
}

func saToMap(m serviceAccountMeta) map[string]any {
	return map[string]any{
		"name":           m.Name,
		"email":          m.Email,
		"displayName":    m.DisplayName,
		"projectId":      m.ProjectID,
		"uniqueId":       m.Email,
		"description":    m.Description,
		"disabled":       m.Disabled,
		"oauth2ClientId": m.Oauth2ClientID,
		"etag":           m.Etag,
	}
}
