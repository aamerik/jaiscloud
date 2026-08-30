// Package secret implements the Google Cloud Secret Manager provider.
package secret

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtSecret  = "gcp_secret"
	rtVersion = "gcp_secret_version"
)

// Provider handles Secret Manager secrets and versions.
type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Secret.Create":         p.Create,
		"Secret.List":           p.List,
		"Secret.Get":            p.Get,
		"Secret.Update":         p.Update,
		"Secret.Delete":         p.Delete,
		"Secret.AddVersion":     p.AddVersion,
		"Secret.Access":         p.Access,
		"Secret.GetVersion":     p.GetVersion,
		"Secret.DestroyVersion": p.DestroyVersion,
	}
}

type secretMeta struct {
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels,omitempty"`
	CreateTime string            `json:"createTime"`
	NextVer    int               `json:"nextVersion"`
}

type versionMeta struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	CreateTime string `json:"createTime"`
	Data       string `json:"data"` // base64 payload
}

// parseSecretName splits a relative resource name (e.g. "secrets/foo" or
// "secrets/foo/versions/2") into the secret ID and version.
func parseSecretName(name string) (secret, version string) {
	name = strings.TrimPrefix(name, "secrets/")
	if i := strings.Index(name, "/versions/"); i >= 0 {
		return name[:i], name[i+len("/versions/"):]
	}
	return name, ""
}

// secretID extracts the secret ID from a relative name or a full GCP name.
func secretID(name string) string {
	if strings.Contains(name, "/secrets/") {
		if i := strings.Index(name, "/secrets/"); i >= 0 {
			return name[i+len("/secrets/"):]
		}
	}
	return strings.TrimPrefix(name, "secrets/")
}

func (p *Provider) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, _ := nr.Params["secretId"].(string)
	if id == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				id = secretID(n)
			}
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing secretId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	m := secretMeta{
		Name:       nr.ResourceID("secret", id),
		CreateTime: clock.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		NextVer:    1,
	}
	if labels, ok := body["labels"].(map[string]any); ok {
		m.Labels = make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				m.Labels[k] = s
			}
		}
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSecret, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("AlreadyExists", "secret already exists", 409)
		}
		return nil, err
	}
	return provider.OK(secretToMap(m)), nil
}

func (p *Provider) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtSecret, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(entries))
	for _, e := range entries {
		var m secretMeta
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, secretToMap(m))
		}
	}
	return provider.OK(map[string]any{"secrets": items, "totalSize": len(items)}), nil
}

func (p *Provider) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := secretID(nr.Params["name"].(string))
	m, err := p.loadSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(secretToMap(*m)), nil
}

func (p *Provider) Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := secretID(nr.Params["name"].(string))
	m, err := p.loadSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, err
	}
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if labels, ok := body["labels"].(map[string]any); ok {
			m.Labels = make(map[string]string, len(labels))
			for k, v := range labels {
				if s, ok := v.(string); ok {
					m.Labels[k] = s
				}
			}
		}
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSecret, ID: id, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(secretToMap(*m)), nil
}

func (p *Provider) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := secretID(nr.Params["name"].(string))
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtSecret, id); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "secret not found", 404)
		}
		return nil, err
	}
	prefix := id + "/versions/"
	versions, _ := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtVersion, prefix)
	for _, v := range versions {
		_ = p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtVersion, v.ID)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) AddVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := secretID(nr.Params["name"].(string))
	m, err := p.loadSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	payload := ""
	if pl, ok := body["payload"].(map[string]any); ok {
		if d, _ := pl["data"].(string); d != "" {
			payload = d
		}
	}

	version := m.NextVer
	m.NextVer++
	sdata, _ := json.Marshal(m)
	_ = p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSecret, ID: id, Data: sdata})

	v := versionMeta{
		Name:       nr.ResourceID("secret", id) + "/versions/" + fmt.Sprintf("%d", version),
		State:      "ENABLED",
		CreateTime: clock.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		Data:       payload,
	}
	vdata, _ := json.Marshal(v)
	vid := fmt.Sprintf("%s/versions/%d", id, version)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtVersion, ID: vid, Data: vdata}); err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(v)), nil
}

func (p *Provider) Access(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secret, version := parseSecretName(nr.Params["name"].(string))
	v, err := p.loadVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":    v.Name,
		"payload": map[string]any{"data": v.Data},
	}), nil
}

func (p *Provider) GetVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secret, version := parseSecretName(nr.Params["name"].(string))
	v, err := p.loadVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(*v)), nil
}

func (p *Provider) DestroyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secret, version := parseSecretName(nr.Params["name"].(string))
	v, err := p.loadVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, err
	}
	v.State = "DESTROYED"
	data, _ := json.Marshal(v)
	vid := secret + "/versions/" + version
	_ = p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtVersion, ID: vid, Data: data})
	return provider.OK(versionToMap(*v)), nil
}

func (p *Provider) loadSecret(ctx context.Context, account, id string) (*secretMeta, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtSecret, id)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "secret not found", 404)
		}
		return nil, err
	}
	var m secretMeta
	json.Unmarshal(e.Data, &m)
	return &m, nil
}

func (p *Provider) loadVersion(ctx context.Context, account, secret, version string) (*versionMeta, error) {
	vid := secret + "/versions/" + version
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtVersion, vid)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "secret version not found", 404)
		}
		return nil, err
	}
	var v versionMeta
	json.Unmarshal(e.Data, &v)
	return &v, nil
}

func secretToMap(m secretMeta) map[string]any {
	out := map[string]any{
		"name":       m.Name,
		"createTime": m.CreateTime,
		"replication": map[string]any{
			"automatic": map[string]any{},
		},
	}
	if m.Labels != nil {
		out["labels"] = m.Labels
	}
	return out
}

func versionToMap(v versionMeta) map[string]any {
	return map[string]any{
		"name":       v.Name,
		"state":      v.State,
		"createTime": v.CreateTime,
	}
}
