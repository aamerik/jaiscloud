package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtOIDCProvider = "iam_oidc_provider"

type oidcProviderData struct {
	Arn          string            `json:"Arn"`
	URL          string            `json:"Url"`
	ClientIDs    []string          `json:"ClientIDList"`
	Thumbprints  []string          `json:"ThumbprintList"`
	Tags         map[string]string `json:"Tags"`
	CreateDate   time.Time         `json:"CreateDate"`
}

func oidcARN(nr *model.NormalizedRequest, url string) string {
	host := strings.TrimPrefix(url, "https://")
	host = strings.TrimPrefix(host, "http://")
	return nr.ResourceID("iam-oidc-provider", host)
}

// extractStrList reads param.member.N indexed list.
func extractStrList(params map[string]any, prefix string) []string {
	var out []string
	for i := 1; ; i++ {
		v := strParam(params, fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

func (p *IAMProvider) CreateOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	url := strParam(nr.Params, "Url")
	if url == "" {
		return nil, model.NewProviderError("ValidationError", "Url is required", http.StatusBadRequest)
	}
	arn := oidcARN(nr, url)
	op := oidcProviderData{
		Arn:         arn,
		URL:         url,
		ClientIDs:   extractStrList(nr.Params, "ClientIDList"),
		Thumbprints: extractStrList(nr.Params, "ThumbprintList"),
		Tags:        map[string]string{},
		CreateDate:  time.Now().UTC(),
	}
	data, _ := json.Marshal(op)
	if err := p.resources.Create(ctx, nr.AccountID, "", store.ResourceEntry{Type: rtOIDCProvider, ID: arn, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("EntityAlreadyExists", "OIDC provider "+arn+" already exists", http.StatusConflict)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"OpenIDConnectProviderArn": arn}), nil
}

func (p *IAMProvider) GetOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	return provider.OK(oidcToWire(op)), nil
}

func (p *IAMProvider) ListOpenIDConnectProviders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, "", rtOIDCProvider, "")
	var list []map[string]any
	for _, e := range entries {
		var op oidcProviderData
		if json.Unmarshal(e.Data, &op) == nil {
			list = append(list, map[string]any{"Arn": op.Arn})
		}
	}
	if list == nil {
		list = []map[string]any{}
	}
	return provider.OK(map[string]any{"OpenIDConnectProviderList": list}), nil
}

func (p *IAMProvider) DeleteOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	if err := p.resources.Delete(ctx, nr.AccountID, "", rtOIDCProvider, arn); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	return provider.OK(nil), nil
}

func (p *IAMProvider) UpdateOpenIDConnectProviderThumbprint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	op.Thumbprints = extractStrList(nr.Params, "ThumbprintList")
	return nil, saveEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, op)
}

func (p *IAMProvider) AddClientIDToOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	clientID := strParam(nr.Params, "ClientID")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	for _, id := range op.ClientIDs {
		if id == clientID {
			return provider.OK(nil), nil
		}
	}
	op.ClientIDs = append(op.ClientIDs, clientID)
	return nil, saveEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, op)
}

func (p *IAMProvider) RemoveClientIDFromOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	clientID := strParam(nr.Params, "ClientID")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	newIDs := op.ClientIDs[:0]
	for _, id := range op.ClientIDs {
		if id != clientID {
			newIDs = append(newIDs, id)
		}
	}
	op.ClientIDs = newIDs
	return nil, saveEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, op)
}

func (p *IAMProvider) TagOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	tags := extractIAMTags(nr.Params)
	if op.Tags == nil {
		op.Tags = map[string]string{}
	}
	for k, v := range tags {
		op.Tags[k] = v
	}
	return nil, saveEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, op)
}

func (p *IAMProvider) UntagOpenIDConnectProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	keys := extractIAMTagKeys(nr.Params)
	for _, k := range keys {
		delete(op.Tags, k)
	}
	return nil, saveEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, op)
}

func (p *IAMProvider) ListOpenIDConnectProviderTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "OpenIDConnectProviderArn")
	var op oidcProviderData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtOIDCProvider, arn, &op); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "OIDC provider not found", http.StatusNotFound)
	}
	tags := make([]map[string]any, 0, len(op.Tags))
	for k, v := range op.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": tags, "IsTruncated": false}), nil
}

func oidcToWire(op oidcProviderData) map[string]any {
	tags := make([]map[string]any, 0, len(op.Tags))
	for k, v := range op.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	clientIDs := op.ClientIDs
	if clientIDs == nil {
		clientIDs = []string{}
	}
	thumbprints := op.Thumbprints
	if thumbprints == nil {
		thumbprints = []string{}
	}
	return map[string]any{
		"Url":             op.URL,
		"ClientIDList":    clientIDs,
		"ThumbprintList":  thumbprints,
		"Tags":            tags,
		"CreateDate":      op.CreateDate.UTC().Format(time.RFC3339),
	}
}

// extractIAMTags reads Tags.member.N.Key / Tags.member.N.Value.
func extractIAMTags(params map[string]any) map[string]string {
	tags := map[string]string{}
	for i := 1; ; i++ {
		k := strParam(params, fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			break
		}
		tags[k] = strParam(params, fmt.Sprintf("Tags.member.%d.Value", i))
	}
	return tags
}

// extractIAMTagKeys reads TagKeys.member.N.
func extractIAMTagKeys(params map[string]any) []string {
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
