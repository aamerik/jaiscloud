// Package kms implements the Google Cloud KMS provider.
package kms

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtKeyRing   = "gcp_keyring"
	rtCryptoKey = "gcp_cryptokey"
)

// Provider handles Cloud KMS key rings and crypto keys.
type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"KMS.KeyRingCreate":    p.KeyRingCreate,
		"KMS.KeyRingList":      p.KeyRingList,
		"KMS.KeyRingGet":       p.KeyRingGet,
		"KMS.CryptoKeyCreate":  p.CryptoKeyCreate,
		"KMS.CryptoKeyList":    p.CryptoKeyList,
		"KMS.CryptoKeyGet":     p.CryptoKeyGet,
		"KMS.CryptoKeyEncrypt": p.CryptoKeyEncrypt,
		"KMS.CryptoKeyDecrypt": p.CryptoKeyDecrypt,
	}
}

type keyRingMeta struct {
	Name       string `json:"name"`
	CreateTime string `json:"createTime"`
}

type cryptoKeyMeta struct {
	Name       string `json:"name"`
	Purpose    string `json:"purpose"`
	CreateTime string `json:"createTime"`
}

// parseKeyRing splits "locations/{loc}/keyRings/{kr}" into loc and keyring ID.
func parseKeyRing(name string) (loc, kr string) {
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/") // [loc, keyRings, kr]
	if len(parts) >= 3 {
		return parts[0], parts[2]
	}
	return "", ""
}

// parseCryptoKey splits "locations/{loc}/keyRings/{kr}/cryptoKeys/{key}" into
// loc, keyring ID, and key ID.
func parseCryptoKey(name string) (loc, kr, key string) {
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/") // [loc, keyRings, kr, cryptoKeys, key]
	if len(parts) >= 5 {
		return parts[0], parts[2], parts[4]
	}
	return "", "", ""
}

func (p *Provider) KeyRingCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	kr, _ := nr.Params["keyRingId"].(string)
	if kr == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				_, kr = parseKeyRing(n)
			}
		}
	}
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location or keyRingId", 400)
	}
	m := keyRingMeta{
		Name:       nr.ResourceID("kms-keyring", loc+"/"+kr),
		CreateTime: clock.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtKeyRing, ID: loc + "/" + kr, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("AlreadyExists", "key ring already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"name": m.Name, "createTime": m.CreateTime}), nil
}

func (p *Provider) KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	prefix := loc + "/"
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtKeyRing, prefix)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(entries))
	for _, e := range entries {
		var m keyRingMeta
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, map[string]any{"name": m.Name, "createTime": m.CreateTime})
		}
	}
	return provider.OK(map[string]any{"keyRings": items, "totalSize": len(items)}), nil
}

func (p *Provider) KeyRingGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, kr := parseKeyRing(nr.Params["name"].(string))
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtKeyRing, loc+"/"+kr)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "key ring not found", 404)
		}
		return nil, err
	}
	var m keyRingMeta
	json.Unmarshal(e.Data, &m)
	return provider.OK(map[string]any{"name": m.Name, "createTime": m.CreateTime}), nil
}

func (p *Provider) CryptoKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, kr := parseKeyRing(nr.Params["name"].(string))
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location/keyRing", 400)
	}
	key, _ := nr.Params["cryptoKeyId"].(string)
	if key == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				_, _, key = parseCryptoKey(n)
			}
		}
	}
	if key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKeyId", 400)
	}
	purpose := "ENCRYPT_DECRYPT"
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if purp, _ := body["purpose"].(string); purp != "" {
			purpose = purp
		}
	}
	m := cryptoKeyMeta{
		Name:       nr.ResourceID("kms-cryptokey", loc+"/"+kr+"/"+key),
		Purpose:    purpose,
		CreateTime: clock.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtCryptoKey, ID: loc + "/" + kr + "/" + key, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("AlreadyExists", "crypto key already exists", 409)
		}
		return nil, err
	}
	return provider.OK(cryptoKeyToMap(m)), nil
}

func (p *Provider) CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, kr, _ := parseCryptoKey(nr.Params["name"].(string))
	prefix := loc + "/" + kr + "/"
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtCryptoKey, prefix)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(entries))
	for _, e := range entries {
		var m cryptoKeyMeta
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, cryptoKeyToMap(m))
		}
	}
	return provider.OK(map[string]any{"cryptoKeys": items, "totalSize": len(items)}), nil
}

func (p *Provider) CryptoKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, kr, key := parseCryptoKey(nr.Params["name"].(string))
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtCryptoKey, loc+"/"+kr+"/"+key)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "crypto key not found", 404)
		}
		return nil, err
	}
	var m cryptoKeyMeta
	json.Unmarshal(e.Data, &m)
	return provider.OK(cryptoKeyToMap(m)), nil
}

func (p *Provider) CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	plaintext, _ := body["plaintext"].(string)
	raw, err := base64.StdEncoding.DecodeString(plaintext)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "plaintext must be base64", 400)
	}
	ciphertext := base64.StdEncoding.EncodeToString(append([]byte("jaiscloud:"), raw...))
	return provider.OK(map[string]any{
		"name":       nr.Params["name"],
		"ciphertext": ciphertext,
	}), nil
}

func (p *Provider) CryptoKeyDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	ciphertext, _ := body["ciphertext"].(string)
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	raw = []byte(strings.TrimPrefix(string(raw), "jaiscloud:"))
	plaintext := base64.StdEncoding.EncodeToString(raw)
	return provider.OK(map[string]any{
		"name":      nr.Params["name"],
		"plaintext": plaintext,
	}), nil
}

func cryptoKeyToMap(m cryptoKeyMeta) map[string]any {
	return map[string]any{
		"name":       m.Name,
		"purpose":    m.Purpose,
		"createTime": m.CreateTime,
		"primary":    map[string]any{"name": m.Name + "/cryptoKeyVersions/1", "state": "ENABLED"},
	}
}
