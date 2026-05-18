// Package cognitoidentity implements the Cognito Identity provider.
package cognitoidentity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtIdentityPool = "cognito_identity_pool"
	rtIdentity     = "cognito_identity"
)

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CognitoIdentity.CreateIdentityPool":          p.CreateIdentityPool,
		"CognitoIdentity.DescribeIdentityPool":        p.DescribeIdentityPool,
		"CognitoIdentity.DeleteIdentityPool":          p.DeleteIdentityPool,
		"CognitoIdentity.ListIdentityPools":           p.ListIdentityPools,
		"CognitoIdentity.UpdateIdentityPool":          p.UpdateIdentityPool,
		"CognitoIdentity.GetId":                       p.GetId,
		"CognitoIdentity.GetCredentialsForIdentity":   p.GetCredentialsForIdentity,
		"CognitoIdentity.GetOpenIdToken":              p.GetOpenIdToken,
	}
}

type identityPool struct {
	IdentityPoolID                string            `json:"IdentityPoolId"`
	IdentityPoolName              string            `json:"IdentityPoolName"`
	AllowUnauthenticatedIdentities bool             `json:"AllowUnauthenticatedIdentities"`
	Tags                          map[string]string `json:"IdentityPoolTags"`
}

type identity struct {
	IdentityID     string            `json:"IdentityId"`
	IdentityPoolID string            `json:"IdentityPoolId"`
	Logins         map[string]string `json:"Logins"`
	CreationDate   time.Time         `json:"CreationDate"`
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newPoolID(region string) string {
	return fmt.Sprintf("%s:%s", region, randHex(8)+"-"+randHex(4)+"-"+randHex(4)+"-"+randHex(4)+"-"+randHex(12))
}

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func ciErr(code, msg string) error {
	return model.NewProviderError(code, msg, http.StatusBadRequest)
}

func poolToWire(pool identityPool) map[string]any {
	return map[string]any{
		"IdentityPoolId":                  pool.IdentityPoolID,
		"IdentityPoolName":                pool.IdentityPoolName,
		"AllowUnauthenticatedIdentities":  pool.AllowUnauthenticatedIdentities,
		"IdentityPoolTags":                pool.Tags,
	}
}

func (p *Provider) loadPool(ctx context.Context, account, region, poolID string) (identityPool, error) {
	e, err := p.resources.Get(ctx, account, region, rtIdentityPool, poolID)
	if err != nil {
		return identityPool{}, ciErr("ResourceNotFoundException", "Identity pool not found: "+poolID)
	}
	var pool identityPool
	_ = json.Unmarshal(e.Data, &pool)
	return pool, nil
}

func (p *Provider) savePool(ctx context.Context, account, region string, pool identityPool) {
	data, _ := json.Marshal(pool)
	entry := store.ResourceEntry{Type: rtIdentityPool, ID: pool.IdentityPoolID, Data: data}
	if err := p.resources.Create(ctx, account, region, entry); err == store.ErrAlreadyExists {
		p.resources.Update(ctx, account, region, entry)
	}
}

func (p *Provider) CreateIdentityPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "IdentityPoolName")
	if name == "" {
		return nil, ciErr("InvalidParameterException", "IdentityPoolName is required")
	}
	region := nr.Region
	if region == "" {
		region = "us-east-1"
	}
	allowUnauth := false
	if v, ok := nr.Params["AllowUnauthenticatedIdentities"].(bool); ok {
		allowUnauth = v
	}
	pool := identityPool{
		IdentityPoolID:                 newPoolID(region),
		IdentityPoolName:               name,
		AllowUnauthenticatedIdentities: allowUnauth,
		Tags:                           map[string]string{},
	}
	p.savePool(ctx, nr.AccountID, nr.Region, pool)
	return provider.OK(poolToWire(pool)), nil
}

func (p *Provider) DescribeIdentityPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "IdentityPoolId")
	pool, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID)
	if err != nil {
		return nil, err
	}
	return provider.OK(poolToWire(pool)), nil
}

func (p *Provider) DeleteIdentityPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "IdentityPoolId")
	if _, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtIdentityPool, poolID)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListIdentityPools(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtIdentityPool, "")
	pools := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var pool identityPool
		if json.Unmarshal(e.Data, &pool) == nil {
			pools = append(pools, poolToWire(pool))
		}
	}
	return provider.OK(map[string]any{"IdentityPools": pools}), nil
}

func (p *Provider) UpdateIdentityPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "IdentityPoolId")
	pool, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID)
	if err != nil {
		return nil, err
	}
	if v := str(nr.Params, "IdentityPoolName"); v != "" {
		pool.IdentityPoolName = v
	}
	p.savePool(ctx, nr.AccountID, nr.Region, pool)
	return provider.OK(poolToWire(pool)), nil
}

func (p *Provider) GetId(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "IdentityPoolId")
	if _, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID); err != nil {
		return nil, err
	}
	region := nr.Region
	if region == "" {
		region = "us-east-1"
	}
	identityID := fmt.Sprintf("%s:%s", region, randHex(8)+"-"+randHex(4)+"-"+randHex(4)+"-"+randHex(4)+"-"+randHex(12))
	ident := identity{
		IdentityID:     identityID,
		IdentityPoolID: poolID,
		Logins:         map[string]string{},
		CreationDate:   time.Now().UTC(),
	}
	data, _ := json.Marshal(ident)
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtIdentity, ID: identityID, Data: data})
	return provider.OK(map[string]any{"IdentityId": identityID}), nil
}

func (p *Provider) GetCredentialsForIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	identityID := str(nr.Params, "IdentityId")
	expiry := time.Now().Add(time.Hour).Unix()
	return provider.OK(map[string]any{
		"IdentityId": identityID,
		"Credentials": map[string]any{
			"AccessKeyId":  "ASIA" + randHex(8),
			"SecretKey":    randHex(20),
			"SessionToken": "FQoGZXIvYXdzE" + randHex(40),
			"Expiration":   expiry,
		},
	}), nil
}

func (p *Provider) GetOpenIdToken(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	identityID := str(nr.Params, "IdentityId")
	token := "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9." + randHex(32) + "." + randHex(32)
	return provider.OK(map[string]any{
		"IdentityId": identityID,
		"Token":      token,
	}), nil
}
