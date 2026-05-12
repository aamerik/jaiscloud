// Package cognito implements Cognito User Pools (cognito-idp) provider.
package cognito

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtUserPool       = "cognito_user_pool"
	rtPoolClient     = "cognito_pool_client"
	rtPoolUser       = "cognito_pool_user"
)

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// User Pools
		"Cognito.CreateUserPool":  p.CreateUserPool,
		"Cognito.DescribeUserPool": p.DescribeUserPool,
		"Cognito.DeleteUserPool":  p.DeleteUserPool,
		"Cognito.ListUserPools":   p.ListUserPools,
		"Cognito.UpdateUserPool":  p.UpdateUserPool,
		// Clients
		"Cognito.CreateUserPoolClient":  p.CreateUserPoolClient,
		"Cognito.DescribeUserPoolClient": p.DescribeUserPoolClient,
		"Cognito.ListUserPoolClients":   p.ListUserPoolClients,
		"Cognito.DeleteUserPoolClient":  p.DeleteUserPoolClient,
		"Cognito.UpdateUserPoolClient":  p.UpdateUserPoolClient,
		// Users
		"Cognito.AdminCreateUser":            p.AdminCreateUser,
		"Cognito.AdminGetUser":               p.AdminGetUser,
		"Cognito.AdminDeleteUser":            p.AdminDeleteUser,
		"Cognito.AdminUpdateUserAttributes":  p.AdminUpdateUserAttributes,
		"Cognito.AdminConfirmSignUp":         p.AdminConfirmSignUp,
		"Cognito.ListUsers":                  p.ListUsers,
	}
}

// ─── types ────────────────────────────────────────────────────────────────────

type cognitoUserPool struct {
	ID               string            `json:"Id"`
	ARN              string            `json:"Arn"`
	Name             string            `json:"Name"`
	Status           string            `json:"Status"`
	Tags             map[string]string `json:"UserPoolTags"`
	CreationDate     time.Time         `json:"CreationDate"`
	LastModifiedDate time.Time         `json:"LastModifiedDate"`
}

type cognitoPoolClient struct {
	UserPoolID   string    `json:"UserPoolId"`
	ClientID     string    `json:"ClientId"`
	ClientName   string    `json:"ClientName"`
	ClientSecret string    `json:"ClientSecret,omitempty"`
	CreationDate time.Time `json:"CreationDate"`
}

type cognitoUser struct {
	UserPoolID           string            `json:"UserPoolId"`
	Username             string            `json:"Username"`
	Attributes           []map[string]string `json:"Attributes"`
	UserStatus           string            `json:"UserStatus"`
	Enabled              bool              `json:"Enabled"`
	UserCreateDate       time.Time         `json:"UserCreateDate"`
	UserLastModifiedDate time.Time         `json:"UserLastModifiedDate"`
}

// ─── ID generators ────────────────────────────────────────────────────────────

const alphanum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func randAlphaNum(n int) string {
	b := make([]byte, n)
	for i := range b {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphanum))))
		b[i] = alphanum[idx.Int64()]
	}
	return string(b)
}

func newPoolID(region string) string {
	return region + "_" + randAlphaNum(9)
}

func newClientID() string { return randAlphaNum(26) }

func newClientSecret() string { return randAlphaNum(51) }

func poolClientKey(poolID, clientID string) string { return poolID + "/" + clientID }
func poolUserKey(poolID, username string) string   { return poolID + "/" + username }

// ─── helpers ─────────────────────────────────────────────────────────────────

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func cognErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func (p *Provider) loadPool(ctx context.Context, poolID string) (cognitoUserPool, error) {
	e, err := p.resources.Get(ctx, rtUserPool, poolID)
	if err != nil {
		return cognitoUserPool{}, cognErr("ResourceNotFoundException", "User pool not found: "+poolID, http.StatusBadRequest)
	}
	var pool cognitoUserPool
	_ = json.Unmarshal(e.Data, &pool)
	return pool, nil
}

func (p *Provider) savePool(ctx context.Context, pool cognitoUserPool) error {
	data, _ := json.Marshal(pool)
	entry := store.ResourceEntry{Type: rtUserPool, ID: pool.ID, Data: data}
	if err := p.resources.Create(ctx, entry); err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, entry)
	} else {
		return err
	}
}

func poolToWire(pool cognitoUserPool) map[string]any {
	return map[string]any{
		"Id":               pool.ID,
		"Arn":              pool.ARN,
		"Name":             pool.Name,
		"Status":           pool.Status,
		"UserPoolTags":     pool.Tags,
		"CreationDate":     pool.CreationDate.Unix(),
		"LastModifiedDate": pool.LastModifiedDate.Unix(),
	}
}

func clientToWire(c cognitoPoolClient, includeSecret bool) map[string]any {
	m := map[string]any{
		"UserPoolId":   c.UserPoolID,
		"ClientId":     c.ClientID,
		"ClientName":   c.ClientName,
		"CreationDate": c.CreationDate.Unix(),
	}
	if includeSecret && c.ClientSecret != "" {
		m["ClientSecret"] = c.ClientSecret
	}
	return m
}

func userToWire(u cognitoUser) map[string]any {
	return map[string]any{
		"Username":             u.Username,
		"Attributes":           u.Attributes,
		"UserStatus":           u.UserStatus,
		"Enabled":              u.Enabled,
		"UserCreateDate":       u.UserCreateDate.Unix(),
		"UserLastModifiedDate": u.UserLastModifiedDate.Unix(),
	}
}

// ─── User Pool operations ─────────────────────────────────────────────────────

func (p *Provider) CreateUserPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "PoolName")
	if name == "" {
		return nil, cognErr("InvalidParameterException", "PoolName is required", http.StatusBadRequest)
	}
	region := nr.Region
	if region == "" {
		region = "us-east-1"
	}
	now := time.Now().UTC()
	pool := cognitoUserPool{
		ID:               newPoolID(region),
		Name:             name,
		Status:           "Enabled",
		Tags:             map[string]string{},
		CreationDate:     now,
		LastModifiedDate: now,
	}
	pool.ARN = nr.ResourceID("cognito-userpool", pool.ID)
	if err := p.savePool(ctx, pool); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"UserPool": poolToWire(pool)}), nil
}

func (p *Provider) DescribeUserPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	pool, err := p.loadPool(ctx, poolID)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"UserPool": poolToWire(pool)}), nil
}

func (p *Provider) DeleteUserPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	if _, err := p.loadPool(ctx, poolID); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, rtUserPool, poolID)
	// Cascade: delete clients and users
	if entries, err := p.resources.List(ctx, rtPoolClient, ""); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.ID, poolID+"/") {
				_ = p.resources.Delete(ctx, rtPoolClient, e.ID)
			}
		}
	}
	if entries, err := p.resources.List(ctx, rtPoolUser, ""); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.ID, poolID+"/") {
				_ = p.resources.Delete(ctx, rtPoolUser, e.ID)
			}
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListUserPools(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtUserPool, "")
	pools := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var pool cognitoUserPool
		if json.Unmarshal(e.Data, &pool) == nil {
			pools = append(pools, poolToWire(pool))
		}
	}
	return provider.OK(map[string]any{"UserPools": pools}), nil
}

func (p *Provider) UpdateUserPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	pool, err := p.loadPool(ctx, poolID)
	if err != nil {
		return nil, err
	}
	pool.LastModifiedDate = time.Now().UTC()
	_ = p.savePool(ctx, pool)
	return provider.OK(map[string]any{}), nil
}

// ─── Client operations ────────────────────────────────────────────────────────

func (p *Provider) CreateUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	if _, err := p.loadPool(ctx, poolID); err != nil {
		return nil, err
	}
	clientName := str(nr.Params, "ClientName")
	if clientName == "" {
		return nil, cognErr("InvalidParameterException", "ClientName is required", http.StatusBadRequest)
	}
	genSecret := false
	if v, ok := nr.Params["GenerateSecret"].(bool); ok {
		genSecret = v
	}
	clientID := newClientID()
	c := cognitoPoolClient{
		UserPoolID:   poolID,
		ClientID:     clientID,
		ClientName:   clientName,
		CreationDate: time.Now().UTC(),
	}
	if genSecret {
		c.ClientSecret = newClientSecret()
	}
	data, _ := json.Marshal(c)
	_ = p.resources.Create(ctx, store.ResourceEntry{Type: rtPoolClient, ID: poolClientKey(poolID, clientID), Data: data})
	return provider.OK(map[string]any{"UserPoolClient": clientToWire(c, true)}), nil
}

func (p *Provider) DescribeUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	clientID := str(nr.Params, "ClientId")
	e, err := p.resources.Get(ctx, rtPoolClient, poolClientKey(poolID, clientID))
	if err != nil {
		return nil, cognErr("ResourceNotFoundException", fmt.Sprintf("Client %s not found in pool %s", clientID, poolID), http.StatusBadRequest)
	}
	var c cognitoPoolClient
	_ = json.Unmarshal(e.Data, &c)
	return provider.OK(map[string]any{"UserPoolClient": clientToWire(c, true)}), nil
}

func (p *Provider) ListUserPoolClients(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	entries, _ := p.resources.List(ctx, rtPoolClient, "")
	prefix := poolID + "/"
	clients := make([]map[string]any, 0)
	for _, e := range entries {
		if !strings.HasPrefix(e.ID, prefix) {
			continue
		}
		var c cognitoPoolClient
		if json.Unmarshal(e.Data, &c) == nil {
			clients = append(clients, clientToWire(c, false))
		}
	}
	return provider.OK(map[string]any{"UserPoolClients": clients}), nil
}

func (p *Provider) DeleteUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	clientID := str(nr.Params, "ClientId")
	key := poolClientKey(poolID, clientID)
	if _, err := p.resources.Get(ctx, rtPoolClient, key); err != nil {
		return nil, cognErr("ResourceNotFoundException", "Client not found", http.StatusBadRequest)
	}
	_ = p.resources.Delete(ctx, rtPoolClient, key)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UpdateUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	clientID := str(nr.Params, "ClientId")
	key := poolClientKey(poolID, clientID)
	e, err := p.resources.Get(ctx, rtPoolClient, key)
	if err != nil {
		return nil, cognErr("ResourceNotFoundException", "Client not found", http.StatusBadRequest)
	}
	var c cognitoPoolClient
	_ = json.Unmarshal(e.Data, &c)
	if v := str(nr.Params, "ClientName"); v != "" {
		c.ClientName = v
	}
	data, _ := json.Marshal(c)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: rtPoolClient, ID: key, Data: data})
	return provider.OK(map[string]any{"UserPoolClient": clientToWire(c, true)}), nil
}

// ─── User operations ──────────────────────────────────────────────────────────

func parseAttributes(params map[string]any) []map[string]string {
	raw, ok := params["UserAttributes"].([]any)
	if !ok {
		return []map[string]string{}
	}
	attrs := make([]map[string]string, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			name, _ := m["Name"].(string)
			val, _ := m["Value"].(string)
			if name != "" {
				attrs = append(attrs, map[string]string{"Name": name, "Value": val})
			}
		}
	}
	return attrs
}

func (p *Provider) AdminCreateUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	if _, err := p.loadPool(ctx, poolID); err != nil {
		return nil, err
	}
	username := str(nr.Params, "Username")
	if username == "" {
		return nil, cognErr("InvalidParameterException", "Username is required", http.StatusBadRequest)
	}
	key := poolUserKey(poolID, username)
	if _, err := p.resources.Get(ctx, rtPoolUser, key); err == nil {
		return nil, cognErr("UsernameExistsException", "User "+username+" already exists", http.StatusBadRequest)
	}
	now := time.Now().UTC()
	u := cognitoUser{
		UserPoolID:           poolID,
		Username:             username,
		Attributes:           parseAttributes(nr.Params),
		UserStatus:           "FORCE_CHANGE_PASSWORD",
		Enabled:              true,
		UserCreateDate:       now,
		UserLastModifiedDate: now,
	}
	data, _ := json.Marshal(u)
	_ = p.resources.Create(ctx, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	return provider.OK(map[string]any{"User": userToWire(u)}), nil
}

func (p *Provider) AdminGetUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	e, err := p.resources.Get(ctx, rtPoolUser, poolUserKey(poolID, username))
	if err != nil {
		return nil, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
	}
	var u cognitoUser
	_ = json.Unmarshal(e.Data, &u)
	return provider.OK(userToWire(u)), nil
}

func (p *Provider) AdminDeleteUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	key := poolUserKey(poolID, username)
	if _, err := p.resources.Get(ctx, rtPoolUser, key); err != nil {
		return nil, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
	}
	_ = p.resources.Delete(ctx, rtPoolUser, key)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) AdminUpdateUserAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	key := poolUserKey(poolID, username)
	e, err := p.resources.Get(ctx, rtPoolUser, key)
	if err != nil {
		return nil, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
	}
	var u cognitoUser
	_ = json.Unmarshal(e.Data, &u)
	newAttrs := parseAttributes(nr.Params)
	// Merge: overwrite existing keys, add new ones
	for _, na := range newAttrs {
		found := false
		for i, ea := range u.Attributes {
			if ea["Name"] == na["Name"] {
				u.Attributes[i] = na
				found = true
				break
			}
		}
		if !found {
			u.Attributes = append(u.Attributes, na)
		}
	}
	u.UserLastModifiedDate = time.Now().UTC()
	data, _ := json.Marshal(u)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) AdminConfirmSignUp(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	key := poolUserKey(poolID, username)
	e, err := p.resources.Get(ctx, rtPoolUser, key)
	if err != nil {
		return nil, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
	}
	var u cognitoUser
	_ = json.Unmarshal(e.Data, &u)
	u.UserStatus = "CONFIRMED"
	u.UserLastModifiedDate = time.Now().UTC()
	data, _ := json.Marshal(u)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListUsers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	entries, _ := p.resources.List(ctx, rtPoolUser, "")
	prefix := poolID + "/"
	users := make([]map[string]any, 0)
	for _, e := range entries {
		if !strings.HasPrefix(e.ID, prefix) {
			continue
		}
		var u cognitoUser
		if json.Unmarshal(e.Data, &u) == nil {
			users = append(users, userToWire(u))
		}
	}
	return provider.OK(map[string]any{"Users": users}), nil
}
