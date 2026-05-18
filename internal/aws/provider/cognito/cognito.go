// Package cognito implements Cognito User Pools (cognito-idp) provider.
package cognito

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
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
	rtConfirmCode    = "cognito_confirm_code"
	rtResetCode      = "cognito_reset_code"
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
		// Self-service auth flows
		"Cognito.SignUp":                      p.SignUp,
		"Cognito.ConfirmSignUp":               p.ConfirmSignUp,
		"Cognito.InitiateAuth":                p.InitiateAuth,
		"Cognito.AdminInitiateAuth":           p.AdminInitiateAuth,
		"Cognito.RespondToAuthChallenge":      p.RespondToAuthChallenge,
		"Cognito.ForgotPassword":              p.ForgotPassword,
		"Cognito.ConfirmForgotPassword":       p.ConfirmForgotPassword,
		"Cognito.ResendConfirmationCode":      p.ResendConfirmationCode,
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
	UserPoolID           string              `json:"UserPoolId"`
	Username             string              `json:"Username"`
	UserID               string              `json:"UserId"`
	Attributes           []map[string]string `json:"Attributes"`
	UserStatus           string              `json:"UserStatus"`
	Enabled              bool                `json:"Enabled"`
	Password             string              `json:"Password,omitempty"`
	UserCreateDate       time.Time           `json:"UserCreateDate"`
	UserLastModifiedDate time.Time           `json:"UserLastModifiedDate"`
}

type confirmCodeRecord struct {
	Code string `json:"Code"`
}

type resetCodeRecord struct {
	Code string `json:"Code"`
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

func (p *Provider) loadPool(ctx context.Context, account, region, poolID string) (cognitoUserPool, error) {
	e, err := p.resources.Get(ctx, account, region, rtUserPool, poolID)
	if err != nil {
		return cognitoUserPool{}, cognErr("ResourceNotFoundException", "User pool not found: "+poolID, http.StatusBadRequest)
	}
	var pool cognitoUserPool
	_ = json.Unmarshal(e.Data, &pool)
	return pool, nil
}

func (p *Provider) savePool(ctx context.Context, account, region string, pool cognitoUserPool) error {
	data, _ := json.Marshal(pool)
	entry := store.ResourceEntry{Type: rtUserPool, ID: pool.ID, Data: data}
	if err := p.resources.Create(ctx, account, region, entry); err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, account, region, entry)
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
	if err := p.savePool(ctx, nr.AccountID, nr.Region, pool); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"UserPool": poolToWire(pool)}), nil
}

func (p *Provider) DescribeUserPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	pool, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"UserPool": poolToWire(pool)}), nil
}

func (p *Provider) DeleteUserPool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	if _, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtUserPool, poolID)
	// Cascade: delete clients and users
	if entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtPoolClient, ""); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.ID, poolID+"/") {
				_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPoolClient, e.ID)
			}
		}
	}
	if entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtPoolUser, ""); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.ID, poolID+"/") {
				_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPoolUser, e.ID)
			}
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListUserPools(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtUserPool, "")
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
	pool, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID)
	if err != nil {
		return nil, err
	}
	pool.LastModifiedDate = time.Now().UTC()
	_ = p.savePool(ctx, nr.AccountID, nr.Region, pool)
	return provider.OK(map[string]any{}), nil
}

// ─── Client operations ────────────────────────────────────────────────────────

func (p *Provider) CreateUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	if _, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID); err != nil {
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
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolClient, ID: poolClientKey(poolID, clientID), Data: data})
	return provider.OK(map[string]any{"UserPoolClient": clientToWire(c, true)}), nil
}

func (p *Provider) DescribeUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	clientID := str(nr.Params, "ClientId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolClient, poolClientKey(poolID, clientID))
	if err != nil {
		return nil, cognErr("ResourceNotFoundException", fmt.Sprintf("Client %s not found in pool %s", clientID, poolID), http.StatusBadRequest)
	}
	var c cognitoPoolClient
	_ = json.Unmarshal(e.Data, &c)
	return provider.OK(map[string]any{"UserPoolClient": clientToWire(c, true)}), nil
}

func (p *Provider) ListUserPoolClients(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtPoolClient, "")
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
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolClient, key); err != nil {
		return nil, cognErr("ResourceNotFoundException", "Client not found", http.StatusBadRequest)
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPoolClient, key)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UpdateUserPoolClient(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	clientID := str(nr.Params, "ClientId")
	key := poolClientKey(poolID, clientID)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolClient, key)
	if err != nil {
		return nil, cognErr("ResourceNotFoundException", "Client not found", http.StatusBadRequest)
	}
	var c cognitoPoolClient
	_ = json.Unmarshal(e.Data, &c)
	if v := str(nr.Params, "ClientName"); v != "" {
		c.ClientName = v
	}
	data, _ := json.Marshal(c)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolClient, ID: key, Data: data})
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
	if _, err := p.loadPool(ctx, nr.AccountID, nr.Region, poolID); err != nil {
		return nil, err
	}
	username := str(nr.Params, "Username")
	if username == "" {
		return nil, cognErr("InvalidParameterException", "Username is required", http.StatusBadRequest)
	}
	key := poolUserKey(poolID, username)
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, key); err == nil {
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
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	return provider.OK(map[string]any{"User": userToWire(u)}), nil
}

func (p *Provider) AdminGetUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, poolUserKey(poolID, username))
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
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, key); err != nil {
		return nil, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPoolUser, key)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) AdminUpdateUserAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	key := poolUserKey(poolID, username)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, key)
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
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) AdminConfirmSignUp(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	username := str(nr.Params, "Username")
	key := poolUserKey(poolID, username)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, key)
	if err != nil {
		return nil, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
	}
	var u cognitoUser
	_ = json.Unmarshal(e.Data, &u)
	u.UserStatus = "CONFIRMED"
	u.UserLastModifiedDate = time.Now().UTC()
	data, _ := json.Marshal(u)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListUsers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolID := str(nr.Params, "UserPoolId")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtPoolUser, "")
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

// ─── Self-service auth flows ──────────────────────────────────────────────────

// rand6Digits returns a random 6-digit string for confirmation codes.
func rand6Digits() string {
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	return fmt.Sprintf("%06d", n.Int64())
}

// resolvePoolIDFromClient resolves the pool ID from a client ID.
// Falls back to using the clientID as pool ID for emulator simplicity.
func (p *Provider) resolvePoolIDFromClient(ctx context.Context, account, region, clientID string) string {
	entries, _ := p.resources.List(ctx, account, region, rtPoolClient, "")
	for _, e := range entries {
		var c cognitoPoolClient
		if json.Unmarshal(e.Data, &c) == nil && c.ClientID == clientID {
			return c.UserPoolID
		}
	}
	return clientID
}

// findUserByUsername scans all pool users for a matching username.
func (p *Provider) findUserByUsername(ctx context.Context, account, region, username string) (cognitoUser, error) {
	entries, _ := p.resources.List(ctx, account, region, rtPoolUser, "")
	for _, e := range entries {
		var u cognitoUser
		if json.Unmarshal(e.Data, &u) == nil && u.Username == username {
			return u, nil
		}
	}
	return cognitoUser{}, cognErr("UserNotFoundException", "User "+username+" not found", http.StatusBadRequest)
}

// saveUser creates or updates a user record.
func (p *Provider) saveUser(ctx context.Context, account, region string, u cognitoUser) error {
	key := poolUserKey(u.UserPoolID, u.Username)
	data, _ := json.Marshal(u)
	entry := store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data}
	if err := p.resources.Create(ctx, account, region, entry); err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, account, region, entry)
	} else {
		return err
	}
}

func maskEmail(email string) string {
	at := strings.Index(email, "@")
	if at < 2 {
		return "***"
	}
	return email[:2] + "***" + email[at:]
}

func findAttr(attrs []map[string]string, name string) string {
	for _, a := range attrs {
		if a["Name"] == name {
			return a["Value"]
		}
	}
	return ""
}

func buildMockJWT(userID, username, poolID, tokenType string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	exp := time.Now().Add(time.Hour).Unix()
	payload := fmt.Sprintf(`{"sub":%q,"email":%q,"iss":"https://cognito-idp.us-east-1.amazonaws.com/%s","token_use":%q,"exp":%d}`,
		userID, username, poolID, tokenType, exp)
	claims := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return header + "." + claims + ".JAISCLOUD_MOCK_SIG"
}

func buildAuthResult(u cognitoUser, poolID string) map[string]any {
	refreshToken := base64.StdEncoding.EncodeToString([]byte("refresh-" + u.Username))
	return map[string]any{
		"AuthenticationResult": map[string]any{
			"AccessToken":  buildMockJWT(u.UserID, u.Username, poolID, "access"),
			"IdToken":      buildMockJWT(u.UserID, u.Username, poolID, "id"),
			"RefreshToken": refreshToken,
			"ExpiresIn":    3600,
			"TokenType":    "Bearer",
		},
	}
}

// parseAuthParameters handles both direct map and member.N style params.
func parseAuthParameters(params map[string]any) map[string]string {
	out := make(map[string]string)
	if m, ok := params["AuthParameters"].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	// member.N style: AuthParameters.member.1.key / .value
	for i := 1; i <= 20; i++ {
		pfx := fmt.Sprintf("AuthParameters.member.%d.", i)
		k, ok1 := params[pfx+"key"].(string)
		v, ok2 := params[pfx+"value"].(string)
		if !ok1 || !ok2 {
			break
		}
		out[k] = v
	}
	return out
}

func (p *Provider) SignUp(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	username := str(nr.Params, "Username")
	password := str(nr.Params, "Password")
	if username == "" {
		return nil, cognErr("InvalidParameterException", "Username is required", http.StatusBadRequest)
	}

	clientID := str(nr.Params, "ClientId")
	poolID := p.resolvePoolIDFromClient(ctx, nr.AccountID, nr.Region, clientID)

	key := poolUserKey(poolID, username)
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, key); err == nil {
		return nil, cognErr("UsernameExistsException", "User "+username+" already exists", http.StatusBadRequest)
	}

	now := time.Now().UTC()
	u := cognitoUser{
		UserPoolID:           poolID,
		Username:             username,
		UserID:               randAlphaNum(32),
		Attributes:           parseAttributes(nr.Params),
		UserStatus:           "UNCONFIRMED",
		Enabled:              true,
		Password:             password,
		UserCreateDate:       now,
		UserLastModifiedDate: now,
	}
	data, _ := json.Marshal(u)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data}); err != nil {
		return nil, err
	}

	// Generate 6-digit confirmation code
	code := rand6Digits()
	slog.Info("cognito: confirmation code", "user", username, "code", code)
	codeData, _ := json.Marshal(confirmCodeRecord{Code: code})
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtConfirmCode, ID: key, Data: codeData})

	return provider.OK(map[string]any{
		"UserConfirmed": false,
		"UserSub":       u.UserID,
	}), nil
}

func (p *Provider) ConfirmSignUp(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	username := str(nr.Params, "Username")
	confirmCode := str(nr.Params, "ConfirmationCode")

	clientID := str(nr.Params, "ClientId")
	poolID := p.resolvePoolIDFromClient(ctx, nr.AccountID, nr.Region, clientID)

	key := poolUserKey(poolID, username)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPoolUser, key)
	if err != nil {
		return nil, cognErr("UserNotFoundException", "User not found", http.StatusBadRequest)
	}

	// Validate code if stored
	if codeEntry, cerr := p.resources.Get(ctx, nr.AccountID, nr.Region, rtConfirmCode, key); cerr == nil {
		var rec confirmCodeRecord
		_ = json.Unmarshal(codeEntry.Data, &rec)
		if confirmCode != "" && rec.Code != "" && confirmCode != rec.Code {
			return nil, cognErr("CodeMismatchException", "Invalid verification code provided, please try again.", http.StatusBadRequest)
		}
	}

	var u cognitoUser
	_ = json.Unmarshal(e.Data, &u)
	u.UserStatus = "CONFIRMED"
	u.UserLastModifiedDate = time.Now().UTC()
	data, _ := json.Marshal(u)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPoolUser, ID: key, Data: data})
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtConfirmCode, key)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) InitiateAuth(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.handleAuth(ctx, nr.AccountID, nr.Region, nr.Params)
}

func (p *Provider) AdminInitiateAuth(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.handleAuth(ctx, nr.AccountID, nr.Region, nr.Params)
}

func (p *Provider) handleAuth(ctx context.Context, account, region string, params map[string]any) (*model.ProviderResponse, error) {
	authFlow := str(params, "AuthFlow")
	authParams := parseAuthParameters(params)
	username := authParams["USERNAME"]
	password := authParams["PASSWORD"]

	if username == "" {
		return nil, cognErr("InvalidParameterException", "USERNAME is required in AuthParameters", http.StatusBadRequest)
	}

	// Determine pool
	poolID := str(params, "UserPoolId")
	if poolID == "" {
		clientID := str(params, "ClientId")
		poolID = p.resolvePoolIDFromClient(ctx, account, region, clientID)
	}

	// Find user — first try pool-scoped lookup, then global scan
	var u cognitoUser
	key := poolUserKey(poolID, username)
	if e, err := p.resources.Get(ctx, account, region, rtPoolUser, key); err == nil {
		_ = json.Unmarshal(e.Data, &u)
	} else {
		found, ferr := p.findUserByUsername(ctx, account, region, username)
		if ferr != nil {
			return nil, cognErr("UserNotFoundException", "User not found", http.StatusBadRequest)
		}
		u = found
	}

	if u.UserStatus == "UNCONFIRMED" {
		return nil, cognErr("UserNotConfirmedException", "User is not confirmed.", http.StatusBadRequest)
	}

	// Verify password for password-based flows
	if (authFlow == "USER_PASSWORD_AUTH" || authFlow == "USER_SRP_AUTH") &&
		password != "" && u.Password != "" && password != u.Password {
		return nil, cognErr("NotAuthorizedException", "Incorrect username or password.", http.StatusBadRequest)
	}

	return provider.OK(buildAuthResult(u, u.UserPoolID)), nil
}

func (p *Provider) RespondToAuthChallenge(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	challengeName := str(nr.Params, "ChallengeName")
	responses := parseAuthParameters(nr.Params)
	username := responses["USERNAME"]

	u, err := p.findUserByUsername(ctx, nr.AccountID, nr.Region, username)
	if err != nil {
		return nil, err
	}

	if challengeName == "NEW_PASSWORD_REQUIRED" {
		newPw := responses["NEW_PASSWORD"]
		if newPw != "" {
			u.Password = newPw
		}
		u.UserStatus = "CONFIRMED"
		u.UserLastModifiedDate = time.Now().UTC()
		_ = p.saveUser(ctx, nr.AccountID, nr.Region, u)
	}

	return provider.OK(buildAuthResult(u, u.UserPoolID)), nil
}

func (p *Provider) ForgotPassword(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	username := str(nr.Params, "Username")

	u, err := p.findUserByUsername(ctx, nr.AccountID, nr.Region, username)
	if err != nil {
		return nil, err
	}

	code := rand6Digits()
	slog.Info("cognito: password reset code", "user", username, "code", code)
	key := poolUserKey(u.UserPoolID, username)
	codeData, _ := json.Marshal(resetCodeRecord{Code: code})
	if cerr := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtResetCode, ID: key, Data: codeData}); cerr == store.ErrAlreadyExists {
		_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtResetCode, ID: key, Data: codeData})
	}

	email := findAttr(u.Attributes, "email")
	if email == "" {
		email = username + "@example.com"
	}
	return provider.OK(map[string]any{
		"CodeDeliveryDetails": map[string]any{
			"Destination":    maskEmail(email),
			"DeliveryMedium": "EMAIL",
			"AttributeName":  "email",
		},
	}), nil
}

func (p *Provider) ConfirmForgotPassword(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	username := str(nr.Params, "Username")
	code := str(nr.Params, "ConfirmationCode")
	newPassword := str(nr.Params, "Password")

	u, err := p.findUserByUsername(ctx, nr.AccountID, nr.Region, username)
	if err != nil {
		return nil, err
	}

	key := poolUserKey(u.UserPoolID, username)
	if codeEntry, cerr := p.resources.Get(ctx, nr.AccountID, nr.Region, rtResetCode, key); cerr == nil {
		var rec resetCodeRecord
		_ = json.Unmarshal(codeEntry.Data, &rec)
		if code != "" && rec.Code != "" && code != rec.Code {
			return nil, cognErr("CodeMismatchException", "Invalid verification code provided, please try again.", http.StatusBadRequest)
		}
		_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtResetCode, key)
	}

	if newPassword != "" {
		u.Password = newPassword
	}
	u.UserStatus = "CONFIRMED"
	u.UserLastModifiedDate = time.Now().UTC()
	_ = p.saveUser(ctx, nr.AccountID, nr.Region, u)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ResendConfirmationCode(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	username := str(nr.Params, "Username")

	u, err := p.findUserByUsername(ctx, nr.AccountID, nr.Region, username)
	if err != nil {
		return nil, err
	}

	code := rand6Digits()
	slog.Info("cognito: confirmation code", "user", username, "code", code)
	key := poolUserKey(u.UserPoolID, username)
	codeData, _ := json.Marshal(confirmCodeRecord{Code: code})
	if cerr := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtConfirmCode, ID: key, Data: codeData}); cerr == store.ErrAlreadyExists {
		_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtConfirmCode, ID: key, Data: codeData})
	}

	email := findAttr(u.Attributes, "email")
	if email == "" {
		email = username + "@example.com"
	}
	return provider.OK(map[string]any{
		"CodeDeliveryDetails": map[string]any{
			"Destination":    maskEmail(email),
			"DeliveryMedium": "EMAIL",
			"AttributeName":  "email",
		},
	}), nil
}
