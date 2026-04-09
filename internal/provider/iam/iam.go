// Package iam implements the IAM and STS providers.
// IAM resources (roles, policies, users, access keys) are stored as control-plane
// entries in the ResourceStore. No separate data-plane store is needed.
package iam

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// IAMProvider handles IAM and STS operations.
type IAMProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *IAMProvider {
	return &IAMProvider{resources: resources}
}

// Routes returns handler registrations for both IAM and STS prefixes.
func (p *IAMProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Roles
		"IAM.CreateRole":                p.CreateRole,
		"IAM.GetRole":                   p.GetRole,
		"IAM.DeleteRole":                p.DeleteRole,
		"IAM.ListRoles":                 p.ListRoles,
		"IAM.UpdateAssumeRolePolicy":    p.UpdateAssumeRolePolicy,
		// Policies
		"IAM.CreatePolicy":              p.CreatePolicy,
		"IAM.GetPolicy":                 p.GetPolicy,
		"IAM.DeletePolicy":              p.DeletePolicy,
		"IAM.ListPolicies":              p.ListPolicies,
		// Role-policy attachments
		"IAM.AttachRolePolicy":          p.AttachRolePolicy,
		"IAM.DetachRolePolicy":          p.DetachRolePolicy,
		"IAM.ListAttachedRolePolicies":  p.ListAttachedRolePolicies,
		"IAM.PutRolePolicy":             p.PutRolePolicy,
		"IAM.GetRolePolicy":             p.GetRolePolicy,
		"IAM.DeleteRolePolicy":          p.DeleteRolePolicy,
		"IAM.ListRolePolicies":          p.ListRolePolicies,
		// Users
		"IAM.CreateUser":                p.CreateUser,
		"IAM.GetUser":                   p.GetUser,
		"IAM.DeleteUser":                p.DeleteUser,
		"IAM.ListUsers":                 p.ListUsers,
		// Access keys
		"IAM.CreateAccessKey":           p.CreateAccessKey,
		"IAM.DeleteAccessKey":           p.DeleteAccessKey,
		"IAM.ListAccessKeys":            p.ListAccessKeys,
		// Tags
		"IAM.TagRole":                   p.TagRole,
		"IAM.UntagRole":                 p.UntagRole,
		"IAM.ListRoleTags":              p.ListRoleTags,
		// STS
		"STS.AssumeRole":                p.AssumeRole,
		"STS.GetCallerIdentity":         p.GetCallerIdentity,
		"STS.GetSessionToken":           p.GetSessionToken,
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

func randID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func saveEntry(ctx context.Context, rs store.ResourceStore, resType, id string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	entry := store.ResourceEntry{Type: resType, ID: id, Data: json.RawMessage(raw)}
	if err := rs.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			return rs.Update(ctx, entry)
		}
		return err
	}
	return nil
}

func loadEntry(ctx context.Context, rs store.ResourceStore, resType, id string, out any) error {
	e, err := rs.Get(ctx, resType, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(e.Data, out)
}

func arnForRole(accountID, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:role/%s", accountID, name)
}
func arnForPolicy(accountID, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:policy/%s", accountID, name)
}
func arnForUser(accountID, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, name)
}

// ─── Roles ───────────────────────────────────────────────────────────────────

type roleData struct {
	RoleName                 string            `json:"RoleName"`
	RoleID                   string            `json:"RoleId"`
	Arn                      string            `json:"Arn"`
	Path                     string            `json:"Path"`
	AssumeRolePolicyDocument string            `json:"AssumeRolePolicyDocument"`
	Description              string            `json:"Description"`
	MaxSessionDuration       int               `json:"MaxSessionDuration"`
	Tags                     map[string]string `json:"Tags"`
	CreateDate               time.Time         `json:"CreateDate"`
}

func (p *IAMProvider) CreateRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "RoleName")
	if name == "" {
		return nil, model.NewProviderError("ValidationError", "RoleName is required", 400)
	}
	arn := arnForRole(nr.AccountID, name)
	path := strParam(nr.Params, "Path")
	if path == "" {
		path = "/"
	}
	maxDur := 3600
	r := roleData{
		RoleName:                 name,
		RoleID:                   "AROA" + randID(16),
		Arn:                      arn,
		Path:                     path,
		AssumeRolePolicyDocument: strParam(nr.Params, "AssumeRolePolicyDocument"),
		Description:              strParam(nr.Params, "Description"),
		MaxSessionDuration:       maxDur,
		Tags:                     map[string]string{},
		CreateDate:               time.Now().UTC(),
	}
	if err := saveEntry(ctx, p.resources, "iam_roles", arn, r); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("EntityAlreadyExists", "Role already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Role": roleMap(r)}), nil
}

func (p *IAMProvider) GetRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "RoleName")
	arn := arnForRole(nr.AccountID, name)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NoSuchEntity", "Role not found", 404)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Role": roleMap(r)}), nil
}

func (p *IAMProvider) DeleteRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "RoleName")
	arn := arnForRole(nr.AccountID, name)
	if err := p.resources.Delete(ctx, "iam_roles", arn); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NoSuchEntity", "Role not found", 404)
		}
		return nil, err
	}
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListRoles(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, "iam_roles", "")
	if err != nil {
		return nil, err
	}
	var roles []map[string]any
	for _, e := range entries {
		var r roleData
		if err := json.Unmarshal(e.Data, &r); err == nil {
			roles = append(roles, roleMap(r))
		}
	}
	if roles == nil {
		roles = []map[string]any{}
	}
	return provider.OK(map[string]any{"Roles": roles, "IsTruncated": false}), nil
}

func (p *IAMProvider) UpdateAssumeRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "RoleName")
	arn := arnForRole(nr.AccountID, name)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Role not found", 404)
	}
	r.AssumeRolePolicyDocument = strParam(nr.Params, "PolicyDocument")
	return provider.OK(nil), saveEntry(ctx, p.resources, "iam_roles", arn, r)
}

func roleMap(r roleData) map[string]any {
	return map[string]any{
		"RoleName":                 r.RoleName,
		"RoleId":                   r.RoleID,
		"Arn":                      r.Arn,
		"Path":                     r.Path,
		"AssumeRolePolicyDocument": r.AssumeRolePolicyDocument,
		"Description":              r.Description,
		"MaxSessionDuration":       r.MaxSessionDuration,
		"CreateDate":               r.CreateDate.Format(time.RFC3339),
	}
}

// ─── Policies ────────────────────────────────────────────────────────────────

type policyData struct {
	PolicyName     string    `json:"PolicyName"`
	PolicyID       string    `json:"PolicyId"`
	Arn            string    `json:"Arn"`
	Path           string    `json:"Path"`
	Description    string    `json:"Description"`
	Document       string    `json:"Document"`
	CreateDate     time.Time `json:"CreateDate"`
	UpdateDate     time.Time `json:"UpdateDate"`
	AttachmentCount int      `json:"AttachmentCount"`
}

func (p *IAMProvider) CreatePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "PolicyName")
	if name == "" {
		return nil, model.NewProviderError("ValidationError", "PolicyName is required", 400)
	}
	arn := arnForPolicy(nr.AccountID, name)
	now := time.Now().UTC()
	pol := policyData{
		PolicyName:  name,
		PolicyID:    "ANPA" + randID(16),
		Arn:         arn,
		Path:        "/",
		Description: strParam(nr.Params, "Description"),
		Document:    strParam(nr.Params, "PolicyDocument"),
		CreateDate:  now,
		UpdateDate:  now,
	}
	if err := saveEntry(ctx, p.resources, "iam_policies", arn, pol); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("EntityAlreadyExists", "Policy already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Policy": policyMap(pol)}), nil
}

func (p *IAMProvider) GetPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	var pol policyData
	if err := loadEntry(ctx, p.resources, "iam_policies", arn, &pol); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Policy not found", 404)
	}
	return provider.OK(map[string]any{"Policy": policyMap(pol)}), nil
}

func (p *IAMProvider) DeletePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	if err := p.resources.Delete(ctx, "iam_policies", arn); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Policy not found", 404)
	}
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, "iam_policies", "")
	if err != nil {
		return nil, err
	}
	var policies []map[string]any
	for _, e := range entries {
		var pol policyData
		if err := json.Unmarshal(e.Data, &pol); err == nil {
			policies = append(policies, policyMap(pol))
		}
	}
	if policies == nil {
		policies = []map[string]any{}
	}
	return provider.OK(map[string]any{"Policies": policies, "IsTruncated": false}), nil
}

func policyMap(pol policyData) map[string]any {
	return map[string]any{
		"PolicyName":      pol.PolicyName,
		"PolicyId":        pol.PolicyID,
		"Arn":             pol.Arn,
		"Path":            pol.Path,
		"Description":     pol.Description,
		"AttachmentCount": pol.AttachmentCount,
		"CreateDate":      pol.CreateDate.Format(time.RFC3339),
		"UpdateDate":      pol.UpdateDate.Format(time.RFC3339),
	}
}

// ─── Role-policy attachments ──────────────────────────────────────────────────

type attachmentData struct {
	RoleArn   string `json:"RoleArn"`
	PolicyArn string `json:"PolicyArn"`
}

func attachKey(roleArn, policyArn string) string { return roleArn + "::" + policyArn }

func (p *IAMProvider) AttachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyArn := strParam(nr.Params, "PolicyArn")
	roleArn := arnForRole(nr.AccountID, roleName)
	d := attachmentData{RoleArn: roleArn, PolicyArn: policyArn}
	_ = saveEntry(ctx, p.resources, "iam_attachments", attachKey(roleArn, policyArn), d)
	return provider.OK(nil), nil
}

func (p *IAMProvider) DetachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyArn := strParam(nr.Params, "PolicyArn")
	roleArn := arnForRole(nr.AccountID, roleName)
	_ = p.resources.Delete(ctx, "iam_attachments", attachKey(roleArn, policyArn))
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListAttachedRolePolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	roleArn := arnForRole(nr.AccountID, roleName)
	entries, _ := p.resources.List(ctx, "iam_attachments", roleArn)
	var attached []map[string]any
	for _, e := range entries {
		var d attachmentData
		if err := json.Unmarshal(e.Data, &d); err == nil {
			parts := strings.Split(d.PolicyArn, "/")
			attached = append(attached, map[string]any{
				"PolicyName": parts[len(parts)-1],
				"PolicyArn":  d.PolicyArn,
			})
		}
	}
	if attached == nil {
		attached = []map[string]any{}
	}
	return provider.OK(map[string]any{"AttachedPolicies": attached, "IsTruncated": false}), nil
}

// Inline role policies.
type inlinePolicyData struct {
	RoleName       string `json:"RoleName"`
	PolicyName     string `json:"PolicyName"`
	PolicyDocument string `json:"PolicyDocument"`
}

func (p *IAMProvider) PutRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyName := strParam(nr.Params, "PolicyName")
	doc := strParam(nr.Params, "PolicyDocument")
	d := inlinePolicyData{RoleName: roleName, PolicyName: policyName, PolicyDocument: doc}
	_ = saveEntry(ctx, p.resources, "iam_inline_policies", roleName+"::"+policyName, d)
	return provider.OK(nil), nil
}

func (p *IAMProvider) GetRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyName := strParam(nr.Params, "PolicyName")
	var d inlinePolicyData
	if err := loadEntry(ctx, p.resources, "iam_inline_policies", roleName+"::"+policyName, &d); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Policy not found", 404)
	}
	return provider.OK(map[string]any{
		"RoleName":       roleName,
		"PolicyName":     policyName,
		"PolicyDocument": d.PolicyDocument,
	}), nil
}

func (p *IAMProvider) DeleteRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyName := strParam(nr.Params, "PolicyName")
	_ = p.resources.Delete(ctx, "iam_inline_policies", roleName+"::"+policyName)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListRolePolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	entries, _ := p.resources.List(ctx, "iam_inline_policies", roleName+"::")
	var names []string
	for _, e := range entries {
		var d inlinePolicyData
		if err := json.Unmarshal(e.Data, &d); err == nil {
			names = append(names, d.PolicyName)
		}
	}
	if names == nil {
		names = []string{}
	}
	return provider.OK(map[string]any{"PolicyNames": names, "IsTruncated": false}), nil
}

// ─── Users ────────────────────────────────────────────────────────────────────

type userData struct {
	UserName   string            `json:"UserName"`
	UserID     string            `json:"UserID"`
	Arn        string            `json:"Arn"`
	Path       string            `json:"Path"`
	Tags       map[string]string `json:"Tags"`
	CreateDate time.Time         `json:"CreateDate"`
}

func (p *IAMProvider) CreateUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "UserName")
	if name == "" {
		return nil, model.NewProviderError("ValidationError", "UserName is required", 400)
	}
	arn := arnForUser(nr.AccountID, name)
	u := userData{
		UserName:   name,
		UserID:     "AIDA" + randID(16),
		Arn:        arn,
		Path:       "/",
		Tags:       map[string]string{},
		CreateDate: time.Now().UTC(),
	}
	if err := saveEntry(ctx, p.resources, "iam_users", arn, u); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("EntityAlreadyExists", "User already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"User": userMap(u)}), nil
}

func (p *IAMProvider) GetUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "UserName")
	if name == "" {
		// GetUser with no UserName returns the caller's identity.
		return provider.OK(map[string]any{"User": map[string]any{
			"UserName": "root",
			"UserId":   nr.AccountID,
			"Arn":      fmt.Sprintf("arn:aws:iam::%s:root", nr.AccountID),
			"Path":     "/",
		}}), nil
	}
	arn := arnForUser(nr.AccountID, name)
	var u userData
	if err := loadEntry(ctx, p.resources, "iam_users", arn, &u); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "User not found", 404)
	}
	return provider.OK(map[string]any{"User": userMap(u)}), nil
}

func (p *IAMProvider) DeleteUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "UserName")
	arn := arnForUser(nr.AccountID, name)
	_ = p.resources.Delete(ctx, "iam_users", arn)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListUsers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "iam_users", "")
	var users []map[string]any
	for _, e := range entries {
		var u userData
		if err := json.Unmarshal(e.Data, &u); err == nil {
			users = append(users, userMap(u))
		}
	}
	if users == nil {
		users = []map[string]any{}
	}
	return provider.OK(map[string]any{"Users": users, "IsTruncated": false}), nil
}

func userMap(u userData) map[string]any {
	return map[string]any{
		"UserName":   u.UserName,
		"UserId":     u.UserID,
		"Arn":        u.Arn,
		"Path":       u.Path,
		"CreateDate": u.CreateDate.Format(time.RFC3339),
	}
}

// ─── Access keys ─────────────────────────────────────────────────────────────

type accessKeyData struct {
	AccessKeyID     string    `json:"AccessKeyId"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	UserName        string    `json:"UserName"`
	Status          string    `json:"Status"`
	CreateDate      time.Time `json:"CreateDate"`
}

func (p *IAMProvider) CreateAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	keyID := "AKIA" + randID(16)
	secret := randID(20)
	ak := accessKeyData{
		AccessKeyID:     keyID,
		SecretAccessKey: secret,
		UserName:        userName,
		Status:          "Active",
		CreateDate:      time.Now().UTC(),
	}
	_ = saveEntry(ctx, p.resources, "iam_access_keys", keyID, ak)
	return provider.OK(map[string]any{"AccessKey": map[string]any{
		"AccessKeyId":     ak.AccessKeyID,
		"SecretAccessKey": ak.SecretAccessKey,
		"UserName":        ak.UserName,
		"Status":          ak.Status,
		"CreateDate":      ak.CreateDate.Format(time.RFC3339),
	}}), nil
}

func (p *IAMProvider) DeleteAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID := strParam(nr.Params, "AccessKeyId")
	_ = p.resources.Delete(ctx, "iam_access_keys", keyID)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListAccessKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	entries, _ := p.resources.List(ctx, "iam_access_keys", "")
	var keys []map[string]any
	for _, e := range entries {
		var ak accessKeyData
		if err := json.Unmarshal(e.Data, &ak); err == nil {
			if userName == "" || ak.UserName == userName {
				keys = append(keys, map[string]any{
					"AccessKeyId": ak.AccessKeyID,
					"UserName":    ak.UserName,
					"Status":      ak.Status,
					"CreateDate":  ak.CreateDate.Format(time.RFC3339),
				})
			}
		}
	}
	if keys == nil {
		keys = []map[string]any{}
	}
	return provider.OK(map[string]any{"AccessKeyMetadata": keys, "IsTruncated": false}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *IAMProvider) TagRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	arn := arnForRole(nr.AccountID, roleName)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Role not found", 404)
	}
	if tags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range tags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				r.Tags[k] = v
			}
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "iam_roles", arn, r)
}

func (p *IAMProvider) UntagRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	arn := arnForRole(nr.AccountID, roleName)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Role not found", 404)
	}
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(r.Tags, fmt.Sprintf("%v", k))
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "iam_roles", arn, r)
}

func (p *IAMProvider) ListRoleTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	arn := arnForRole(nr.AccountID, roleName)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Role not found", 404)
	}
	var tags []map[string]any
	for k, v := range r.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	if tags == nil {
		tags = []map[string]any{}
	}
	return provider.OK(map[string]any{"Tags": tags, "IsTruncated": false}), nil
}

// ─── STS ─────────────────────────────────────────────────────────────────────

func (p *IAMProvider) AssumeRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	sessionName := strParam(nr.Params, "RoleSessionName")
	if sessionName == "" {
		sessionName = "session"
	}
	expiry := time.Now().UTC().Add(time.Hour)
	return provider.OK(map[string]any{
		"Credentials": map[string]any{
			"AccessKeyId":     "ASIA" + randID(16),
			"SecretAccessKey": randID(20),
			"SessionToken":    randID(32),
			"Expiration":      expiry.Format(time.RFC3339),
		},
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": roleArn + ":" + sessionName,
			"Arn":           roleArn + "/" + sessionName,
		},
	}), nil
}

func (p *IAMProvider) GetCallerIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"Account": nr.AccountID,
		"Arn":     fmt.Sprintf("arn:aws:iam::%s:root", nr.AccountID),
		"UserId":  nr.AccountID,
	}), nil
}

func (p *IAMProvider) GetSessionToken(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	expiry := time.Now().UTC().Add(12 * time.Hour)
	return provider.OK(map[string]any{
		"Credentials": map[string]any{
			"AccessKeyId":     "ASIA" + randID(16),
			"SecretAccessKey": randID(20),
			"SessionToken":    randID(32),
			"Expiration":      expiry.Format(time.RFC3339),
		},
	}), nil
}
