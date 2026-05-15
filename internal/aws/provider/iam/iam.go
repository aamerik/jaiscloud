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
	p := &IAMProvider{resources: resources}
	p.seedManagedPolicies(context.Background())
	return p
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
		"IAM.UpdateUser":                p.UpdateUser,
		// User policy attachments
		"IAM.AttachUserPolicy":          p.AttachUserPolicy,
		"IAM.DetachUserPolicy":          p.DetachUserPolicy,
		"IAM.ListAttachedUserPolicies":  p.ListAttachedUserPolicies,
		"IAM.PutUserPolicy":             p.PutUserPolicy,
		"IAM.GetUserPolicy":             p.GetUserPolicy,
		"IAM.DeleteUserPolicy":          p.DeleteUserPolicy,
		"IAM.ListUserPolicies":          p.ListUserPolicies,
		// User tags
		"IAM.TagUser":                   p.TagUser,
		"IAM.UntagUser":                 p.UntagUser,
		"IAM.ListUserTags":              p.ListUserTags,
		// Access keys
		"IAM.CreateAccessKey":           p.CreateAccessKey,
		"IAM.DeleteAccessKey":           p.DeleteAccessKey,
		"IAM.ListAccessKeys":            p.ListAccessKeys,
		"IAM.UpdateAccessKey":           p.UpdateAccessKey,
		// Tags
		"IAM.TagRole":                   p.TagRole,
		"IAM.UntagRole":                 p.UntagRole,
		"IAM.ListRoleTags":              p.ListRoleTags,
		// Groups
		"IAM.CreateGroup":               p.CreateGroup,
		"IAM.GetGroup":                  p.GetGroup,
		"IAM.DeleteGroup":               p.DeleteGroup,
		"IAM.ListGroups":                p.ListGroups,
		"IAM.AddUserToGroup":            p.AddUserToGroup,
		"IAM.RemoveUserFromGroup":       p.RemoveUserFromGroup,
		"IAM.ListGroupsForUser":         p.ListGroupsForUser,
		// Instance profiles
		"IAM.CreateInstanceProfile":         p.CreateInstanceProfile,
		"IAM.GetInstanceProfile":            p.GetInstanceProfile,
		"IAM.DeleteInstanceProfile":         p.DeleteInstanceProfile,
		"IAM.AddRoleToInstanceProfile":      p.AddRoleToInstanceProfile,
		"IAM.RemoveRoleFromInstanceProfile": p.RemoveRoleFromInstanceProfile,
		"IAM.ListInstanceProfiles":          p.ListInstanceProfiles,
		// Policy simulation
		"IAM.SimulatePrincipalPolicy":   p.SimulatePrincipalPolicy,
		"IAM.SimulateCustomPolicy":      p.SimulateCustomPolicy,
		// Service-linked roles
		"IAM.CreateServiceLinkedRole":              p.CreateServiceLinkedRole,
		"IAM.DeleteServiceLinkedRole":              p.DeleteServiceLinkedRole,
		"IAM.GetServiceLinkedRoleDeletionStatus":   p.GetServiceLinkedRoleDeletionStatus,
		// Policy versioning (14.9)
		"IAM.CreatePolicyVersion":     p.CreatePolicyVersion,
		"IAM.GetPolicyVersion":        p.GetPolicyVersion,
		"IAM.DeletePolicyVersion":     p.DeletePolicyVersion,
		"IAM.ListPolicyVersions":      p.ListPolicyVersions,
		"IAM.SetDefaultPolicyVersion": p.SetDefaultPolicyVersion,
		// OIDC providers (14.10)
		"IAM.CreateOpenIDConnectProvider":                   p.CreateOpenIDConnectProvider,
		"IAM.GetOpenIDConnectProvider":                      p.GetOpenIDConnectProvider,
		"IAM.ListOpenIDConnectProviders":                    p.ListOpenIDConnectProviders,
		"IAM.DeleteOpenIDConnectProvider":                   p.DeleteOpenIDConnectProvider,
		"IAM.UpdateOpenIDConnectProviderThumbprint":         p.UpdateOpenIDConnectProviderThumbprint,
		"IAM.AddClientIDToOpenIDConnectProvider":            p.AddClientIDToOpenIDConnectProvider,
		"IAM.RemoveClientIDFromOpenIDConnectProvider":       p.RemoveClientIDFromOpenIDConnectProvider,
		"IAM.TagOpenIDConnectProvider":                      p.TagOpenIDConnectProvider,
		"IAM.UntagOpenIDConnectProvider":                    p.UntagOpenIDConnectProvider,
		"IAM.ListOpenIDConnectProviderTags":                 p.ListOpenIDConnectProviderTags,
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

func randID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

const akAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

func randAccessKeyID() string {
	b := make([]byte, 16)
	rand.Read(b)
	out := make([]byte, 16)
	for i := range b {
		out[i] = akAlphabet[int(b[i])%len(akAlphabet)]
	}
	return "AKIA" + string(out)
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
	assumeDoc := strParam(nr.Params, "AssumeRolePolicyDocument")
	if err := ValidatePolicyDocument(assumeDoc); err != nil {
		return nil, model.NewProviderError("MalformedPolicyDocument", err.Error(), 400)
	}
	arn := nr.ResourceID("iam-role", name)
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
		AssumeRolePolicyDocument: assumeDoc,
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
	arn := nr.ResourceID("iam-role", name)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		if err == store.ErrNotFound {
			return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Role not found")
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Role": roleMap(r)}), nil
}

func (p *IAMProvider) DeleteRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "RoleName")
	arn := nr.ResourceID("iam-role", name)
	// Check for attached managed policies
	attachments, _ := p.resources.List(ctx, "iam_attachments", arn)
	if len(attachments) > 0 {
		return nil, model.NewProviderError("DeleteConflict", "Cannot delete entity, must detach all policies first", 409)
	}
	if err := p.resources.Delete(ctx, "iam_roles", arn); err != nil {
		if err == store.ErrNotFound {
			return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Role not found")
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
	arn := nr.ResourceID("iam-role", name)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Role not found")
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
		"CreateDate":               r.CreateDate.UTC().Format(time.RFC3339),
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
	doc := strParam(nr.Params, "PolicyDocument")
	if err := ValidatePolicyDocument(doc); err != nil {
		return nil, model.NewProviderError("MalformedPolicyDocument", err.Error(), 400)
	}
	arn := nr.ResourceID("iam-policy", name)
	now := time.Now().UTC()
	pol := policyData{
		PolicyName:  name,
		PolicyID:    "ANPA" + randID(16),
		Arn:         arn,
		Path:        "/",
		Description: strParam(nr.Params, "Description"),
		Document:    doc,
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
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Policy not found")
	}
	return provider.OK(map[string]any{"Policy": policyMap(pol)}), nil
}

func (p *IAMProvider) DeletePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	var pol policyData
	if err := loadEntry(ctx, p.resources, "iam_policies", arn, &pol); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Policy not found")
	}
	if pol.AttachmentCount > 0 {
		return nil, model.NewProviderError("DeleteConflict", "Cannot delete a default version of a policy. To delete a policy, delete all versions of the policy and then delete the policy", 409)
	}
	if err := p.resources.Delete(ctx, "iam_policies", arn); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Policy not found")
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
		"CreateDate":      pol.CreateDate.UTC().Format(time.RFC3339),
		"UpdateDate":      pol.UpdateDate.UTC().Format(time.RFC3339),
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
	roleArn := nr.ResourceID("iam-role", roleName)
	d := attachmentData{RoleArn: roleArn, PolicyArn: policyArn}
	_ = saveEntry(ctx, p.resources, "iam_attachments", attachKey(roleArn, policyArn), d)
	return provider.OK(nil), nil
}

func (p *IAMProvider) DetachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyArn := strParam(nr.Params, "PolicyArn")
	roleArn := nr.ResourceID("iam-role", roleName)
	_ = p.resources.Delete(ctx, "iam_attachments", attachKey(roleArn, policyArn))
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListAttachedRolePolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	roleArn := nr.ResourceID("iam-role", roleName)
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
	if err := ValidatePolicyDocument(doc); err != nil {
		return nil, model.NewProviderError("MalformedPolicyDocument", err.Error(), 400)
	}
	d := inlinePolicyData{RoleName: roleName, PolicyName: policyName, PolicyDocument: doc}
	_ = saveEntry(ctx, p.resources, "iam_inline_policies", roleName+"::"+policyName, d)
	return provider.OK(nil), nil
}

func (p *IAMProvider) GetRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	policyName := strParam(nr.Params, "PolicyName")
	var d inlinePolicyData
	if err := loadEntry(ctx, p.resources, "iam_inline_policies", roleName+"::"+policyName, &d); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Policy not found")
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
	arn := nr.ResourceID("iam-user", name)
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
			"Arn":      nr.ResourceID("iam-root", ""),
			"Path":     "/",
		}}), nil
	}
	arn := nr.ResourceID("iam-user", name)
	var u userData
	if err := loadEntry(ctx, p.resources, "iam_users", arn, &u); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "User not found")
	}
	return provider.OK(map[string]any{"User": userMap(u)}), nil
}

func (p *IAMProvider) DeleteUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "UserName")
	arn := nr.ResourceID("iam-user", name)
	// Check for attached managed policies
	attachments, _ := p.resources.List(ctx, "iam_user_attachments", arn)
	if len(attachments) > 0 {
		return nil, model.NewProviderError("DeleteConflict", "Cannot delete entity, must detach all policies first", 409)
	}
	// Check for access keys
	allKeys, _ := p.resources.List(ctx, "iam_access_keys", "")
	for _, e := range allKeys {
		var ak accessKeyData
		if json.Unmarshal(e.Data, &ak) == nil && ak.UserName == name {
			return nil, model.NewProviderError("DeleteConflict", "Cannot delete entity, must delete access keys first", 409)
		}
	}
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
		"CreateDate": u.CreateDate.UTC().Format(time.RFC3339),
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
	keyID := randAccessKeyID()
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
		"CreateDate":      ak.CreateDate.UTC().Format(time.RFC3339),
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
					"CreateDate":  ak.CreateDate.UTC().Format(time.RFC3339),
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
	arn := nr.ResourceID("iam-role", roleName)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Role not found")
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
	arn := nr.ResourceID("iam-role", roleName)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Role not found")
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
	arn := nr.ResourceID("iam-role", roleName)
	var r roleData
	if err := loadEntry(ctx, p.resources, "iam_roles", arn, &r); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Role not found")
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

// ─── UpdateUser ───────────────────────────────────────────────────────────────

func (p *IAMProvider) UpdateUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "UserName")
	arn := nr.ResourceID("iam-user", name)
	var u userData
	if err := loadEntry(ctx, p.resources, "iam_users", arn, &u); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "User not found")
	}
	if newName := strParam(nr.Params, "NewUserName"); newName != "" {
		_ = p.resources.Delete(ctx, "iam_users", arn)
		u.UserName = newName
		u.Arn = nr.ResourceID("iam-user", newName)
		_ = saveEntry(ctx, p.resources, "iam_users", u.Arn, u)
	}
	return provider.OK(nil), nil
}

// ─── User policy attachments ──────────────────────────────────────────────────

func (p *IAMProvider) AttachUserPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	policyArn := strParam(nr.Params, "PolicyArn")
	userArn := nr.ResourceID("iam-user", userName)
	d := attachmentData{RoleArn: userArn, PolicyArn: policyArn}
	_ = saveEntry(ctx, p.resources, "iam_user_attachments", attachKey(userArn, policyArn), d)
	return provider.OK(nil), nil
}

func (p *IAMProvider) DetachUserPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	policyArn := strParam(nr.Params, "PolicyArn")
	userArn := nr.ResourceID("iam-user", userName)
	_ = p.resources.Delete(ctx, "iam_user_attachments", attachKey(userArn, policyArn))
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListAttachedUserPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	userArn := nr.ResourceID("iam-user", userName)
	entries, _ := p.resources.List(ctx, "iam_user_attachments", userArn)
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

func (p *IAMProvider) PutUserPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	policyName := strParam(nr.Params, "PolicyName")
	doc := strParam(nr.Params, "PolicyDocument")
	if err := ValidatePolicyDocument(doc); err != nil {
		return nil, model.NewProviderError("MalformedPolicyDocument", err.Error(), 400)
	}
	d := inlinePolicyData{RoleName: userName, PolicyName: policyName, PolicyDocument: doc}
	_ = saveEntry(ctx, p.resources, "iam_user_inline_policies", userName+"::"+policyName, d)
	return provider.OK(nil), nil
}

func (p *IAMProvider) GetUserPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	policyName := strParam(nr.Params, "PolicyName")
	var d inlinePolicyData
	if err := loadEntry(ctx, p.resources, "iam_user_inline_policies", userName+"::"+policyName, &d); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Policy not found")
	}
	return provider.OK(map[string]any{
		"UserName":       userName,
		"PolicyName":     policyName,
		"PolicyDocument": d.PolicyDocument,
	}), nil
}

func (p *IAMProvider) DeleteUserPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	policyName := strParam(nr.Params, "PolicyName")
	_ = p.resources.Delete(ctx, "iam_user_inline_policies", userName+"::"+policyName)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListUserPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	entries, _ := p.resources.List(ctx, "iam_user_inline_policies", userName+"::")
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

// ─── User tags ────────────────────────────────────────────────────────────────

func (p *IAMProvider) TagUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	arn := nr.ResourceID("iam-user", userName)
	var u userData
	if err := loadEntry(ctx, p.resources, "iam_users", arn, &u); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "User not found")
	}
	if tags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range tags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				u.Tags[k] = v
			}
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "iam_users", arn, u)
}

func (p *IAMProvider) UntagUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	arn := nr.ResourceID("iam-user", userName)
	var u userData
	if err := loadEntry(ctx, p.resources, "iam_users", arn, &u); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "User not found")
	}
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(u.Tags, fmt.Sprintf("%v", k))
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "iam_users", arn, u)
}

func (p *IAMProvider) ListUserTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	arn := nr.ResourceID("iam-user", userName)
	var u userData
	if err := loadEntry(ctx, p.resources, "iam_users", arn, &u); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "User not found")
	}
	var tags []map[string]any
	for k, v := range u.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	if tags == nil {
		tags = []map[string]any{}
	}
	return provider.OK(map[string]any{"Tags": tags, "IsTruncated": false}), nil
}

// ─── UpdateAccessKey ──────────────────────────────────────────────────────────

func (p *IAMProvider) UpdateAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID := strParam(nr.Params, "AccessKeyId")
	status := strParam(nr.Params, "Status")
	var ak accessKeyData
	if err := loadEntry(ctx, p.resources, "iam_access_keys", keyID, &ak); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "AccessKey not found")
	}
	if status != "" {
		ak.Status = status
	}
	_ = saveEntry(ctx, p.resources, "iam_access_keys", keyID, ak)
	return provider.OK(nil), nil
}

// ─── Groups ───────────────────────────────────────────────────────────────────

type groupData struct {
	GroupName  string    `json:"GroupName"`
	GroupID    string    `json:"GroupId"`
	Arn        string    `json:"Arn"`
	Path       string    `json:"Path"`
	CreateDate time.Time `json:"CreateDate"`
}

type groupMembershipData struct {
	GroupName string `json:"GroupName"`
	UserName  string `json:"UserName"`
}

func (p *IAMProvider) CreateGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "GroupName")
	if name == "" {
		return nil, model.NewProviderError("ValidationError", "GroupName is required", 400)
	}
	arn := nr.ResourceID("iam-group", name)
	g := groupData{
		GroupName:  name,
		GroupID:    "AGPA" + randID(16),
		Arn:        arn,
		Path:       "/",
		CreateDate: time.Now().UTC(),
	}
	if err := saveEntry(ctx, p.resources, "iam_groups", arn, g); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("EntityAlreadyExists", "Group already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Group": groupMap(g)}), nil
}

func (p *IAMProvider) GetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "GroupName")
	arn := nr.ResourceID("iam-group", name)
	var g groupData
	if err := loadEntry(ctx, p.resources, "iam_groups", arn, &g); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "Group not found")
	}
	// List members
	entries, _ := p.resources.List(ctx, "iam_group_members", name+"::")
	var users []map[string]any
	for _, e := range entries {
		var m groupMembershipData
		if err := json.Unmarshal(e.Data, &m); err == nil {
			userArn := nr.ResourceID("iam-user", m.UserName)
			var u userData
			if err := loadEntry(ctx, p.resources, "iam_users", userArn, &u); err == nil {
				users = append(users, userMap(u))
			}
		}
	}
	if users == nil {
		users = []map[string]any{}
	}
	return provider.OK(map[string]any{
		"Group":       groupMap(g),
		"Users":       users,
		"IsTruncated": false,
	}), nil
}

func (p *IAMProvider) DeleteGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "GroupName")
	arn := nr.ResourceID("iam-group", name)
	_ = p.resources.Delete(ctx, "iam_groups", arn)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "iam_groups", "")
	var groups []map[string]any
	for _, e := range entries {
		var g groupData
		if err := json.Unmarshal(e.Data, &g); err == nil {
			groups = append(groups, groupMap(g))
		}
	}
	if groups == nil {
		groups = []map[string]any{}
	}
	return provider.OK(map[string]any{"Groups": groups, "IsTruncated": false}), nil
}

func (p *IAMProvider) AddUserToGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := strParam(nr.Params, "GroupName")
	userName := strParam(nr.Params, "UserName")
	m := groupMembershipData{GroupName: groupName, UserName: userName}
	_ = saveEntry(ctx, p.resources, "iam_group_members", groupName+"::"+userName, m)
	return provider.OK(nil), nil
}

func (p *IAMProvider) RemoveUserFromGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := strParam(nr.Params, "GroupName")
	userName := strParam(nr.Params, "UserName")
	_ = p.resources.Delete(ctx, "iam_group_members", groupName+"::"+userName)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListGroupsForUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	userName := strParam(nr.Params, "UserName")
	// Scan all group memberships for this user
	allEntries, _ := p.resources.List(ctx, "iam_group_members", "")
	var groups []map[string]any
	for _, e := range allEntries {
		var m groupMembershipData
		if err := json.Unmarshal(e.Data, &m); err == nil && m.UserName == userName {
			groupArn := nr.ResourceID("iam-group", m.GroupName)
			var g groupData
			if err := loadEntry(ctx, p.resources, "iam_groups", groupArn, &g); err == nil {
				groups = append(groups, groupMap(g))
			}
		}
	}
	if groups == nil {
		groups = []map[string]any{}
	}
	return provider.OK(map[string]any{"Groups": groups, "IsTruncated": false}), nil
}

func groupMap(g groupData) map[string]any {
	return map[string]any{
		"GroupName":  g.GroupName,
		"GroupId":    g.GroupID,
		"Arn":        g.Arn,
		"Path":       g.Path,
		"CreateDate": g.CreateDate.UTC().Format(time.RFC3339),
	}
}

// ─── Instance profiles ────────────────────────────────────────────────────────

type instanceProfileData struct {
	InstanceProfileName string    `json:"InstanceProfileName"`
	InstanceProfileID   string    `json:"InstanceProfileId"`
	Arn                 string    `json:"Arn"`
	Path                string    `json:"Path"`
	RoleNames           []string  `json:"RoleNames"`
	CreateDate          time.Time `json:"CreateDate"`
}

func (p *IAMProvider) CreateInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "InstanceProfileName")
	if name == "" {
		return nil, model.NewProviderError("ValidationError", "InstanceProfileName is required", 400)
	}
	arn := nr.ResourceID("iam-instance-profile", name)
	ip := instanceProfileData{
		InstanceProfileName: name,
		InstanceProfileID:   "AIPA" + randID(16),
		Arn:                 arn,
		Path:                "/",
		RoleNames:           []string{},
		CreateDate:          time.Now().UTC(),
	}
	if err := saveEntry(ctx, p.resources, "iam_instance_profiles", arn, ip); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("EntityAlreadyExists", "InstanceProfile already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"InstanceProfile": instanceProfileMap(ip, nil)}), nil
}

func (p *IAMProvider) GetInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "InstanceProfileName")
	arn := nr.ResourceID("iam-instance-profile", name)
	var ip instanceProfileData
	if err := loadEntry(ctx, p.resources, "iam_instance_profiles", arn, &ip); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "InstanceProfile not found")
	}
	roles := p.loadProfileRoles(ctx, ip, nr.ResourceID)
	return provider.OK(map[string]any{"InstanceProfile": instanceProfileMap(ip, roles)}), nil
}

func (p *IAMProvider) DeleteInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "InstanceProfileName")
	arn := nr.ResourceID("iam-instance-profile", name)
	_ = p.resources.Delete(ctx, "iam_instance_profiles", arn)
	return provider.OK(nil), nil
}

func (p *IAMProvider) AddRoleToInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	profileName := strParam(nr.Params, "InstanceProfileName")
	roleName := strParam(nr.Params, "RoleName")
	arn := nr.ResourceID("iam-instance-profile", profileName)
	var ip instanceProfileData
	if err := loadEntry(ctx, p.resources, "iam_instance_profiles", arn, &ip); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "InstanceProfile not found")
	}
	for _, r := range ip.RoleNames {
		if r == roleName {
			return provider.OK(nil), nil // already attached
		}
	}
	ip.RoleNames = append(ip.RoleNames, roleName)
	_ = saveEntry(ctx, p.resources, "iam_instance_profiles", arn, ip)
	return provider.OK(nil), nil
}

func (p *IAMProvider) RemoveRoleFromInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	profileName := strParam(nr.Params, "InstanceProfileName")
	roleName := strParam(nr.Params, "RoleName")
	arn := nr.ResourceID("iam-instance-profile", profileName)
	var ip instanceProfileData
	if err := loadEntry(ctx, p.resources, "iam_instance_profiles", arn, &ip); err != nil {
		return nil, provider.StoreNotFoundError(err, "NoSuchEntity", "InstanceProfile not found")
	}
	filtered := ip.RoleNames[:0]
	for _, r := range ip.RoleNames {
		if r != roleName {
			filtered = append(filtered, r)
		}
	}
	ip.RoleNames = filtered
	_ = saveEntry(ctx, p.resources, "iam_instance_profiles", arn, ip)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListInstanceProfiles(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "iam_instance_profiles", "")
	var profiles []map[string]any
	for _, e := range entries {
		var ip instanceProfileData
		if err := json.Unmarshal(e.Data, &ip); err == nil {
			roles := p.loadProfileRoles(ctx, ip, nr.ResourceID)
			profiles = append(profiles, instanceProfileMap(ip, roles))
		}
	}
	if profiles == nil {
		profiles = []map[string]any{}
	}
	return provider.OK(map[string]any{"InstanceProfiles": profiles, "IsTruncated": false}), nil
}

func (p *IAMProvider) loadProfileRoles(ctx context.Context, ip instanceProfileData, resourceIDFn func(string, string) string) []map[string]any {
	var roles []map[string]any
	for _, roleName := range ip.RoleNames {
		roleArn := resourceIDFn("iam-role", roleName)
		var r roleData
		if err := loadEntry(ctx, p.resources, "iam_roles", roleArn, &r); err == nil {
			roles = append(roles, roleMap(r))
		}
	}
	if roles == nil {
		roles = []map[string]any{}
	}
	return roles
}

func instanceProfileMap(ip instanceProfileData, roles []map[string]any) map[string]any {
	if roles == nil {
		roles = []map[string]any{}
	}
	return map[string]any{
		"InstanceProfileName": ip.InstanceProfileName,
		"InstanceProfileId":   ip.InstanceProfileID,
		"Arn":                 ip.Arn,
		"Path":                ip.Path,
		"Roles":               roles,
		"CreateDate":          ip.CreateDate.UTC().Format(time.RFC3339),
	}
}

// ─── Policy simulation ────────────────────────────────────────────────────────

func (p *IAMProvider) SimulatePrincipalPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Gather inline + attached policy documents for the principal ARN.
	principalArn := strParam(nr.Params, "PolicySourceArn")
	var docs []string

	// Try role inline policies first.
	roleName := ""
	if parts := strings.Split(principalArn, "/"); len(parts) > 1 {
		roleName = parts[len(parts)-1]
	}
	if roleName != "" {
		inlineEntries, _ := p.resources.List(ctx, "iam_inline_policies", roleName+"::")
		for _, e := range inlineEntries {
			var d inlinePolicyData
			if json.Unmarshal(e.Data, &d) == nil && d.PolicyDocument != "" {
				docs = append(docs, d.PolicyDocument)
			}
		}
		// Attached managed policies.
		roleArn := nr.ResourceID("iam-role", roleName)
		attachEntries, _ := p.resources.List(ctx, "iam_attachments", roleArn)
		for _, e := range attachEntries {
			var att attachmentData
			if json.Unmarshal(e.Data, &att) != nil {
				continue
			}
			var pol policyData
			if loadEntry(ctx, p.resources, "iam_policies", att.PolicyArn, &pol) == nil && pol.Document != "" {
				docs = append(docs, pol.Document)
			}
		}
	}

	return evalSimulation(nr.Params, docs), nil
}

func (p *IAMProvider) SimulateCustomPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Use caller-supplied policy documents.
	var docs []string
	if v, ok := nr.Params["PolicyInputList"].([]any); ok {
		for _, item := range v {
			if s, ok := item.(string); ok {
				docs = append(docs, s)
			}
		}
	}
	return evalSimulation(nr.Params, docs), nil
}

func evalSimulation(params map[string]any, policyDocs []string) *model.ProviderResponse {
	var actions []string
	if v, ok := params["ActionNames"].([]any); ok {
		for _, a := range v {
			if s, ok := a.(string); ok {
				actions = append(actions, s)
			}
		}
	}
	var resources []string
	if v, ok := params["ResourceArns"].([]any); ok {
		for _, r := range v {
			if s, ok := r.(string); ok {
				resources = append(resources, s)
			}
		}
	}
	if len(resources) == 0 {
		resources = []string{"*"}
	}

	evalResults := SimulatePolicies(policyDocs, actions, resources)

	results := make([]map[string]any, 0, len(evalResults))
	for _, er := range evalResults {
		results = append(results, map[string]any{
			"EvalActionName":       er.EvalActionName,
			"EvalResourceName":     er.EvalResourceName,
			"EvalDecision":         er.EvalDecision,
			"MatchedStatements":    []any{},
			"MissingContextValues": []any{},
		})
	}
	// Fallback: if no policy docs were provided, default to allowed (open emulator).
	if len(policyDocs) == 0 {
		results = make([]map[string]any, 0, len(actions))
		for _, action := range actions {
			for _, resource := range resources {
				results = append(results, map[string]any{
					"EvalActionName":       action,
					"EvalResourceName":     resource,
					"EvalDecision":         "allowed",
					"MatchedStatements":    []any{},
					"MissingContextValues": []any{},
				})
			}
		}
	}
	return provider.OK(map[string]any{
		"EvaluationResults": results,
		"IsTruncated":       false,
	})
}

// ─── Service-linked roles ──────────────────────────────────────────────────────

func (p *IAMProvider) CreateServiceLinkedRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	serviceName := strParam(nr.Params, "AWSServiceName")
	if serviceName == "" {
		serviceName = strParam(nr.Params, "ServiceName")
	}
	if serviceName == "" {
		return nil, model.NewProviderError("ValidationError", "AWSServiceName is required", 400)
	}

	entry, ok := slrCatalog[serviceName]
	if !ok {
		// Unknown service — create a generic SLR.
		entry = slrEntry{
			RoleName:    "AWSServiceRoleFor" + strings.Title(strings.Split(serviceName, ".")[0]),
			PolicyARN:   "",
			Description: "Service-linked role for " + serviceName,
		}
	}

	customSuffix := strParam(nr.Params, "CustomSuffix")
	roleName := "aws-service-role/" + serviceName + "/" + entry.RoleName
	if customSuffix != "" {
		roleName = roleName + "_" + customSuffix
	}
	displayRoleName := entry.RoleName
	if customSuffix != "" {
		displayRoleName = entry.RoleName + "_" + customSuffix
	}

	// Build the assume role policy document granting the service principal.
	assumeDoc := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"%s"},"Action":"sts:AssumeRole"}]}`,
		serviceName,
	)

	arn := nr.ResourceID("iam-role", roleName)
	r := roleData{
		RoleName:                 displayRoleName,
		RoleID:                   "AROA" + randID(16),
		Arn:                      arn,
		Path:                     "/aws-service-role/" + serviceName + "/",
		AssumeRolePolicyDocument: assumeDoc,
		Description:              entry.Description,
		MaxSessionDuration:       3600,
		Tags:                     map[string]string{},
		CreateDate:               time.Now().UTC(),
	}
	if err := saveEntry(ctx, p.resources, "iam_roles", arn, r); err != nil && err != store.ErrAlreadyExists {
		return nil, err
	}

	return provider.OK(map[string]any{"Role": roleMap(r)}), nil
}

func (p *IAMProvider) DeleteServiceLinkedRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleName := strParam(nr.Params, "RoleName")
	if roleName == "" {
		return nil, model.NewProviderError("ValidationError", "RoleName is required", 400)
	}
	// The deletion task ID is the role name (simplified — no async tracking needed).
	deletionTaskID := "task/" + roleName
	// Attempt deletion — ignore not-found.
	_ = p.resources.Delete(ctx, "iam_roles", nr.ResourceID("iam-role", roleName))
	return provider.OK(map[string]any{"DeletionTaskId": deletionTaskID}), nil
}

func (p *IAMProvider) GetServiceLinkedRoleDeletionStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Always report SUCCEEDED — deletion is synchronous in the emulator.
	return provider.OK(map[string]any{"Status": "SUCCEEDED"}), nil
}
