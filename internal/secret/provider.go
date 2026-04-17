package secret

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// SecretProvider handles SecretsManager API operations.
type SecretProvider struct {
	store  SecretStore
	kms    model.KeyEncryptor // nil → NoopKeyEncryptor behaviour
}

// New constructs a SecretProvider.
// kms may be nil, in which case secret values are stored unencrypted (dev/lite mode).
func New(store SecretStore, kms model.KeyEncryptor) *SecretProvider {
	return &SecretProvider{store: store, kms: kms}
}

// Routes returns all SecretsManager handler registrations.
func (p *SecretProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Secret.CreateSecret":          p.CreateSecret,
		"Secret.DescribeSecret":        p.DescribeSecret,
		"Secret.UpdateSecret":          p.UpdateSecret,
		"Secret.DeleteSecret":          p.DeleteSecret,
		"Secret.RestoreSecret":         p.RestoreSecret,
		"Secret.ListSecrets":           p.ListSecrets,
		"Secret.PutSecretValue":        p.PutSecretValue,
		"Secret.GetSecretValue":        p.GetSecretValue,
		"Secret.ListSecretVersionIds":  p.ListSecretVersionIds,
		"Secret.TagResource":           p.TagResource,
		"Secret.UntagResource":         p.UntagResource,
		"Secret.RotateSecret":          p.RotateSecret,
		"Secret.CancelRotateSecret":    p.CancelRotateSecret,
		"Secret.GetResourcePolicy":     p.GetResourcePolicy,
		"Secret.PutResourcePolicy":     p.PutResourcePolicy,
		"Secret.DeleteResourcePolicy":  p.DeleteResourcePolicy,
	}
}

// ─── Secret lifecycle ─────────────────────────────────────────────────────────

func (p *SecretProvider) CreateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "Name is required", 400)
	}
	secretID := newID()
	kmsKeyID, _ := nr.Params["KmsKeyId"].(string)
	desc, _ := nr.Params["Description"].(string)
	tags := extractTags(nr.Params)

	e := SecretEntry{
		SecretID:    secretID,
		Name:        name,
		Description: desc,
		KMSKeyID:    kmsKeyID,
		Tags:        tags,
	}
	if err := p.store.CreateSecret(ctx, e); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, model.NewProviderError("ResourceExistsException", "secret already exists: "+name, 400)
		}
		return nil, fmt.Errorf("sm: create secret: %w", err)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, name)

	// Store initial value if provided.
	if sv, _ := nr.Params["SecretString"].(string); sv != "" {
		if err := p.putValue(ctx, secretID, kmsKeyID, []byte(sv), name); err != nil {
			return nil, err
		}
	} else if svb, _ := nr.Params["SecretBinary"].(string); svb != "" {
		raw, err := base64.StdEncoding.DecodeString(svb)
		if err != nil {
			return nil, model.NewProviderError("ValidationException", "SecretBinary must be base64-encoded", 400)
		}
		if err := p.putValue(ctx, secretID, kmsKeyID, raw, name); err != nil {
			return nil, err
		}
	}

	return provider.OK(map[string]any{
		"ARN":  secretARN,
		"Name": name,
	}), nil
}

func (p *SecretProvider) DescribeSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	versions, _ := p.store.ListVersions(ctx, e.SecretID)
	return provider.OK(p.secretDetail(e, versions, nr)), nil
}

func (p *SecretProvider) UpdateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is scheduled for deletion", 400)
	}
	if d, ok := nr.Params["Description"].(string); ok {
		e.Description = d
	}
	if k, ok := nr.Params["KmsKeyId"].(string); ok {
		e.KMSKeyID = k
	}
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: update secret: %w", err)
	}
	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.Name)
	return provider.OK(map[string]any{
		"ARN":  secretARN,
		"Name": e.Name,
	}), nil
}

func (p *SecretProvider) DeleteSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}

	forceDelete := false
	if v, ok := nr.Params["ForceDeleteWithoutRecovery"].(bool); ok {
		forceDelete = v
	}
	recoveryDays := int64(30)
	if v, ok := nr.Params["RecoveryWindowInDays"].(float64); ok {
		recoveryDays = int64(v)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.Name)
	deletionDate := time.Now().Add(time.Duration(recoveryDays) * 24 * time.Hour)

	if forceDelete {
		if err := p.store.DeleteSecret(ctx, e.SecretID); err != nil {
			return nil, fmt.Errorf("sm: force delete: %w", err)
		}
		return provider.OK(map[string]any{
			"ARN":          secretARN,
			"Name":         e.Name,
			"DeletionDate": deletionDate.Unix(),
		}), nil
	}

	// Soft delete — mark DeletedAt, keep data.
	now := time.Now()
	e.DeletedAt = &now
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: soft delete: %w", err)
	}
	return provider.OK(map[string]any{
		"ARN":          secretARN,
		"Name":         e.Name,
		"DeletionDate": deletionDate.Unix(),
	}), nil
}

func (p *SecretProvider) RestoreSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt == nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is not scheduled for deletion", 400)
	}
	e.DeletedAt = nil
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: restore secret: %w", err)
	}
	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.Name)
	return provider.OK(map[string]any{
		"ARN":  secretARN,
		"Name": e.Name,
	}), nil
}

func (p *SecretProvider) ListSecrets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secrets, err := p.store.ListSecrets(ctx)
	if err != nil {
		return nil, fmt.Errorf("sm: list secrets: %w", err)
	}
	items := make([]map[string]any, 0, len(secrets))
	for _, e := range secrets {
		items = append(items, map[string]any{
			"ARN":  nr.ResourceID(model.RTSecretsManagerSecret, e.Name),
			"Name": e.Name,
		})
	}
	return provider.OK(map[string]any{"SecretList": items}), nil
}

// ─── Secret values ────────────────────────────────────────────────────────────

func (p *SecretProvider) PutSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is scheduled for deletion", 400)
	}

	var raw []byte
	if sv, _ := nr.Params["SecretString"].(string); sv != "" {
		raw = []byte(sv)
	} else if svb, _ := nr.Params["SecretBinary"].(string); svb != "" {
		raw, err = base64.StdEncoding.DecodeString(svb)
		if err != nil {
			return nil, model.NewProviderError("ValidationException", "SecretBinary must be base64-encoded", 400)
		}
	} else {
		return nil, model.NewProviderError("ValidationException", "SecretString or SecretBinary is required", 400)
	}

	versionID, _ := nr.Params["ClientRequestToken"].(string)
	if versionID == "" {
		versionID = newID()
	}

	if err := p.putValueWithID(ctx, e.SecretID, e.KMSKeyID, raw, e.Name, versionID); err != nil {
		return nil, err
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.Name)
	return provider.OK(map[string]any{
		"ARN":       secretARN,
		"Name":      e.Name,
		"VersionId": versionID,
		"VersionStages": []string{"AWSCURRENT"},
	}), nil
}

func (p *SecretProvider) GetSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is scheduled for deletion", 400)
	}

	var v VersionEntry
	if vid, _ := nr.Params["VersionId"].(string); vid != "" {
		v, err = p.store.GetVersion(ctx, e.SecretID, vid)
	} else if stage, _ := nr.Params["VersionStage"].(string); stage != "" {
		v, err = p.store.GetVersionByStage(ctx, e.SecretID, stage)
	} else {
		v, err = p.store.GetVersionByStage(ctx, e.SecretID, "AWSCURRENT")
	}
	if err != nil {
		if errors.Is(err, ErrVersionNotFound) {
			return nil, model.NewProviderError("ResourceNotFoundException", "no secret value found", 400)
		}
		return nil, fmt.Errorf("sm: get version: %w", err)
	}

	// Decrypt the value.
	pt, err := p.decrypt(ctx, e.KMSKeyID, v.SecretBinary, e.Name)
	if err != nil {
		return nil, fmt.Errorf("sm: decrypt value: %w", err)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.Name)
	return provider.OK(map[string]any{
		"ARN":           secretARN,
		"Name":          e.Name,
		"VersionId":     v.VersionID,
		"VersionStages": v.Stages,
		"SecretString":  string(pt),
		"CreatedDate":   v.CreatedAt.Unix(),
	}), nil
}

func (p *SecretProvider) ListSecretVersionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	versions, err := p.store.ListVersions(ctx, e.SecretID)
	if err != nil {
		return nil, fmt.Errorf("sm: list versions: %w", err)
	}
	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, map[string]any{
			"VersionId":     v.VersionID,
			"VersionStages": v.Stages,
			"CreatedDate":   v.CreatedAt.Unix(),
		})
	}
	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.Name)
	return provider.OK(map[string]any{
		"ARN":      secretARN,
		"Name":     e.Name,
		"Versions": items,
	}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *SecretProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	newTags := extractTags(nr.Params)
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	for k, v := range newTags {
		e.Tags[k] = v
	}
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: tag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *SecretProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if tagKeys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range tagKeys {
			delete(e.Tags, fmt.Sprint(k))
		}
	}
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: untag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Rotation stubs ───────────────────────────────────────────────────────────

func (p *SecretProvider) RotateSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("NotImplementedException", "RotateSecret is not supported in JaisCloud", 501)
}

func (p *SecretProvider) CancelRotateSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// ─── Resource policy stubs ────────────────────────────────────────────────────

func (p *SecretProvider) GetResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

func (p *SecretProvider) PutResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

func (p *SecretProvider) DeleteResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// ─── internal helpers ─────────────────────────────────────────────────────────

func (p *SecretProvider) resolveSecret(ctx context.Context, nr *model.NormalizedRequest) (SecretEntry, error) {
	id, _ := nr.Params["SecretId"].(string)
	if id == "" {
		return SecretEntry{}, model.NewProviderError("ValidationException", "SecretId is required", 400)
	}
	// Try by ID first, then by name (AWS accepts both).
	e, err := p.store.GetSecret(ctx, id)
	if errors.Is(err, ErrSecretNotFound) {
		e, err = p.store.GetSecretByName(ctx, id)
	}
	if errors.Is(err, ErrSecretNotFound) {
		return SecretEntry{}, model.NewProviderError("ResourceNotFoundException", "secret not found: "+id, 400)
	}
	if err != nil {
		return SecretEntry{}, fmt.Errorf("sm: resolve secret: %w", err)
	}
	return e, nil
}

func (p *SecretProvider) putValue(ctx context.Context, secretID, kmsKeyID string, raw []byte, name string) error {
	return p.putValueWithID(ctx, secretID, kmsKeyID, raw, name, newID())
}

func (p *SecretProvider) putValueWithID(ctx context.Context, secretID, kmsKeyID string, raw []byte, name, versionID string) error {
	ct, err := p.encrypt(ctx, kmsKeyID, raw, name)
	if err != nil {
		return fmt.Errorf("sm: encrypt: %w", err)
	}
	return p.store.PutVersion(ctx, VersionEntry{
		SecretID:     secretID,
		VersionID:    versionID,
		SecretBinary: ct,
		Stages:       []string{"AWSCURRENT"},
	})
}

func (p *SecretProvider) encrypt(ctx context.Context, kmsKeyID string, pt []byte, name string) ([]byte, error) {
	if p.kms == nil {
		return pt, nil
	}
	encCtx := map[string]string{"SecretARN": name}
	return p.kms.Encrypt(ctx, kmsKeyID, pt, encCtx)
}

func (p *SecretProvider) decrypt(ctx context.Context, kmsKeyID string, ct []byte, name string) ([]byte, error) {
	if p.kms == nil {
		return ct, nil
	}
	encCtx := map[string]string{"SecretARN": name}
	return p.kms.Decrypt(ctx, kmsKeyID, ct, encCtx)
}

func (p *SecretProvider) secretDetail(e SecretEntry, versions []VersionEntry, nr *model.NormalizedRequest) map[string]any {
	versionIDs := make(map[string][]string, len(versions))
	for _, v := range versions {
		versionIDs[v.VersionID] = v.Stages
	}
	tags := make([]map[string]string, 0, len(e.Tags))
	for k, v := range e.Tags {
		tags = append(tags, map[string]string{"Key": k, "Value": v})
	}
	d := map[string]any{
		"ARN":              nr.ResourceID(model.RTSecretsManagerSecret, e.Name),
		"Name":             e.Name,
		"Description":      e.Description,
		"KmsKeyId":         e.KMSKeyID,
		"Tags":             tags,
		"VersionIdsToStages": versionIDs,
		"CreatedDate":      e.CreatedAt.Unix(),
		"LastChangedDate":  e.UpdatedAt.Unix(),
	}
	if e.DeletedAt != nil {
		d["DeletedDate"] = e.DeletedAt.Unix()
	}
	return d
}

func newID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func extractTags(params map[string]any) map[string]string {
	raw, ok := params["Tags"]
	if !ok {
		return nil
	}
	out := make(map[string]string)
	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				val, _ := m["Value"].(string)
				if k != "" {
					out[k] = val
				}
			}
		}
	case map[string]any:
		for k, val := range v {
			out[k] = fmt.Sprint(val)
		}
	}
	return out
}
