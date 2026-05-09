package key

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/resourcemgr"
)

const (
	ciphertextKeyIDLen = 36 // UUID length
)

// KeyProvider handles KMS API operations.
type KeyProvider struct {
	store KeyStore
	rm    *resourcemgr.Manager
	// serverDEK is the server data-encryption key used to protect per-KMS-key material.
	serverDEK []byte
}

// New constructs a KeyProvider.
// serverDEK is the 32-byte DEK from the bootstrap process.
func New(store KeyStore, rm *resourcemgr.Manager, serverDEK []byte) *KeyProvider {
	return &KeyProvider{store: store, rm: rm, serverDEK: serverDEK}
}

// Routes returns all KMS handler registrations.
func (p *KeyProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Key.CreateKey":         p.CreateKey,
		"Key.DescribeKey":       p.DescribeKey,
		"Key.EnableKey":         p.EnableKey,
		"Key.DisableKey":        p.DisableKey,
		"Key.ScheduleKeyDeletion": p.ScheduleKeyDeletion,
		"Key.CancelKeyDeletion":   p.CancelKeyDeletion,
		"Key.ListKeys":          p.ListKeys,
		"Key.TagResource":       p.TagResource,
		"Key.UntagResource":     p.UntagResource,
		"Key.ListResourceTags":  p.ListResourceTags,
		"Key.CreateAlias":       p.CreateAlias,
		"Key.DeleteAlias":       p.DeleteAlias,
		"Key.ListAliases":       p.ListAliases,
		"Key.UpdateAlias":       p.UpdateAlias,
		"Key.CreateGrant":       p.CreateGrant,
		"Key.RevokeGrant":       p.RevokeGrant,
		"Key.RetireGrant":       p.RetireGrant,
		"Key.ListGrants":        p.ListGrants,
		"Key.Encrypt":           p.Encrypt,
		"Key.Decrypt":           p.Decrypt,
		"Key.GenerateDataKey":   p.GenerateDataKey,
		"Key.GenerateDataKeyWithoutPlaintext": p.GenerateDataKeyWithoutPlaintext,
		"Key.ReEncrypt":         p.ReEncrypt,
		"Key.GetKeyPolicy":      p.GetKeyPolicy,
		"Key.PutKeyPolicy":      p.PutKeyPolicy,
		"Key.GetKeyRotationStatus": p.GetKeyRotationStatus,
		"Key.EnableKeyRotation":    p.EnableKeyRotation,
		"Key.DisableKeyRotation":   p.DisableKeyRotation,
	}
}

// ─── Key lifecycle ────────────────────────────────────────────────────────────

func (p *KeyProvider) CreateKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID := newID()
	keyUsage, _ := nr.Params["KeyUsage"].(string)
	if keyUsage == "" {
		keyUsage = "ENCRYPT_DECRYPT"
	}
	keySpec, _ := nr.Params["KeySpec"].(string)
	if keySpec == "" {
		keySpec = "SYMMETRIC_DEFAULT"
	}
	desc, _ := nr.Params["Description"].(string)
	tags := extractTags(nr.Params)

	// Generate per-key material (32 bytes) and encrypt with server DEK.
	keyMaterial, err := Generate32()
	if err != nil {
		return nil, fmt.Errorf("kms: generate key material: %w", err)
	}
	encMaterial, err := encryptData(p.serverDEK, keyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt key material: %w", err)
	}

	e := KeyEntry{
		KeyID:       keyID,
		Enabled:     true,
		Description: desc,
		KeyUsage:    keyUsage,
		KeySpec:     keySpec,
		Origin:      "AWS_KMS",
		Tags:        tags,
		KeyMaterial: encMaterial,
	}
	if err := p.store.CreateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: create key: %w", err)
	}

	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyMetadata": keyMetadata(e, keyARN, nr.Region, nr.AccountID),
	}), nil
}

func (p *KeyProvider) DescribeKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyMetadata": keyMetadata(e, keyARN, nr.Region, nr.AccountID),
	}), nil
}

func (p *KeyProvider) EnableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setEnabled(ctx, nr, true)
}

func (p *KeyProvider) DisableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setEnabled(ctx, nr, false)
}

func (p *KeyProvider) setEnabled(ctx context.Context, nr *model.NormalizedRequest, enabled bool) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	e.Enabled = enabled
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: update key: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ScheduleKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	pendingDays := int64(30)
	if v, ok := nr.Params["PendingWindowInDays"]; ok {
		switch n := v.(type) {
		case float64:
			pendingDays = int64(n)
		case json.Number:
			pendingDays, _ = n.Int64()
		}
	}
	if pendingDays < 7 || pendingDays > 30 {
		return nil, model.NewProviderError("ValidationException",
			fmt.Sprintf("PendingWindowInDays should be between 7 and 30, but it is %d", pendingDays), 400)
	}

	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is already pending deletion: "+keyID, 400)
	}

	deletionDate := time.Now().Add(time.Duration(pendingDays) * 24 * time.Hour)
	e.Enabled = false
	e.PendingDeletion = true
	e.DeletionDate = deletionDate
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: schedule deletion: %w", err)
	}
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyId":               keyARN,
		"DeletionDate":        deletionDate.Unix(),
		"PendingWindowInDays": pendingDays,
	}), nil
}

func (p *KeyProvider) CancelKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if !e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is not pending deletion: "+keyID, 400)
	}
	e.PendingDeletion = false
	e.DeletionDate = time.Time{}
	e.Enabled = false // AWS: CancelKeyDeletion transitions to Disabled, not Enabled
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: cancel key deletion: %w", err)
	}
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{"KeyId": keyARN}), nil
}

func (p *KeyProvider) ListKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keys, err := p.store.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("kms: list keys: %w", err)
	}
	items := make([]map[string]any, 0, len(keys))
	for _, e := range keys {
		items = append(items, map[string]any{
			"KeyId":  e.KeyID,
			"KeyArn": nr.ResourceID(model.RTKMSKey, e.KeyID),
		})
	}
	return provider.OK(map[string]any{"Keys": items, "Truncated": false}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *KeyProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	newTags := extractTags(nr.Params)
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	for k, v := range newTags {
		e.Tags[k] = v
	}
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: tag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if tagKeys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range tagKeys {
			delete(e.Tags, fmt.Sprint(k))
		}
	}
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: untag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ListResourceTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	tags := make([]map[string]string, 0, len(e.Tags))
	for k, v := range e.Tags {
		tags = append(tags, map[string]string{"TagKey": k, "TagValue": v})
	}
	return provider.OK(map[string]any{"Tags": tags, "Truncated": false}), nil
}

// ─── Aliases ──────────────────────────────────────────────────────────────────

func (p *KeyProvider) CreateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasName, _ := nr.Params["AliasName"].(string)
	targetKeyID, _ := nr.Params["TargetKeyId"].(string)
	if aliasName == "" || targetKeyID == "" {
		return nil, model.NewProviderError("ValidationException", "AliasName and TargetKeyId are required", 400)
	}
	if !strings.HasPrefix(aliasName, "alias/") {
		return nil, model.NewProviderError("ValidationException", "AliasName must begin with 'alias/'", 400)
	}
	resolvedID, err := p.resolveKeyIDStr(ctx, targetKeyID)
	if err != nil {
		return nil, err
	}
	if err := p.store.CreateAlias(ctx, AliasEntry{AliasName: aliasName, TargetKeyID: resolvedID}); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExistsException", "alias already exists: "+aliasName, 400)
		}
		return nil, fmt.Errorf("kms: create alias: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) DeleteAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasName, _ := nr.Params["AliasName"].(string)
	if err := p.store.DeleteAlias(ctx, aliasName); err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return nil, model.NewProviderError("NotFoundException", "alias not found: "+aliasName, 400)
		}
		return nil, fmt.Errorf("kms: delete alias: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) UpdateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasName, _ := nr.Params["AliasName"].(string)
	newTarget, _ := nr.Params["TargetKeyId"].(string)
	resolvedID, err := p.resolveKeyIDStr(ctx, newTarget)
	if err != nil {
		return nil, err
	}
	if err := p.store.DeleteAlias(ctx, aliasName); err != nil && !errors.Is(err, ErrAliasNotFound) {
		return nil, fmt.Errorf("kms: update alias delete: %w", err)
	}
	if err := p.store.CreateAlias(ctx, AliasEntry{AliasName: aliasName, TargetKeyID: resolvedID}); err != nil {
		return nil, fmt.Errorf("kms: update alias create: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ListAliases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, _ := nr.Params["KeyId"].(string)
	if keyID != "" {
		var err error
		keyID, err = p.resolveKeyIDStr(ctx, keyID)
		if err != nil {
			return nil, err
		}
	}
	aliases, err := p.store.ListAliases(ctx, keyID)
	if err != nil {
		return nil, fmt.Errorf("kms: list aliases: %w", err)
	}
	items := make([]map[string]any, 0, len(aliases))
	for _, a := range aliases {
		items = append(items, map[string]any{
			"AliasName":   a.AliasName,
			"TargetKeyId": a.TargetKeyID,
			"AliasArn":    nr.ResourceID(model.RTKMSAlias, strings.TrimPrefix(a.AliasName, "alias/")),
		})
	}
	return provider.OK(map[string]any{"Aliases": items, "Truncated": false}), nil
}

// ─── Grants ───────────────────────────────────────────────────────────────────

func (p *KeyProvider) CreateGrant(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	granteeARN, _ := nr.Params["GranteePrincipal"].(string)
	ops := extractStringList(nr.Params, "Operations")
	token := newID()
	grantID := newID()
	e := GrantEntry{
		GrantID: grantID, KeyID: keyID,
		GranteeARN: granteeARN, Operations: ops, Token: token,
	}
	if err := p.store.CreateGrant(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: create grant: %w", err)
	}
	return provider.OK(map[string]any{
		"GrantId":    grantID,
		"GrantToken": token,
	}), nil
}

func (p *KeyProvider) RevokeGrant(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	grantID, _ := nr.Params["GrantId"].(string)
	if err := p.store.RevokeGrant(ctx, grantID); err != nil {
		if errors.Is(err, ErrGrantNotFound) {
			return nil, model.NewProviderError("NotFoundException", "grant not found: "+grantID, 400)
		}
		return nil, fmt.Errorf("kms: revoke grant: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) RetireGrant(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Retire is semantically equivalent to Revoke in this emulator.
	return p.RevokeGrant(ctx, nr)
}

func (p *KeyProvider) ListGrants(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	grants, err := p.store.ListGrants(ctx, keyID)
	if err != nil {
		return nil, fmt.Errorf("kms: list grants: %w", err)
	}
	items := make([]map[string]any, 0, len(grants))
	for _, g := range grants {
		items = append(items, map[string]any{
			"GrantId":          g.GrantID,
			"KeyId":            g.KeyID,
			"GranteePrincipal": g.GranteeARN,
			"Operations":       g.Operations,
		})
	}
	return provider.OK(map[string]any{"Grants": items, "Truncated": false}), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (p *KeyProvider) Encrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	ptB64, _ := nr.Params["Plaintext"].(string)
	pt, err := base64.StdEncoding.DecodeString(ptB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "Plaintext must be base64-encoded", 400)
	}
	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)

	// Decrypt per-key material, then encrypt the plaintext.
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	ct, err := encryptData(keyMat, pt, aad)
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt: %w", err)
	}
	// Prepend key ID so Decrypt can identify the key without caller providing it.
	blob := buildCiphertextBlob(keyID, ct)
	return provider.OK(map[string]any{
		"KeyId":          nr.ResourceID(model.RTKMSKey, keyID),
		"CiphertextBlob": base64.StdEncoding.EncodeToString(blob),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

func (p *KeyProvider) Decrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ctB64, _ := nr.Params["CiphertextBlob"].(string)
	blob, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "CiphertextBlob must be base64-encoded", 400)
	}
	keyID, ct, err := parseCiphertextBlob(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "invalid ciphertext blob", 400)
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)

	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	pt, err := decryptData(keyMat, ct, aad)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "decryption failed", 400)
	}
	return provider.OK(map[string]any{
		"KeyId":               nr.ResourceID(model.RTKMSKey, keyID),
		"Plaintext":           base64.StdEncoding.EncodeToString(pt),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

func (p *KeyProvider) GenerateDataKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}

	bits := 256
	if spec, _ := nr.Params["KeySpec"].(string); spec == "AES_128" {
		bits = 128
	}
	dataKey := make([]byte, bits/8)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		return nil, fmt.Errorf("kms: generate data key: %w", err)
	}

	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	ct, err := encryptData(keyMat, dataKey, aad)
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt data key: %w", err)
	}
	blob := buildCiphertextBlob(keyID, ct)
	return provider.OK(map[string]any{
		"KeyId":                 nr.ResourceID(model.RTKMSKey, keyID),
		"Plaintext":             base64.StdEncoding.EncodeToString(dataKey),
		"CiphertextBlob":        base64.StdEncoding.EncodeToString(blob),
		"EncryptionAlgorithm":   "SYMMETRIC_DEFAULT",
	}), nil
}

func (p *KeyProvider) GenerateDataKeyWithoutPlaintext(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resp, err := p.GenerateDataKey(ctx, nr)
	if err != nil {
		return nil, err
	}
	// Remove the plaintext from the response.
	delete(resp.Data, "Plaintext")
	return resp, nil
}

func (p *KeyProvider) ReEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Decrypt with source key, then re-encrypt with destination key.
	ctB64, _ := nr.Params["CiphertextBlob"].(string)
	blob, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "CiphertextBlob must be base64-encoded", 400)
	}
	srcKeyID, ct, err := parseCiphertextBlob(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "invalid source ciphertext", 400)
	}

	srcKey, err := p.store.GetKey(ctx, srcKeyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	srcEncCtx := extractEncCtxFrom(nr.Params, "SourceEncryptionContext")
	srcAAD := marshalEncCtx(srcEncCtx)
	srcMat, err := decryptData(p.serverDEK, srcKey.KeyMaterial, []byte(srcKeyID))
	if err != nil {
		return nil, fmt.Errorf("kms: reencrypt load src material: %w", err)
	}
	pt, err := decryptData(srcMat, ct, srcAAD)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "decryption failed", 400)
	}

	dstKeyIDParam, _ := nr.Params["DestinationKeyId"].(string)
	dstKeyID, err := p.resolveKeyIDStr(ctx, dstKeyIDParam)
	if err != nil {
		return nil, err
	}
	dstKey, err := p.store.GetKey(ctx, dstKeyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	dstEncCtx := extractEncCtxFrom(nr.Params, "DestinationEncryptionContext")
	dstAAD := marshalEncCtx(dstEncCtx)
	dstMat, err := decryptData(p.serverDEK, dstKey.KeyMaterial, []byte(dstKeyID))
	if err != nil {
		return nil, fmt.Errorf("kms: reencrypt load dst material: %w", err)
	}
	newCT, err := encryptData(dstMat, pt, dstAAD)
	if err != nil {
		return nil, fmt.Errorf("kms: reencrypt: %w", err)
	}
	newBlob := buildCiphertextBlob(dstKeyID, newCT)
	return provider.OK(map[string]any{
		"KeyId":               nr.ResourceID(model.RTKMSKey, dstKeyID),
		"SourceKeyId":         nr.ResourceID(model.RTKMSKey, srcKeyID),
		"CiphertextBlob":      base64.StdEncoding.EncodeToString(newBlob),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

// ─── Key policy (stub) ────────────────────────────────────────────────────────

func (p *KeyProvider) GetKeyPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"Policy": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"kms:*","Resource":"*"}]}`,
	}), nil
}

func (p *KeyProvider) PutKeyPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// ─── Key rotation (stub) ──────────────────────────────────────────────────────

func (p *KeyProvider) GetKeyRotationStatus(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"KeyRotationEnabled": false}), nil
}

func (p *KeyProvider) EnableKeyRotation(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) DisableKeyRotation(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// ─── KeyEncryptor interface (injected into SecretProvider etc.) ───────────────

// Encrypt implements model.KeyEncryptor.
func (p *KeyProvider) Encrypt2(ctx context.Context, keyID string, pt []byte, encCtx map[string]string) ([]byte, error) {
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if !e.Enabled {
		return nil, fmt.Errorf("kms: key disabled")
	}
	aad := marshalEncCtx(encCtx)
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	ct, err := encryptData(keyMat, pt, aad)
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt: %w", err)
	}
	return buildCiphertextBlob(keyID, ct), nil
}

// Decrypt2 implements model.KeyEncryptor.
func (p *KeyProvider) Decrypt2(ctx context.Context, _ string, ctBlob []byte, encCtx map[string]string) ([]byte, error) {
	keyID, ct, err := parseCiphertextBlob(ctBlob)
	if err != nil {
		return nil, fmt.Errorf("kms: invalid ciphertext blob")
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if !e.Enabled {
		return nil, fmt.Errorf("kms: key disabled")
	}
	aad := marshalEncCtx(encCtx)
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	return decryptData(keyMat, ct, aad)
}

// GenerateDataKey2 implements model.KeyEncryptor.
func (p *KeyProvider) GenerateDataKey2(ctx context.Context, keyID string, bits int) (ptDEK, ctDEK []byte, err error) {
	fakeNR := &model.NormalizedRequest{
		Params:     map[string]any{"KeyId": keyID},
		ResourceID: func(_, name string) string { return name },
	}
	resp, err := p.GenerateDataKey(ctx, fakeNR)
	if err != nil {
		return nil, nil, err
	}
	ptB64, _ := resp.Data["Plaintext"].(string)
	ctB64, _ := resp.Data["CiphertextBlob"].(string)
	ptDEK, _ = base64.StdEncoding.DecodeString(ptB64)
	ctDEK, _ = base64.StdEncoding.DecodeString(ctB64)
	return ptDEK, ctDEK, nil
}

// AsKeyEncryptor returns the KeyProvider as a model.KeyEncryptor by wrapping it.
func (p *KeyProvider) AsKeyEncryptor() model.KeyEncryptor {
	return &keyEncryptorAdapter{p}
}

type keyEncryptorAdapter struct{ kp *KeyProvider }

func (a *keyEncryptorAdapter) Encrypt(ctx context.Context, keyID string, pt []byte, encCtx map[string]string) ([]byte, error) {
	return a.kp.Encrypt2(ctx, keyID, pt, encCtx)
}
func (a *keyEncryptorAdapter) Decrypt(ctx context.Context, keyID string, ct []byte, encCtx map[string]string) ([]byte, error) {
	return a.kp.Decrypt2(ctx, keyID, ct, encCtx)
}
func (a *keyEncryptorAdapter) GenerateDataKey(ctx context.Context, keyID string, bits int) ([]byte, []byte, error) {
	return a.kp.GenerateDataKey2(ctx, keyID, bits)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (p *KeyProvider) resolveKeyID(ctx context.Context, nr *model.NormalizedRequest) (string, error) {
	raw, _ := nr.Params["KeyId"].(string)
	return p.resolveKeyIDStr(ctx, raw)
}

func (p *KeyProvider) resolveKeyIDStr(ctx context.Context, raw string) (string, error) {
	if raw == "" {
		return "", model.NewProviderError("ValidationException", "KeyId is required", 400)
	}
	// Already a bare UUID.
	if len(raw) == ciphertextKeyIDLen && !strings.Contains(raw, ":") && !strings.Contains(raw, "/") {
		return raw, nil
	}
	// ARN: arn:aws:kms:region:account:key/UUID
	if strings.Contains(raw, ":key/") {
		parts := strings.Split(raw, "/")
		return parts[len(parts)-1], nil
	}
	// Alias ARN: arn:aws:kms:region:account:alias/name
	if strings.Contains(raw, ":alias/") {
		idx := strings.Index(raw, ":alias/")
		raw = raw[idx+1:] // "alias/name"
	}
	// alias/name
	if strings.HasPrefix(raw, "alias/") {
		a, err := p.store.GetAlias(ctx, raw)
		if err != nil {
			if errors.Is(err, ErrAliasNotFound) {
				return "", model.NewProviderError("NotFoundException", "alias not found: "+raw, 400)
			}
			return "", fmt.Errorf("kms: get alias: %w", err)
		}
		return a.TargetKeyID, nil
	}
	return raw, nil
}

func (p *KeyProvider) keyErr(err error) error {
	if errors.Is(err, ErrKeyNotFound) {
		return model.NewProviderError("NotFoundException", "key not found", 400)
	}
	if errors.Is(err, ErrKeyDisabled) {
		return model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	return err
}

func newID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func buildCiphertextBlob(keyID string, ct []byte) []byte {
	// Format: keyID(36 ASCII bytes) | ciphertext
	blob := make([]byte, ciphertextKeyIDLen+len(ct))
	copy(blob[:ciphertextKeyIDLen], keyID)
	copy(blob[ciphertextKeyIDLen:], ct)
	return blob
}

func parseCiphertextBlob(blob []byte) (keyID string, ct []byte, err error) {
	if len(blob) < ciphertextKeyIDLen+ivLen+tagLen {
		return "", nil, fmt.Errorf("ciphertext blob too short")
	}
	keyID = string(blob[:ciphertextKeyIDLen])
	ct = blob[ciphertextKeyIDLen:]
	return keyID, ct, nil
}

// marshalEncCtx serialises an encryption context map deterministically (sorted keys).
// Used as AES-GCM additional data so different contexts produce different ciphertexts.
func marshalEncCtx(m map[string]string) []byte {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		pair, _ := json.Marshal(map[string]string{k: m[k]})
		parts = append(parts, string(pair))
	}
	return []byte(strings.Join(parts, ","))
}

func extractEncCtx(params map[string]any) map[string]string {
	return extractEncCtxFrom(params, "EncryptionContext")
}

func extractEncCtxFrom(params map[string]any, key string) map[string]string {
	raw, ok := params[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case map[string]any:
		out := make(map[string]string, len(v))
		for k, val := range v {
			out[k] = fmt.Sprint(val)
		}
		return out
	case map[string]string:
		return v
	}
	return nil
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
				k, _ := m["TagKey"].(string)
				val, _ := m["TagValue"].(string)
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

func extractStringList(params map[string]any, key string) []string {
	raw, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		out = append(out, fmt.Sprint(v))
	}
	return out
}

func keyMetadata(e KeyEntry, arn, region, accountID string) map[string]any {
	m := map[string]any{
		"KeyId":        e.KeyID,
		"Arn":          arn,
		"Enabled":      e.Enabled,
		"Description":  e.Description,
		"KeyUsage":     e.KeyUsage,
		"KeySpec":      e.KeySpec,
		"Origin":       e.Origin,
		"KeyState":     keyState(e),
		"AWSAccountId": accountID,
		"CreationDate": time.Now().Unix(),
		"MultiRegion":  e.MultiRegion,
	}
	if e.PendingDeletion && !e.DeletionDate.IsZero() {
		m["DeletionDate"] = e.DeletionDate.Unix()
	}
	return m
}

func keyState(e KeyEntry) string {
	if e.PendingDeletion {
		return "PendingDeletion"
	}
	if e.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

// ensure hex import is used
var _ = hex.EncodeToString
