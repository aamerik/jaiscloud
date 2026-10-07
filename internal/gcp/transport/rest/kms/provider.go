// Package kms is the REST transport adapter for Cloud KMS. It maps the
// Discovery-derived Cloud KMS JSON API onto the transport-neutral core
// (internal/gcp/service/kms).
package kms

import (
	"context"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/gcp/paging"
	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles Cloud KMS key rings, crypto keys and crypto-key versions.
type Provider struct {
	core *kmscore.Service
}

// New returns a REST KMS provider over the shared core.
func New(core *kmscore.Service) *Provider { return &Provider{core: core} }

// Routes maps the emulator registry dispatch actions to handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"KMS.KeyRingCreate":                     p.KeyRingCreate,
		"KMS.KeyRingList":                       p.KeyRingList,
		"KMS.KeyRingGet":                        p.KeyRingGet,
		"KMS.CryptoKeyCreate":                   p.CryptoKeyCreate,
		"KMS.CryptoKeyList":                     p.CryptoKeyList,
		"KMS.CryptoKeyGet":                      p.CryptoKeyGet,
		"KMS.CryptoKeyEncrypt":                  p.CryptoKeyEncrypt,
		"KMS.CryptoKeyDecrypt":                  p.CryptoKeyDecrypt,
		"KMS.CryptoKeyVersionCreate":            p.CryptoKeyVersionCreate,
		"KMS.CryptoKeyVersionList":              p.CryptoKeyVersionList,
		"KMS.CryptoKeyVersionGet":               p.CryptoKeyVersionGet,
		"KMS.CryptoKeyVersionUpdate":            p.CryptoKeyVersionUpdate,
		"KMS.CryptoKeyVersionDestroy":           p.CryptoKeyVersionDestroy,
		"KMS.CryptoKeyVersionRestore":           p.CryptoKeyVersionRestore,
		"KMS.CryptoKeyVersionDelete":            p.CryptoKeyVersionDelete,
		"KMS.CryptoKeyDelete":                   p.CryptoKeyDelete,
		"KMS.CryptoKeyUpdatePrimaryVersion":     p.CryptoKeyUpdatePrimaryVersion,
		"KMS.CryptoKeyVersionAsymmetricSign":    p.CryptoKeyVersionAsymmetricSign,
		"KMS.CryptoKeyVersionAsymmetricDecrypt": p.CryptoKeyVersionAsymmetricDecrypt,
		"KMS.CryptoKeyVersionMacSign":           p.CryptoKeyVersionMacSign,
		"KMS.CryptoKeyVersionMacVerify":         p.CryptoKeyVersionMacVerify,
		"KMS.CryptoKeyVersionGetPublicKey":      p.CryptoKeyVersionGetPublicKey,
		"KMS.CryptoKeyVersionImport":            p.CryptoKeyVersionImport,
		"KMS.CryptoKeyVersionImportTrusted":     p.CryptoKeyVersionImportTrusted,
		"KMS.CryptoKeyVersionExportTrusted":     p.CryptoKeyVersionExportTrusted,
		"KMS.CryptoKeyVersionDecapsulate":       p.CryptoKeyVersionDecapsulate,
		"KMS.ImportJobCreate":                   p.ImportJobCreate,
		"KMS.ImportJobGet":                      p.ImportJobGet,
		"KMS.ImportJobList":                     p.ImportJobList,
		"KMS.KeyRingGetIamPolicy":               p.GetIamPolicy,
		"KMS.KeyRingSetIamPolicy":               p.SetIamPolicy,
		"KMS.KeyRingTestIamPermissions":         p.TestIamPermissions,
		"KMS.CryptoKeyGetIamPolicy":             p.GetIamPolicy,
		"KMS.CryptoKeySetIamPolicy":             p.SetIamPolicy,
		"KMS.CryptoKeyTestIamPermissions":       p.TestIamPermissions,
	}
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// ─── KeyRings ─────────────────────────────────────────────────────────────────

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
	meta, err := p.core.CreateKeyRing(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":       kmscore.KeyRingName(nr.AccountID, loc, meta.ID),
		"createTime": meta.CreateTime.UTC().Format(time.RFC3339Nano),
	}), nil
}

func (p *Provider) KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	krs, err := p.core.ListKeyRings(ctx, nr.AccountID, loc)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(krs, func(kr kmsstore.KeyRing) string { return kr.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, kr := range page {
		items = append(items, map[string]any{
			"name":       kmscore.KeyRingName(nr.AccountID, kr.Location, kr.ID),
			"createTime": kr.CreateTime.UTC().Format(time.RFC3339Nano),
		})
	}
	resp := map[string]any{"keyRings": items, "totalSize": len(krs)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) KeyRingGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(name)
	meta, err := p.core.GetKeyRing(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":       kmscore.KeyRingName(nr.AccountID, loc, meta.ID),
		"createTime": meta.CreateTime.UTC().Format(time.RFC3339Nano),
	}), nil
}

// ─── CryptoKeys ───────────────────────────────────────────────────────────────

func (p *Provider) CryptoKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(name)
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
	in := kmscore.CryptoKeyInput{}
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if purp, _ := body["purpose"].(string); purp != "" {
			in.Purpose = purp
		}
		if vt, ok := body["versionTemplate"].(map[string]any); ok {
			if alg, _ := vt["algorithm"].(string); alg != "" {
				in.Algorithm = alg
			}
			if pl, _ := vt["protectionLevel"].(string); pl != "" {
				in.ProtectionLevel = pl
			}
		}
		in.Labels = parseLabels(body["labels"])
		period, set, err := parseRotationPeriod(body["rotationPeriod"])
		if err != nil {
			return nil, err
		}
		if set {
			in.RotationPeriod = period
		}
		if io, _ := body["importOnly"].(bool); io {
			in.ImportOnly = true
		}
	}
	ck, err := p.core.CreateCryptoKey(ctx, nr.AccountID, loc, kr, key, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, kmsstore.Version{State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: ck.CreateTime, ProtectionLevel: ck.ProtectionLevel})), nil
}

func (p *Provider) CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(strings.TrimSuffix(name, "/cryptoKeys"))
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location/keyRing", 400)
	}
	keys, err := p.core.ListCryptoKeys(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(keys, func(k kmsstore.CryptoKey) string { return k.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, k := range page {
		items = append(items, cryptoKeyMap(nr, k, p.core.PrimaryVersion(ctx, nr.AccountID, k)))
	}
	resp := map[string]any{"cryptoKeys": items, "totalSize": len(keys)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CryptoKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	k, err := p.core.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, k, p.core.PrimaryVersion(ctx, nr.AccountID, k))), nil
}

func (p *Provider) CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	body, _ := nr.Params["body"].(map[string]any)
	ptStr, _ := body["plaintext"].(string)
	pt, err := base64.StdEncoding.DecodeString(ptStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "plaintext must be base64", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	res, err := p.core.Encrypt(ctx, nr.AccountID, loc, kr, key, "", pt, aad)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":                    kmscore.VersionName(nr.AccountID, loc, kr, key, res.Version),
		"ciphertext":              base64.StdEncoding.EncodeToString(res.Ciphertext),
		"ciphertextCrc32c":        crc32cString(res.Ciphertext),
		"protectionLevel":         "SOFTWARE",
		"verifiedPlaintextCrc32c": true,
		"verifiedAdditionalAuthenticatedDataCrc32c": len(aad) > 0,
	}), nil
}

func (p *Provider) CryptoKeyDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	body, _ := nr.Params["body"].(map[string]any)
	ctStr, _ := body["ciphertext"].(string)
	blob, err := base64.StdEncoding.DecodeString(ctStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	res, err := p.core.Decrypt(ctx, nr.AccountID, loc, kr, key, blob, aad)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"plaintext":       base64.StdEncoding.EncodeToString(res.Plaintext),
		"plaintextCrc32c": crc32cString(res.Plaintext),
		"protectionLevel": "SOFTWARE",
		"usedPrimary":     res.UsedPrimary,
	}), nil
}

// ─── CryptoKeyVersions ────────────────────────────────────────────────────────

func (p *Provider) CryptoKeyVersionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	if loc == "" || kr == "" || key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	v, err := p.core.CreateVersion(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	versions, err := p.core.ListVersions(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(versions, func(v kmsstore.Version) string { return versionPageKey(v) }, nr.Params)
	items := make([]any, 0, len(page))
	for _, v := range page {
		items = append(items, versionMap(nr, loc, kr, key, v))
	}
	resp := map[string]any{"cryptoKeyVersions": items, "totalSize": len(versions)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CryptoKeyVersionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionDestroy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.DestroyVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionRestore(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.RestoreVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.core.DeleteVersion(ctx, nr.AccountID, loc, kr, key, version); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CryptoKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	if _, err := p.core.DeleteCryptoKey(ctx, nr.AccountID, loc, kr, key); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CryptoKeyVersionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	state, err := versionStateFromPatch(nr)
	if err != nil {
		return nil, err
	}
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.UpdateVersionState(ctx, nr.AccountID, loc, kr, key, version, state)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func versionStateFromPatch(nr *model.NormalizedRequest) (string, error) {
	if mask, _ := nr.Params["updateMask"].(string); mask != "" && mask != "state" {
		return "", model.NewProviderError("InvalidArgument",
			"unsupported update_mask path "+mask+"; cryptoKeyVersions.patch only supports state", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	state, _ := body["state"].(string)
	if state == "" {
		return "", model.NewProviderError("InvalidArgument", "cryptoKeyVersion.state is required", 400)
	}
	switch state {
	case "ENABLED", "DISABLED":
		return state, nil
	default:
		return "", model.NewProviderError("InvalidArgument",
			"cryptoKeyVersion.state must be ENABLED or DISABLED, got "+state, 400)
	}
}

func (p *Provider) CryptoKeyUpdatePrimaryVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	body, _ := nr.Params["body"].(map[string]any)
	versionID, _ := body["cryptoKeyVersionId"].(string)
	if versionID == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKeyVersionId", 400)
	}
	ck, err := p.core.SetPrimaryVersion(ctx, nr.AccountID, loc, kr, key, versionID)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, p.core.PrimaryVersion(ctx, nr.AccountID, ck))), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (p *Provider) CryptoKeyVersionAsymmetricSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	digestStr := ""
	if d, ok := body["digest"].(map[string]any); ok {
		for _, k := range []string{"sha256", "sha384", "sha512"} {
			if s, _ := d[k].(string); s != "" {
				digestStr = s
				break
			}
		}
	} else if s, _ := body["digest"].(string); s != "" {
		digestStr = s
	}
	digest, err := base64.StdEncoding.DecodeString(digestStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "digest must be base64", 400)
	}
	res, err := p.core.AsymmetricSign(ctx, nr.AccountID, loc, kr, key, version, digest)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":                 kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"signature":            base64.StdEncoding.EncodeToString(res.Signature),
		"signatureCrc32c":      crc32cString(res.Signature),
		"verifiedDigestCrc32c": true,
		"protectionLevel":      "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionAsymmetricDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	ctStr, _ := body["ciphertext"].(string)
	if ctStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext is required", 400)
	}
	ct, err := base64.StdEncoding.DecodeString(ctStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	pt, err := p.core.AsymmetricDecrypt(ctx, nr.AccountID, loc, kr, key, version, ct)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"plaintext":                base64.StdEncoding.EncodeToString(pt),
		"plaintextCrc32c":          crc32cString(pt),
		"verifiedCiphertextCrc32c": true,
		"protectionLevel":          "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionMacSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	dataStr, _ := body["data"].(string)
	if dataStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "data is required", 400)
	}
	data, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	res, err := p.core.MacSign(ctx, nr.AccountID, loc, kr, key, version, data)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":               kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"mac":                base64.StdEncoding.EncodeToString(res.Mac),
		"macCrc32c":          crc32cString(res.Mac),
		"verifiedDataCrc32c": true,
		"protectionLevel":    "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionMacVerify(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	dataStr, _ := body["data"].(string)
	if dataStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "data is required", 400)
	}
	data, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	macStr, _ := body["mac"].(string)
	if macStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "mac is required", 400)
	}
	mac, err := base64.StdEncoding.DecodeString(macStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "mac must be base64", 400)
	}
	success, err := p.core.MacVerify(ctx, nr.AccountID, loc, kr, key, version, data, mac)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"success":            success,
		"verifiedDataCrc32c": true,
		"verifiedMacCrc32c":  true,
		"protectionLevel":    "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionGetPublicKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	name = strings.TrimSuffix(name, "/publicKey")
	loc, kr, key, version := parseVersion(name)
	res, err := p.core.GetPublicKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"algorithm":       res.Algorithm,
		"name":            kmscore.PublicKeyName(nr.AccountID, loc, kr, key, version),
		"protectionLevel": "SOFTWARE",
	}
	switch res.Format {
	case "NIST_PQC":
		out["publicKeyFormat"] = "NIST_PQC"
		out["publicKey"] = base64.StdEncoding.EncodeToString(res.Raw)
		out["publicKeyCrc32c"] = crc32cString(res.Raw)
	case "XWING_RAW_BYTES":
		out["publicKeyFormat"] = "XWING_RAW_BYTES"
		out["publicKey"] = base64.StdEncoding.EncodeToString(res.Raw)
		out["publicKeyCrc32c"] = crc32cString(res.Raw)
	default:
		out["pem"] = res.PEM
		out["pemCrc32c"] = crc32cString([]byte(res.PEM))
	}
	return provider.OK(out), nil
}

// ─── Import / trusted wrapping / KEM ──────────────────────────────────────────

func (p *Provider) CryptoKeyVersionImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseImportParent(name)
	if key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	wrapped := decodeBase64Field(body, "wrappedKey")
	if len(wrapped) == 0 {
		wrapped = decodeBase64Field(body, "rsaAesWrappedKey")
	}
	if len(wrapped) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "wrappedKey is required", 400)
	}
	alg, _ := body["algorithm"].(string)
	job, _ := body["importJob"].(string)
	v, err := p.core.ImportCryptoKeyVersion(ctx, nr.AccountID, loc, kr, key, kmscore.ImportVersionRequest{
		ImportJob:              job,
		Algorithm:              alg,
		WrappedKey:             wrapped,
		CryptoKeyVersion:       strField(body, "cryptoKeyVersion"),
		TrustedWrappingEnabled: boolField(body, "trustedWrappingEnabled"),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionImportTrusted(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseImportParent(name)
	if key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	wrapped := decodeBase64Field(body, "wrappedKey")
	if len(wrapped) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "wrappedKey is required", 400)
	}
	v, err := p.core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, nr.AccountID, loc, kr, key, kmscore.ImportVersionRequest{
		ImportingKey:     strField(body, "importingKey"),
		Algorithm:        strField(body, "algorithm"),
		WrappedKey:       wrapped,
		CryptoKeyVersion: strField(body, "cryptoKeyVersion"),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionExportTrusted(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	wrappingKey, _ := nr.Params["wrappingKey"].(string)
	wrapped, err := p.core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, nr.AccountID, loc, kr, key, version, wrappingKey)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"wrappedKey":       base64.StdEncoding.EncodeToString(wrapped),
		"wrappedKeyCrc32c": crc32cString(wrapped),
	}), nil
}

func (p *Provider) CryptoKeyVersionDecapsulate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	ct := decodeBase64Field(body, "ciphertext")
	if len(ct) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext is required", 400)
	}
	ss, err := p.core.Decapsulate(ctx, nr.AccountID, loc, kr, key, version, ct)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":               kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"sharedSecret":       base64.StdEncoding.EncodeToString(ss),
		"sharedSecretCrc32c": crc32cString(ss),
	}), nil
}

// ─── ImportJobs ───────────────────────────────────────────────────────────────

func (p *Provider) ImportJobCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(name)
	if kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing keyRing parent", 400)
	}
	id, _ := nr.Params["importJobId"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	if id == "" {
		if n, _ := body["name"].(string); n != "" {
			_, _, _, id, _ = kmscore.SplitImportJobName(n)
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing importJobId", 400)
	}
	in := kmscore.ImportJobInput{}
	if body != nil {
		in.ImportMethod = strField(body, "importMethod")
		in.ProtectionLevel = strField(body, "protectionLevel")
	}
	ij, err := p.core.CreateImportJob(ctx, nr.AccountID, loc, kr, id, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(importJobMap(ij)), nil
}

func (p *Provider) ImportJobGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, loc, kr, id, ok := kmscore.SplitImportJobName(name)
	if !ok {
		return nil, model.NewProviderError("InvalidRequest", "invalid resource name", 400)
	}
	ij, err := p.core.GetImportJob(ctx, nr.AccountID, loc, kr, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(importJobMap(ij)), nil
}

func (p *Provider) ImportJobList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(strings.TrimSuffix(name, "/importJobs"))
	jobs, err := p.core.ListImportJobs(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(jobs))
	for _, ij := range jobs {
		items = append(items, importJobMap(ij))
	}
	return provider.OK(map[string]any{"importJobs": items, "totalSize": len(items)}), nil
}

// ─── IAM ──────────────────────────────────────────────────────────────────────

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	pol, err := p.core.GetIamPolicy(ctx, nr.AccountID, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(iamPolicyMap(pol)), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := p.core.SetIamPolicy(ctx, nr.AccountID, name, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(iamPolicyMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	perms, err := p.core.TestIamPermissions(ctx, nr.AccountID, name, permissionsField(body))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func crc32cString(b []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))), 10)
}

func decodeAAD(v any) []byte {
	return decodeBase64Field(map[string]any{"v": v}, "v")
}

func decodeBase64Field(body map[string]any, field string) []byte {
	s, _ := body[field].(string)
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

func strField(body map[string]any, field string) string {
	s, _ := body[field].(string)
	return s
}

func boolField(body map[string]any, field string) bool {
	b, _ := body[field].(bool)
	return b
}

func permissionsField(body map[string]any) []string {
	items, _ := body["permissions"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func parseLabels(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil
	}
	labels := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			labels[k] = s
		}
	}
	return labels
}

func parseRotationPeriod(v any) (time.Duration, bool, error) {
	s, _ := v.(string)
	if s == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false, model.NewProviderError("InvalidRequest", "rotationPeriod must be a duration string (e.g. \"86400s\")", 400)
	}
	if d <= 0 {
		return 0, false, model.NewProviderError("InvalidRequest", "rotationPeriod must be positive", 400)
	}
	return d, true, nil
}

func durationString(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Second), 10) + "s"
}

func versionPageKey(v kmsstore.Version) string {
	n, err := strconv.ParseInt(v.Version, 10, 64)
	if err != nil {
		return v.Version
	}
	return fmt.Sprintf("%020d", n)
}

// parseKeyRing parses a name's location/keyRing pair, tolerating a full
// "projects/{p}/..." prefix.
func parseKeyRing(name string) (loc, kr string) {
	if _, l, r, ok := kmscore.SplitKeyRingName(name); ok {
		return l, r
	}
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 3 && parts[1] == "keyRings" {
		return parts[0], parts[2]
	}
	return "", ""
}

func parseCryptoKey(name string) (loc, kr, key string) {
	if _, l, r, k, ok := kmscore.SplitCryptoKeyName(name); ok {
		return l, r, k
	}
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 5 && parts[1] == "keyRings" && parts[3] == "cryptoKeys" {
		return parts[0], parts[2], parts[4]
	}
	return "", "", ""
}

func parseVersion(name string) (loc, kr, key, version string) {
	if _, l, r, k, v, ok := kmscore.SplitVersionName(name); ok {
		return l, r, k, v
	}
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 7 && parts[1] == "keyRings" && parts[3] == "cryptoKeys" && parts[5] == "cryptoKeyVersions" {
		return parts[0], parts[2], parts[4], parts[6]
	}
	return "", "", "", ""
}

func parseCryptoKeyFromParent(name string) (loc, kr, key string) {
	return parseCryptoKey(strings.TrimSuffix(name, "/cryptoKeyVersions"))
}

// parseImportParent parses a cryptoKey parent from a ":import"-style call, whose
// parent may be ".../cryptoKeys/{key}/cryptoKeyVersions".
func parseImportParent(name string) (loc, kr, key string) {
	name = strings.TrimSuffix(name, ":import")
	name = strings.TrimSuffix(name, ":importTrustedKeyWrappedCryptoKeyVersion")
	name = strings.TrimSuffix(name, "/cryptoKeyVersions")
	return parseCryptoKey(name)
}
