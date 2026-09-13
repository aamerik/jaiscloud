// Package kms implements the Google Cloud KMS provider.
package kms

import (
	"context"
	"encoding/base64"
	"errors"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles Cloud KMS key rings, crypto keys, and crypto-key versions.
type Provider struct {
	keys kmsstore.Store
}

func New(keys kmsstore.Store) *Provider {
	return &Provider{keys: keys}
}

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
		"KMS.CryptoKeyVersionDestroy":           p.CryptoKeyVersionDestroy,
		"KMS.CryptoKeyVersionDisable":           p.CryptoKeyVersionDisable,
		"KMS.CryptoKeyVersionEnable":            p.CryptoKeyVersionEnable,
		"KMS.CryptoKeyUpdatePrimaryVersion":     p.CryptoKeyUpdatePrimaryVersion,
		"KMS.CryptoKeyVersionAsymmetricSign":    p.CryptoKeyVersionAsymmetricSign,
		"KMS.CryptoKeyVersionAsymmetricDecrypt": p.CryptoKeyVersionAsymmetricDecrypt,
		"KMS.CryptoKeyVersionMacSign":           p.CryptoKeyVersionMacSign,
		"KMS.CryptoKeyVersionMacVerify":         p.CryptoKeyVersionMacVerify,
		"KMS.CryptoKeyVersionGetPublicKey":      p.CryptoKeyVersionGetPublicKey,
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

// parseVersion splits "locations/{loc}/keyRings/{kr}/cryptoKeys/{key}/cryptoKeyVersions/{v}".
func parseVersion(name string) (loc, kr, key, version string) {
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/") // [loc, keyRings, kr, cryptoKeys, key, cryptoKeyVersions, v]
	if len(parts) >= 7 {
		return parts[0], parts[2], parts[4], parts[6]
	}
	return "", "", "", ""
}

// parseCryptoKeyFromParent parses a version-collection parent name
// (".../cryptoKeys/{key}/cryptoKeyVersions") into its crypto key.
func parseCryptoKeyFromParent(name string) (loc, kr, key string) {
	return parseCryptoKey(strings.TrimSuffix(name, "/cryptoKeyVersions"))
}

func keyRingName(nr *model.NormalizedRequest, loc, kr string) string {
	return nr.ResourceID("kms-keyring", loc+"/"+kr)
}

func cryptoKeyName(nr *model.NormalizedRequest, loc, kr, key string) string {
	return nr.ResourceID("kms-cryptokey", loc+"/"+kr+"/"+key)
}

func cryptoKeyVersionName(nr *model.NormalizedRequest, loc, kr, key, version string) string {
	return nr.ResourceID("kms-cryptokey-version", loc+"/"+kr+"/"+key+"/"+version)
}

// cryptoKeyMap renders a CryptoKey as its GCP response object.
func cryptoKeyMap(nr *model.NormalizedRequest, k kmsstore.CryptoKey, primaryState string) map[string]any {
	if primaryState == "" {
		primaryState = "ENABLED"
	}
	out := map[string]any{
		"name":       cryptoKeyName(nr, k.Location, k.KeyRingID, k.ID),
		"purpose":    k.Purpose,
		"createTime": k.CreateTime.UTC().Format(time.RFC3339Nano),
		"primary": map[string]any{
			"name":      cryptoKeyVersionName(nr, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion),
			"state":     primaryState,
			"algorithm": k.Algorithm,
		},
		"versionTemplate": map[string]any{
			"algorithm":       k.Algorithm,
			"protectionLevel": "SOFTWARE",
		},
	}
	if len(k.Labels) > 0 {
		out["labels"] = k.Labels
	}
	if k.RotationPeriod > 0 {
		out["rotationPeriod"] = durationString(k.RotationPeriod)
	}
	if !k.NextRotationTime.IsZero() {
		out["nextRotationTime"] = k.NextRotationTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// durationString formats a duration the way GCP's JSON mapping encodes a
// google.protobuf.Duration: a decimal seconds value with an "s" suffix
// (e.g. 86400s).
func durationString(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Second), 10) + "s"
}

// parseRotationPeriod parses a GCP JSON duration string (e.g. "86400s").
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

// parseLabels converts a decoded JSON labels object into a string map.
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

// versionMap renders a CryptoKeyVersion as its GCP response object.
func versionMap(nr *model.NormalizedRequest, loc, kr, key, version, state, algorithm string, ct time.Time) map[string]any {
	return map[string]any{
		"name":       cryptoKeyVersionName(nr, loc, kr, key, version),
		"state":      state,
		"algorithm":  algorithm,
		"createTime": ct.UTC().Format(time.RFC3339Nano),
	}
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
	now := clock.Now()
	if err := p.keys.CreateKeyRing(ctx, nr.AccountID, loc, kr, kmsstore.KeyRing{
		Location: loc, ID: kr, CreateTime: now,
	}); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "key ring already exists", 409)
		}
		return nil, err
	}
	name := keyRingName(nr, loc, kr)
	return provider.OK(map[string]any{"name": name, "createTime": now.UTC().Format(time.RFC3339Nano)}), nil
}

func (p *Provider) KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	krs, err := p.keys.ListKeyRings(ctx, nr.AccountID, loc)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(krs, func(kr kmsstore.KeyRing) string { return kr.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, kr := range page {
		items = append(items, map[string]any{
			"name":       keyRingName(nr, kr.Location, kr.ID),
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
	krMeta, err := p.keys.GetKeyRing(ctx, nr.AccountID, loc, kr)
	if err != nil {
		if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
			return nil, model.NewProviderError("NotFound", "key ring not found", 404)
		}
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":       keyRingName(nr, loc, krMeta.ID),
		"createTime": krMeta.CreateTime.UTC().Format(time.RFC3339Nano),
	}), nil
}

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
	purpose := "ENCRYPT_DECRYPT"
	algorithm := ""
	var labels map[string]string
	var rotationPeriod time.Duration
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if purp, _ := body["purpose"].(string); purp != "" {
			purpose = purp
		}
		if vt, ok := body["versionTemplate"].(map[string]any); ok {
			if alg, _ := vt["algorithm"].(string); alg != "" {
				algorithm = alg
			}
		}
		labels = parseLabels(body["labels"])
		period, set, err := parseRotationPeriod(body["rotationPeriod"])
		if err != nil {
			return nil, err
		}
		if set {
			rotationPeriod = period
		}
	}
	if algorithm == "" {
		algorithm = defaultAlgorithmForPurpose(purpose)
	}
	now := clock.Now()
	ck := kmsstore.CryptoKey{Location: loc, KeyRingID: kr, ID: key, Purpose: purpose, CreateTime: now, PrimaryVersion: "1", Algorithm: algorithm, Labels: labels, RotationPeriod: rotationPeriod}
	if rotationPeriod > 0 {
		ck.NextRotationTime = now.Add(rotationPeriod)
	}
	if err := p.keys.CreateCryptoKey(ctx, nr.AccountID, loc, kr, key, ck); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "crypto key already exists", 409)
		}
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, "ENABLED")), nil
}

func (p *Provider) CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, _ := parseCryptoKey(name)
	keys, err := p.keys.ListCryptoKeys(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(keys, func(k kmsstore.CryptoKey) string { return k.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, k := range page {
		items = append(items, cryptoKeyMap(nr, k, p.primaryState(ctx, nr.AccountID, k)))
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
	k, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	return provider.OK(cryptoKeyMap(nr, k, p.primaryState(ctx, nr.AccountID, k))), nil
}

func (p *Provider) CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	ck, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if err := p.requireVersionEnabled(ctx, nr.AccountID, loc, kr, key, ck.PrimaryVersion); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pt, err := base64.StdEncoding.DecodeString(body["plaintext"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "plaintext must be base64", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	keyMat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, ck.PrimaryVersion)
	if err != nil {
		return nil, p.versionErr(err)
	}
	ct, err := kmsstore.EncryptData(keyMat, pt, aad)
	if err != nil {
		return nil, model.NewProviderError("Internal", "encryption failed", 500)
	}
	blob := kmsstore.EncodeVersionedCiphertext(ck.PrimaryVersion, ct)
	return provider.OK(map[string]any{
		"name":                    cryptoKeyVersionName(nr, loc, kr, key, ck.PrimaryVersion),
		"ciphertext":              base64.StdEncoding.EncodeToString(blob),
		"ciphertextCrc32c":        crc32cString(blob),
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
	ck, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if err := p.requireVersionEnabled(ctx, nr.AccountID, loc, kr, key, ck.PrimaryVersion); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	blob, err := base64.StdEncoding.DecodeString(body["ciphertext"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	version, ct, err := kmsstore.DecodeVersionedCiphertext(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertext", "invalid ciphertext", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	keyMat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	pt, err := kmsstore.DecryptData(keyMat, ct, aad)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertext", "decryption failed", 400)
	}
	return provider.OK(map[string]any{
		"plaintext":       base64.StdEncoding.EncodeToString(pt),
		"plaintextCrc32c": crc32cString(pt),
		"protectionLevel": "SOFTWARE",
		"usedPrimary":     version == ck.PrimaryVersion,
	}), nil
}

func (p *Provider) CryptoKeyVersionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	if loc == "" || kr == "" || key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	if err := p.requireCryptoKey(ctx, nr.AccountID, loc, kr, key); err != nil {
		return nil, err
	}
	now := clock.Now()
	ck, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	version, err := p.keys.CreateVersion(ctx, nr.AccountID, loc, kr, key, kmsstore.Version{CreateTime: now, Algorithm: ck.Algorithm})
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(versionMap(nr, loc, kr, key, version, "ENABLED", ck.Algorithm, now)), nil
}

func (p *Provider) CryptoKeyVersionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	versions, err := p.keys.ListVersions(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.versionErr(err)
	}
	page, next := paging.Page(versions, func(v kmsstore.Version) string { return v.Version }, nr.Params)
	items := make([]any, 0, len(page))
	for _, v := range page {
		items = append(items, versionMap(nr, loc, kr, key, v.Version, v.State, v.Algorithm, v.CreateTime))
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
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(versionMap(nr, loc, kr, key, v.Version, v.State, v.Algorithm, v.CreateTime)), nil
}

func (p *Provider) CryptoKeyVersionDestroy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "DESTROYED")
}

func (p *Provider) CryptoKeyVersionDisable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "DISABLED")
}

func (p *Provider) CryptoKeyVersionEnable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "ENABLED")
}

func (p *Provider) setVersionState(ctx context.Context, nr *model.NormalizedRequest, state string) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.keys.UpdateVersionState(ctx, nr.AccountID, loc, kr, key, version, state); err != nil {
		return nil, p.versionErr(err)
	}
	v, _ := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	return provider.OK(versionMap(nr, loc, kr, key, v.Version, v.State, v.Algorithm, v.CreateTime)), nil
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
	if err := p.keys.UpdatePrimaryVersion(ctx, nr.AccountID, loc, kr, key, versionID); err != nil {
		return nil, p.versionErr(err)
	}
	ck, _ := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	return provider.OK(cryptoKeyMap(nr, ck, p.primaryState(ctx, nr.AccountID, ck))), nil
}

// defaultAlgorithmForPurpose maps a GCP KMS purpose to its default algorithm.
func defaultAlgorithmForPurpose(purpose string) string {
	switch purpose {
	case "ASYMMETRIC_SIGN":
		return "RSA_SIGN_PKCS1_2048_SHA256"
	case "ASYMMETRIC_DECRYPT":
		return "RSA_DECRYPT_OAEP_2048_SHA256"
	case "MAC":
		return "HMAC_SHA256"
	default:
		return "GOOGLE_SYMMETRIC_ENCRYPTION"
	}
}

func (p *Provider) CryptoKeyVersionAsymmetricSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	body, _ := nr.Params["body"].(map[string]any)
	// GCP's AsymmetricSignRequest carries the digest as an object with one of
	// sha256/sha384/sha512; accept a bare base64 string too for backward
	// compatibility with earlier emulator callers.
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
	priv, err := p.keys.PrivateKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	var sig []byte
	switch {
	case strings.HasPrefix(v.Algorithm, "RSA_SIGN"):
		sig, err = kmsstore.RSASign(priv, digest, v.Algorithm)
	case strings.HasPrefix(v.Algorithm, "EC_SIGN"):
		sig, err = kmsstore.ECSign(priv, digest)
	default:
		return nil, model.NewProviderError("FailedPrecondition", "key is not for asymmetric signing", 400)
	}
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "signing failed", 400)
	}
	return provider.OK(map[string]any{
		"name":                 cryptoKeyVersionName(nr, loc, kr, key, version),
		"signature":            base64.StdEncoding.EncodeToString(sig),
		"signatureCrc32c":      crc32cString(sig),
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
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	body, _ := nr.Params["body"].(map[string]any)
	ct, err := base64.StdEncoding.DecodeString(body["ciphertext"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	priv, err := p.keys.PrivateKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if !strings.HasPrefix(v.Algorithm, "RSA_DECRYPT") {
		return nil, model.NewProviderError("FailedPrecondition", "key is not for asymmetric decryption", 400)
	}
	pt, err := kmsstore.RSADecryptOAEP(priv, ct, v.Algorithm)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "decryption failed", 400)
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
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, model.NewProviderError("FailedPrecondition", "key is not for MAC", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	data, err := base64.StdEncoding.DecodeString(body["data"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	mat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	mac, err := kmsstore.HMACSign(mat, data, v.Algorithm)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "mac sign failed", 400)
	}
	return provider.OK(map[string]any{
		"name":               cryptoKeyVersionName(nr, loc, kr, key, version),
		"mac":                base64.StdEncoding.EncodeToString(mac),
		"macCrc32c":          crc32cString(mac),
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
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, model.NewProviderError("FailedPrecondition", "key is not for MAC", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	data, err := base64.StdEncoding.DecodeString(body["data"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	mac, err := base64.StdEncoding.DecodeString(body["mac"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "mac must be base64", 400)
	}
	mat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(map[string]any{
		"success":            kmsstore.HMACVerify(mat, data, mac, v.Algorithm),
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
	// name may be ".../cryptoKeyVersions/{v}/publicKey" — strip the suffix.
	name = strings.TrimSuffix(name, "/publicKey")
	loc, kr, key, version := parseVersion(name)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	pub, err := p.keys.PublicKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	pemStr, err := kmsstore.PublicKeyPEM(pub)
	if err != nil {
		return nil, model.NewProviderError("Internal", "public key encode failed", 500)
	}
	return provider.OK(map[string]any{
		"pem":             pemStr,
		"algorithm":       v.Algorithm,
		"pemCrc32c":       crc32cString([]byte(pemStr)),
		"name":            cryptoKeyVersionName(nr, loc, kr, key, version) + "/publicKey",
		"protectionLevel": "SOFTWARE",
	}), nil
}

// decodeAAD decodes the base64 additionalAuthenticatedData field (or empty).
func decodeAAD(v any) []byte {
	s, _ := v.(string)
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// requireVersionEnabled rejects use of a crypto-key version that is not
// ENABLED (DISABLED/DESTROYED/PENDING) with FailedPrecondition.
func (p *Provider) requireVersionEnabled(ctx context.Context, accountID, loc, kr, key, version string) error {
	v, err := p.keys.GetVersion(ctx, accountID, loc, kr, key, version)
	if err != nil {
		return p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return versionNotEnabledErr(version, v.State)
	}
	return nil
}

// primaryState reports the current state of a crypto key's primary version.
func (p *Provider) primaryState(ctx context.Context, accountID string, k kmsstore.CryptoKey) string {
	if k.PrimaryVersion == "" {
		return "ENABLED"
	}
	v, err := p.keys.GetVersion(ctx, accountID, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion)
	if err != nil || v.State == "" {
		return "ENABLED"
	}
	return v.State
}

func versionNotEnabledErr(version, state string) error {
	return &model.ProviderError{
		Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
		Message: "CryptoKeyVersion " + version + " is " + state,
	}
}

// keyErr maps crypto-key store errors to GCP provider errors.
func (p *Provider) keyErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchCryptoKey) {
		return model.NewProviderError("NotFound", "crypto key not found", 404)
	}
	return err
}

// versionErr maps version store errors to GCP provider errors.
func (p *Provider) versionErr(err error) error {
	switch {
	case errors.Is(err, kmsstore.ErrNoSuchCryptoKey):
		return model.NewProviderError("NotFound", "crypto key not found", 404)
	case errors.Is(err, kmsstore.ErrNoSuchVersion):
		return model.NewProviderError("NotFound", "crypto key version not found", 404)
	}
	return err
}

// requireCryptoKey verifies the crypto key exists, returning 404 otherwise.
func (p *Provider) requireCryptoKey(ctx context.Context, accountID, loc, kr, key string) error {
	if _, err := p.keys.GetCryptoKey(ctx, accountID, loc, kr, key); err != nil {
		return p.keyErr(err)
	}
	return nil
}

// crc32cString returns the CRC32C-Castagnoli checksum of b as a decimal string
// (google.protobuf.Int64Value encoding).
func crc32cString(b []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))), 10)
}
