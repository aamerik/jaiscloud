// Package secret implements the Google Cloud Secret Manager provider.
package secretmanager

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	secretmanagersvc "jaiscloud/internal/gcp/service/secretmanager"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// rtSecretPolicy is the generic ResourceStore type for secret IAM policies.
const rtSecretPolicy = "gcp_secret_policy"

// Provider handles Secret Manager secrets and versions.
type Provider struct {
	secrets   secretmanagerstore.Store
	resources store.ResourceStore // IAM policies (control-plane)
	encryptor crypto.EnvelopeEncryptor
}

func New(secrets secretmanagerstore.Store, resources store.ResourceStore, encryptor crypto.EnvelopeEncryptor) *Provider {
	return &Provider{secrets: secrets, resources: resources, encryptor: encryptor}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Secret.Create":                p.Create,
		"Secret.List":                  p.List,
		"Secret.Get":                   p.Get,
		"Secret.Update":                p.Update,
		"Secret.Delete":                p.Delete,
		"Secret.AddVersion":            p.AddVersion,
		"Secret.Access":                p.Access,
		"Secret.ListVersions":          p.ListVersions,
		"Secret.GetVersion":            p.GetVersion,
		"Secret.DestroyVersion":        p.DestroyVersion,
		"Secret.DisableVersion":        p.DisableVersion,
		"Secret.EnableVersion":         p.EnableVersion,
		"Secret.EnableManagedRotation": p.EnableManagedRotation,
		"Secret.RotateSecret":          p.RotateSecret,
		"Secret.GetIamPolicy":          p.GetIamPolicy,
		"Secret.SetIamPolicy":          p.SetIamPolicy,
		"Secret.TestIamPermissions":    p.TestIamPermissions,
	}
}

type secretMeta struct {
	Name           string
	Labels         map[string]string
	Annotations    map[string]string
	CreateTime     string
	NextVer        int
	Rotation       *secretmanagerstore.Rotation
	VersionAliases map[string]int
	KmsKeyName     string
}

type versionMeta struct {
	Name        string `json:"name"`
	State       string `json:"state"`
	CreateTime  string `json:"createTime"`
	DestroyTime string `json:"destroyTime,omitempty"` // only when DESTROYED
	Data        string `json:"data"`                  // base64 payload (encrypted at rest)
	// KmsKeyVersionName is the full CryptoKeyVersion name (see
	// secretmanagersvc.KmsKeyVersionName) reported back through
	// replicationStatus.automatic.customerManagedEncryption; empty when the
	// version has no customer-managed encryption.
	KmsKeyVersionName string `json:"kmsKeyVersionName,omitempty"`
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// parseSecretName splits a relative resource name or full GCP name into the secret ID and version.
func parseSecretName(name string) (secret, version string) {
	if i := strings.Index(name, "/secrets/"); i >= 0 {
		name = name[i+len("/secrets/"):]
	} else {
		name = strings.TrimPrefix(name, "secrets/")
	}
	if i := strings.Index(name, "/versions/"); i >= 0 {
		return name[:i], name[i+len("/versions/"):]
	}
	// A trailing "/versions" is the versions collection (secrets list), not a
	// version resource; treat the secret as having no version component.
	if strings.HasSuffix(name, "/versions") {
		return strings.TrimSuffix(name, "/versions"), ""
	}
	return name, ""
}

// secretID extracts the secret ID from a relative name or a full GCP name.
func secretID(name string) string {
	if i := strings.Index(name, "/secrets/"); i >= 0 {
		return name[i+len("/secrets/"):]
	}
	return strings.TrimPrefix(name, "secrets/")
}

func toStoreSecret(m secretMeta) secretmanagerstore.Secret {
	tc, _ := time.Parse(time.RFC3339Nano, m.CreateTime)
	return secretmanagerstore.Secret{
		ID: secretID(m.Name), Labels: m.Labels, Annotations: m.Annotations, CreateTime: tc, NextVer: m.NextVer,
		Rotation: m.Rotation, VersionAliases: m.VersionAliases,
		KmsKeyName: m.KmsKeyName,
	}
}

func fromStoreSecret(nr *model.NormalizedRequest, s secretmanagerstore.Secret) secretMeta {
	m := secretMeta{Name: nr.ResourceID("secret", s.ID), Labels: s.Labels, Annotations: s.Annotations, NextVer: s.NextVer, Rotation: s.Rotation, VersionAliases: s.VersionAliases, KmsKeyName: s.KmsKeyName}
	if !s.CreateTime.IsZero() {
		m.CreateTime = timestamp.Canonical(s.CreateTime)
	}
	return m
}

// secretListKey orders secrets newest-first. paging.Page sorts ascending by
// this key, so the create time is inverted (a newer time maps to a smaller
// key) and the secret ID breaks ties deterministically. The fixed-width
// numeric prefix keeps lexicographic string comparison identical to numeric
// comparison, and the NUL separator avoids collisions between the time and ID
// components. Official Secret Manager docs: secrets are "sorted in reverse by
// createTime (newest first)".
func secretListKey(s secretmanagerstore.Secret) string {
	const maxInt64 = int64(1<<63 - 1)
	inv := maxInt64 // a zero create time sorts last (treated as oldest)
	if !s.CreateTime.IsZero() {
		inv = maxInt64 - s.CreateTime.UnixNano()
	}
	return fmt.Sprintf("%020d", inv) + "\x00" + s.ID
}

// annotationsFromBody extracts the annotations map from a request body.
func annotationsFromBody(body map[string]any) map[string]string {
	m, ok := body["annotations"].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// rotationFromBody extracts the rotation schedule from a request body.
func rotationFromBody(body map[string]any) *secretmanagerstore.Rotation {
	r, ok := body["rotation"].(map[string]any)
	if !ok {
		return nil
	}
	out := &secretmanagerstore.Rotation{}
	if v, ok := r["nextRotationTime"].(string); ok {
		out.NextRotationTime = v
	}
	if v, ok := r["rotationPeriod"].(string); ok {
		out.RotationPeriod = v
	}
	return out
}

// versionAliasesFromBody extracts the versionAliases map (alias → version
// number) from a request body. Values arrive as float64 from parseJSON.
func versionAliasesFromBody(body map[string]any) map[string]int {
	m, ok := body["versionAliases"].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		if f, ok := v.(float64); ok {
			out[k] = int(f)
		}
	}
	return out
}

func kmsKeyNameFromBody(body map[string]any) string {
	if repl, ok := body["replication"].(map[string]any); ok {
		if auto, ok := repl["automatic"].(map[string]any); ok {
			if cmek, ok := auto["customerManagedEncryption"].(map[string]any); ok {
				if k, ok := cmek["kmsKeyName"].(string); ok {
					return k
				}
			}
		}
		if um, ok := repl["userManaged"].(map[string]any); ok {
			if reps, ok := um["replicas"].([]any); ok && len(reps) > 0 {
				if rMap, ok := reps[0].(map[string]any); ok {
					if cmek, ok := rMap["customerManagedEncryption"].(map[string]any); ok {
						if k, ok := cmek["kmsKeyName"].(string); ok {
							return k
						}
					}
				}
			}
		}
	}
	return ""
}

func fromStoreVersion(nr *model.NormalizedRequest, v secretmanagerstore.Version) versionMeta {
	m := versionMeta{
		Name:  nr.ResourceID("secret", v.SecretID) + "/versions/" + v.VersionID,
		State: v.State,
		Data:  v.Data,
		// The version's replicationStatus echoes the CryptoKeyVersion that
		// actually wrapped it (DC44); it is per-version because the wrap is
		// stored with the version.
		KmsKeyVersionName: secretmanagersvc.KmsKeyVersionName(v.KmsKeyName, v.WrappedDEK),
	}
	if !v.CreateTime.IsZero() {
		m.CreateTime = timestamp.Canonical(v.CreateTime)
	}
	if !v.DestroyTime.IsZero() {
		m.DestroyTime = timestamp.Canonical(v.DestroyTime)
	}
	return m
}

func (p *Provider) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, _ := nr.Params["secretId"].(string)
	if id == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				id = secretID(n)
			}
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing secretId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	m := secretMeta{
		Name:       nr.ResourceID("secret", id),
		CreateTime: timestamp.Canonical(clock.Now()),
		NextVer:    1,
	}
	if labels, ok := body["labels"].(map[string]any); ok {
		m.Labels = make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				m.Labels[k] = s
			}
		}
	}
	if annotations := annotationsFromBody(body); annotations != nil {
		m.Annotations = annotations
	}
	if r := rotationFromBody(body); r != nil {
		m.Rotation = r
	}
	if va := versionAliasesFromBody(body); va != nil {
		m.VersionAliases = va
	}
	if kmsKeyName := kmsKeyNameFromBody(body); kmsKeyName != "" {
		m.KmsKeyName = kmsKeyName
	}
	sec := toStoreSecret(m)
	if err := p.secrets.CreateSecret(ctx, nr.AccountID, id, sec); err != nil {
		if errors.Is(err, secretmanagerstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "secret already exists", 409)
		}
		return nil, err
	}
	return provider.OK(secretToMap(nr, sec)), nil
}

func (p *Provider) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secrets, err := p.secrets.ListSecrets(ctx, nr.AccountID)
	if err != nil {
		return nil, err
	}
	filter, _ := nr.Params["filter"].(string)
	matched, applied, err := secretmanagersvc.FilterSecrets(nr.AccountID, filter, secrets)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(matched, secretListKey, nr.Params)
	if err != nil {
		return nil, err
	}

	items := make([]any, 0, len(page))
	for _, s := range page {
		items = append(items, secretToMap(nr, s))
	}
	// Secret Manager does not report a total count once a filter is applied
	// ("If filter is set in a list request, the response does not indicate the
	// total result count").
	total := len(secrets)
	if applied {
		total = 0
	}
	resp := map[string]any{"secrets": items, "totalSize": total}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	s, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	s = p.maybeRotate(ctx, nr.AccountID, id, s)
	return provider.OK(secretToMap(nr, s)), nil
}

func (p *Provider) Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	body, _ := nr.Params["body"].(map[string]any)

	// UpdateSecretAtomic reads, merges, and writes under one lock, so a
	// concurrent AddVersion's NextVersion() counter advance can't land between
	// our read and our write and get silently rolled back by it.
	updated, err := p.secrets.UpdateSecretAtomic(ctx, nr.AccountID, id, func(current secretmanagerstore.Secret) (secretmanagerstore.Secret, error) {
		// Secret.etag is the resource's optimistic-concurrency token (AIP-154).
		// The Discovery document spells the delete and version-request
		// preconditions out; update's follows from the field's documented role
		// and is enforced here for the same reason. Checking it inside the
		// atomic mutate keeps it race-free (it sees the state this write
		// replaces).
		if err := secretmanagersvc.CheckSecretEtag(nr.ResourceID("secret", id), current, bodyEtag(nr)); err != nil {
			return secretmanagerstore.Secret{}, err
		}
		m := fromStoreSecret(nr, current)
		if body != nil {
			if labels, ok := body["labels"].(map[string]any); ok {
				m.Labels = make(map[string]string, len(labels))
				for k, v := range labels {
					if sv, ok := v.(string); ok {
						m.Labels[k] = sv
					}
				}
			}
			if annotations := annotationsFromBody(body); annotations != nil {
				m.Annotations = annotations
			}
			if r := rotationFromBody(body); r != nil {
				// managedRotationStatus is output-only: keep the stored
				// config/status across a schedule update that doesn't carry it.
				if m.Rotation != nil {
					r.ManagedRotation = m.Rotation.ManagedRotation
				}
				m.Rotation = r
			}
			if va := versionAliasesFromBody(body); va != nil {
				m.VersionAliases = va
			}
			if kmsKeyName := kmsKeyNameFromBody(body); kmsKeyName != "" {
				m.KmsKeyName = kmsKeyName
			}
		}
		return toStoreSecret(m), nil
	})
	if err != nil {
		return nil, mapSecretErr(err)
	}
	return provider.OK(secretToMap(nr, updated)), nil
}

// errSkipRotation aborts maybeRotate's atomic mutate without writing, when
// rotation turns out not to be due after all (re-checked under the store's
// lock, since the Secret this function receives may have been read well
// before this call by a caller several frames up).
var errSkipRotation = errors.New("secret rotation not due")

// maybeRotate advances a secret's rotation schedule when it is due: it
// allocates the next version number and creates an empty version, and
// advances nextRotationTime by rotationPeriod. This is GCP's
// automatic-rotation behavior, evaluated lazily on read (no background
// worker), mirroring how AWS SQS enforces visibility lazily on receive.
//
// s is the caller's (possibly stale — read by Get/Access some time before
// this call) snapshot, used only for the cheap up-front "is rotation even
// configured and due" check, to avoid taking the store lock at all in the
// overwhelmingly common case where it isn't. If that check passes,
// everything that actually matters — whether rotation is STILL due, the
// rotation-schedule advance, and the version-counter allocation — is
// re-done from a fresh read inside UpdateSecretAtomic's locked section, so
// a concurrent Update/AddVersion/maybeRotate on the same secret can't
// interleave with this one.
func (p *Provider) maybeRotate(ctx context.Context, account, id string, s secretmanagerstore.Secret) secretmanagerstore.Secret {
	if s.Rotation == nil || s.Rotation.NextRotationTime == "" {
		return s
	}
	if next, err := time.Parse(time.RFC3339Nano, s.Rotation.NextRotationTime); err != nil || clock.Now().Before(next) {
		return s
	}

	var allocatedVer int
	updated, err := p.secrets.UpdateSecretAtomic(ctx, account, id, func(current secretmanagerstore.Secret) (secretmanagerstore.Secret, error) {
		if current.Rotation == nil || current.Rotation.NextRotationTime == "" {
			return secretmanagerstore.Secret{}, errSkipRotation
		}
		next, err := time.Parse(time.RFC3339Nano, current.Rotation.NextRotationTime)
		if err != nil || clock.Now().Before(next) {
			return secretmanagerstore.Secret{}, errSkipRotation
		}
		if current.Rotation.RotationPeriod != "" {
			if d, err := time.ParseDuration(current.Rotation.RotationPeriod); err == nil {
				rotation := *current.Rotation
				rotation.NextRotationTime = timestamp.Canonical(clock.Now().Add(d))
				current.Rotation = &rotation
			}
		}
		allocatedVer = current.NextVer
		current.NextVer++
		return current, nil
	})
	if err != nil {
		// errSkipRotation (already rotated, or rotation disabled, since our
		// caller's read) or a store error: behave as if untouched. Re-read so
		// the response — and its etag, which covers nextRotationTime — reflects
		// the stored state rather than this caller's possibly stale snapshot
		// (returning the snapshot could hand the client an etag the store no
		// longer has, and its next etag-carrying write would spuriously ABORT).
		if fresh, gerr := p.secrets.GetSecret(ctx, account, id); gerr == nil {
			return fresh
		}
		return s
	}
	_ = p.secrets.CreateVersion(ctx, account, secretmanagerstore.Version{
		SecretID: id, VersionID: strconv.Itoa(allocatedVer), State: "ENABLED", CreateTime: clock.Now(),
	})
	return updated
}

func (p *Provider) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	// The documented `etag` precondition: the delete succeeds only when the
	// caller's etag matches the currently stored secret (ABORTED otherwise).
	// The check runs inside the store's atomic delete, so a concurrent update
	// cannot land between the comparison and the removal (which would let a
	// stale etag through).
	err = p.secrets.DeleteSecretIf(ctx, nr.AccountID, id, func(sec secretmanagerstore.Secret) error {
		return secretmanagersvc.CheckSecretEtag(nr.ResourceID("secret", id), sec, etagParam(nr))
	})
	if err != nil {
		return nil, mapSecretErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// etagParam returns the request's `etag` query parameter (empty when absent).
func etagParam(nr *model.NormalizedRequest) string {
	e, _ := nr.Params["etag"].(string)
	return e
}

// bodyEtag returns the `etag` carried in the request body (empty when absent).
func bodyEtag(nr *model.NormalizedRequest) string {
	body, _ := nr.Params["body"].(map[string]any)
	e, _ := body["etag"].(string)
	return e
}

func (p *Provider) AddVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	body, _ := nr.Params["body"].(map[string]any)
	payload := ""
	if pl, ok := body["payload"].(map[string]any); ok {
		if d, _ := pl["data"].(string); d != "" {
			payload = d
		}
	}

	// Allocate the version number atomically (memory mutex / DB sequence-like
	// UPDATE ... RETURNING), avoiding a Get→increment→Update race.
	ver, err := p.secrets.NextVersion(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	version := strconv.Itoa(ver)

	sec, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}

	payloadBytes, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "payload is not base64", 400)
	}

	rawDEK, wrappedDEK, err := p.encryptor.Wrap(ctx, nr.AccountID, sec.KmsKeyName)
	if err != nil {
		return nil, err
	}

	encryptedPayloadBytes, err := kms.EncryptData(rawDEK, payloadBytes, nil)
	if err != nil {
		return nil, err
	}
	encryptedPayloadBase64 := base64.StdEncoding.EncodeToString(encryptedPayloadBytes)

	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: version, State: "ENABLED",
		CreateTime: clock.Now(), Data: encryptedPayloadBase64,
		KmsKeyName: sec.KmsKeyName, WrappedDEK: wrappedDEK,
	}
	if err := p.secrets.CreateVersion(ctx, nr.AccountID, stv); err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(fromStoreVersion(nr, stv))), nil
}

// ─── Cloud SQL managed rotation ──────────────────────────────────────────────

// managedRotationConfigured reports whether Cloud SQL managed rotation has been
// enabled for s. The API allows enabling it only once, so the enable
// precondition keys on presence (not on the ACTIVE state).
func managedRotationConfigured(s secretmanagerstore.Secret) bool {
	return s.Rotation != nil && s.Rotation.ManagedRotation != nil
}

// managedRotationEnabled reports whether s has ACTIVE Cloud SQL managed
// rotation.
func managedRotationEnabled(s secretmanagerstore.Secret) bool {
	return s.Rotation != nil && s.Rotation.ManagedRotation != nil && s.Rotation.ManagedRotation.State == "ACTIVE"
}

func managedRotationErr(msg string) error {
	return &model.ProviderError{Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION", Message: msg}
}

// EnableManagedRotation (secrets:enableManagedRotation) validates the Cloud SQL
// single-user credentials, records them, and stores a password (the supplied
// one, else a generated one) as a new ENABLED version. The emulator has no Cloud
// SQL data plane, so the password is not applied to a database user (GA7-D1).
func (p *Provider) EnableManagedRotation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	body, _ := nr.Params["body"].(map[string]any)
	creds, _ := body["cloudSqlSingleUserCredentials"].(map[string]any)
	if creds == nil {
		return nil, model.NewProviderError("InvalidRequest", "cloudSqlSingleUserCredentials is required", 400)
	}
	instanceID, _ := creds["instanceId"].(string)
	username, _ := creds["username"].(string)
	if instanceID == "" || username == "" {
		return nil, model.NewProviderError("InvalidRequest", "cloudSqlSingleUserCredentials requires instanceId and username", 400)
	}
	sec, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	if managedRotationConfigured(sec) {
		return nil, managedRotationErr("managed rotation is already enabled for secret " + id)
	}
	password, _ := creds["password"].(string)
	if password == "" {
		if password, err = secretmanagerstore.GeneratePassword(); err != nil {
			return nil, err
		}
	}
	cfg := &secretmanagerstore.ManagedRotation{State: "ACTIVE", InstanceID: instanceID, Username: username}
	v, err := p.appendManagedVersion(ctx, nr, id, []byte(password), cfg)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(v)), nil
}

// RotateSecret (secrets:rotateSecret) rotates a secret whose managed rotation is
// already enabled, storing a freshly generated password as a new ENABLED
// version. Without a Cloud SQL data plane the password is not applied to the
// linked database user (GA7-D1).
func (p *Provider) RotateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	sec, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	if !managedRotationEnabled(sec) {
		return nil, managedRotationErr("managed rotation is not enabled for secret " + id)
	}
	password, err := secretmanagerstore.GeneratePassword()
	if err != nil {
		return nil, err
	}
	v, err := p.appendManagedVersion(ctx, nr, id, []byte(password), nil)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(v)), nil
}

// appendManagedVersion stores data as a new ENABLED version, encrypting it with
// the secret's CMEK. When cfg is non-nil it records the managed-rotation config
// and allocates the version number in one locked read-modify-write, so managed
// rotation cannot be enabled twice and a concurrent AddVersion cannot roll the
// counter back. The payload is encrypted before the locked write, so a crypto
// failure cannot leave the secret one-shot-enabled with no version.
func (p *Provider) appendManagedVersion(ctx context.Context, nr *model.NormalizedRequest, id string, data []byte, cfg *secretmanagerstore.ManagedRotation) (versionMeta, error) {
	sec, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return versionMeta{}, mapSecretErr(err)
	}
	rawDEK, wrappedDEK, err := p.encryptor.Wrap(ctx, nr.AccountID, sec.KmsKeyName)
	if err != nil {
		return versionMeta{}, err
	}
	encrypted, err := kms.EncryptData(rawDEK, data, nil)
	if err != nil {
		return versionMeta{}, err
	}
	var ver int
	if _, err := p.secrets.UpdateSecretAtomic(ctx, nr.AccountID, id, func(cur secretmanagerstore.Secret) (secretmanagerstore.Secret, error) {
		if cfg != nil && managedRotationConfigured(cur) {
			return secretmanagerstore.Secret{}, secretmanagerstore.ErrManagedRotationEnabled
		}
		ver = cur.NextVer
		cur.NextVer++
		if cfg != nil {
			// Copy the Rotation before setting the new field: GetSecret returns
			// a struct whose *Rotation aliases the store's map entry, so an
			// in-place field write would race a concurrent reader.
			rot := secretmanagerstore.Rotation{}
			if cur.Rotation != nil {
				rot = *cur.Rotation
			}
			rot.ManagedRotation = cfg
			cur.Rotation = &rot
		}
		return cur, nil
	}); err != nil {
		if errors.Is(err, secretmanagerstore.ErrManagedRotationEnabled) {
			return versionMeta{}, managedRotationErr("managed rotation is already enabled for secret " + id)
		}
		return versionMeta{}, mapSecretErr(err)
	}
	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: strconv.Itoa(ver), State: "ENABLED",
		CreateTime: clock.Now(), Data: base64.StdEncoding.EncodeToString(encrypted),
		KmsKeyName: sec.KmsKeyName, WrappedDEK: wrappedDEK,
	}
	if err := p.secrets.CreateVersion(ctx, nr.AccountID, stv); err != nil {
		return versionMeta{}, err
	}
	return fromStoreVersion(nr, stv), nil
}

func (p *Provider) Access(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, version := parseSecretName(name)
	version, err = p.resolveVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v, err := p.secrets.GetVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, &model.ProviderError{
			Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
			Message: "secret version " + v.VersionID + " is " + v.State + ", not ENABLED",
		}
	}

	encryptedPayloadBytes, err := base64.StdEncoding.DecodeString(v.Data)
	if err != nil {
		return nil, err
	}

	rawDEK, err := p.encryptor.Unwrap(ctx, nr.AccountID, v.KmsKeyName, v.WrappedDEK)
	if err != nil {
		return nil, err
	}

	decryptedPayloadBytes, err := kms.DecryptData(rawDEK, encryptedPayloadBytes, nil)
	if err != nil {
		return nil, err
	}
	decryptedPayloadBase64 := base64.StdEncoding.EncodeToString(decryptedPayloadBytes)

	m := fromStoreVersion(nr, v)
	m.Data = decryptedPayloadBase64

	return provider.OK(map[string]any{
		"name": m.Name,
		"payload": map[string]any{
			"data":       m.Data,
			"dataCrc32c": crc32c(m.Data),
		},
	}), nil
}

func (p *Provider) ListVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, _ := parseSecretName(name)
	// The parent secret must exist: listing the versions of a missing secret
	// is a 404 at the parent, not an empty list.
	if err := p.requireSecret(ctx, nr.AccountID, secret); err != nil {
		return nil, err
	}
	versions, err := p.secrets.ListVersions(ctx, nr.AccountID, secret)
	if err != nil {
		return nil, err
	}
	filter, _ := nr.Params["filter"].(string)
	matched, applied, err := secretmanagersvc.FilterVersions(nr.AccountID, secret, filter, versions)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(matched, func(v secretmanagerstore.Version) string { return v.VersionID }, nr.Params)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, v := range page {
		items = append(items, versionToMap(fromStoreVersion(nr, v)))
	}
	total := len(versions)
	if applied {
		total = 0
	}
	resp := map[string]any{"versions": items, "totalSize": total}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, version := parseSecretName(name)
	version, err = p.resolveVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v, err := p.secrets.GetVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	m := fromStoreVersion(nr, v)
	return provider.OK(versionToMap(m)), nil
}

// resolveVersion resolves the "latest" version alias to the highest existing
// version number. Any other version id passes through unchanged.
func (p *Provider) resolveVersion(ctx context.Context, project, secret, version string) (string, error) {
	if version != "latest" {
		return version, nil
	}
	versions, err := p.secrets.ListVersions(ctx, project, secret)
	if err != nil {
		return "", err
	}
	latest := ""
	latestN := -1
	for _, v := range versions {
		if n, err := strconv.Atoi(v.VersionID); err == nil && n > latestN {
			latestN = n
			latest = v.VersionID
		}
	}
	if latest == "" {
		return "", secretmanagerstore.ErrNoSuchVersion
	}
	return latest, nil
}

func (p *Provider) DestroyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "DESTROYED")
}

func (p *Provider) DisableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "DISABLED")
}

func (p *Provider) EnableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "ENABLED")
}

// setVersionState applies a lifecycle transition (DESTROYED/DISABLED/ENABLED)
// to a secret version.
func (p *Provider) setVersionState(ctx context.Context, nr *model.NormalizedRequest, state string) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, version := parseSecretName(name)
	// The etag precondition and the transition run inside one atomic store
	// mutate, so a concurrent transition cannot invalidate the etag between the
	// check and the write.
	v, err := p.secrets.UpdateVersionIf(ctx, nr.AccountID, secret, version, func(v secretmanagerstore.Version) (secretmanagerstore.Version, error) {
		if err := secretmanagersvc.CheckVersionEtag(fromStoreVersion(nr, v).Name, v, bodyEtag(nr)); err != nil {
			return secretmanagerstore.Version{}, err
		}
		// DESTROYED is terminal: a version may not leave this state once entered.
		if v.State == "DESTROYED" {
			return secretmanagerstore.Version{}, &model.ProviderError{
				Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
				Message: "secret version " + v.VersionID + " is DESTROYED",
			}
		}
		v.State = state
		if state == "DESTROYED" {
			v.DestroyTime = clock.Now()
		}
		return v, nil
	})
	if err != nil {
		return nil, mapVersionErr(err)
	}
	m := fromStoreVersion(nr, v)
	return provider.OK(versionToMap(m)), nil
}

func (p *Provider) requireSecret(ctx context.Context, account, id string) error {
	if _, err := p.secrets.GetSecret(ctx, account, id); err != nil {
		return mapSecretErr(err)
	}
	return nil
}

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	if err := p.requireSecret(ctx, nr.AccountID, id); err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtSecretPolicy, id))), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	if err := p.requireSecret(ctx, nr.AccountID, id); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtSecretPolicy, id, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	if err := p.requireSecret(ctx, nr.AccountID, id); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func mapSecretErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchSecret) {
		return model.NewProviderError("NotFound", "secret not found", 404)
	}
	return err
}

func mapVersionErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchVersion) {
		return model.NewProviderError("NotFound", "secret version not found", 404)
	}
	return err
}

// crc32c returns the CRC32C-Castagnoli checksum of the base64-decoded data as
// a decimal string (google.protobuf.Int64Value encoding).
func crc32c(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "0"
	}
	return strconv.FormatUint(uint64(crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli))), 10)
}

func secretToMap(nr *model.NormalizedRequest, s secretmanagerstore.Secret) map[string]any {
	m := fromStoreSecret(nr, s)
	out := map[string]any{
		"name":       m.Name,
		"createTime": m.CreateTime,
		// etag is the shared Secret Manager etag (see service/secretmanager):
		// a hash of the secret's mutable metadata, so it is stable across reads
		// of an unchanged secret and different after any update — the same
		// contract GCP's opaque token provides for the delete precondition.
		"etag":        secretmanagersvc.SecretEtag(m.Name, s),
		"replication": replicationToMap(m.KmsKeyName),
	}
	if m.Labels != nil {
		out["labels"] = m.Labels
	}
	if m.Annotations != nil {
		out["annotations"] = m.Annotations
	}
	if m.Rotation != nil {
		rot := map[string]any{}
		if m.Rotation.NextRotationTime != "" {
			rot["nextRotationTime"] = m.Rotation.NextRotationTime
		}
		if m.Rotation.RotationPeriod != "" {
			rot["rotationPeriod"] = m.Rotation.RotationPeriod
		}
		if m.Rotation.ManagedRotation != nil {
			rot["managedRotationStatus"] = map[string]any{"state": m.Rotation.ManagedRotation.State}
		}
		if len(rot) > 0 {
			out["rotation"] = rot
		}
	}
	if m.VersionAliases != nil {
		out["versionAliases"] = m.VersionAliases
	}
	return out
}

// replicationToMap renders the secret's replication policy. JaisCloud models
// the automatic policy only (see versionToMap), so the response always carries
// the automatic arm; when the secret was created/patched with CMEK, the
// configured Cloud KMS key is echoed under
// automatic.customerManagedEncryption.kmsKeyName — real GCP returns the key a
// caller set, and Terraform's computed
// replication[0].auto[0].customer_managed_encryption[0].kms_key_name (and the
// console's CMEK chip) read it back from here. With no CMEK the nested object
// is omitted entirely, as real GCP does.
func replicationToMap(kmsKeyName string) map[string]any {
	auto := map[string]any{}
	if kmsKeyName != "" {
		auto["customerManagedEncryption"] = map[string]any{"kmsKeyName": kmsKeyName}
	}
	return map[string]any{"automatic": auto}
}

func versionToMap(v versionMeta) map[string]any {
	out := map[string]any{
		"name":       v.Name,
		"state":      v.State,
		"createTime": v.CreateTime,
		// etag is the shared Secret Manager version etag: it changes on every
		// lifecycle transition (the version's only mutable field), so a stale
		// disable/enable/destroy precondition is detectable.
		"etag": secretmanagersvc.VersionEtag(v.Name, v.CreateTime, v.State),
		// replicationStatus mirrors the parent secret's replication. JaisCloud
		// only models automatic replication, which GCP renders as an empty
		// automatic object when the secret uses Google-managed encryption.
		// When the version was wrapped with a CMEK, real GCP also reports the
		// exact CryptoKeyVersion that wrapped this payload (DC44).
		"replicationStatus": versionReplicationStatusToMap(v),
		// clientSpecifiedPayloadChecksum is the only integrity field the real
		// SecretVersion carries: the Discovery schema and the v1 proto both
		// define it and nothing else (no `checksum` property). gcloud's
		// `secrets versions add` checks it and aborts with a false
		// data-corruption warning when it is absent.
		"clientSpecifiedPayloadChecksum": true,
	}
	if v.DestroyTime != "" {
		out["destroyTime"] = v.DestroyTime
	}
	return out
}

// versionReplicationStatusToMap renders a SecretVersion's replicationStatus.
// Real GCP populates automatic.customerManagedEncryption.kmsKeyVersionName only
// when the version's payload was wrapped with a customer-managed key, and omits
// the nested message otherwise; the automatic arm itself is always present.
func versionReplicationStatusToMap(v versionMeta) map[string]any {
	auto := map[string]any{}
	if v.KmsKeyVersionName != "" {
		auto["customerManagedEncryption"] = map[string]any{"kmsKeyVersionName": v.KmsKeyVersionName}
	}
	return map[string]any{"automatic": auto}
}
