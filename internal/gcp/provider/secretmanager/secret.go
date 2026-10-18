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
	// Replication is the stored policy, nil for a record written before it was
	// modelled (one that carries only legacyKmsKeyName). Keeping the raw value
	// means a metadata-only update does not pin a reconstructed arm onto a
	// legacy record.
	Replication *secretmanagerstore.Replication
	// legacyKmsKeyName is the stored effective key of a record that predates the
	// modelled policy; it is preserved verbatim so such a record keeps its CMEK.
	legacyKmsKeyName string
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
	// KmsKeyName is the Cloud KMS key this version's DEK was wrapped with; it
	// decides which user-managed replica reports a CMEK status.
	KmsKeyName string `json:"kmsKeyName,omitempty"`
	// WrappedDEK/ReplicaDEKs are the raw wraps the replicationStatus is derived
	// from (KmsKeyVersionName above is their rendered form). They are not part
	// of the wire response.
	WrappedDEK  []byte                          `json:"-"`
	ReplicaDEKs []secretmanagerstore.ReplicaDEK `json:"-"`
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
	s := secretmanagerstore.Secret{
		ID: secretID(m.Name), Labels: m.Labels, Annotations: m.Annotations, CreateTime: tc, NextVer: m.NextVer,
		Rotation: m.Rotation, VersionAliases: m.VersionAliases,
		Replication: m.Replication, KmsKeyName: m.legacyKmsKeyName,
	}
	if m.Replication != nil {
		s.KmsKeyName = m.Replication.WrapKey()
	}
	return s
}

func fromStoreSecret(nr *model.NormalizedRequest, s secretmanagerstore.Secret) secretMeta {
	m := secretMeta{
		Name: nr.ResourceID("secret", s.ID), Labels: s.Labels, Annotations: s.Annotations, NextVer: s.NextVer,
		Rotation: s.Rotation, VersionAliases: s.VersionAliases,
		Replication: s.Replication, legacyKmsKeyName: s.KmsKeyName,
	}
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

// replicationFromBody parses the `replication` oneof from a request body. It
// returns (nil, nil) when the body carries no replication policy (the API's
// default, automatic replication). The shape is validated by
// secretmanagersvc.ValidateReplication, not here.
func replicationFromBody(body map[string]any) (*secretmanagerstore.Replication, error) {
	replRaw, ok := body["replication"]
	if !ok || replRaw == nil {
		return nil, nil
	}
	repl, ok := replRaw.(map[string]any)
	if !ok {
		return nil, model.NewProviderError("InvalidRequest", "replication must be an object", 400)
	}
	out := &secretmanagerstore.Replication{}

	if autoRaw, ok := repl["automatic"]; ok && autoRaw != nil {
		auto, ok := autoRaw.(map[string]any)
		if !ok {
			return nil, model.NewProviderError("InvalidRequest", "replication.automatic must be an object", 400)
		}
		out.Automatic = &secretmanagerstore.AutomaticReplication{
			CustomerManagedEncryption: customerManagedEncryptionFromBody(auto),
		}
	}
	if umRaw, ok := repl["userManaged"]; ok && umRaw != nil {
		um, ok := umRaw.(map[string]any)
		if !ok {
			return nil, model.NewProviderError("InvalidRequest", "replication.userManaged must be an object", 400)
		}
		// replicas is the only field of UserManaged; a missing list is an empty
		// one, which ValidateReplication rejects (the API documents it as
		// required and non-empty).
		umOut := &secretmanagerstore.UserManagedReplication{}
		if repsRaw, ok := um["replicas"]; ok && repsRaw != nil {
			reps, ok := repsRaw.([]any)
			if !ok {
				return nil, model.NewProviderError("InvalidRequest", "replication.userManaged.replicas must be an array", 400)
			}
			for _, repRaw := range reps {
				rep, ok := repRaw.(map[string]any)
				if !ok {
					return nil, model.NewProviderError("InvalidRequest", "replication.userManaged.replicas entries must be objects", 400)
				}
				loc, _ := rep["location"].(string)
				umOut.Replicas = append(umOut.Replicas, secretmanagerstore.Replica{
					Location:                  loc,
					CustomerManagedEncryption: customerManagedEncryptionFromBody(rep),
				})
			}
		}
		out.UserManaged = umOut
	}
	return out, nil
}

// customerManagedEncryptionFromBody reads an optional customerManagedEncryption
// object out of a Replica or Automatic body. The nested object's presence is
// meaningful (it is what names a CMEK key), so an absent object is nil while a
// present one is returned even with an empty key — ValidateReplication then
// rejects the missing key instead of silently treating it as Google-managed.
func customerManagedEncryptionFromBody(m map[string]any) *secretmanagerstore.CustomerManagedEncryption {
	raw, ok := m["customerManagedEncryption"]
	if !ok || raw == nil {
		return nil
	}
	cme, ok := raw.(map[string]any)
	if !ok {
		return &secretmanagerstore.CustomerManagedEncryption{}
	}
	key, _ := cme["kmsKeyName"].(string)
	return &secretmanagerstore.CustomerManagedEncryption{KmsKeyName: key}
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
		KmsKeyName:        v.KmsKeyName,
		WrappedDEK:        v.WrappedDEK,
		ReplicaDEKs:       v.ReplicaDEKs,
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
	replication, err := replicationFromBody(body)
	if err != nil {
		return nil, err
	}
	if err := secretmanagersvc.ValidateReplicationKeys(ctx, p.encryptor, nr.AccountID, replication); err != nil {
		return nil, err
	}
	m.Replication = replication
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

	// The REST patch documents `update_mask` as required. The emulator still
	// accepts a maskless patch (the console sends none) and then applies every
	// populated body field, but a supplied mask is honored: writing a field the
	// mask does not name would be destructive now that replication is modelled
	// (a labels-only patch carrying a stale `replication` must not clear CMEK).
	mask := restUpdateMask(nr)

	// Parse the incoming replication policy once: it is request data, not store
	// state, so it must not be re-derived inside the (retried) atomic mutate.
	// A request whose mask does not select replication is ignored entirely.
	var incomingReplication *secretmanagerstore.Replication
	if mask.has("replication") {
		incomingReplication, err = replicationFromBody(body)
		if err != nil {
			return nil, err
		}
		if err := secretmanagersvc.ValidateReplicationKeys(ctx, p.encryptor, nr.AccountID, incomingReplication); err != nil {
			return nil, err
		}
	}

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
			if labels, ok := body["labels"].(map[string]any); ok && mask.has("labels") {
				m.Labels = make(map[string]string, len(labels))
				for k, v := range labels {
					if sv, ok := v.(string); ok {
						m.Labels[k] = sv
					}
				}
			}
			if annotations := annotationsFromBody(body); annotations != nil && mask.has("annotations") {
				m.Annotations = annotations
			}
			if r := rotationFromBody(body); r != nil && mask.has("rotation") {
				// managedRotationStatus is output-only: keep the stored
				// config/status across a schedule update that doesn't carry it.
				if m.Rotation != nil {
					r.ManagedRotation = m.Rotation.ManagedRotation
				}
				m.Rotation = r
			}
			if va := versionAliasesFromBody(body); va != nil && mask.hasAny("versionAliases", "version_aliases") {
				m.VersionAliases = va
			}
			merged, err := secretmanagersvc.MergeReplication(current.Replication, incomingReplication)
			if err != nil {
				return secretmanagerstore.Secret{}, err
			}
			m.Replication = merged
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

// maybeRotate advances a secret's rotation schedule when it is due: it moves
// nextRotationTime forward by rotationPeriod. Real Secret Manager's rotation
// schedule is notification-only — it publishes a SECRET_ROTATE message to the
// secret's Pub/Sub topics and reschedules the next one; it never creates a
// SecretVersion (only *managed* rotation, the Cloud SQL typed secret, adds
// versions, see RotateSecret). The emulator evaluates the schedule lazily on
// read (no background worker) and does not deliver the notification yet (the
// `topics` field is unmodelled, recorded as DC64), so the reschedule is the
// only observable effect of reading a due secret.
//
// s is the caller's (possibly stale — read by Get/Access some time before
// this call) snapshot, used only for the cheap up-front "is rotation even
// configured and due" check, to avoid taking the store lock at all in the
// overwhelmingly common case where it isn't. If that check passes, everything
// that actually matters — whether rotation is STILL due and the
// rotation-schedule advance — is re-done from a fresh read inside
// UpdateSecretAtomic's locked section, so a concurrent Update/AddVersion/
// maybeRotate on the same secret can't interleave with this one.
func (p *Provider) maybeRotate(ctx context.Context, account, id string, s secretmanagerstore.Secret) secretmanagerstore.Secret {
	if s.Rotation == nil || s.Rotation.NextRotationTime == "" {
		return s
	}
	if next, err := time.Parse(time.RFC3339Nano, s.Rotation.NextRotationTime); err != nil || clock.Now().Before(next) {
		return s
	}

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

// updateMask is the set of paths a REST patch asked to update. A nil mask means
// the request supplied none, which the emulator reads as "every populated
// field": Secret Manager's REST patch documents `update_mask` as required, but
// the console builds a maskless patch and real GCP clients always send one.
type updateMask map[string]bool

// restUpdateMask reads the comma-separated `updateMask` query parameter.
func restUpdateMask(nr *model.NormalizedRequest) updateMask {
	raw, _ := nr.Params["updateMask"].(string)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	mask := updateMask{}
	for _, path := range strings.Split(raw, ",") {
		if path = strings.TrimSpace(path); path != "" {
			mask[path] = true
		}
	}
	return mask
}

// has reports whether the mask selects field, exactly or as the root of a
// nested path (`replication.automatic.customerManagedEncryption.kmsKeyName`).
// A nil mask selects everything.
func (m updateMask) has(field string) bool {
	if m == nil {
		return true
	}
	if m[field] {
		return true
	}
	for path := range m {
		if strings.HasPrefix(path, field+".") {
			return true
		}
	}
	return false
}

// hasAny is has for a field the two transports spell differently (the REST
// API uses camelCase, the proto snake_case).
func (m updateMask) hasAny(fields ...string) bool {
	if m == nil {
		return true
	}
	for _, field := range fields {
		if m.has(field) {
			return true
		}
	}
	return false
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

	// The wrap keys are derived from the policy, never read off the
	// denormalised KmsKeyName mirror: a stale mirror must not wrap a version
	// with Google-managed encryption while the policy claims CMEK, and a
	// user-managed policy must wrap the same DEK under every replica's key
	// (DC57).
	wrap, err := secretmanagersvc.EncryptPayload(ctx, p.encryptor, nr.AccountID, sec.ReplicationPolicy(), payloadBytes)
	if err != nil {
		return nil, err
	}

	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: version, State: "ENABLED",
		CreateTime: clock.Now(), Data: wrap.Data,
		KmsKeyName: wrap.KmsKeyName, WrappedDEK: wrap.WrappedDEK,
		ReplicaDEKs: wrap.ReplicaDEKs,
	}
	if err := p.secrets.CreateVersion(ctx, nr.AccountID, stv); err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(fromStoreVersion(nr, stv), sec.ReplicationPolicy())), nil
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
	v, repl, err := p.appendManagedVersion(ctx, nr, id, []byte(password), cfg)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(v, repl)), nil
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
	v, repl, err := p.appendManagedVersion(ctx, nr, id, []byte(password), nil)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(v, repl)), nil
}

// appendManagedVersion stores data as a new ENABLED version, encrypting it with
// the secret's CMEK. When cfg is non-nil it records the managed-rotation config
// and allocates the version number in one locked read-modify-write, so managed
// rotation cannot be enabled twice and a concurrent AddVersion cannot roll the
// counter back. The payload is encrypted before the locked write, so a crypto
// failure cannot leave the secret one-shot-enabled with no version.
func (p *Provider) appendManagedVersion(ctx context.Context, nr *model.NormalizedRequest, id string, data []byte, cfg *secretmanagerstore.ManagedRotation) (versionMeta, *secretmanagerstore.Replication, error) {
	sec, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return versionMeta{}, nil, mapSecretErr(err)
	}
	// The wrap keys are derived from the policy, never read off the
	// denormalised KmsKeyName mirror (and one DEK is wrapped per CMEK binding
	// for a user-managed policy, DC57).
	wrap, err := secretmanagersvc.EncryptPayload(ctx, p.encryptor, nr.AccountID, sec.ReplicationPolicy(), data)
	if err != nil {
		return versionMeta{}, nil, err
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
			return versionMeta{}, nil, managedRotationErr("managed rotation is already enabled for secret " + id)
		}
		return versionMeta{}, nil, mapSecretErr(err)
	}
	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: strconv.Itoa(ver), State: "ENABLED",
		CreateTime: clock.Now(), Data: wrap.Data,
		KmsKeyName: wrap.KmsKeyName, WrappedDEK: wrap.WrappedDEK,
		ReplicaDEKs: wrap.ReplicaDEKs,
	}
	if err := p.secrets.CreateVersion(ctx, nr.AccountID, stv); err != nil {
		return versionMeta{}, nil, err
	}
	return fromStoreVersion(nr, stv), sec.ReplicationPolicy(), nil
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

	// Unwrap through the primary wrap, falling back to another replica's when
	// that replica's key is unavailable (a user-managed version carries one wrap
	// per CMEK replica, DC57).
	decryptedPayloadBytes, err := secretmanagersvc.DecryptVersion(ctx, p.encryptor, nr.AccountID, v)
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
	// is a 404 at the parent, not an empty list. Its replication policy shapes
	// every version's replicationStatus.
	parentReplication, err := secretmanagersvc.ParentReplication(ctx, p.secrets, nr.AccountID, secret)
	if err != nil {
		return nil, mapSecretErr(err)
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
		items = append(items, versionToMap(fromStoreVersion(nr, v), parentReplication))
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
	repl, err := secretmanagersvc.ParentReplication(ctx, p.secrets, nr.AccountID, secret)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	m := fromStoreVersion(nr, v)
	return provider.OK(versionToMap(m, repl)), nil
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
	repl, err := secretmanagersvc.ParentReplication(ctx, p.secrets, nr.AccountID, secret)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	m := fromStoreVersion(nr, v)
	return provider.OK(versionToMap(m, repl)), nil
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
		"replication": replicationToMap(s.ReplicationPolicy()),
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

// replicationToMap renders the secret's replication policy — the `replication`
// oneof exactly as the client set it (DC45): the automatic arm with its
// optional CMEK key, or the user-managed arm with every replica location and
// its per-replica key. Real GCP returns the policy the caller configured, so a
// client (notably Terraform) reads back what it wrote instead of a rewritten
// automatic policy. A nil policy (the default) renders as an empty automatic
// arm, and a CMEK-less arm omits the nested object entirely rather than sending
// it empty.
func replicationToMap(repl *secretmanagerstore.Replication) map[string]any {
	if repl != nil && repl.UserManaged != nil {
		replicas := make([]any, 0, len(repl.UserManaged.Replicas))
		for _, rep := range repl.UserManaged.Replicas {
			m := map[string]any{"location": rep.Location}
			if cme := rep.CustomerManagedEncryption; cme != nil && cme.KmsKeyName != "" {
				m["customerManagedEncryption"] = map[string]any{"kmsKeyName": cme.KmsKeyName}
			}
			replicas = append(replicas, m)
		}
		return map[string]any{"userManaged": map[string]any{"replicas": replicas}}
	}
	auto := map[string]any{}
	if repl != nil && repl.Automatic != nil {
		if cme := repl.Automatic.CustomerManagedEncryption; cme != nil && cme.KmsKeyName != "" {
			auto["customerManagedEncryption"] = map[string]any{"kmsKeyName": cme.KmsKeyName}
		}
	}
	return map[string]any{"automatic": auto}
}

func versionToMap(v versionMeta, repl *secretmanagerstore.Replication) map[string]any {
	out := map[string]any{
		"name":       v.Name,
		"state":      v.State,
		"createTime": v.CreateTime,
		// etag is the shared Secret Manager version etag: it changes on every
		// lifecycle transition (the version's only mutable field), so a stale
		// disable/enable/destroy precondition is detectable.
		"etag": secretmanagersvc.VersionEtag(v.Name, v.CreateTime, v.State),
		// replicationStatus mirrors the parent secret's replication policy: the
		// automatic arm (with the CryptoKeyVersion that wrapped this payload,
		// DC44) or one status per user-managed replica (DC45).
		"replicationStatus": versionReplicationStatusToMap(v, repl),
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
// For the automatic policy real GCP populates
// automatic.customerManagedEncryption.kmsKeyVersionName only when the version's
// payload was wrapped with a customer-managed key, and omits the nested message
// otherwise; the automatic arm itself is always present. For a user-managed
// policy it reports the parent secret's replica locations, with the CMEK status
// on the replica whose key wrapped this version (DC45).
func versionReplicationStatusToMap(v versionMeta, repl *secretmanagerstore.Replication) map[string]any {
	if repl != nil && repl.UserManaged != nil {
		statuses := secretmanagersvc.VersionReplicaStatuses(secretmanagersvc.VersionWrap{
			KmsKeyName: v.KmsKeyName, WrappedDEK: v.WrappedDEK, ReplicaDEKs: v.ReplicaDEKs,
		}, repl)
		replicas := make([]any, 0, len(statuses))
		for _, st := range statuses {
			m := map[string]any{"location": st.Location}
			if st.KmsKeyVersionName != "" {
				m["customerManagedEncryption"] = map[string]any{"kmsKeyVersionName": st.KmsKeyVersionName}
			}
			replicas = append(replicas, m)
		}
		return map[string]any{"userManaged": map[string]any{"replicas": replicas}}
	}
	auto := map[string]any{}
	if v.KmsKeyVersionName != "" {
		auto["customerManagedEncryption"] = map[string]any{"kmsKeyVersionName": v.KmsKeyVersionName}
	}
	return map[string]any{"automatic": auto}
}
