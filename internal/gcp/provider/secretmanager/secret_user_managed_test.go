package secretmanager

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/store"
)

// newTestKey creates an enabled CryptoKey with one primary version and returns
// its resource name plus that version number.
func newTestKey(t *testing.T, ks *kms.MemoryStore, project, location, ring, keyID string) (string, string) {
	t.Helper()
	ctx := context.Background()
	if err := ks.CreateKeyRing(ctx, project, location, ring, kms.KeyRing{ID: ring}); err != nil && err != kms.ErrAlreadyExists {
		t.Fatalf("create key ring %s/%s: %v", location, ring, err)
	}
	if err := ks.CreateCryptoKey(ctx, project, location, ring, keyID, kms.CryptoKey{ID: keyID, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatalf("create crypto key %s: %v", keyID, err)
	}
	version, err := ks.CreateVersion(ctx, project, location, ring, keyID, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatalf("create key version %s: %v", keyID, err)
	}
	if err := ks.UpdatePrimaryVersion(ctx, project, location, ring, keyID, version); err != nil {
		t.Fatalf("set primary %s: %v", keyID, err)
	}
	return "projects/" + project + "/locations/" + location + "/keyRings/" + ring + "/cryptoKeys/" + keyID, version
}

// replFromData returns the rendered replication object of a secret response.
func replFromData(t *testing.T, data map[string]any) map[string]any {
	t.Helper()
	repl, ok := data["replication"].(map[string]any)
	if !ok {
		t.Fatalf("replication = %#v, want an object", data["replication"])
	}
	return repl
}

// userReplicas returns the rendered userManaged replicas of a secret response,
// failing when the response carries the automatic arm instead.
func userReplicas(t *testing.T, data map[string]any) []map[string]any {
	t.Helper()
	repl := replFromData(t, data)
	if _, ok := repl["automatic"]; ok {
		t.Fatalf("replication = %#v, want userManaged", repl)
	}
	um, ok := repl["userManaged"].(map[string]any)
	if !ok {
		t.Fatalf("replication.userManaged = %#v, want an object", repl["userManaged"])
	}
	raw, ok := um["replicas"].([]any)
	if !ok {
		t.Fatalf("replication.userManaged.replicas = %#v, want an array", um["replicas"])
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("replica = %#v, want an object", item)
		}
		out = append(out, m)
	}
	return out
}

// replicaKeyName returns the CMEK key named by a rendered replica ("" when the
// replica is Google-managed).
func replicaKeyName(t *testing.T, rep map[string]any) string {
	t.Helper()
	raw, ok := rep["customerManagedEncryption"]
	if !ok {
		return ""
	}
	cme, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("replica customerManagedEncryption = %#v, want an object", raw)
	}
	return cme["kmsKeyName"].(string)
}

// replicaStatusKeys returns the kmsKeyVersionName of each rendered user-managed
// replica status of a version response ("" when that replica has no status).
func replicaStatusKeys(t *testing.T, data map[string]any) []string {
	t.Helper()
	rs, ok := data["replicationStatus"].(map[string]any)
	if !ok {
		t.Fatalf("replicationStatus = %#v, want an object", data["replicationStatus"])
	}
	if _, ok := rs["automatic"]; ok {
		t.Fatalf("replicationStatus = %#v, want userManaged", rs)
	}
	um, ok := rs["userManaged"].(map[string]any)
	if !ok {
		t.Fatalf("replicationStatus.userManaged = %#v, want an object", rs["userManaged"])
	}
	raw, ok := um["replicas"].([]any)
	if !ok {
		t.Fatalf("replicationStatus.userManaged.replicas = %#v, want an array", um["replicas"])
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		rep, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("replica status = %#v, want an object", item)
		}
		if raw, ok := rep["customerManagedEncryption"]; ok {
			cme, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("replica status customerManagedEncryption = %#v", raw)
			}
			out = append(out, cme["kmsKeyVersionName"].(string))
			continue
		}
		out = append(out, "")
	}
	return out
}

// TestSecretManagerUserManagedReplication covers DC45 on the REST provider: a
// secret created with a user-managed replication policy must read back exactly
// that policy (arm, replica locations and per-replica keys) on create, get, list
// and update, instead of the automatic arm the emulator used to rewrite it to.
// The version's replicationStatus follows the policy too, and the payload stays
// readable through the wrap of the replica that supplied the CMEK key.
func TestSecretManagerUserManagedReplication(t *testing.T) {
	ctx := context.Background()
	ks := kms.NewMemoryStore()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(ks))

	const project = "proj"
	keyEast, versionEast := newTestKey(t, ks, project, "us-east1", "kr-east", "key-east")
	keyEast2, _ := newTestKey(t, ks, project, "us-east1", "kr-east", "key-east-2")

	replicationBody := func(eastKey string) map[string]any {
		body := map[string]any{
			"replication": map[string]any{
				"userManaged": map[string]any{
					"replicas": []any{
						map[string]any{"location": "us-east1"},
						map[string]any{"location": "us-central1"},
					},
				},
			},
		}
		if eastKey != "" {
			body["replication"].(map[string]any)["userManaged"].(map[string]any)["replicas"].([]any)[0].(map[string]any)["customerManagedEncryption"] = map[string]any{"kmsKeyName": eastKey}
		}
		return body
	}

	created, err := p.Create(ctx, newNR(map[string]any{"secretId": "um-secret", "body": replicationBody(keyEast)}))
	if err != nil {
		t.Fatalf("create user-managed secret: %v", err)
	}
	assertPolicy := func(what string, data map[string]any, eastKey string) {
		t.Helper()
		replicas := userReplicas(t, data)
		if len(replicas) != 2 {
			t.Fatalf("%s: replicas = %#v, want 2", what, replicas)
		}
		if replicas[0]["location"] != "us-east1" || replicas[1]["location"] != "us-central1" {
			t.Fatalf("%s: replica locations = %#v", what, replicas)
		}
		if got := replicaKeyName(t, replicas[0]); got != eastKey {
			t.Fatalf("%s: us-east1 key = %q, want %q", what, got, eastKey)
		}
		if got := replicaKeyName(t, replicas[1]); got != "" {
			t.Fatalf("%s: us-central1 key = %q, want none", what, got)
		}
	}
	assertPolicy("create", created.Data, keyEast)
	etagBefore, _ := created.Data["etag"].(string)

	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/um-secret"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertPolicy("get", got.Data, keyEast)

	listed, err := p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := listed.Data["secrets"].([]any)
	if len(items) != 1 {
		t.Fatalf("list returned %d secrets, want 1", len(items))
	}
	assertPolicy("list", items[0].(map[string]any), keyEast)

	// A version is wrapped with the policy's CMEK key and reports one status per
	// replica, with the CryptoKeyVersion only on the wrapping replica.
	payload := "aGVsbG8sIHdvcmxk" // "hello, world"
	addResp, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/um-secret",
		"body": map[string]any{"payload": map[string]any{"data": payload}},
	}))
	if err != nil {
		t.Fatalf("add version: %v", err)
	}
	wantKeyVersion := keyEast + "/versions/" + versionEast
	if keys := replicaStatusKeys(t, addResp.Data); len(keys) != 2 || keys[0] != wantKeyVersion || keys[1] != "" {
		t.Fatalf("add version replica status = %#v, want [%q \"\"]", keys, wantKeyVersion)
	}
	getVersion, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/um-secret/versions/1"}))
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if keys := replicaStatusKeys(t, getVersion.Data); len(keys) != 2 || keys[0] != wantKeyVersion {
		t.Fatalf("get version replica status = %#v, want [%q ...]", keys, wantKeyVersion)
	}
	access, err := p.Access(ctx, newNR(map[string]any{"name": "secrets/um-secret/versions/1"}))
	if err != nil {
		t.Fatalf("access version: %v", err)
	}
	if got := access.Data["payload"].(map[string]any)["data"]; got != payload {
		t.Fatalf("access payload = %q, want %q", got, payload)
	}

	// Updating a replica's key (the only mutable part of the policy) is echoed
	// and moves the etag, and only affects versions added afterwards.
	patched, err := p.Update(ctx, newNR(map[string]any{"name": "secrets/um-secret", "body": replicationBody(keyEast2)}))
	if err != nil {
		t.Fatalf("patch replica key: %v", err)
	}
	assertPolicy("patch", patched.Data, keyEast2)
	if etagAfter, _ := patched.Data["etag"].(string); etagAfter == "" || etagAfter == etagBefore {
		t.Fatalf("etag after a replica key change = %q, want a fresh value (before %q)", etagAfter, etagBefore)
	}
	firstVersion, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/um-secret/versions/1"}))
	if err != nil {
		t.Fatalf("get version after patch: %v", err)
	}
	if keys := replicaStatusKeys(t, firstVersion.Data); keys[0] != wantKeyVersion {
		t.Fatalf("version 1 status after the patch = %#v, want its original wrap %q", keys, wantKeyVersion)
	}

	// The arm and the replica set are immutable.
	if _, err := p.Update(ctx, newNR(map[string]any{"name": "secrets/um-secret", "body": map[string]any{
		"replication": map[string]any{"automatic": map[string]any{}},
	}})); errStatus(err) != 400 {
		t.Fatalf("switching to automatic = %v, want 400", err)
	}
	renamed := replicationBody(keyEast2)
	renamed["replication"].(map[string]any)["userManaged"].(map[string]any)["replicas"].([]any)[1].(map[string]any)["location"] = "us-west1"
	if _, err := p.Update(ctx, newNR(map[string]any{"name": "secrets/um-secret", "body": renamed})); errStatus(err) != 400 {
		t.Fatalf("moving a replica = %v, want 400", err)
	}

	// The policy the client set is still the one stored after the rejected
	// updates.
	final, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/um-secret"}))
	if err != nil {
		t.Fatalf("get after rejected updates: %v", err)
	}
	assertPolicy("get after rejected updates", final.Data, keyEast2)

	// Clearing a replica's CMEK is the documented way to disable it, and it
	// applies to versions added afterwards only.
	cleared, err := p.Update(ctx, newNR(map[string]any{"name": "secrets/um-secret", "body": replicationBody("")}))
	if err != nil {
		t.Fatalf("clear replica cmek: %v", err)
	}
	assertPolicy("clear", cleared.Data, "")
	if _, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/um-secret",
		"body": map[string]any{"payload": map[string]any{"data": payload}},
	})); err != nil {
		t.Fatalf("add version after clearing cmek: %v", err)
	}
	newVersion, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/um-secret/versions/2"}))
	if err != nil {
		t.Fatalf("get version after clearing cmek: %v", err)
	}
	if keys := replicaStatusKeys(t, newVersion.Data); keys[0] != "" || keys[1] != "" {
		t.Fatalf("version added after clearing CMEK = %#v, want no CMEK status", keys)
	}
}

// TestSecretManagerReplicationNegatives covers the replication policies real
// GCP rejects: an empty replica list, duplicate locations, a CMEK key outside
// the replica's location, and an automatic policy whose key is not in global.
func TestSecretManagerReplicationNegatives(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	cases := []struct {
		name string
		body map[string]any
	}{
		{"empty replicas", map[string]any{"replication": map[string]any{"userManaged": map[string]any{"replicas": []any{}}}}},
		{"missing replicas", map[string]any{"replication": map[string]any{"userManaged": map[string]any{}}}},
		{"duplicate location", map[string]any{"replication": map[string]any{"userManaged": map[string]any{"replicas": []any{
			map[string]any{"location": "us-east1"},
			map[string]any{"location": "us-east1"},
		}}}}},
		{"replica key in another location", map[string]any{"replication": map[string]any{"userManaged": map[string]any{"replicas": []any{
			map[string]any{
				"location": "us-east1",
				"customerManagedEncryption": map[string]any{
					"kmsKeyName": "projects/proj/locations/us-central1/keyRings/kr/cryptoKeys/k",
				},
			},
		}}}}},
		{"automatic key outside global", map[string]any{"replication": map[string]any{"automatic": map[string]any{
			"customerManagedEncryption": map[string]any{
				"kmsKeyName": "projects/proj/locations/us-east1/keyRings/kr/cryptoKeys/k",
			},
		}}}},
		{"both arms", map[string]any{"replication": map[string]any{
			"automatic":   map[string]any{},
			"userManaged": map[string]any{"replicas": []any{map[string]any{"location": "us-east1"}}},
		}}},
	}
	for _, tc := range cases {
		if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "bad-secret", "body": tc.body})); errStatus(err) != 400 {
			t.Errorf("%s: Create = %v, want 400", tc.name, err)
		}
	}
}

// TestSecretManagerLegacyReplicationRendering covers records written before the
// policy was modelled (only kms_key_name set): they keep rendering the automatic
// arm with their CMEK key, a metadata-only update leaves the legacy record
// untouched (so it is never pinned to a guessed arm), and a later replication
// update adopts the policy the client set — the repair path for such a record.
func TestSecretManagerLegacyReplicationRendering(t *testing.T) {
	ctx := context.Background()
	secrets := secretmanagerstore.NewMemoryStore()
	p := New(secrets, store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const key = "projects/proj/locations/global/keyRings/kr/cryptoKeys/k"
	if err := secrets.CreateSecret(ctx, "proj", "legacy", secretmanagerstore.Secret{
		ID: "legacy", NextVer: 1, KmsKeyName: key,
	}); err != nil {
		t.Fatalf("seed legacy secret: %v", err)
	}

	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/legacy"}))
	if err != nil {
		t.Fatalf("get legacy secret: %v", err)
	}
	repl := replFromData(t, got.Data)
	auto, ok := repl["automatic"].(map[string]any)
	if !ok {
		t.Fatalf("legacy replication = %#v, want the automatic arm", repl)
	}
	cmes, _ := auto["customerManagedEncryption"].(map[string]any)
	if cmes["kmsKeyName"] != key {
		t.Fatalf("legacy automatic cmek = %#v, want %q", auto["customerManagedEncryption"], key)
	}
	legacyEtag, _ := got.Data["etag"].(string)

	// A metadata-only patch must not pin a reconstructed policy onto the record:
	// only the arm the client actually sets may be persisted.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name": "secrets/legacy",
		"body": map[string]any{"labels": map[string]any{"env": "test"}},
	})); err != nil {
		t.Fatalf("patch legacy secret: %v", err)
	}
	stored, err := secrets.GetSecret(ctx, "proj", "legacy")
	if err != nil {
		t.Fatalf("read patched secret: %v", err)
	}
	if stored.Replication != nil {
		t.Fatalf("stored replication after a labels patch = %#v, want the legacy (nil) form", stored.Replication)
	}
	if stored.KmsKeyName != key {
		t.Fatalf("effective key after a labels patch = %q, want %q", stored.KmsKeyName, key)
	}
	patched, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/legacy"}))
	if err != nil {
		t.Fatalf("get patched secret: %v", err)
	}
	if etag, _ := patched.Data["etag"].(string); etag == "" || etag == legacyEtag {
		t.Fatalf("etag after the labels patch = %q, want a fresh value (before %q)", etag, legacyEtag)
	}

	// Re-applying the policy the record really had is accepted (the legacy row
	// carries no arm to conflict with).
	userManaged := map[string]any{
		"replication": map[string]any{
			"userManaged": map[string]any{"replicas": []any{map[string]any{"location": "us-east1"}}},
		},
	}
	if _, err := p.Update(ctx, newNR(map[string]any{"name": "secrets/legacy", "body": userManaged})); err != nil {
		t.Fatalf("adopt user-managed policy on a legacy record: %v", err)
	}
	adopted, err := secrets.GetSecret(ctx, "proj", "legacy")
	if err != nil {
		t.Fatalf("read adopted secret: %v", err)
	}
	if adopted.Replication == nil || adopted.Replication.UserManaged == nil || len(adopted.Replication.UserManaged.Replicas) != 1 {
		t.Fatalf("stored replication after adoption = %#v, want the user-managed arm", adopted.Replication)
	}
	if adopted.KmsKeyName != "" {
		t.Fatalf("effective key after adopting a CMEK-less policy = %q, want empty", adopted.KmsKeyName)
	}
}

// TestSecretManagerReplicationUpdateMask pins the REST patch mask: a field the
// mask does not select is never written, so a labels-only patch that happens to
// carry the read-back replication policy cannot clear CMEK (DC45 made that
// destructive), while a mask naming replication — exactly or as a nested path —
// is honored.
func TestSecretManagerReplicationUpdateMask(t *testing.T) {
	ctx := context.Background()
	secrets := secretmanagerstore.NewMemoryStore()
	p := New(secrets, store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const key = "projects/proj/locations/global/keyRings/kr/cryptoKeys/k"
	created, err := p.Create(ctx, newNR(map[string]any{
		"secretId": "masked",
		"body": map[string]any{"replication": map[string]any{"automatic": map[string]any{
			"customerManagedEncryption": map[string]any{"kmsKeyName": key},
		}}},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := cmekFromSecretMap(t, created.Data); got != key {
		t.Fatalf("created cmek = %q, want %q", got, key)
	}

	// A labels-only mask must leave the policy (and therefore the key) alone.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/masked",
		"updateMask": "labels",
		"body": map[string]any{
			"labels":      map[string]any{"env": "test"},
			"replication": map[string]any{"automatic": map[string]any{}},
		},
	})); err != nil {
		t.Fatalf("labels-only patch: %v", err)
	}
	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/masked"}))
	if err != nil {
		t.Fatalf("get after labels-only patch: %v", err)
	}
	if cmek := cmekFromSecretMap(t, got.Data); cmek != key {
		t.Fatalf("cmek after a labels-only patch = %q, want the key preserved", cmek)
	}

	// A nested replication path selects the field too: setting a key applies.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/masked",
		"updateMask": "replication.automatic.customerManagedEncryption.kmsKeyName",
		"body": map[string]any{"replication": map[string]any{"automatic": map[string]any{
			"customerManagedEncryption": map[string]any{"kmsKeyName": "projects/proj/locations/global/keyRings/kr/cryptoKeys/k2"},
		}}},
	})); err != nil {
		t.Fatalf("nested replication patch: %v", err)
	}
	got, err = p.Get(ctx, newNR(map[string]any{"name": "secrets/masked"}))
	if err != nil {
		t.Fatalf("get after nested replication patch: %v", err)
	}
	if cmek := cmekFromSecretMap(t, got.Data); cmek != "projects/proj/locations/global/keyRings/kr/cryptoKeys/k2" {
		t.Fatalf("cmek after the nested replication patch = %q", cmek)
	}

	// A mask naming replication applies the documented "disable CMEK" form.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name":       "secrets/masked",
		"updateMask": "replication",
		"body":       map[string]any{"replication": map[string]any{"automatic": map[string]any{}}},
	})); err != nil {
		t.Fatalf("disable cmek: %v", err)
	}
	got, err = p.Get(ctx, newNR(map[string]any{"name": "secrets/masked"}))
	if err != nil {
		t.Fatalf("get after disabling cmek: %v", err)
	}
	if cmek := cmekFromSecretMap(t, got.Data); cmek != "" {
		t.Fatalf("cmek after the documented disable form = %q, want empty", cmek)
	}
}
