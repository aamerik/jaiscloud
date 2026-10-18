package secretmanager

import (
	"strings"
	"testing"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

func cme(key string) *secretmanagerstore.CustomerManagedEncryption {
	if key == "" {
		return nil
	}
	return &secretmanagerstore.CustomerManagedEncryption{KmsKeyName: key}
}

func auto(key string) *secretmanagerstore.Replication {
	return &secretmanagerstore.Replication{
		Automatic: &secretmanagerstore.AutomaticReplication{CustomerManagedEncryption: cme(key)},
	}
}

func userManaged(replicas ...secretmanagerstore.Replica) *secretmanagerstore.Replication {
	return &secretmanagerstore.Replication{
		UserManaged: &secretmanagerstore.UserManagedReplication{Replicas: replicas},
	}
}

func replica(location, key string) secretmanagerstore.Replica {
	return secretmanagerstore.Replica{Location: location, CustomerManagedEncryption: cme(key)}
}

// TestValidateReplication pins the documented shape of the replication oneof:
// one arm only, non-empty and distinct user-managed locations, and a CMEK key
// that resides where the arm requires (global for automatic, the replica's own
// location for user-managed). Both transports call it, so this is the single
// contract.
func TestValidateReplication(t *testing.T) {
	const (
		globalKey    = "projects/p/locations/global/keyRings/kr/cryptoKeys/k"
		usEastKey    = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usCentralKey = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"
	)

	valid := []struct {
		name string
		repl *secretmanagerstore.Replication
	}{
		{"nil defaults to automatic", nil},
		{"automatic without cmek", auto("")},
		{"automatic with global cmek", auto(globalKey)},
		{"one user-managed replica without cmek", userManaged(replica("us-east1", ""))},
		{"replica cmek in the replica location", userManaged(replica("us-east1", usEastKey))},
		{"mixed replicas", userManaged(replica("us-east1", usEastKey), replica("us-central1", ""))},
	}
	for _, tc := range valid {
		if err := ValidateReplication(tc.repl); err != nil {
			t.Errorf("%s: ValidateReplication = %v, want nil", tc.name, err)
		}
	}

	both := auto(globalKey)
	both.UserManaged = &secretmanagerstore.UserManagedReplication{Replicas: []secretmanagerstore.Replica{replica("us-east1", "")}}

	invalid := []struct {
		name string
		repl *secretmanagerstore.Replication
		want string
	}{
		{"both arms", both, "exactly one of automatic or userManaged"},
		{"no arm", &secretmanagerstore.Replication{}, "exactly one of automatic or userManaged"},
		{"automatic cmek without key", &secretmanagerstore.Replication{Automatic: &secretmanagerstore.AutomaticReplication{CustomerManagedEncryption: &secretmanagerstore.CustomerManagedEncryption{}}}, "kmsKeyName is required"},
		{"automatic cmek outside global", auto(usCentralKey), "must reside in global"},
		{"automatic cmek not a crypto key name", auto("not-a-key"), "is not a Cloud KMS CryptoKey name"},
		{"user-managed with no replicas", userManaged(), "cannot be empty"},
		{"replica without location", userManaged(replica("", "")), "location is required"},
		{"duplicate location", userManaged(replica("us-east1", ""), replica("us-east1", usEastKey)), "duplicate location"},
		{
			"replica cmek without key",
			userManaged(secretmanagerstore.Replica{
				Location:                  "us-east1",
				CustomerManagedEncryption: &secretmanagerstore.CustomerManagedEncryption{},
			}),
			"kmsKeyName is required",
		},
		{"replica cmek in another location", userManaged(replica("us-east1", usCentralKey)), "must reside in replica location"},
		{"replica cmek not a crypto key name", userManaged(replica("us-east1", "not-a-key")), "is not a Cloud KMS CryptoKey name"},
	}
	for _, tc := range invalid {
		err := ValidateReplication(tc.repl)
		if err == nil {
			t.Errorf("%s: ValidateReplication = nil, want an error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %q, want it to contain %q", tc.name, err.Error(), tc.want)
		}
	}
}

// TestMergeReplication pins the update semantics: the oneof arm and the
// user-managed replica locations are immutable, only the CMEK configuration
// changes (including clearing it), and a nil incoming policy is a no-op.
func TestMergeReplication(t *testing.T) {
	const (
		globalKey  = "projects/p/locations/global/keyRings/kr/cryptoKeys/k"
		globalKey2 = "projects/p/locations/global/keyRings/kr/cryptoKeys/k2"
		usEastKey  = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usEastKey2 = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k2"
	)
	storedAuto := auto(globalKey)
	storedUser := userManaged(replica("us-east1", usEastKey), replica("us-central1", ""))

	// No incoming policy leaves the stored one alone.
	if got, err := MergeReplication(storedAuto, nil); err != nil || got != storedAuto {
		t.Fatalf("MergeReplication(stored, nil) = %v, %v; want the stored policy", got, err)
	}
	// A record written before the policy was modelled is replaced wholesale.
	if got, err := MergeReplication(nil, storedAuto); err != nil || got != storedAuto {
		t.Fatalf("MergeReplication(nil, incoming) = %v, %v; want incoming", got, err)
	}

	// Automatic: the key is replaced, including clearing it (GCP's documented
	// "remove CMEK configuration" form).
	got, err := MergeReplication(storedAuto, auto(globalKey2))
	if err != nil {
		t.Fatalf("automatic key update: %v", err)
	}
	if key := got.Automatic.CustomerManagedEncryption; key == nil || key.KmsKeyName != globalKey2 {
		t.Fatalf("automatic key = %#v, want %q", key, globalKey2)
	}
	got, err = MergeReplication(storedAuto, auto(""))
	if err != nil {
		t.Fatalf("automatic cmek clear: %v", err)
	}
	if got.Automatic.CustomerManagedEncryption != nil {
		t.Fatalf("automatic cmek clear = %#v, want nil", got.Automatic.CustomerManagedEncryption)
	}

	// The arm itself is immutable.
	if _, err := MergeReplication(storedAuto, userManaged(replica("us-east1", ""))); err == nil || !strings.Contains(err.Error(), "cannot be changed") {
		t.Fatalf("automatic -> user-managed = %v, want an immutability error", err)
	}
	if _, err := MergeReplication(storedUser, auto("")); err == nil || !strings.Contains(err.Error(), "cannot be changed") {
		t.Fatalf("user-managed -> automatic = %v, want an immutability error", err)
	}

	// User-managed: the replica key changes in place, the stored order is kept,
	// and the replica that had no CMEK stays Google-managed.
	got, err = MergeReplication(storedUser, userManaged(replica("us-central1", ""), replica("us-east1", usEastKey2)))
	if err != nil {
		t.Fatalf("user-managed key update: %v", err)
	}
	if len(got.UserManaged.Replicas) != 2 || got.UserManaged.Replicas[0].Location != "us-east1" {
		t.Fatalf("user-managed replicas = %#v, want the stored order (us-east1 first)", got.UserManaged.Replicas)
	}
	if key := got.UserManaged.Replicas[0].CustomerManagedEncryption; key == nil || key.KmsKeyName != usEastKey2 {
		t.Fatalf("us-east1 key = %#v, want %q", key, usEastKey2)
	}
	if key := got.UserManaged.Replicas[1].CustomerManagedEncryption; key != nil {
		t.Fatalf("us-central1 key = %#v, want nil", key)
	}

	// The replica set is immutable: renaming or adding a location is rejected.
	if _, err := MergeReplication(storedUser, userManaged(replica("us-west1", ""), replica("us-central1", ""))); err == nil || !strings.Contains(err.Error(), "replica locations") {
		t.Fatalf("location rename = %v, want a locations error", err)
	}
	if _, err := MergeReplication(storedUser, userManaged(replica("us-east1", usEastKey), replica("us-central1", ""), replica("us-west1", ""))); err == nil || !strings.Contains(err.Error(), "replica locations") {
		t.Fatalf("location add = %v, want a locations error", err)
	}

	// Validation still runs on the incoming policy.
	if _, err := MergeReplication(storedUser, userManaged()); err == nil {
		t.Fatal("MergeReplication accepted an empty replica list")
	}
}

// TestReplicationWrapKey pins which key a new version is wrapped with: the
// automatic arm's, or the first user-managed replica that names one.
func TestReplicationWrapKey(t *testing.T) {
	const (
		key1 = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k1"
		key2 = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k2"
	)
	if got := (*secretmanagerstore.Replication)(nil).WrapKey(); got != "" {
		t.Errorf("nil policy WrapKey = %q, want empty", got)
	}
	if got := auto("").WrapKey(); got != "" {
		t.Errorf("automatic without cmek WrapKey = %q, want empty", got)
	}
	if got := auto(key1).WrapKey(); got != key1 {
		t.Errorf("automatic WrapKey = %q, want %q", got, key1)
	}
	if got := userManaged(replica("us-east1", ""), replica("us-central1", key2)).WrapKey(); got != key2 {
		t.Errorf("user-managed WrapKey = %q, want %q", got, key2)
	}
	if got := userManaged(replica("us-east1", ""), replica("us-central1", "")).WrapKey(); got != "" {
		t.Errorf("user-managed without cmek WrapKey = %q, want empty", got)
	}
}

// TestVersionReplicaStatuses pins the per-replica status: every location is
// reported, and only the replica whose key wrapped the version carries a
// CryptoKeyVersion name.
func TestVersionReplicaStatuses(t *testing.T) {
	const (
		usEastKey    = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usCentralKey = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"
		keyVersion   = usEastKey + "/versions/7"
	)
	repl := userManaged(replica("us-east1", usEastKey), replica("us-central1", usCentralKey), replica("us-west1", ""))

	if got := VersionReplicaStatuses(usEastKey, keyVersion, auto("")); got != nil {
		t.Fatalf("automatic policy statuses = %#v, want nil", got)
	}
	got := VersionReplicaStatuses(usEastKey, keyVersion, repl)
	if len(got) != 3 {
		t.Fatalf("statuses = %#v, want 3 replicas", got)
	}
	if got[0].Location != "us-east1" || got[0].KmsKeyVersionName != keyVersion {
		t.Errorf("us-east1 status = %#v, want the wrapping key version", got[0])
	}
	// Only the replica in the wrapping key's location carries the status: the
	// payload was wrapped in us-east1, so us-central1 and us-west1 report their
	// location alone.
	if got[1].KmsKeyVersionName != "" || got[2].KmsKeyVersionName != "" {
		t.Errorf("non-wrapping-location replicas = %#v, want no kmsKeyVersionName", got[1:])
	}
}
