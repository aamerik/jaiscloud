// secretmanager_replication_test.go closes DC45 (plan_docs/DC10-depth-closure):
// Secret Manager accepted a user-managed replication policy but stored only the
// first replica's CMEK key and rendered every response as the automatic arm, so
// a client (Terraform's `replication { user_managed { … } }`) read back a
// different policy than it set.
//
// Asserted through the official apiary REST client:
//   - create/get/list echo the user-managed arm, every replica location and the
//     per-replica key;
//   - a version of a user-managed secret reports a user-managed
//     replicationStatus (one entry per replica);
//   - only the CMEK configuration may be updated (the arm and the replica
//     locations are immutable), and a key change is echoed and bumps the etag;
//   - switching the arm is INVALID_ARGUMENT.
package sdkrest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	secretmanager "google.golang.org/api/secretmanager/v1"
)

// requireInvalidArgumentAPI asserts a 400 INVALID_ARGUMENT error envelope.
func requireInvalidArgumentAPI(t *testing.T, err error, what string) {
	t.Helper()
	require.Error(t, err, what)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr, "%s: want a googleapi.Error, got %T", what, err)
	require.Equal(t, 400, apiErr.Code, "%s: HTTP status", what)
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(apiErr.Body), &envelope), "%s: error body %q", what, apiErr.Body)
	require.Equal(t, "INVALID_ARGUMENT", envelope.Error.Status, "%s: error body %q", what, apiErr.Body)
}

func TestSDKSecretManagerUserManagedReplication(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	userManaged := func(replicas ...*secretmanager.Replica) *secretmanager.Replication {
		return &secretmanager.Replication{
			UserManaged: &secretmanager.UserManaged{Replicas: replicas},
		}
	}
	// A user-managed secret with Google-managed encryption: both replicas report
	// a location and no CMEK key.
	plain, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: userManaged(
			&secretmanager.Replica{Location: "us-east1"},
			&secretmanager.Replica{Location: "us-central1"},
		),
	}).SecretId(unique("um")).Do()
	require.NoError(t, err)
	require.NotNil(t, plain.Replication, "replication must be returned")
	require.Nil(t, plain.Replication.Automatic, "a user-managed secret must not report the automatic arm")
	require.Len(t, plain.Replication.UserManaged.Replicas, 2, "replica count")
	require.Equal(t, "us-east1", plain.Replication.UserManaged.Replicas[0].Location)
	require.Equal(t, "us-central1", plain.Replication.UserManaged.Replicas[1].Location)
	require.Nil(t, plain.Replication.UserManaged.Replicas[0].CustomerManagedEncryption, "no CMEK named")

	readBack, err := svc.Projects.Secrets.Get(plain.Name).Do()
	require.NoError(t, err)
	require.Len(t, readBack.Replication.UserManaged.Replicas, 2, "get must echo the policy")

	var found *secretmanager.Secret
	err = svc.Projects.Secrets.List("projects/proj").Pages(ctx, func(resp *secretmanager.ListSecretsResponse) error {
		for _, item := range resp.Secrets {
			if item.Name == plain.Name {
				found = item
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, found, "created secret must be listed")
	require.Len(t, found.Replication.UserManaged.Replicas, 2, "list must echo the policy")

	// A version of a user-managed secret reports the user-managed status: one
	// entry per replica, none carrying a CMEK key here.
	version, err := svc.Projects.Secrets.AddVersion(plain.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGVsbG8="},
	}).Do()
	require.NoError(t, err)
	require.NotNil(t, version.ReplicationStatus, "replicationStatus must be returned")
	require.Nil(t, version.ReplicationStatus.Automatic, "a user-managed version must not report the automatic status")
	require.Len(t, version.ReplicationStatus.UserManaged.Replicas, 2, "one status per replica")
	require.Equal(t, "us-east1", version.ReplicationStatus.UserManaged.Replicas[0].Location)
	require.Nil(t, version.ReplicationStatus.UserManaged.Replicas[0].CustomerManagedEncryption, "no CMEK status")

	// Only the CMEK configuration is mutable; the key it names is echoed back and
	// moves the etag. The key is not resolved at create (a known emulator gap,
	// DC58 — real GCP rejects a missing key there), and no further version is
	// added, which is what would resolve it.
	key := func(location, id string) string {
		return "projects/proj/locations/" + location + "/keyRings/kr/cryptoKeys/" + id
	}
	withKey := func(id string) *secretmanager.Replication {
		return userManaged(&secretmanager.Replica{
			Location:                  "us-east1",
			CustomerManagedEncryption: &secretmanager.CustomerManagedEncryption{KmsKeyName: key("us-east1", id)},
		}, &secretmanager.Replica{Location: "us-central1"})
	}
	cmekSecret, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: withKey("k1"),
	}).SecretId(unique("um-cmek")).Do()
	require.NoError(t, err)
	require.Equal(t, key("us-east1", "k1"),
		cmekSecret.Replication.UserManaged.Replicas[0].CustomerManagedEncryption.KmsKeyName)

	patched, err := svc.Projects.Secrets.Patch(cmekSecret.Name, &secretmanager.Secret{
		Replication: withKey("k2"),
	}).UpdateMask("replication").Do()
	require.NoError(t, err, "patching a replica's CMEK key is the supported policy update")
	require.Equal(t, key("us-east1", "k2"),
		patched.Replication.UserManaged.Replicas[0].CustomerManagedEncryption.KmsKeyName)
	require.NotEqual(t, cmekSecret.Etag, patched.Etag, "a replica key change must change the secret etag")

	// The arm and the replica locations are immutable.
	_, err = svc.Projects.Secrets.Patch(cmekSecret.Name, &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).UpdateMask("replication").Do()
	requireInvalidArgumentAPI(t, err, "switching to automatic replication")

	_, err = svc.Projects.Secrets.Patch(cmekSecret.Name, &secretmanager.Secret{
		Replication: userManaged(
			&secretmanager.Replica{Location: "us-east1"},
			&secretmanager.Replica{Location: "us-west1"},
		),
	}).UpdateMask("replication").Do()
	requireInvalidArgumentAPI(t, err, "moving a replica")

	// The rejected updates left the policy as the client set it.
	final, err := svc.Projects.Secrets.Get(cmekSecret.Name).Do()
	require.NoError(t, err)
	require.Equal(t, key("us-east1", "k2"),
		final.Replication.UserManaged.Replicas[0].CustomerManagedEncryption.KmsKeyName)
	require.Equal(t, "us-central1", final.Replication.UserManaged.Replicas[1].Location)
}
