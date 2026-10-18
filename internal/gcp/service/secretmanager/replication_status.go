// SecretVersion replication status shared by the Secret Manager REST provider
// and gRPC service.
//
// Real Secret Manager reports, per version, the Cloud KMS CryptoKeyVersion that
// actually wrapped that version's payload, as
// `replicationStatus.automatic.customerManagedEncryption.kmsKeyVersionName`
// (the Discovery schema's CustomerManagedEncryptionStatus). The emulator wraps
// each version with the secret's CMEK and tags the wrapped DEK with the
// CryptoKeyVersion it used (kms.EncodeVersionedCiphertext), so the exact
// version — not just the key — is known and can be echoed. It is a per-version
// fact: changing the secret's CMEK, or rotating the key's primary version,
// only affects versions added afterwards, which is precisely what the stored
// wrap-time tag models.
package secretmanager

import (
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

// ReplicaStatus is one user-managed replica's view of a SecretVersion: its
// location and, when that replica's CMEK key wrapped the payload, the exact
// CryptoKeyVersion that did.
type ReplicaStatus struct {
	Location          string
	KmsKeyVersionName string
}

// VersionReplicaStatuses renders the per-replica status of a version whose parent
// secret uses user-managed replication, or nil when the policy is automatic
// (the automatic arm is rendered instead). Every replica location is reported;
// the replica in the location of the key that wrapped this version carries the
// kmsKeyVersionName (kmsKeyVersionName is the name KmsKeyVersionName derived
// from the version's own wrap). Like real GCP, that status is per-version: it
// keeps naming the key that actually wrapped the payload even after the
// policy's CMEK key is updated, which only affects versions added afterwards.
//
// The emulator stores one wrapped DEK per version, wrapped with the policy's
// first CMEK key (Replication.WrapKey), so a policy whose replicas name
// different keys cannot report a per-replica status for the others. That
// residual is recorded in the DC10 wave plan rather than fabricated here: a
// CryptoKeyVersion name under a key that never wrapped the payload would be a
// lie a client could act on.
func VersionReplicaStatuses(kmsKeyName, kmsKeyVersionName string, repl *secretmanagerstore.Replication) []ReplicaStatus {
	if repl == nil || repl.UserManaged == nil {
		return nil
	}
	wrapLocation, hasWrapLocation := cryptoKeyLocation(kmsKeyName)
	replicas := repl.UserManaged.Replicas
	out := make([]ReplicaStatus, 0, len(replicas))
	for _, rep := range replicas {
		status := ReplicaStatus{Location: rep.Location}
		if hasWrapLocation && rep.Location == wrapLocation {
			status.KmsKeyVersionName = kmsKeyVersionName
		}
		out = append(out, status)
	}
	return out
}

// KmsKeyVersionName renders the SecretVersion status's
// `kmsKeyVersionName` from the version's wrapping key and wrapped DEK. It
// returns "" when the version carries no customer-managed encryption, in which
// case the nested status message is omitted (real GCP only populates it when
// CMEK is used).
//
// Note the Secret Manager spelling: `.../cryptoKeys/{key}/versions/{version}`,
// not the Cloud KMS API's `.../cryptoKeyVersions/{version}`.
func KmsKeyVersionName(kmsKeyName string, wrappedDEK []byte) string {
	if kmsKeyName == "" || len(wrappedDEK) == 0 {
		return ""
	}
	version, _, err := kms.DecodeVersionedCiphertext(wrappedDEK)
	if err != nil || version == "" {
		return ""
	}
	return kmsKeyName + "/versions/" + version
}
