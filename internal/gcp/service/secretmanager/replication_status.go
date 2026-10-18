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
)

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
