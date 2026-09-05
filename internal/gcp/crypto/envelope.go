package crypto

import (
	"context"
	"strings"

	"jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

type EnvelopeEncryptor interface {
	// Wrap generates a 32-byte DEK. If kmsKeyName is empty, it returns the Server DEK
	// and nil wrappedDEK. If kmsKeyName is provided, it returns a random DEK and
	// the DEK encrypted by the KMS key (wrappedDEK).
	Wrap(ctx context.Context, accountID, kmsKeyName string) (rawDEK, wrappedDEK []byte, err error)

	// Unwrap decrypts wrappedDEK using kmsKeyName. If kmsKeyName is empty, it returns the Server DEK.
	Unwrap(ctx context.Context, accountID, kmsKeyName string, wrappedDEK []byte) (rawDEK []byte, err error)
}

type encryptor struct {
	kmsStore kms.Store
}

func NewEnvelopeEncryptor(kmsStore kms.Store) EnvelopeEncryptor {
	return &encryptor{kmsStore: kmsStore}
}

// parseCryptoKeyName splits "projects/{proj}/locations/{loc}/keyRings/{kr}/cryptoKeys/{key}"
// or returns empty strings if the format is invalid.
func parseCryptoKeyName(name string) (project, loc, kr, key string) {
	parts := strings.Split(name, "/")
	if len(parts) >= 8 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "cryptoKeys" {
		return parts[1], parts[3], parts[5], parts[7]
	}
	return "", "", "", ""
}

func (e *encryptor) Wrap(ctx context.Context, accountID, kmsKeyName string) ([]byte, []byte, error) {
	if kmsKeyName == "" {
		dek, err := e.kmsStore.ServerDEK(ctx)
		return dek, nil, err
	}

	project, loc, kr, key := parseCryptoKeyName(kmsKeyName)
	if project == "" {
		return nil, nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	cryptoKey, err := e.kmsStore.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		if err == kms.ErrNoSuchCryptoKey {
			return nil, nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
		}
		return nil, nil, err
	}

	version, err := e.kmsStore.GetVersion(ctx, project, loc, kr, key, cryptoKey.PrimaryVersion)
	if err != nil {
		return nil, nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	if version.State != "ENABLED" {
		return nil, nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	rawKMSDEK, err := e.kmsStore.KeyMaterial(ctx, project, loc, kr, key, cryptoKey.PrimaryVersion)
	if err != nil {
		return nil, nil, err
	}

	rawDEK, err := kms.Generate32()
	if err != nil {
		return nil, nil, err
	}

	wrappedDEK, err := kms.EncryptData(rawKMSDEK, rawDEK, nil)
	if err != nil {
		return nil, nil, err
	}

	return rawDEK, wrappedDEK, nil
}

func (e *encryptor) Unwrap(ctx context.Context, accountID, kmsKeyName string, wrappedDEK []byte) ([]byte, error) {
	if kmsKeyName == "" {
		return e.kmsStore.ServerDEK(ctx)
	}

	project, loc, kr, key := parseCryptoKeyName(kmsKeyName)
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	cryptoKey, err := e.kmsStore.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		if err == kms.ErrNoSuchCryptoKey {
			return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
		}
		return nil, err
	}

	version, err := e.kmsStore.GetVersion(ctx, project, loc, kr, key, cryptoKey.PrimaryVersion)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	if version.State != "ENABLED" {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	rawKMSDEK, err := e.kmsStore.KeyMaterial(ctx, project, loc, kr, key, cryptoKey.PrimaryVersion)
	if err != nil {
		return nil, err
	}

	rawDEK, err := kms.DecryptData(rawKMSDEK, wrappedDEK, nil)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	return rawDEK, nil
}
