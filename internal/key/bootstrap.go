package key

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// LoadOrCreateDEK loads the server DEK from the store, or generates and
// persists a new one on first boot.
//
// If kekHex is non-empty the DEK is wrapped with the KEK before storage.
// If kekHex is empty the DEK is stored plaintext (VERSION 0x00) — dev only.
func LoadOrCreateDEK(ctx context.Context, store KeyStore, kekHex string) ([]byte, error) {
	blob, err := store.LoadDEK(ctx)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, fmt.Errorf("kms bootstrap: load dek: %w", err)
	}

	if err == nil {
		// DEK already exists — unwrap and return.
		if kekHex == "" {
			// Stored plaintext (version 0x00).
			if len(blob) > 0 && blob[0] == versionPlaintext {
				return blob[1:], nil
			}
			// Stored plaintext without version byte (legacy).
			return blob, nil
		}
		kek, parseErr := ParseHexKey(kekHex)
		if parseErr != nil {
			return nil, parseErr
		}
		dek, unwrapErr := UnwrapDEK(kek, blob)
		if unwrapErr != nil {
			return nil, fmt.Errorf("kms bootstrap: unwrap dek: %w", unwrapErr)
		}
		return dek, nil
	}

	// First boot — generate a fresh DEK.
	dek, err := Generate32()
	if err != nil {
		return nil, fmt.Errorf("kms bootstrap: generate dek: %w", err)
	}

	var storeBlob []byte
	if kekHex == "" {
		slog.Warn("kms: no master key configured — DEK stored plaintext (dev mode only)")
		storeBlob = plaintextBlob(dek)
	} else {
		kek, parseErr := ParseHexKey(kekHex)
		if parseErr != nil {
			return nil, parseErr
		}
		storeBlob, err = WrapDEK(kek, dek)
		if err != nil {
			return nil, fmt.Errorf("kms bootstrap: wrap dek: %w", err)
		}
	}

	if err := store.StoreDEK(ctx, storeBlob); err != nil {
		return nil, fmt.Errorf("kms bootstrap: store dek: %w", err)
	}
	slog.Info("kms: new DEK generated and persisted")
	return dek, nil
}
