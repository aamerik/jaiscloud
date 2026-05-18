package secret

import (
	"context"
	"errors"
	"fmt"
)

// InternalGetSecretValue retrieves the plaintext secret value by name or ARN.
// It returns the SecretString (or base64 for binary) of the AWSCURRENT version.
// Used by SSM Parameter Store for the /aws/reference/secretsmanager/ bridge.
func (p *SecretProvider) InternalGetSecretValue(ctx context.Context, accountID, secretID string) (string, error) {
	// Try by ARN/ID first, then by name (scoped to accountID).
	e, err := p.store.GetSecret(ctx, secretID)
	if errors.Is(err, ErrSecretNotFound) {
		e, err = p.store.GetSecretByName(ctx, accountID, secretID)
	}
	if errors.Is(err, ErrSecretNotFound) {
		return "", fmt.Errorf("secret not found: %s", secretID)
	}
	if err != nil {
		return "", fmt.Errorf("sm: resolve secret: %w", err)
	}

	v, err := p.store.GetVersionByStage(ctx, e.SecretID, "AWSCURRENT")
	if err != nil {
		return "", fmt.Errorf("sm: get current version: %w", err)
	}

	pt, err := p.decrypt(ctx, e.KMSKeyID, v.SecretBinary, e.Name)
	if err != nil {
		return "", fmt.Errorf("sm: decrypt: %w", err)
	}
	return string(pt), nil
}
