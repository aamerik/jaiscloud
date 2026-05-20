package secret

import (
	"context"
	"encoding/json"
	"log/slog"
)

// RotationInvoker is the narrow interface SecretProvider uses to invoke the rotation Lambda.
type RotationInvoker interface {
	InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error
}

// SetInvoker wires the Lambda invoker for rotation (second-pass wiring).
func (p *SecretProvider) SetInvoker(inv RotationInvoker) { p.invoker = inv }

// runRotationLambda fires the four-step rotation sequence described in the AWS docs:
// createSecret → setSecret → testSecret → finishSecret.
// Called in a goroutine from RotateSecret when RotationLambdaARN is set and
// RotateImmediately is true.
func (p *SecretProvider) runRotationLambda(ctx context.Context, secretID, secretARN, pendingVersionID, lambdaARN string) {
	steps := []string{"createSecret", "setSecret", "testSecret", "finishSecret"}
	for _, step := range steps {
		payload, _ := json.Marshal(map[string]any{
			"Step":               step,
			"SecretId":           secretARN,
			"ClientRequestToken": pendingVersionID,
		})
		if err := p.invoker.InternalInvokeRaw(ctx, lambdaARN, payload); err != nil {
			slog.Warn("secretsmanager: rotation step failed", "step", step, "secret", secretID, "err", err)
			return
		}
	}
}
