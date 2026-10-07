//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/iterator"
)

// kmsScenario compares the Cloud KMS key-admin surface over REST and gRPC. KMS
// key rings and crypto keys cannot be deleted by the real API (a key is only
// scheduled for destruction), so the flow is create → get → list → get key; the
// ephemeral emulator resets the state between runs.
func kmsScenario() Scenario {
	ringID := "parity-ring"
	keyID := "parity-key"
	location := "global"
	ringName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", e.Cfg.Project, location, e.Resource(ringID))
	}
	keyName := func(e *Env) string { return ringName(e) + "/cryptoKeys/" + e.Resource(keyID) }
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, location) }

	newClient := func(ctx context.Context, e *Env) (*kms.KeyManagementClient, error) {
		return kms.NewKeyManagementClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "kms", Steps: []Step{
		{
			Op: "CreateKeyRing",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent(e), KeyRingId: e.Resource(ringID)})
				return err
			},
		},
		{
			Op: "GetKeyRing",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: ringName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+ringName(e), "")
			},
		},
		{
			Op:    "ListKeyRings",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: parent(e)})
				out := &kmspb.ListKeyRingsResponse{}
				for {
					kr, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.KeyRings = append(out.KeyRings, kr)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/keyRings", "")
			},
		},
		{
			Op: "CreateCryptoKey",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
					Parent:      ringName(e),
					CryptoKeyId: e.Resource(keyID),
					CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
				})
				return err
			},
		},
		{
			Op: "GetCryptoKey",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: keyName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+keyName(e), "")
			},
		},
	}}
}
