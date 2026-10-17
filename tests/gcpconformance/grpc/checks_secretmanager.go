package grpcconformance

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// secretManagerChecks covers the Secret Manager surface
// (google.cloud.secretmanager.v1.SecretManagerService) via the official client.
func secretManagerChecks() []Check {
	return []Check{
		{Service: "secretmanager", RPC: "CreateSecret", KeyField: "success", Run: checkSecretCreate},
		{Service: "secretmanager", RPC: "GetSecret", KeyField: "name", Run: checkSecretGet},
		{Service: "secretmanager", RPC: "GetSecret", KeyField: "replication.automatic.customerManagedEncryption.kmsKeyName", Run: checkSecretCMEKReplication},
		{Service: "secretmanager", RPC: "ListSecrets", KeyField: "secrets[].name", Run: checkSecretList},
		{Service: "secretmanager", RPC: "AddSecretVersion", KeyField: "name", Run: checkSecretAddVersion},
		{Service: "secretmanager", RPC: "AccessSecretVersion", KeyField: "payload.data", Run: checkSecretAccessVersion},
		{Service: "secretmanager", RPC: "DeleteSecret", KeyField: "success", Run: checkSecretDelete},
	}
}

const secretPayload = "grpc-conformance-secret-payload"

func newSecretClient(ctx context.Context, cfg Config) (*secretmanager.Client, error) {
	return secretmanager.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func secretProject(cfg Config) string { return "projects/" + cfg.Project }

func secretName(cfg Config) string {
	return secretProject(cfg) + "/secrets/" + cfg.ResourceName("gcpc-grpc-secret")
}

func checkSecretCreate(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	secret, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: cfg.ResourceName("gcpc-grpc-secret"),
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	if secret.GetName() != secretName(cfg) {
		return fmt.Errorf("CreateSecret returned name %q, want %q", secret.GetName(), secretName(cfg))
	}
	return nil
}

func checkSecretGet(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	secret, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: secretName(cfg)})
	if err != nil {
		return err
	}
	if secret.GetName() != secretName(cfg) {
		return fmt.Errorf("GetSecret returned name %q, want %q", secret.GetName(), secretName(cfg))
	}
	return nil
}

// checkSecretCMEKReplication asserts that a Secret created with a
// customer-managed encryption key echoes the key back through the official
// client: real GCP returns the configured key under
// replication.automatic.customerManagedEncryption.kmsKeyName (DC31), not an
// empty automatic policy.
func checkSecretCMEKReplication(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-secret-cmek")
	name := secretProject(cfg) + "/secrets/" + id
	// The key only has to be a well-formed CryptoKey name: CreateSecret stores
	// the policy and never wraps until a version is added, which this probe
	// deliberately does not do.
	key := secretProject(cfg) + "/locations/global/keyRings/gcpc-cmek/cryptoKeys/gcpc-cmek-key"

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: id,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{
						CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = client.DeleteSecret(context.Background(), &secretmanagerpb.DeleteSecretRequest{Name: name})
	}()

	if got := created.GetReplication().GetAutomatic().GetCustomerManagedEncryption().GetKmsKeyName(); got != key {
		return fmt.Errorf("CreateSecret CMEK = %q, want %q", got, key)
	}
	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	if err != nil {
		return err
	}
	if gotKey := got.GetReplication().GetAutomatic().GetCustomerManagedEncryption().GetKmsKeyName(); gotKey != key {
		return fmt.Errorf("GetSecret CMEK = %q, want %q", gotKey, key)
	}
	return nil
}

func checkSecretList(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	it := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: secretProject(cfg)})
	for {
		secret, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if secret.GetName() == secretName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListSecrets did not include %q", secretName(cfg))
}

func checkSecretAddVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	version, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secretName(cfg),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte(secretPayload)},
	})
	if err != nil {
		return err
	}
	if version.GetName() == "" {
		return fmt.Errorf("AddSecretVersion returned an empty name")
	}
	return nil
}

func checkSecretAccessVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	resp, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretName(cfg) + "/versions/1",
	})
	if err != nil {
		return err
	}
	if got := string(resp.GetPayload().GetData()); got != secretPayload {
		return fmt.Errorf("AccessSecretVersion payload = %q, want %q", got, secretPayload)
	}
	return nil
}

func checkSecretDelete(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	if err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secretName(cfg)}); err != nil {
		return err
	}
	return nil
}
