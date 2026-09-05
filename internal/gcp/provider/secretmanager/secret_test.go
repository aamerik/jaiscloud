package secretmanager

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func TestSecretRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	// Create.
	nr := newNR(map[string]any{"secretId": "my-secret", "body": map[string]any{"replication": map[string]any{"automatic": map[string]any{}}}})
	createResp, err := p.Create(ctx, nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	createEtag, _ := createResp.Data["etag"].(string)
	if createEtag == "" {
		t.Error("expected etag on secret create response")
	}

	// Get returns a matching (stable) etag.
	nr = newNR(map[string]any{"name": "secrets/my-secret"})
	getResp, err := p.Get(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	getEtag, _ := getResp.Data["etag"].(string)
	if getEtag == "" || getEtag != createEtag {
		t.Errorf("expected stable etag on get (create=%q get=%q)", createEtag, getEtag)
	}

	// Add version.
	nr = newNR(map[string]any{"name": "secrets/my-secret", "body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}}})
	resp, err := p.AddVersion(ctx, nr)
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	if resp.Data["name"] != "projects/proj/secrets/my-secret/versions/1" {
		t.Errorf("unexpected version name: %v", resp.Data["name"])
	}

	// Access version 1.
	nr = newNR(map[string]any{"name": "secrets/my-secret/versions/1"})
	resp, err = p.Access(ctx, nr)
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	payload, _ := resp.Data["payload"].(map[string]any)
	if payload["data"] != "aGVsbG8=" {
		t.Errorf("unexpected payload data: %v", payload["data"])
	}

	// List has one secret.
	nr = newNR(nil)
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	secrets, _ := resp.Data["secrets"].([]any)
	if len(secrets) != 1 {
		t.Errorf("expected 1 secret, got %d", len(secrets))
	}
}
