package secretmanager

import (
	"context"
	"sync"
	"testing"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestSecretNegativesAndPagination(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore())

	for _, id := range []string{"a", "b", "c"} {
		nr := newNR(map[string]any{"secretId": id})
		if _, err := p.Create(ctx, nr); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	// Pagination.
	nr := newNR(map[string]any{"pageSize": "2"})
	resp, err := p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	secrets, _ := resp.Data["secrets"].([]any)
	if len(secrets) != 2 {
		t.Fatalf("page 1 expected 2 secrets, got %d", len(secrets))
	}
	if resp.Data["totalSize"] != 3 {
		t.Errorf("expected totalSize 3 (total across pages), got %v", resp.Data["totalSize"])
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}
	nr = newNR(map[string]any{"pageSize": "2", "pageToken": token})
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	secrets, _ = resp.Data["secrets"].([]any)
	if len(secrets) != 1 {
		t.Fatalf("page 2 expected 1 secret, got %d", len(secrets))
	}

	// 409 duplicate.
	nr = newNR(map[string]any{"secretId": "a"})
	if _, err := p.Create(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate secret, got %v", err)
	}

	// 404 get missing.
	nr = newNR(map[string]any{"name": "secrets/missing"})
	if _, err := p.Get(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing secret, got %v", err)
	}

	// Access response includes dataCrc32c.
	nr = newNR(map[string]any{"name": "secrets/a", "body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}}})
	if _, err := p.AddVersion(ctx, nr); err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	nr = newNR(map[string]any{"name": "secrets/a/versions/1"})
	resp, err = p.Access(ctx, nr)
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	payload, _ := resp.Data["payload"].(map[string]any)
	if _, ok := payload["dataCrc32c"]; !ok {
		t.Error("expected dataCrc32c in access payload")
	}

	// 400 on missing name.
	nr = newNR(nil)
	if _, err := p.Get(ctx, nr); err == nil || errStatus(err) != 400 {
		t.Fatalf("expected 400 on missing name, got %v", err)
	}
}

// TestSecretVersionLifecycle verifies disable/enable/destroy transitions apply
// and are reflected in the returned version state.
func TestSecretVersionLifecycle(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore())

	nr := newNR(map[string]any{"secretId": "s"})
	if _, err := p.Create(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}
	nr = newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})
	if _, err := p.AddVersion(ctx, nr); err != nil {
		t.Fatalf("addVersion: %v", err)
	}

	nr = newNR(map[string]any{"name": "secrets/s/versions/1"})
	for state, fn := range map[string]func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error){
		"DISABLED":  p.DisableVersion,
		"ENABLED":   p.EnableVersion,
		"DESTROYED": p.DestroyVersion,
	} {
		resp, err := fn(ctx, nr)
		if err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		if resp.Data["state"] != state {
			t.Errorf("expected state %s, got %v", state, resp.Data["state"])
		}
	}

	// 404 on a missing version.
	nr = newNR(map[string]any{"name": "secrets/s/versions/99"})
	if _, err := p.DisableVersion(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing version, got %v", err)
	}
}

// TestSecretAddVersionConcurrent verifies concurrent AddVersion calls allocate
// distinct version numbers (no lost or colliding versions).
func TestSecretAddVersionConcurrent(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nr := newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})
			if _, err := p.AddVersion(ctx, nr); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("addVersion: %v", err)
	}

	versions, err := p.secrets.ListVersions(ctx, "proj", "s")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != n {
		t.Fatalf("expected %d versions, got %d", n, len(versions))
	}
}

// TestSecretIamPolicy verifies secret getIamPolicy/setIamPolicy/testIamPermissions.
func TestSecretIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if resp.Data["version"] != 1 {
		t.Errorf("expected version 1, got %v", resp.Data["version"])
	}

	bindings := []any{map[string]any{"role": "roles/secretmanager.secretAccessor", "members": []any{"allUsers"}}}
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"bindings": bindings, "etag": "BOGUS="}})); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on stale etag, got %v", err)
	}
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"bindings": bindings}})); err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}

	tr, err := p.TestIamPermissions(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"permissions": []any{"secretmanager.secrets.access"}}}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	if perms, _ := tr.Data["permissions"].([]string); len(perms) != 1 {
		t.Errorf("expected 1 granted permission, got %v", tr.Data["permissions"])
	}

	if _, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/missing"})); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing secret, got %v", err)
	}
}

// TestSecretRotationAndAliases verifies GCP-native rotation + version aliases
// (no AWS version stages): they persist round-trip, and a due rotation creates
// a new version and advances nextRotationTime.
func TestSecretRotationAndAliases(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore())

	// Create with aliases (rotation unset).
	create := newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"versionAliases": map[string]any{"latest": float64(1)}},
	})
	resp, err := p.Create(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if aliases, _ := resp.Data["versionAliases"].(map[string]int); aliases["latest"] != 1 {
		t.Errorf("expected versionAliases latest=1, got %v", resp.Data["versionAliases"])
	}

	// Add a real version 1.
	if _, err := p.AddVersion(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}

	// Enable rotation, already due, and update the alias to the new version.
	upd := newNR(map[string]any{"name": "secrets/s", "body": map[string]any{
		"rotation":       map[string]any{"nextRotationTime": "2000-01-01T00:00:00Z", "rotationPeriod": "3600s"},
		"versionAliases": map[string]any{"latest": float64(2)},
	}})
	resp, err = p.Update(ctx, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.Data["rotation"] == nil {
		t.Error("expected rotation in update response")
	}

	// Get triggers lazy rotation (nextRotationTime is in the past).
	resp, err = p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	rot, _ := resp.Data["rotation"].(map[string]any)
	if rot["nextRotationTime"] == "2000-01-01T00:00:00Z" {
		t.Error("expected nextRotationTime to advance after rotation")
	}

	// A rotated empty version 2 now exists.
	if _, err := p.secrets.GetVersion(ctx, "proj", "s", "2"); err != nil {
		t.Fatalf("expected rotated version 2, got %v", err)
	}
}

// TestSecretRotationNotDue verifies a future rotation does not create a version.
func TestSecretRotationNotDue(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore())

	create := newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"rotation": map[string]any{
				"nextRotationTime": "2999-01-01T00:00:00Z",
				"rotationPeriod":   "3600s",
			},
		},
	})
	if _, err := p.Create(ctx, create); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := p.secrets.GetVersion(ctx, "proj", "s", "2"); err == nil {
		t.Fatal("expected no rotated version when not due")
	}
}
