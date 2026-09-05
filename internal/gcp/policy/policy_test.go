package policy

import (
	"context"
	"testing"

	"jaiscloud/internal/store"
)

func TestSetIamPolicyEtagOCC(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()

	body := map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{"role": "roles/pubsub.publisher", "members": []any{"user:a@example.com"}}},
		},
	}

	pol, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if pol.Etag == "" || pol.Etag == DefaultEtag {
		t.Fatalf("expected fresh etag, got %q", pol.Etag)
	}

	// A set with a stale etag must be rejected.
	body["policy"].(map[string]any)["etag"] = "stale"
	if _, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body); err == nil {
		t.Fatal("expected 409 on stale etag")
	}

	// A set with the correct etag must succeed.
	body["policy"].(map[string]any)["etag"] = pol.Etag
	if _, err := Set(ctx, s, "proj", "gcp_topic_policy", "t1", body); err != nil {
		t.Fatalf("set with current etag: %v", err)
	}
}

func TestTestPermissionsEchoesRequest(t *testing.T) {
	perms := []string{"pubsub.topics.publish", "pubsub.topics.get"}
	got := TestPermissions(perms)
	if len(got) != len(perms) {
		t.Fatalf("expected %d granted permissions, got %d", len(perms), len(got))
	}
	for i, p := range perms {
		if got[i] != p {
			t.Errorf("granted[%d] = %q, want %q", i, got[i], p)
		}
	}
}
