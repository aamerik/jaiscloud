package logging

import (
	"context"
	"testing"

	loggingstore "jaiscloud/internal/gcp/store/logging"
	store "jaiscloud/internal/store"
)

func newAdminService() *Service {
	return NewService(loggingstore.NewMemoryStore(), "test", WithResources(store.NewMemoryResourceStore()))
}

const (
	testBucketParent = "projects/p/locations/global"
	testBucketName   = "projects/p/locations/global/buckets/b1"
	testViewName     = testBucketName + "/views/v1"
	testLinkName     = testBucketName + "/links/l1"
	testScopeName    = testBucketParent + "/logScopes/s1"
)

func seedBucket(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.CreateBucket(context.Background(), testBucketParent, "b1", loggingstore.LogBucket{Description: "b"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func TestBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	b, err := s.GetBucket(ctx, testBucketName)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if b.Name != "b1" || b.RetentionDays != 30 || b.LifecycleState != "ACTIVE" {
		t.Fatalf("bucket defaults = %+v", b)
	}

	if _, err := s.CreateBucket(ctx, testBucketParent, "b1", loggingstore.LogBucket{}); err == nil {
		t.Fatalf("duplicate CreateBucket succeeded")
	}

	upd, err := s.UpdateBucket(ctx, testBucketName, loggingstore.LogBucket{Description: "new", RetentionDays: 7}, []string{"description", "retentionDays"})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if upd.Description != "new" || upd.RetentionDays != 7 {
		t.Fatalf("update = %+v", upd)
	}

	page, _, err := s.ListBuckets(ctx, testBucketParent, 0, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("ListBuckets = %v, %v", page, err)
	}

	if err := s.DeleteBucket(ctx, testBucketName); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	b, _ = s.GetBucket(ctx, testBucketName)
	if b.LifecycleState != "DELETE_REQUESTED" {
		t.Fatalf("after delete state = %q", b.LifecycleState)
	}
	if err := s.UndeleteBucket(ctx, testBucketName); err != nil {
		t.Fatalf("UndeleteBucket: %v", err)
	}
	b, _ = s.GetBucket(ctx, testBucketName)
	if b.LifecycleState != "ACTIVE" {
		t.Fatalf("after undelete state = %q", b.LifecycleState)
	}
}

func TestViewLifecycleAndIAM(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	v, err := s.CreateView(ctx, testBucketName, "v1", loggingstore.LogView{Filter: "severity>=ERROR"})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if v.Filter != "severity>=ERROR" {
		t.Fatalf("view = %+v", v)
	}
	if _, err := s.CreateView(ctx, testBucketName, "v1", loggingstore.LogView{}); err == nil {
		t.Fatalf("duplicate CreateView succeeded")
	}

	upd, err := s.UpdateView(ctx, testViewName, loggingstore.LogView{Description: "d"}, []string{"description"})
	if err != nil {
		t.Fatalf("UpdateView: %v", err)
	}
	if upd.Description != "d" || upd.Filter != "severity>=ERROR" {
		t.Fatalf("view update = %+v", upd)
	}

	page, _, err := s.ListViews(ctx, testBucketName, 0, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("ListViews = %v, %v", page, err)
	}

	pol, err := s.ViewGetIamPolicy(ctx, testViewName)
	if err != nil {
		t.Fatalf("ViewGetIamPolicy: %v", err)
	}
	pol, err = s.ViewSetIamPolicy(ctx, testViewName, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:a@example.com"}}}},
	})
	if err != nil {
		t.Fatalf("ViewSetIamPolicy: %v", err)
	}
	if len(pol.Bindings) != 1 {
		t.Fatalf("policy bindings = %v", pol.Bindings)
	}
	perms, err := s.ViewTestIamPermissions(ctx, testViewName, []string{"logging.views.get"})
	if err != nil || len(perms) != 1 {
		t.Fatalf("ViewTestIamPermissions = %v, %v", perms, err)
	}

	if err := s.DeleteView(ctx, testViewName); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if _, err := s.GetView(ctx, testViewName); err == nil {
		t.Fatalf("GetView after delete succeeded")
	}
}

func TestLinkAndLogScope(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	l, err := s.CreateLink(ctx, testBucketName, "l1", loggingstore.LogLink{BigQueryDatasetID: "ds"})
	if err != nil {
		t.Fatalf("CreateLink: %v", err)
	}
	if l.BigQueryDatasetID != "ds" || l.LifecycleState != "ACTIVE" {
		t.Fatalf("link = %+v", l)
	}
	if _, err := s.CreateLink(ctx, testBucketName, "l1", loggingstore.LogLink{BigQueryDatasetID: "ds"}); err == nil {
		t.Fatalf("duplicate CreateLink succeeded")
	}
	if _, err := s.CreateLink(ctx, testBucketName, "l2", loggingstore.LogLink{}); err == nil {
		t.Fatalf("CreateLink without dataset succeeded")
	}
	if got, err := s.GetLink(ctx, testLinkName); err != nil || got.Name != "l1" {
		t.Fatalf("GetLink = %+v, %v", got, err)
	}
	if page, _, err := s.ListLinks(ctx, testBucketName, 0, ""); err != nil || len(page) != 1 {
		t.Fatalf("ListLinks = %v, %v", page, err)
	}
	if err := s.DeleteLink(ctx, testLinkName); err != nil {
		t.Fatalf("DeleteLink: %v", err)
	}

	ls, err := s.CreateLogScope(ctx, testBucketParent, "s1", loggingstore.LogScope{ResourceNames: []string{"projects/p"}})
	if err != nil {
		t.Fatalf("CreateLogScope: %v", err)
	}
	if len(ls.ResourceNames) != 1 {
		t.Fatalf("log scope = %+v", ls)
	}
	if _, err := s.CreateLogScope(ctx, testBucketParent, "s2", loggingstore.LogScope{}); err == nil {
		t.Fatalf("CreateLogScope without resource_names succeeded")
	}
	if _, err := s.UpdateLogScope(ctx, testScopeName, loggingstore.LogScope{Description: "d"}, []string{"description"}); err != nil {
		t.Fatalf("UpdateLogScope: %v", err)
	}
	if page, _, err := s.ListLogScopes(ctx, testBucketParent, 0, ""); err != nil || len(page) != 1 {
		t.Fatalf("ListLogScopes = %v, %v", page, err)
	}
	if err := s.DeleteLogScope(ctx, testScopeName); err != nil {
		t.Fatalf("DeleteLogScope: %v", err)
	}
}

func TestSettingsAndCmek(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	const name = "projects/p"

	st, err := s.GetSettings(ctx, name)
	if err != nil {
		t.Fatalf("GetSettings: %v", err)
	}
	if st.KmsServiceAccountID == "" {
		t.Fatalf("default settings has no service account id")
	}
	upd, err := s.UpdateSettings(ctx, name, loggingstore.LogSettings{StorageLocation: "us"}, []string{"storageLocation"})
	if err != nil {
		t.Fatalf("UpdateSettings: %v", err)
	}
	if upd.StorageLocation != "us" || upd.KmsServiceAccountID == "" {
		t.Fatalf("settings = %+v", upd)
	}

	cm, err := s.GetCmekSettings(ctx, name)
	if err != nil {
		t.Fatalf("GetCmekSettings: %v", err)
	}
	if cm.ServiceAccountID == "" {
		t.Fatalf("default cmek has no service account id")
	}
	if _, err := s.UpdateCmekSettings(ctx, name, loggingstore.LogCmekSettings{}, []string{"kmsKeyName"}); err == nil {
		t.Fatalf("UpdateCmekSettings without kms_key_name succeeded")
	}
	cm, err = s.UpdateCmekSettings(ctx, name, loggingstore.LogCmekSettings{KmsKeyName: "projects/p/locations/global/keyRings/r/cryptoKeys/k"}, []string{"kmsKeyName"})
	if err != nil {
		t.Fatalf("UpdateCmekSettings: %v", err)
	}
	if cm.KmsKeyName == "" {
		t.Fatalf("cmek = %+v", cm)
	}
}

func TestAdminNameValidation(t *testing.T) {
	if _, _, err := ParseBucketName("projects/p/buckets/b"); err == nil {
		t.Fatalf("ParseBucketName accepted a missing location")
	}
	if _, err := ParseLocationParent("projects/p"); err == nil {
		t.Fatalf("ParseLocationParent accepted a bare container")
	}
	if _, _, err := ParseViewName(testBucketName); err == nil {
		t.Fatalf("ParseViewName accepted a bucket name")
	}
	if _, err := ParseSettingsName("projects/p/locations/global/settings"); err == nil {
		t.Fatalf("ParseSettingsName accepted a settings-suffixed name")
	}
}
