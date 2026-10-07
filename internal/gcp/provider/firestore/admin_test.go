package firestore

import (
	"context"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/store"
)

func newAdminService() *Service { return newService(nil, store.NewMemoryResourceStore()) }

func TestDatabaseCRUD(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, op, err := p.CreateDatabaseDef(ctx, "proj", "test-db", databaseDef{LocationID: "nam5"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if want := "projects/proj/databases/test-db"; created.Name != want {
		t.Errorf("name = %q, want %q", created.Name, want)
	}
	if created.Type != "FIRESTORE_NATIVE" || created.ConcurrencyMode != "OPTIMISTIC" {
		t.Errorf("defaults = type %q concurrency %q", created.Type, created.ConcurrencyMode)
	}
	if !strings.Contains(op, "/operations/") {
		t.Errorf("operation name = %q, want /operations/", op)
	}
	if created.Etag == "" {
		t.Error("etag empty")
	}

	got, err := p.GetDatabase(ctx, "proj", "test-db")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.Uid != created.Uid || got.LocationID != "nam5" {
		t.Errorf("GetDatabase round-trip = %+v", got)
	}

	// The (default) database is synthesized when no record exists.
	def, err := p.GetDatabase(ctx, "proj", "(default)")
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if def.Name != "projects/proj/databases/(default)" {
		t.Errorf("default name = %q", def.Name)
	}

	list, err := p.ListDatabases(ctx, "proj", false)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("ListDatabases len = %d, want 2", len(list))
	}

	// Unknown database is NotFound.
	if _, err := p.GetDatabase(ctx, "proj", "nope"); !isNotFound(err) {
		t.Errorf("GetDatabase(missing) err = %v, want NotFound", err)
	}

	// Create of (default) is AlreadyExists.
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "(default)", databaseDef{}); err == nil {
		t.Error("CreateDatabase((default)) succeeded, want AlreadyExists")
	}
}

func TestDatabaseUpdateEtagAndDelete(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "db01", databaseDef{})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}

	updated, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "PESSIMISTIC", DeleteProtectionState: "DELETE_PROTECTION_ENABLED"},
		[]string{"concurrency_mode", "delete_protection_state"}, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef: %v", err)
	}
	if updated.ConcurrencyMode != "PESSIMISTIC" || updated.DeleteProtectionState != "DELETE_PROTECTION_ENABLED" {
		t.Errorf("update = %+v", updated)
	}
	if updated.Etag == created.Etag {
		t.Error("etag not rotated on update")
	}

	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "OPTIMISTIC"}, []string{"concurrency_mode"}, "stale"); err == nil {
		t.Error("stale etag accepted, want Aborted")
	}

	// Empty mask is rejected.
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01", databaseDef{}, nil, ""); err == nil {
		t.Error("empty update_mask accepted")
	}

	if _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", updated.Etag); err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}
	if _, err := p.GetDatabase(ctx, "proj", "db01"); !isNotFound(err) {
		t.Errorf("GetDatabase after delete err = %v, want NotFound", err)
	}
	// A delete tombstone is only shown when requested.
	list, _ := p.ListDatabases(ctx, "proj", false)
	for _, d := range list {
		if strings.HasSuffix(d.Name, "/db01") {
			t.Error("deleted database returned without showDeleted")
		}
	}
	all, _ := p.ListDatabases(ctx, "proj", true)
	found := false
	for _, d := range all {
		if strings.HasSuffix(d.Name, "/db01") {
			found = true
		}
	}
	if !found {
		t.Error("deleted database missing with showDeleted")
	}
}

func TestFieldUpdateGetList(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	_, op, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "population",
		fieldDef{IndexConfig: &fieldIndexConfig{UsesAncestorConfig: true, AncestorField: "cities"}},
		[]string{"index_config"})
	if err != nil {
		t.Fatalf("UpdateFieldDef: %v", err)
	}
	if !strings.Contains(op, "/operations/") {
		t.Errorf("operation name = %q", op)
	}

	got, err := p.GetField(ctx, "proj", "(default)", "cities", "population")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if got.IndexConfig == nil || !got.IndexConfig.UsesAncestorConfig || got.IndexConfig.AncestorField != "cities" {
		t.Errorf("field index config = %+v", got.IndexConfig)
	}

	// A second field so pagination has something to page over.
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "region",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: time.Hour}}, []string{"ttl_config"}); err != nil {
		t.Fatalf("UpdateFieldDef(region): %v", err)
	}

	page1, next, err := p.ListFields(ctx, "proj", "(default)", "-", "", NewPageParams(1, ""))
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if len(page1) != 1 || next == "" {
		t.Fatalf("ListFields page1 len=%d next=%q", len(page1), next)
	}
	page2, next2, err := p.ListFields(ctx, "proj", "(default)", "-", "", NewPageParams(1, next))
	if err != nil {
		t.Fatalf("ListFields page2: %v", err)
	}
	if len(page2) != 1 || next2 != "" {
		t.Errorf("ListFields page2 len=%d next=%q", len(page2), next2)
	}

	// Empty mask is rejected.
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "x", fieldDef{}, nil); err == nil {
		t.Error("empty update_mask accepted")
	}

	// Documented filter expressions select by config, not by name substring.
	byAncestor, _, err := p.ListFields(ctx, "proj", "(default)", "-", "indexConfig.usesAncestorConfig:true", NewPageParams(0, ""))
	if err != nil || len(byAncestor) != 1 || !strings.HasSuffix(byAncestor[0].Name, "/population") {
		t.Errorf("ListFields indexConfig.usesAncestorConfig:true = %+v err=%v", byAncestor, err)
	}
	byTTL, _, err := p.ListFields(ctx, "proj", "(default)", "-", "ttlConfig:*", NewPageParams(0, ""))
	if err != nil || len(byTTL) != 1 || !strings.HasSuffix(byTTL[0].Name, "/region") {
		t.Errorf("ListFields ttlConfig:* = %+v err=%v", byTTL, err)
	}
}

func TestCreateDatabaseValidatesID(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	for _, bad := range []string{"", "ABC", "1bad", "no"} {
		if _, _, err := p.CreateDatabaseDef(ctx, "proj", bad, databaseDef{}); err == nil {
			t.Errorf("CreateDatabaseDef(%q) succeeded, want InvalidArgument", bad)
		}
	}
}

func TestUserCredsLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, err := p.CreateUserCredsDef(ctx, "proj", "(default)", "cred1", userCredsDef{})
	if err != nil {
		t.Fatalf("CreateUserCredsDef: %v", err)
	}
	if created.SecretPassword == "" {
		t.Error("create did not return a secret password")
	}
	if created.State != "ENABLED" {
		t.Errorf("state = %q, want ENABLED", created.State)
	}
	if want := "cred1@proj.iam.gserviceaccount.com"; created.Principal != want {
		t.Errorf("principal = %q, want %q", created.Principal, want)
	}

	got, err := p.GetUserCreds(ctx, "proj", "(default)", "cred1")
	if err != nil {
		t.Fatalf("GetUserCreds: %v", err)
	}
	if got.SecretPassword != "" {
		t.Error("get exposed the secret password")
	}

	list, err := p.ListUserCreds(ctx, "proj", "(default)")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListUserCreds len=%d err=%v", len(list), err)
	}
	if list[0].SecretPassword != "" {
		t.Error("list exposed the secret password")
	}

	dis, err := p.SetUserCredsState(ctx, "proj", "(default)", "cred1", "DISABLED")
	if err != nil || dis.State != "DISABLED" {
		t.Fatalf("disable = %+v err=%v", dis, err)
	}

	reset, err := p.ResetUserPassword(ctx, "proj", "(default)", "cred1")
	if err != nil {
		t.Fatalf("ResetUserPassword: %v", err)
	}
	if reset.SecretPassword == "" || reset.SecretPassword == created.SecretPassword {
		t.Errorf("reset password = %q (old %q)", reset.SecretPassword, created.SecretPassword)
	}

	if err := p.DeleteUserCreds(ctx, "proj", "(default)", "cred1"); err != nil {
		t.Fatalf("DeleteUserCreds: %v", err)
	}
	if _, err := p.GetUserCreds(ctx, "proj", "(default)", "cred1"); !isNotFound(err) {
		t.Errorf("GetUserCreds after delete err = %v, want NotFound", err)
	}
}

func TestBackupScheduleLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"})
	if err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	if want := "projects/proj/databases/(default)/backupSchedules/1"; created.Name != want {
		t.Errorf("name = %q, want %q", created.Name, want)
	}

	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "1", backupScheduleDef{}); err == nil {
		t.Error("duplicate schedule accepted, want AlreadyExists")
	}

	got, err := p.GetBackupSchedule(ctx, "proj", "(default)", "1")
	if err != nil || got.Retention != 7*24*time.Hour || got.Recurrence != "DAILY" {
		t.Fatalf("GetBackupSchedule = %+v err=%v", got, err)
	}

	upd, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 14 * 24 * time.Hour}, []string{"retention"})
	if err != nil || upd.Retention != 14*24*time.Hour {
		t.Fatalf("UpdateBackupScheduleDef = %+v err=%v", upd, err)
	}

	list, err := p.ListBackupSchedules(ctx, "proj", "(default)")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListBackupSchedules len=%d err=%v", len(list), err)
	}

	if err := p.DeleteBackupSchedule(ctx, "proj", "(default)", "1"); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "(default)", "1"); !isNotFound(err) {
		t.Errorf("GetBackupSchedule after delete err = %v, want NotFound", err)
	}
}

func TestBackupReadOnly(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	list, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(list) != 0 {
		t.Fatalf("ListBackups len=%d err=%v", len(list), err)
	}
	if _, err := p.GetBackup(ctx, "proj", "us-central1", "b1"); !isNotFound(err) {
		t.Errorf("GetBackup(missing) err = %v, want NotFound", err)
	}
	if err := p.DeleteBackup(ctx, "proj", "us-central1", "b1"); !isNotFound(err) {
		t.Errorf("DeleteBackup(missing) err = %v, want NotFound", err)
	}
}
