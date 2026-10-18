package firestore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newAdminService() *Service { return newService(nil, store.NewMemoryResourceStore()) }

// assertDeletePrecondition asserts the failure projects.databases.delete
// answers for a blocked delete (FAILED_PRECONDITION on HTTP 400) — whether the
// block is a stale etag or enabled delete protection. It is deliberately not
// the AIP-154 ABORTED an update answers.
func assertDeletePrecondition(t *testing.T, err error, what string) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.Code != "FailedPrecondition" || pe.HTTPStatus != 400 {
		t.Fatalf("%s delete = %s/%d (%v), want FailedPrecondition/400", what, pe.Code, pe.HTTPStatus, err)
	}
	// The wire status both transports resolve (REST envelope, gRPC code).
	if name, httpStatus := gcperr.Resolve(pe); name != gcperr.FailedPrecondition || httpStatus != 400 {
		t.Fatalf("%s delete resolves to %s/%d, want FAILED_PRECONDITION/400", what, name, httpStatus)
	}
}

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
	// Delete protection is documented as disabled by default.
	if created.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("default delete protection = %q, want %q", created.DeleteProtectionState, deleteProtectionDisabled)
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
		databaseDef{ConcurrencyMode: "PESSIMISTIC", DeleteProtectionState: deleteProtectionEnabled},
		[]string{"concurrency_mode", "delete_protection_state"}, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef: %v", err)
	}
	if updated.ConcurrencyMode != "PESSIMISTIC" || updated.DeleteProtectionState != deleteProtectionEnabled {
		t.Errorf("update = %+v", updated)
	}
	if updated.Etag == created.Etag {
		t.Error("etag not rotated on update")
	}

	// created.Etag was superseded by the update above, so it is genuinely
	// stale (not just an arbitrary non-matching token).
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "OPTIMISTIC"}, []string{"concurrency_mode"}, created.Etag); err == nil {
		t.Error("stale etag accepted, want Aborted")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "Aborted" || pe.HTTPStatus != 409 {
			t.Errorf("stale etag on update = %v, want Aborted/409 (AIP-154)", err)
		}
	}

	// A stale etag blocks the delete with the proto's documented
	// FAILED_PRECONDITION, and the database is left intact.
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", created.Etag); err == nil {
		t.Error("stale etag accepted by DeleteDatabaseDef, want FailedPrecondition")
	} else {
		assertDeletePrecondition(t, err, "stale-etag")
	}
	if got, err := p.GetDatabase(ctx, "proj", "db01"); err != nil || got.Etag != updated.Etag {
		t.Errorf("stale-etag delete changed the database: etag %q (err %v), want %q", got.Etag, err, updated.Etag)
	}

	// An omitted update_mask is an implied mask of the populated fields
	// (AIP-134): the patch applies and fields absent from it are untouched.
	implied, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "OPTIMISTIC"}, nil, updated.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(omitted mask): %v", err)
	}
	if implied.ConcurrencyMode != "OPTIMISTIC" {
		t.Errorf("omitted mask did not apply concurrency_mode: %+v", implied)
	}
	if implied.DeleteProtectionState != deleteProtectionEnabled {
		t.Errorf("omitted mask cleared an unpopulated field: %+v", implied)
	}
	if implied.Etag == updated.Etag {
		t.Error("etag not rotated on maskless update")
	}

	// Delete protection survived the maskless patch and now blocks the delete
	// (Firestore: "You cannot delete a database with delete protection enabled
	// until you disable delete protection"), leaving the record intact.
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", implied.Etag); err == nil {
		t.Error("delete succeeded with delete protection enabled, want FailedPrecondition")
	} else {
		assertDeletePrecondition(t, err, "delete-protected")
	}
	kept, err := p.GetDatabase(ctx, "proj", "db01")
	if err != nil || kept.Deleted {
		t.Errorf("blocked delete changed the database: %+v (err %v)", kept, err)
	} else if kept.Etag != implied.Etag {
		t.Errorf("blocked delete rotated the etag %q -> %q", implied.Etag, kept.Etag)
	}

	// Disabling delete protection then admits the delete. An explicit
	// DELETE_PROTECTION_STATE_UNSPECIFIED is documented as equivalent to
	// disabled, so it must normalize rather than persist verbatim.
	unprotected, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{DeleteProtectionState: deleteProtectionUnspecified}, []string{"delete_protection_state"}, implied.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(disable delete protection): %v", err)
	}
	if unprotected.DeleteProtectionState != deleteProtectionDisabled {
		t.Fatalf("delete protection = %q, want %q", unprotected.DeleteProtectionState, deleteProtectionDisabled)
	}

	deleted, _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", unprotected.Etag)
	if err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}
	// The operation's documented response is the soft-deleted Database.
	if !deleted.Deleted || deleted.DeleteTime.IsZero() {
		t.Errorf("DeleteDatabaseDef returned %+v, want a soft-deleted record with deleteTime", deleted)
	}
	if !strings.HasSuffix(deleted.Name, "/databases/db01") {
		t.Errorf("deleted database name = %q", deleted.Name)
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

	// Documented filter expressions select by config, not by name substring.
	byAncestor, _, err := p.ListFields(ctx, "proj", "(default)", "-", "indexConfig.usesAncestorConfig:true", NewPageParams(0, ""))
	if err != nil || len(byAncestor) != 1 || !strings.HasSuffix(byAncestor[0].Name, "/population") {
		t.Errorf("ListFields indexConfig.usesAncestorConfig:true = %+v err=%v", byAncestor, err)
	}
	byTTL, _, err := p.ListFields(ctx, "proj", "(default)", "-", "ttlConfig:*", NewPageParams(0, ""))
	if err != nil || len(byTTL) != 1 || !strings.HasSuffix(byTTL[0].Name, "/region") {
		t.Errorf("ListFields ttlConfig:* = %+v err=%v", byTTL, err)
	}

	// An omitted update_mask is the implied mask of every populated field
	// (AIP-134, the same contract UpdateDatabaseDef honours since DC20): a
	// maskless patch applies what it carries instead of answering
	// INVALID_ARGUMENT, leaving unpopulated configs untouched.
	maskless, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "score",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: 2 * time.Hour}}, nil)
	if err != nil {
		t.Fatalf("maskless UpdateFieldDef: %v", err)
	}
	if maskless.TtlConfig == nil || maskless.TtlConfig.State != "ACTIVE" || maskless.TtlConfig.ExpirationOffset != 2*time.Hour {
		t.Errorf("maskless patch ttl_config = %+v", maskless.TtlConfig)
	}
	if maskless.IndexConfig != nil {
		t.Errorf("maskless patch invented index_config = %+v", maskless.IndexConfig)
	}

	// The widened `*` arm carries both configs in one maskless patch.
	// Assert on `expiration_offset` (an input field): `ttl_config.state` is
	// OUTPUT_ONLY and derived by real GCP, so a maskless patch must not be
	// pinned on a server-computed value.
	both, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "score",
		fieldDef{
			IndexConfig: &fieldIndexConfig{Indexes: []indexDef{{
				Name:       "projects/proj/databases/(default)/collectionGroups/cities/indexes/ix1",
				QueryScope: "COLLECTION",
				Fields:     []indexField{{FieldPath: "score", Order: "ASCENDING"}},
			}}},
			TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: 3 * time.Hour},
		}, nil)
	if err != nil {
		t.Fatalf("maskless UpdateFieldDef(both configs): %v", err)
	}
	if both.IndexConfig == nil || len(both.IndexConfig.Indexes) != 1 {
		t.Errorf("maskless patch index_config = %+v", both.IndexConfig)
	}
	if both.TtlConfig == nil || both.TtlConfig.ExpirationOffset != 3*time.Hour {
		t.Errorf("maskless patch ttl_config = %+v", both.TtlConfig)
	}
}

func TestDatabaseDeleteProtectionDefaults(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// Create defaults the field to the documented default.
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "defdb", databaseDef{})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if created.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("create default = %q, want %q", created.DeleteProtectionState, deleteProtectionDisabled)
	}

	// An explicit DELETE_PROTECTION_STATE_UNSPECIFIED is documented as
	// equivalent to disabled, not stored verbatim.
	unspec, _, err := p.CreateDatabaseDef(ctx, "proj", "unspecdb", databaseDef{DeleteProtectionState: deleteProtectionUnspecified})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(unspecified): %v", err)
	}
	if unspec.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("unspecified create = %q, want %q", unspec.DeleteProtectionState, deleteProtectionDisabled)
	}

	// A record persisted before the default existed (no field on disk) reads
	// back as the documented default rather than as an unknown state.
	legacy := databaseDef{Name: databaseName("proj", "legacydb"), Uid: "legacy", Etag: "legacy-etag"}
	if err := p.putResource(ctx, "proj", rtDatabase, databaseRel("legacydb"), legacy, true); err != nil {
		t.Fatalf("putResource(legacy): %v", err)
	}
	got, err := p.GetDatabase(ctx, "proj", "legacydb")
	if err != nil {
		t.Fatalf("GetDatabase(legacy): %v", err)
	}
	if got.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("legacy record read back = %q, want %q", got.DeleteProtectionState, deleteProtectionDisabled)
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

	// An omitted mask is the implied mask of every populated field: it must
	// carry the retention *and* the recurrence oneof, not only `retention`.
	maskless, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 2 * 24 * time.Hour, Recurrence: "WEEKLY", DayOfWeek: 1}, nil)
	if err != nil {
		t.Fatalf("maskless UpdateBackupScheduleDef: %v", err)
	}
	if maskless.Retention != 2*24*time.Hour || maskless.Recurrence != "WEEKLY" || maskless.DayOfWeek != 1 {
		t.Errorf("maskless update = %+v, want 2d weekly Monday", maskless)
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
