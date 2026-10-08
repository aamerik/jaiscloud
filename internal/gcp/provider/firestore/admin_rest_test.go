package firestore

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newAdminProvider() *Provider { return New(nil, store.NewMemoryResourceStore()) }

func adminNR(name string, extra map[string]any) *model.NormalizedRequest {
	params := map[string]any{"name": name}
	for k, v := range extra {
		params[k] = v
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

func TestRESTDatabaseLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	// create -> terminal operation wrapping the Database.
	resp, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "test-db",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	op := resp.Data
	if op["done"] != true {
		t.Errorf("create op done = %v, want true", op["done"])
	}
	inner, _ := op["response"].(map[string]any)
	if inner["name"] != "projects/proj/databases/test-db" {
		t.Errorf("created db name = %v", inner["name"])
	}
	if inner["@type"] != "type.googleapis.com/google.firestore.admin.v1.Database" {
		t.Errorf("create op @type = %v", inner["@type"])
	}

	// get round-trips the mutable fields, with the protobuf-duration string form.
	got, err := p.GetDatabase(ctx, adminNR("databases/test-db", nil))
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.Data["locationId"] != "nam5" {
		t.Errorf("locationId = %v, want nam5", got.Data["locationId"])
	}
	if got.Data["versionRetentionPeriod"] != "3600s" {
		t.Errorf("versionRetentionPeriod = %v, want 3600s", got.Data["versionRetentionPeriod"])
	}

	// list includes the implicit (default) database.
	list, err := p.ListDatabases(ctx, adminNR("databases", nil))
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	dbs, _ := list.Data["databases"].([]any)
	if len(dbs) != 2 {
		t.Errorf("ListDatabases len = %d, want 2", len(dbs))
	}

	// patch applies updateMask=concurrencyMode.
	upd, err := p.UpdateDatabase(ctx, adminNR("databases/test-db", map[string]any{
		"updateMask": "concurrencyMode",
		"body":       map[string]any{"concurrencyMode": "PESSIMISTIC"},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase: %v", err)
	}
	uin, _ := upd.Data["response"].(map[string]any)
	if uin["concurrencyMode"] != "PESSIMISTIC" {
		t.Errorf("concurrencyMode = %v, want PESSIMISTIC", uin["concurrencyMode"])
	}

	// delete returns a terminal operation.
	del, err := p.DeleteDatabase(ctx, adminNR("databases/test-db", nil))
	if err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if del.Data["done"] != true {
		t.Errorf("delete op done = %v", del.Data["done"])
	}
	if _, err := p.GetDatabase(ctx, adminNR("databases/test-db", nil)); err == nil {
		t.Error("GetDatabase after delete succeeded, want NotFound")
	}

	// data-plane verbs are rejected before dispatch (invalid path here).
	if _, err := p.CreateDatabase(ctx, adminNR("databases/test-db", nil)); err == nil {
		t.Error("CreateDatabase on a database resource succeeded, want InvalidArgument")
	}
}

func TestRESTBackupScheduleLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	created, err := p.CreateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules", map[string]any{
		"body": map[string]any{"retention": "3600s", "weeklyRecurrence": map[string]any{"day": "MONDAY"}},
	}))
	if err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	if created.Data["name"] != "projects/proj/databases/(default)/backupSchedules/1" {
		t.Errorf("schedule name = %v", created.Data["name"])
	}
	if created.Data["retention"] != "3600s" {
		t.Errorf("retention = %v, want 3600s", created.Data["retention"])
	}
	wr, _ := created.Data["weeklyRecurrence"].(map[string]any)
	if wr["day"] != "MONDAY" {
		t.Errorf("weeklyRecurrence.day = %v, want MONDAY", wr["day"])
	}

	got, err := p.GetBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", nil))
	if err != nil {
		t.Fatalf("GetBackupSchedule: %v", err)
	}
	if got.Data["name"] != created.Data["name"] {
		t.Errorf("GetBackupSchedule name = %v", got.Data["name"])
	}

	list, err := p.ListBackupSchedules(ctx, adminNR("databases/(default)/backupSchedules", nil))
	if err != nil {
		t.Fatalf("ListBackupSchedules: %v", err)
	}
	if bss, _ := list.Data["backupSchedules"].([]any); len(bss) != 1 {
		t.Errorf("ListBackupSchedules len = %d, want 1", len(bss))
	}

	upd, err := p.UpdateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", map[string]any{
		"updateMask": "retention",
		"body":       map[string]any{"retention": "7200s"},
	}))
	if err != nil {
		t.Fatalf("UpdateBackupSchedule: %v", err)
	}
	if upd.Data["retention"] != "7200s" {
		t.Errorf("retention after update = %v, want 7200s", upd.Data["retention"])
	}

	if _, err := p.DeleteBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", nil)); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", nil)); err == nil {
		t.Error("GetBackupSchedule after delete succeeded, want NotFound")
	}
}

func TestRESTUserCredsLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	// create surfaces the secret password once.
	created, err := p.CreateUserCreds(ctx, adminNR("databases/(default)/userCreds", map[string]any{"userCredsId": "uc1"}))
	if err != nil {
		t.Fatalf("CreateUserCreds: %v", err)
	}
	if created.Data["name"] != "projects/proj/databases/(default)/userCreds/uc1" {
		t.Errorf("user creds name = %v", created.Data["name"])
	}
	secret, _ := created.Data["securePassword"].(string)
	if secret == "" {
		t.Error("CreateUserCreds returned no securePassword")
	}
	if created.Data["state"] != "ENABLED" {
		t.Errorf("state = %v, want ENABLED", created.Data["state"])
	}
	ri, _ := created.Data["resourceIdentity"].(map[string]any)
	if ri["principal"] != "uc1@proj.iam.gserviceaccount.com" {
		t.Errorf("principal = %v", ri["principal"])
	}

	// get/list never surface the secret.
	got, err := p.GetUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("GetUserCreds: %v", err)
	}
	if _, ok := got.Data["securePassword"]; ok {
		t.Error("GetUserCreds surfaced securePassword")
	}
	list, err := p.ListUserCreds(ctx, adminNR("databases/(default)/userCreds", nil))
	if err != nil {
		t.Fatalf("ListUserCreds: %v", err)
	}
	if ucs, _ := list.Data["userCreds"].([]any); len(ucs) != 1 {
		t.Errorf("ListUserCreds len = %d, want 1", len(ucs))
	}

	dis, err := p.DisableUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("DisableUserCreds: %v", err)
	}
	if dis.Data["state"] != "DISABLED" {
		t.Errorf("state after disable = %v", dis.Data["state"])
	}
	en, err := p.EnableUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("EnableUserCreds: %v", err)
	}
	if en.Data["state"] != "ENABLED" {
		t.Errorf("state after enable = %v", en.Data["state"])
	}

	reset, err := p.ResetUserPassword(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("ResetUserPassword: %v", err)
	}
	if reset.Data["securePassword"] == secret {
		t.Error("ResetUserPassword did not rotate the password")
	}

	if _, err := p.DeleteUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil)); err != nil {
		t.Fatalf("DeleteUserCreds: %v", err)
	}
	if _, err := p.GetUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil)); err == nil {
		t.Error("GetUserCreds after delete succeeded, want NotFound")
	}
}

func TestRESTFieldUpdateAndGet(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	upd, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"updateMask": "indexConfig",
		"body": map[string]any{
			"indexConfig": map[string]any{
				"indexes": []any{
					map[string]any{"fieldPath": "f", "order": "ASCENDING"},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("UpdateField: %v", err)
	}
	respObj, _ := upd.Data["response"].(map[string]any)
	if respObj["name"] != "projects/proj/databases/(default)/collectionGroups/cg/fields/f" {
		t.Errorf("field name = %v", respObj["name"])
	}
	ic, _ := respObj["indexConfig"].(map[string]any)
	if _, ok := ic["indexes"].([]any); !ok {
		t.Errorf("field indexConfig.indexes missing: %v", respObj)
	}

	got, err := p.GetField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", nil))
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if got.Data["name"] != "projects/proj/databases/(default)/collectionGroups/cg/fields/f" {
		t.Errorf("GetField name = %v", got.Data["name"])
	}

	list, err := p.ListFields(ctx, adminNR("databases/(default)/collectionGroups/cg/fields", nil))
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if fields, _ := list.Data["fields"].([]any); len(fields) != 1 {
		t.Errorf("ListFields len = %d, want 1", len(fields))
	}
}

func TestRESTBackupsReadOnly(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	list, err := p.ListBackups(ctx, adminNR("locations/nam5/backups", nil))
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if backups, _ := list.Data["backups"].([]any); len(backups) != 0 {
		t.Errorf("ListBackups len = %d, want 0", len(backups))
	}
	if _, err := p.GetBackup(ctx, adminNR("locations/nam5/backups/b1", nil)); err == nil {
		t.Error("GetBackup(missing) succeeded, want NotFound")
	}
	if _, err := p.DeleteBackup(ctx, adminNR("locations/nam5/backups/b1", nil)); err == nil {
		t.Error("DeleteBackup(missing) succeeded, want NotFound")
	}
	// The list/get survive a stored record.
	if err := p.Service.putResource(ctx, "proj", rtBackup, backupRel("nam5", "b1"), backupDef{
		Name: backupName("proj", "nam5", "b1"), Database: "projects/proj/databases/(default)",
		SizeBytes: 42, SnapshotTime: time.Unix(0, 0),
	}, false); err != nil {
		t.Fatalf("putResource: %v", err)
	}
	b, err := p.GetBackup(ctx, adminNR("locations/nam5/backups/b1", nil))
	if err != nil {
		t.Fatalf("GetBackup(stored): %v", err)
	}
	stats, _ := b.Data["stats"].(map[string]any)
	if stats["sizeBytes"] != "42" {
		t.Errorf("stats.sizeBytes = %v, want \"42\"", stats["sizeBytes"])
	}
}

func TestAdminMaskForms(t *testing.T) {
	// Plain csv updateMask is normalized to snake_case proto paths.
	nr := adminNR("databases/db", map[string]any{"updateMask": "concurrencyMode,locationId"})
	if got := adminMask(nr); len(got) != 2 || got[0] != "concurrency_mode" || got[1] != "location_id" {
		t.Errorf("adminMask(plain) = %v", got)
	}
	// Repeated updateMask.fieldPaths (older clients).
	nr = adminNR("databases/db", map[string]any{"updateMask.fieldPaths": "concurrencyMode"})
	if got := adminMask(nr); len(got) != 1 || got[0] != "concurrency_mode" {
		t.Errorf("adminMask(fieldPaths) = %v", got)
	}
	// Nested JSON path and the wildcard.
	nr = adminNR("databases/db", map[string]any{"updateMask": "indexConfig.indexes,*"})
	if got := adminMask(nr); len(got) != 2 || got[0] != "index_config.indexes" || got[1] != "*" {
		t.Errorf("adminMask(nested) = %v", got)
	}
}
