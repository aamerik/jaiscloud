package firestoreadmin

import (
	"context"
	"testing"
	"time"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"jaiscloud/internal/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const testDBParent = "projects/proj"

func TestDatabaseAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     testDBParent,
		DatabaseId: "test-db",
		Database:   &adminpb.Database{LocationId: "nam5"},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("CreateDatabase done = false, want true")
	}
	created := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("CreateDatabase response unmarshal: %v", err)
	}
	if want := "projects/proj/databases/test-db"; created.GetName() != want {
		t.Fatalf("created name = %q, want %q", created.GetName(), want)
	}
	if created.GetType() != adminpb.Database_FIRESTORE_NATIVE {
		t.Errorf("created type = %v, want FIRESTORE_NATIVE", created.GetType())
	}

	got, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.GetUid() != created.GetUid() || got.GetLocationId() != "nam5" {
		t.Fatalf("GetDatabase = %+v", got)
	}

	list, err := client.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: testDBParent})
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(list.GetDatabases()) != 2 {
		t.Fatalf("ListDatabases len = %d, want 2 (default + test-db)", len(list.GetDatabases()))
	}

	updOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:            created.GetName(),
			Etag:            created.GetEtag(),
			ConcurrencyMode: adminpb.Database_PESSIMISTIC,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase: %v", err)
	}
	updated := &adminpb.Database{}
	if err := updOp.GetResponse().UnmarshalTo(updated); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}
	if updated.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
		t.Fatalf("updated concurrency = %v, want PESSIMISTIC", updated.GetConcurrencyMode())
	}

	// Stale etag is rejected.
	if _, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: created.GetName(), Etag: created.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	}); status.Code(err) != codes.Aborted {
		t.Errorf("stale etag code = %v, want Aborted", status.Code(err))
	}

	if _, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: created.GetName(), Etag: updated.GetEtag()}); err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if _, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetDatabase after delete = %v, want NotFound", err)
	}
}

func TestFieldAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	name := "projects/proj/databases/(default)/collectionGroups/cities/fields/population"
	op, err := client.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: name,
			IndexConfig: &adminpb.Field_IndexConfig{
				UsesAncestorConfig: true,
				AncestorField:      "cities",
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"index_config"}},
	})
	if err != nil {
		t.Fatalf("UpdateField: %v", err)
	}
	field := &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(field); err != nil {
		t.Fatalf("UpdateField response unmarshal: %v", err)
	}
	if field.GetName() != name || !field.GetIndexConfig().GetUsesAncestorConfig() {
		t.Fatalf("field = %+v", field)
	}

	got, err := client.GetField(ctx, &adminpb.GetFieldRequest{Name: name})
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if got.GetIndexConfig().GetAncestorField() != "cities" {
		t.Fatalf("GetField = %+v", got)
	}

	list, err := client.ListFields(ctx, &adminpb.ListFieldsRequest{
		Parent: "projects/proj/databases/(default)/collectionGroups/-",
	})
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if len(list.GetFields()) != 1 {
		t.Fatalf("ListFields len = %d, want 1", len(list.GetFields()))
	}
}

func TestUserCredsAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	parent := "projects/proj/databases/(default)"
	created, err := client.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
		Parent:      parent,
		UserCredsId: "cred1",
	})
	if err != nil {
		t.Fatalf("CreateUserCreds: %v", err)
	}
	if created.GetSecurePassword() == "" {
		t.Fatal("CreateUserCreds did not return a secure_password")
	}
	if created.GetResourceIdentity().GetPrincipal() == "" {
		t.Error("CreateUserCreds principal empty")
	}

	got, err := client.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetUserCreds: %v", err)
	}
	if got.GetSecurePassword() != "" {
		t.Error("GetUserCreds exposed secure_password")
	}

	list, err := client.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: parent})
	if err != nil || len(list.GetUserCreds()) != 1 {
		t.Fatalf("ListUserCreds len=%d err=%v", len(list.GetUserCreds()), err)
	}

	dis, err := client.DisableUserCreds(ctx, &adminpb.DisableUserCredsRequest{Name: created.GetName()})
	if err != nil || dis.GetState() != adminpb.UserCreds_DISABLED {
		t.Fatalf("DisableUserCreds = %+v err=%v", dis, err)
	}
	en, err := client.EnableUserCreds(ctx, &adminpb.EnableUserCredsRequest{Name: created.GetName()})
	if err != nil || en.GetState() != adminpb.UserCreds_ENABLED {
		t.Fatalf("EnableUserCreds = %+v err=%v", en, err)
	}

	reset, err := client.ResetUserPassword(ctx, &adminpb.ResetUserPasswordRequest{Name: created.GetName()})
	if err != nil || reset.GetSecurePassword() == "" {
		t.Fatalf("ResetUserPassword = %+v err=%v", reset, err)
	}

	if _, err := client.DeleteUserCreds(ctx, &adminpb.DeleteUserCredsRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("DeleteUserCreds: %v", err)
	}
	if _, err := client.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetUserCreds after delete = %v, want NotFound", err)
	}
}

func TestBackupScheduleAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	parent := "projects/proj/databases/(default)"
	schedName := parent + "/backupSchedules/1"
	created, err := client.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
		Parent: parent,
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(7 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_DailyRecurrence{
				DailyRecurrence: &adminpb.DailyRecurrence{},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	if created.GetName() != schedName || created.GetDailyRecurrence() == nil {
		t.Fatalf("created schedule = %+v", created)
	}

	got, err := client.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: schedName})
	if err != nil || got.GetRetention() == nil {
		t.Fatalf("GetBackupSchedule = %+v err=%v", got, err)
	}

	list, err := client.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: parent})
	if err != nil || len(list.GetBackupSchedules()) != 1 {
		t.Fatalf("ListBackupSchedules len=%d err=%v", len(list.GetBackupSchedules()), err)
	}

	upd, err := client.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(14 * 24 * time.Hour),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention"}},
	})
	if err != nil || upd.GetRetention().AsDuration() != 14*24*time.Hour {
		t.Fatalf("UpdateBackupSchedule = %+v err=%v", upd, err)
	}

	if _, err := client.DeleteBackupSchedule(ctx, &adminpb.DeleteBackupScheduleRequest{Name: schedName}); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	if _, err := client.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: schedName}); status.Code(err) != codes.NotFound {
		t.Errorf("GetBackupSchedule after delete = %v, want NotFound", err)
	}
}

func TestBackupReadOnlyRPCs(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	list, err := client.ListBackups(ctx, &adminpb.ListBackupsRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil || len(list.GetBackups()) != 0 {
		t.Fatalf("ListBackups len=%d err=%v", len(list.GetBackups()), err)
	}
	if _, err := client.GetBackup(ctx, &adminpb.GetBackupRequest{
		Name: "projects/proj/locations/us-central1/backups/b1",
	}); status.Code(err) != codes.NotFound {
		t.Errorf("GetBackup(missing) = %v, want NotFound", err)
	}
	if _, err := client.DeleteBackup(ctx, &adminpb.DeleteBackupRequest{
		Name: "projects/proj/locations/us-central1/backups/b1",
	}); status.Code(err) != codes.NotFound {
		t.Errorf("DeleteBackup(missing) = %v, want NotFound", err)
	}
}

// TestDatabaseInvalidName checks malformed resource names are InvalidArgument.
func TestDatabaseInvalidName(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	if _, err := client.GetDatabase(context.Background(), &adminpb.GetDatabaseRequest{Name: "not-a-name"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetDatabase(bad name) = %v, want InvalidArgument", err)
	}
}
