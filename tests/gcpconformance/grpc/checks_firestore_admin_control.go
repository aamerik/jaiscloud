package grpcconformance

// FirestoreAdmin control-plane probes (databases, fields, user creds, backup
// schedules, backups) over the official admin client / generated stub. These
// are the RPCs beyond the composite-index CRUD covered in
// checks_firestore_admin.go. Every LRO returned by the emulator is terminal
// (done=true), so the probes unmarshal the inline response rather than polling
// operations.get. Names are run-unique via cfg.Suffix.

import (
	"context"
	"fmt"
	"time"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// firestoreAdminControlChecks returns the control-plane probes in execution
// order. They share the emulator's state: the database probes operate on a
// run-unique named database, all other probes on (default).
func firestoreAdminControlChecks() []Check {
	return []Check{
		{Service: "firestoreadmin", RPC: "CreateDatabase", KeyField: "operation done=true; database name", Run: checkFsAdminCreateDatabase},
		{Service: "firestoreadmin", RPC: "GetDatabase", KeyField: "database name/uid", Run: checkFsAdminGetDatabase},
		{Service: "firestoreadmin", RPC: "ListDatabases", KeyField: "created database present + (default)", Run: checkFsAdminListDatabases},
		{Service: "firestoreadmin", RPC: "UpdateDatabase", KeyField: "concurrency_mode updated under mask", Run: checkFsAdminUpdateDatabase},
		{Service: "firestoreadmin", RPC: "DeleteDatabase", KeyField: "database absent after delete", Run: checkFsAdminDeleteDatabase},
		{Service: "firestoreadmin", RPC: "UpdateField", Method: "UpdateField", KeyField: "operation done=true; index_config", Run: checkFsAdminUpdateField},
		{Service: "firestoreadmin", RPC: "GetField", Method: "GetField", KeyField: "index_config round-trip", Run: checkFsAdminGetField},
		{Service: "firestoreadmin", RPC: "ListFields", Method: "ListFields", KeyField: "field present under wildcard group", Run: checkFsAdminListFields},
		{Service: "firestoreadmin", RPC: "CreateUserCreds", Method: "CreateUserCreds", KeyField: "secure_password returned once", Run: checkFsAdminCreateUserCreds},
		{Service: "firestoreadmin", RPC: "GetUserCreds", Method: "GetUserCreds", KeyField: "name; no secure_password", Run: checkFsAdminGetUserCreds},
		{Service: "firestoreadmin", RPC: "ListUserCreds", Method: "ListUserCreds", KeyField: "user creds present", Run: checkFsAdminListUserCreds},
		{Service: "firestoreadmin", RPC: "DisableUserCreds", Method: "DisableUserCreds", KeyField: "state DISABLED", Run: checkFsAdminDisableUserCreds},
		{Service: "firestoreadmin", RPC: "EnableUserCreds", Method: "EnableUserCreds", KeyField: "state ENABLED", Run: checkFsAdminEnableUserCreds},
		{Service: "firestoreadmin", RPC: "ResetUserPassword", Method: "ResetUserPassword", KeyField: "new secure_password", Run: checkFsAdminResetUserPassword},
		{Service: "firestoreadmin", RPC: "DeleteUserCreds", Method: "DeleteUserCreds", KeyField: "user creds absent after delete", Run: checkFsAdminDeleteUserCreds},
		{Service: "firestoreadmin", RPC: "CreateBackupSchedule", Method: "CreateBackupSchedule", KeyField: "schedule name/retention", Run: checkFsAdminCreateBackupSchedule},
		{Service: "firestoreadmin", RPC: "GetBackupSchedule", Method: "GetBackupSchedule", KeyField: "retention round-trip", Run: checkFsAdminGetBackupSchedule},
		{Service: "firestoreadmin", RPC: "ListBackupSchedules", Method: "ListBackupSchedules", KeyField: "schedule present", Run: checkFsAdminListBackupSchedules},
		{Service: "firestoreadmin", RPC: "UpdateBackupSchedule", Method: "UpdateBackupSchedule", KeyField: "retention updated under mask", Run: checkFsAdminUpdateBackupSchedule},
		{Service: "firestoreadmin", RPC: "DeleteBackupSchedule", Method: "DeleteBackupSchedule", KeyField: "schedule absent after delete", Run: checkFsAdminDeleteBackupSchedule},
		{Service: "firestoreadmin", RPC: "ListBackups", Method: "ListBackups", KeyField: "empty list (no create path)", Run: checkFsAdminListBackups},
		{Service: "firestoreadmin", RPC: "GetBackup", Method: "GetBackup", KeyField: "missing backup -> NotFound", Run: checkFsAdminGetBackupMissing},
		{Service: "firestoreadmin", RPC: "DeleteBackup", Method: "DeleteBackup", KeyField: "missing backup -> NotFound", Run: checkFsAdminDeleteBackupMissing},
	}
}

// ─── naming helpers ───────────────────────────────────────────────────────────

func fsAdminDBName(cfg Config, id string) string {
	return fmt.Sprintf("projects/%s/databases/%s", cfg.Project, id)
}

func fsAdminDefaultDB(cfg Config) string { return fsAdminDBName(cfg, "(default)") }

func fsAdminDBID(cfg Config) string { return cfg.ResourceName("gcpc-fsdb") }

func fsAdminField(cfg Config) string { return cfg.ResourceName("fsaf") }

func fsAdminFieldCG(cfg Config) string { return cfg.ResourceName("gcpc-fsaf") }

func fsAdminFieldName(cfg Config) string {
	return fmt.Sprintf("%s/collectionGroups/%s/fields/%s", fsAdminDefaultDB(cfg), fsAdminFieldCG(cfg), fsAdminField(cfg))
}

func fsAdminUserCredsID(cfg Config) string { return cfg.ResourceName("fscred") }

func fsAdminUserCredsName(cfg Config) string {
	return fmt.Sprintf("%s/userCreds/%s", fsAdminDefaultDB(cfg), fsAdminUserCredsID(cfg))
}

func fsAdminScheduleID(cfg Config) string { return cfg.ResourceName("fssched") }

func fsAdminScheduleName(cfg Config) string {
	return fmt.Sprintf("%s/backupSchedules/%s", fsAdminDefaultDB(cfg), fsAdminScheduleID(cfg))
}

func fsAdminBackupParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/us-central1", cfg.Project)
}

func fsAdminBackupName(cfg Config) string {
	return fsAdminBackupParent(cfg) + "/backups/" + cfg.ResourceName("fsbackup")
}

// ─── database probes ──────────────────────────────────────────────────────────

func checkFsAdminCreateDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	op, err := stub.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     fmt.Sprintf("projects/%s", cfg.Project),
		DatabaseId: fsAdminDBID(cfg),
		Database:   &adminpb.Database{LocationId: "nam5"},
	})
	if err != nil {
		return fmt.Errorf("CreateDatabase: %w", err)
	}
	if !op.GetDone() || op.GetResponse() == nil {
		return fmt.Errorf("CreateDatabase operation = done:%t response:%v, want terminal response", op.GetDone(), op.GetResponse())
	}
	db := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(db); err != nil {
		return fmt.Errorf("CreateDatabase response: %w", err)
	}
	if want := fsAdminDBName(cfg, fsAdminDBID(cfg)); db.GetName() != want {
		return fmt.Errorf("CreateDatabase name = %q, want %q", db.GetName(), want)
	}
	if db.GetType() != adminpb.Database_FIRESTORE_NATIVE {
		return fmt.Errorf("CreateDatabase type = %v, want FIRESTORE_NATIVE", db.GetType())
	}
	return nil
}

func checkFsAdminGetDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: fsAdminDBName(cfg, fsAdminDBID(cfg))})
	if err != nil {
		return fmt.Errorf("GetDatabase: %w", err)
	}
	if got.GetUid() == "" || got.GetLocationId() != "nam5" {
		return fmt.Errorf("GetDatabase = %+v, want uid + location nam5", got)
	}
	return nil
}

func checkFsAdminListDatabases(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: fmt.Sprintf("projects/%s", cfg.Project)})
	if err != nil {
		return fmt.Errorf("ListDatabases: %w", err)
	}
	want := fsAdminDBName(cfg, fsAdminDBID(cfg))
	var haveCreated, haveDefault bool
	for _, d := range resp.GetDatabases() {
		if d.GetName() == want {
			haveCreated = true
		}
		if d.GetName() == fsAdminDefaultDB(cfg) {
			haveDefault = true
		}
	}
	if !haveCreated {
		return fmt.Errorf("ListDatabases did not include %q (%d databases)", want, len(resp.GetDatabases()))
	}
	if !haveDefault {
		return fmt.Errorf("ListDatabases did not include the (default) database")
	}
	return nil
}

func checkFsAdminUpdateDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	name := fsAdminDBName(cfg, fsAdminDBID(cfg))
	cur, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDatabase: %w", err)
	}
	op, err := stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: name, Etag: cur.GetEtag(), ConcurrencyMode: adminpb.Database_PESSIMISTIC},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase: %w", err)
	}
	upd := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(upd); err != nil {
		return fmt.Errorf("UpdateDatabase response: %w", err)
	}
	if upd.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
		return fmt.Errorf("UpdateDatabase concurrency = %v, want PESSIMISTIC", upd.GetConcurrencyMode())
	}
	return nil
}

func checkFsAdminDeleteDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	name := fsAdminDBName(cfg, fsAdminDBID(cfg))
	cur, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDatabase: %w", err)
	}
	if _, err := stub.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: name, Etag: cur.GetEtag()}); err != nil {
		return fmt.Errorf("DeleteDatabase: %w", err)
	}
	if _, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetDatabase after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── field probes ─────────────────────────────────────────────────────────────

func checkFsAdminUpdateField(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	op, err := stub.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: fsAdminFieldName(cfg),
			IndexConfig: &adminpb.Field_IndexConfig{
				UsesAncestorConfig: true,
				AncestorField:      fsAdminFieldCG(cfg),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"index_config"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateField: %w", err)
	}
	field := &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(field); err != nil {
		return fmt.Errorf("UpdateField response: %w", err)
	}
	if field.GetName() != fsAdminFieldName(cfg) || !field.GetIndexConfig().GetUsesAncestorConfig() {
		return fmt.Errorf("UpdateField = %+v", field)
	}
	return nil
}

func checkFsAdminGetField(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetField(ctx, &adminpb.GetFieldRequest{Name: fsAdminFieldName(cfg)})
	if err != nil {
		return fmt.Errorf("GetField: %w", err)
	}
	if got.GetIndexConfig().GetAncestorField() != fsAdminFieldCG(cfg) {
		return fmt.Errorf("GetField index_config = %+v", got.GetIndexConfig())
	}
	return nil
}

func checkFsAdminListFields(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	want := fsAdminFieldName(cfg)
	token := ""
	for {
		resp, err := stub.ListFields(ctx, &adminpb.ListFieldsRequest{
			Parent:    fsAdminDefaultDB(cfg) + "/collectionGroups/-",
			PageSize:  100,
			PageToken: token,
		})
		if err != nil {
			return fmt.Errorf("ListFields: %w", err)
		}
		for _, f := range resp.GetFields() {
			if f.GetName() == want {
				return nil
			}
		}
		token = resp.GetNextPageToken()
		if token == "" {
			return fmt.Errorf("ListFields did not include %q", want)
		}
	}
}

// ─── user creds probes ────────────────────────────────────────────────────────

func checkFsAdminCreateUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
		Parent:      fsAdminDefaultDB(cfg),
		UserCredsId: fsAdminUserCredsID(cfg),
	})
	if err != nil {
		return fmt.Errorf("CreateUserCreds: %w", err)
	}
	if uc.GetName() != fsAdminUserCredsName(cfg) {
		return fmt.Errorf("CreateUserCreds name = %q, want %q", uc.GetName(), fsAdminUserCredsName(cfg))
	}
	if uc.GetSecurePassword() == "" {
		return fmt.Errorf("CreateUserCreds did not return a secure_password")
	}
	if uc.GetState() != adminpb.UserCreds_ENABLED {
		return fmt.Errorf("CreateUserCreds state = %v, want ENABLED", uc.GetState())
	}
	return nil
}

func checkFsAdminGetUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("GetUserCreds: %w", err)
	}
	if got.GetName() != fsAdminUserCredsName(cfg) {
		return fmt.Errorf("GetUserCreds name = %q", got.GetName())
	}
	if got.GetSecurePassword() != "" {
		return fmt.Errorf("GetUserCreds exposed secure_password")
	}
	return nil
}

func checkFsAdminListUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: fsAdminDefaultDB(cfg)})
	if err != nil {
		return fmt.Errorf("ListUserCreds: %w", err)
	}
	for _, uc := range resp.GetUserCreds() {
		if uc.GetName() == fsAdminUserCredsName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListUserCreds did not include %q", fsAdminUserCredsName(cfg))
}

func checkFsAdminDisableUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.DisableUserCreds(ctx, &adminpb.DisableUserCredsRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("DisableUserCreds: %w", err)
	}
	if uc.GetState() != adminpb.UserCreds_DISABLED {
		return fmt.Errorf("DisableUserCreds state = %v, want DISABLED", uc.GetState())
	}
	return nil
}

func checkFsAdminEnableUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.EnableUserCreds(ctx, &adminpb.EnableUserCredsRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("EnableUserCreds: %w", err)
	}
	if uc.GetState() != adminpb.UserCreds_ENABLED {
		return fmt.Errorf("EnableUserCreds state = %v, want ENABLED", uc.GetState())
	}
	return nil
}

func checkFsAdminResetUserPassword(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.ResetUserPassword(ctx, &adminpb.ResetUserPasswordRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("ResetUserPassword: %w", err)
	}
	if uc.GetSecurePassword() == "" {
		return fmt.Errorf("ResetUserPassword did not return a secure_password")
	}
	return nil
}

func checkFsAdminDeleteUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.DeleteUserCreds(ctx, &adminpb.DeleteUserCredsRequest{Name: fsAdminUserCredsName(cfg)}); err != nil {
		return fmt.Errorf("DeleteUserCreds: %w", err)
	}
	if _, err := stub.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: fsAdminUserCredsName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetUserCreds after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── backup schedule probes ───────────────────────────────────────────────────

func checkFsAdminCreateBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	bs, err := stub.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
		Parent: fsAdminDefaultDB(cfg),
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      fsAdminScheduleName(cfg),
			Retention: durationpb.New(7 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_DailyRecurrence{
				DailyRecurrence: &adminpb.DailyRecurrence{},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateBackupSchedule: %w", err)
	}
	if bs.GetName() != fsAdminScheduleName(cfg) || bs.GetDailyRecurrence() == nil {
		return fmt.Errorf("CreateBackupSchedule = %+v", bs)
	}
	return nil
}

func checkFsAdminGetBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: fsAdminScheduleName(cfg)})
	if err != nil {
		return fmt.Errorf("GetBackupSchedule: %w", err)
	}
	if got.GetRetention().AsDuration() != 7*24*time.Hour {
		return fmt.Errorf("GetBackupSchedule retention = %v, want 7d", got.GetRetention())
	}
	return nil
}

func checkFsAdminListBackupSchedules(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: fsAdminDefaultDB(cfg)})
	if err != nil {
		return fmt.Errorf("ListBackupSchedules: %w", err)
	}
	for _, bs := range resp.GetBackupSchedules() {
		if bs.GetName() == fsAdminScheduleName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListBackupSchedules did not include %q", fsAdminScheduleName(cfg))
}

func checkFsAdminUpdateBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      fsAdminScheduleName(cfg),
			Retention: durationpb.New(14 * 24 * time.Hour),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateBackupSchedule: %w", err)
	}
	if got.GetRetention().AsDuration() != 14*24*time.Hour {
		return fmt.Errorf("UpdateBackupSchedule retention = %v, want 14d", got.GetRetention())
	}
	return nil
}

func checkFsAdminDeleteBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.DeleteBackupSchedule(ctx, &adminpb.DeleteBackupScheduleRequest{Name: fsAdminScheduleName(cfg)}); err != nil {
		return fmt.Errorf("DeleteBackupSchedule: %w", err)
	}
	if _, err := stub.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: fsAdminScheduleName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetBackupSchedule after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── backup probes ────────────────────────────────────────────────────────────

func checkFsAdminListBackups(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListBackups(ctx, &adminpb.ListBackupsRequest{Parent: fsAdminBackupParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListBackups: %w", err)
	}
	if len(resp.GetBackups()) != 0 {
		return fmt.Errorf("ListBackups returned %d backups, want 0 (no create path)", len(resp.GetBackups()))
	}
	return nil
}

func checkFsAdminGetBackupMissing(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.GetBackup(ctx, &adminpb.GetBackupRequest{Name: fsAdminBackupName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetBackup(missing) = %v, want NotFound", err)
	}
	return nil
}

func checkFsAdminDeleteBackupMissing(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.DeleteBackup(ctx, &adminpb.DeleteBackupRequest{Name: fsAdminBackupName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("DeleteBackup(missing) = %v, want NotFound", err)
	}
	return nil
}
