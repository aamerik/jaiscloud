package firestore

// Firestore Admin control-plane domain logic (google.firestore.admin.v1) beyond
// the composite-index CRUD in indexes.go: databases, collection-group fields,
// backup schedules, backups and user creds. Every method here is
// transport-agnostic — the gRPC FirestoreAdmin service
// (internal/gcp/grpc/firestoreadmin) is the only caller today — and stores its
// records in the shared ResourceStore, so memory and --dsn (Postgres) backends
// behave identically. Long-running operations are born terminal (done=true),
// matching the index surface: there is no asynchronous control plane to poll.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Resource-store types for the Firestore Admin control plane.
const (
	rtDatabase       = "firestore_database"
	rtField          = "firestore_field"
	rtUserCreds      = "firestore_user_creds"
	rtBackupSchedule = "firestore_backup_schedule"
	rtBackup         = "firestore_backup"
)

// defaultDatabaseID is the implicit database every Firestore project has.
const defaultDatabaseID = "(default)"

// passwordAlphabet is the character set for a synthesized user-creds password.
const passwordAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// databaseDef is the persisted google.firestore.admin.v1.Database (the mutable
// subset the emulator models). Fields the emulator has no source for
// (earliest_version_time, cmek_config, source_info, ...) are omitted rather than
// fabricated.
type databaseDef struct {
	Name                          string        `json:"name,omitempty"`
	Uid                           string        `json:"uid,omitempty"`
	CreateTime                    time.Time     `json:"createTime,omitempty"`
	UpdateTime                    time.Time     `json:"updateTime,omitempty"`
	DeleteTime                    time.Time     `json:"deleteTime,omitempty"`
	LocationID                    string        `json:"locationId,omitempty"`
	Type                          string        `json:"type,omitempty"`
	ConcurrencyMode               string        `json:"concurrencyMode,omitempty"`
	AppEngineIntegrationMode      string        `json:"appEngineIntegrationMode,omitempty"`
	PointInTimeRecoveryEnablement string        `json:"pointInTimeRecoveryEnablement,omitempty"`
	DeleteProtectionState         string        `json:"deleteProtectionState,omitempty"`
	DatabaseEdition               string        `json:"databaseEdition,omitempty"`
	KeyPrefix                     string        `json:"keyPrefix,omitempty"`
	VersionRetentionPeriod        time.Duration `json:"versionRetentionPeriod,omitempty"`
	Etag                          string        `json:"etag,omitempty"`
	Deleted                       bool          `json:"deleted,omitempty"`
}

// fieldDef is the persisted google.firestore.admin.v1.Field.
type fieldDef struct {
	Name        string            `json:"name,omitempty"`
	IndexConfig *fieldIndexConfig `json:"indexConfig,omitempty"`
	TtlConfig   *fieldTtlConfig   `json:"ttlConfig,omitempty"`
}

// fieldIndexConfig mirrors google.firestore.admin.v1.Field.IndexConfig.
type fieldIndexConfig struct {
	Indexes            []indexDef `json:"indexes,omitempty"`
	UsesAncestorConfig bool       `json:"usesAncestorConfig,omitempty"`
	AncestorField      string     `json:"ancestorField,omitempty"`
	Reverting          bool       `json:"reverting,omitempty"`
}

// fieldTtlConfig mirrors google.firestore.admin.v1.Field.TtlConfig.
type fieldTtlConfig struct {
	State            string        `json:"state,omitempty"`
	ExpirationOffset time.Duration `json:"expirationOffset,omitempty"`
}

// userCredsDef is the persisted google.firestore.admin.v1.UserCreds. The secret
// password is stored so a later reset can rotate it, but it is only ever
// surfaced on the create/reset responses (never on get/list/enable/disable), as
// real Firestore does.
type userCredsDef struct {
	Name           string    `json:"name,omitempty"`
	CreateTime     time.Time `json:"createTime,omitempty"`
	UpdateTime     time.Time `json:"updateTime,omitempty"`
	State          string    `json:"state,omitempty"`
	Principal      string    `json:"principal,omitempty"`
	SecretPassword string    `json:"secretPassword,omitempty"`
}

// backupScheduleDef is the persisted google.firestore.admin.v1.BackupSchedule.
// Recurrence is "DAILY" or "WEEKLY"; DayOfWeek is only meaningful for WEEKLY.
type backupScheduleDef struct {
	Name       string        `json:"name,omitempty"`
	CreateTime time.Time     `json:"createTime,omitempty"`
	UpdateTime time.Time     `json:"updateTime,omitempty"`
	Retention  time.Duration `json:"retention,omitempty"`
	Recurrence string        `json:"recurrence,omitempty"`
	DayOfWeek  int           `json:"dayOfWeek,omitempty"`
}

// backupDef is the persisted google.firestore.admin.v1.Backup. Backups are
// read-only in the Admin API (a schedule produces them), so the emulator only
// supports get/list/delete over records; there is no create path.
type backupDef struct {
	Name          string    `json:"name,omitempty"`
	Database      string    `json:"database,omitempty"`
	DatabaseUid   string    `json:"databaseUid,omitempty"`
	SnapshotTime  time.Time `json:"snapshotTime,omitempty"`
	ExpireTime    time.Time `json:"expireTime,omitempty"`
	State         string    `json:"state,omitempty"`
	DocumentCount int64     `json:"documentCount,omitempty"`
	IndexCount    int64     `json:"indexCount,omitempty"`
	SizeBytes     int64     `json:"sizeBytes,omitempty"`
}

// ─── resource-name helpers ────────────────────────────────────────────────────

func databaseName(project, db string) string {
	return resource.ResourceID(project)("firestore-database", db)
}

func fieldName(project, db, cg, fieldPath string) string {
	return resource.ResourceID(project)("firestore-field", db+"/"+cg+"/"+fieldPath)
}

func userCredsName(project, db, id string) string {
	return resource.ResourceID(project)("firestore-user-creds", db+"/"+id)
}

func backupScheduleName(project, db, id string) string {
	return resource.ResourceID(project)("firestore-backup-schedule", db+"/"+id)
}

func backupName(project, loc, id string) string {
	return resource.ResourceID(project)("firestore-backup", loc+"/"+id)
}

func databaseOperationName(project, db, op string) string {
	return resource.ResourceID(project)("firestore-operation", db+"/"+op)
}

// resource-name store keys (relative to the project).

func databaseRel(db string) string { return "databases/" + db }

func fieldRel(db, cg, fieldPath string) string {
	return "databases/" + db + "/collectionGroups/" + cg + "/fields/" + fieldPath
}

func userCredsRel(db, id string) string { return "databases/" + db + "/userCreds/" + id }

func backupScheduleRel(db, id string) string {
	return "databases/" + db + "/backupSchedules/" + id
}

func backupRel(loc, id string) string { return "locations/" + loc + "/backups/" + id }

// ─── shared store helpers ─────────────────────────────────────────────────────

func gcpNotFound(what string) error {
	return model.NewProviderError("NotFound", what+" not found", 404)
}

func gcpAlreadyExists(what string) error {
	return model.NewProviderError("AlreadyExists", what+" already exists", 409)
}

func gcpInvalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func gcpAborted(msg string) error {
	return model.NewProviderError("Aborted", msg, 409)
}

// putResource marshals v and creates (create=true) or upserts the store entry.
func (s *Service) putResource(ctx context.Context, project, rt, id string, v any, create bool) error {
	if s.resources == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	entry := store.ResourceEntry{Type: rt, ID: id, Data: data}
	if create {
		if err := s.resources.Create(ctx, project, store.GlobalRegion, entry); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return gcpAlreadyExists(strings.TrimPrefix(rt, "firestore_"))
			}
			return err
		}
		return nil
	}
	return s.resources.Upsert(ctx, project, store.GlobalRegion, entry)
}

// getResource loads and unmarshals a store entry into v.
func (s *Service) getResource(ctx context.Context, project, rt, id string, v any) error {
	if s.resources == nil {
		return gcpNotFound(strings.TrimPrefix(rt, "firestore_"))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rt, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound(strings.TrimPrefix(rt, "firestore_"))
		}
		return err
	}
	if err := json.Unmarshal(e.Data, v); err != nil {
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	return nil
}

// listResources lists all entries of a type under a prefix, unmarshalling each
// as an independent item with decode.
func listResources[T any](ctx context.Context, s *Service, project, rt, prefix string) ([]T, error) {
	var out []T
	if s.resources == nil {
		return out, nil
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rt, prefix)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var v T
		if json.Unmarshal(e.Data, &v) == nil {
			out = append(out, v)
		}
	}
	return out, nil
}

// randomPassword returns a fresh user-creds password.
func randomPassword() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", 24)
	}
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = passwordAlphabet[int(c)%len(passwordAlphabet)]
	}
	return string(out)
}

// synthesizeDefaultDatabase returns the implicit (default) database record for a
// project. It is never stored unless updated or deleted.
func synthesizeDefaultDatabase(project string) databaseDef {
	return databaseDef{
		Name:                          databaseName(project, defaultDatabaseID),
		Uid:                           "default",
		Type:                          "FIRESTORE_NATIVE",
		ConcurrencyMode:               "OPTIMISTIC",
		VersionRetentionPeriod:        time.Hour,
		PointInTimeRecoveryEnablement: "POINT_IN_TIME_RECOVERY_DISABLED",
		DeleteProtectionState:         "DELETE_PROTECTION_DISABLED",
	}
}

// ─── Databases ────────────────────────────────────────────────────────────────

// databaseIDRe mirrors real Firestore's database-id constraint:
// 4-63 characters, starting with a lowercase letter, lowercase letters/digits/
// hyphens.
var databaseIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{3,62}$`)

// CreateDatabaseDef creates a database metadata record and returns it with the
// name of the terminal long-running operation wrapping it.
func (s *Service) CreateDatabaseDef(ctx context.Context, project, dbID string, db databaseDef) (databaseDef, string, error) {
	if dbID == "" {
		return databaseDef{}, "", gcpInvalidArgument("database id is required")
	}
	if dbID == defaultDatabaseID {
		return databaseDef{}, "", gcpAlreadyExists("database")
	}
	if !databaseIDRe.MatchString(dbID) {
		return databaseDef{}, "", gcpInvalidArgument("database id must be 4-63 chars, start with a lowercase letter, and contain only lowercase letters, digits, or hyphens")
	}
	db.Name = databaseName(project, dbID)
	db.Uid = randomHex(32)
	now := clock.Now()
	db.CreateTime, db.UpdateTime = now, now
	if db.Type == "" {
		db.Type = "FIRESTORE_NATIVE"
	}
	if db.ConcurrencyMode == "" {
		db.ConcurrencyMode = "OPTIMISTIC"
	}
	if db.VersionRetentionPeriod == 0 {
		db.VersionRetentionPeriod = time.Hour
	}
	db.Etag = randomHex(16)
	if err := s.putResource(ctx, project, rtDatabase, databaseRel(dbID), db, true); err != nil {
		return databaseDef{}, "", err
	}
	return db, databaseOperationName(project, dbID, randomHex(12)), nil
}

// GetDatabase returns a database record. The implicit (default) database is
// synthesized when no record (including a delete tombstone) exists.
func (s *Service) GetDatabase(ctx context.Context, project, db string) (databaseDef, error) {
	var rec databaseDef
	if s.resources != nil {
		err := s.getResource(ctx, project, rtDatabase, databaseRel(db), &rec)
		switch {
		case err == nil:
			if rec.Deleted {
				return databaseDef{}, gcpNotFound("database")
			}
			return rec, nil
		case !isNotFound(err):
			return databaseDef{}, err
		}
	}
	if db == defaultDatabaseID {
		return synthesizeDefaultDatabase(project), nil
	}
	return databaseDef{}, gcpNotFound("database")
}

// ListDatabases returns every database in the project, including the implicit
// (default) database. Deleted records are only included when showDeleted is set.
func (s *Service) ListDatabases(ctx context.Context, project string, showDeleted bool) ([]databaseDef, error) {
	recs, err := listResources[databaseDef](ctx, s, project, rtDatabase, "databases/")
	if err != nil {
		return nil, err
	}
	out := make([]databaseDef, 0, len(recs)+1)
	haveDefault := false
	for _, r := range recs {
		if r.Name == databaseName(project, defaultDatabaseID) {
			haveDefault = true
		}
		if r.Deleted && !showDeleted {
			continue
		}
		out = append(out, r)
	}
	if !haveDefault {
		out = append(out, synthesizeDefaultDatabase(project))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// UpdateDatabaseDef applies patch to the database record under mask (field
// paths, or "*" for all), honoring an optional etag for optimistic concurrency.
// The read-check-write runs inside the store's atomic UpsertAtomic so two
// concurrent updates cannot lose one another.
func (s *Service) UpdateDatabaseDef(ctx context.Context, project, db string, patch databaseDef, mask []string, etag string) (databaseDef, string, error) {
	if len(mask) == 0 {
		return databaseDef{}, "", gcpInvalidArgument("update_mask is required")
	}
	if db == "" {
		return databaseDef{}, "", gcpInvalidArgument("database name is required")
	}
	updated, err := s.mutateDatabase(ctx, project, db, func(cur databaseDef) (databaseDef, error) {
		if etag != "" && etag != cur.Etag {
			return databaseDef{}, gcpAborted("etag mismatch")
		}
		for _, path := range mask {
			applyDatabaseField(&cur, path, patch)
		}
		cur.UpdateTime = clock.Now()
		cur.Etag = randomHex(16)
		cur.Deleted = false
		return cur, nil
	})
	if err != nil {
		return databaseDef{}, "", err
	}
	return updated, databaseOperationName(project, db, randomHex(12)), nil
}

// mutateDatabase runs load → mutate → store for a database inside the store's
// atomic read-modify-write, so etag/CAS checks cannot race. A deleted record is
// reported NotFound; the implicit (default) database is synthesized when it has
// no record. When no store is attached (unit tests) the mutation runs against
// the synthesized/served record without persisting.
func (s *Service) mutateDatabase(ctx context.Context, project, db string, mutate func(databaseDef) (databaseDef, error)) (databaseDef, error) {
	if s.resources == nil {
		cur, err := s.GetDatabase(ctx, project, db)
		if err != nil {
			return databaseDef{}, err
		}
		return mutate(cur)
	}
	var result databaseDef
	_, err := s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, rtDatabase, databaseRel(db),
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			var cur databaseDef
			switch {
			case exists:
				if err := json.Unmarshal(current.Data, &cur); err != nil {
					return current, model.NewProviderError("Internal", err.Error(), 500)
				}
				if cur.Deleted {
					return current, gcpNotFound("database")
				}
			case db == defaultDatabaseID:
				cur = synthesizeDefaultDatabase(project)
			default:
				return current, gcpNotFound("database")
			}
			next, err := mutate(cur)
			if err != nil {
				return current, err
			}
			data, err := json.Marshal(next)
			if err != nil {
				return current, model.NewProviderError("Internal", err.Error(), 500)
			}
			result = next
			return store.ResourceEntry{Type: rtDatabase, ID: databaseRel(db), Data: data}, nil
		})
	if err != nil {
		return databaseDef{}, err
	}
	return result, nil
}

// applyDatabaseField copies one mutable Database field (by snake_case proto
// field path) from patch into cur. Unknown paths are ignored so a mask carrying
// a field the emulator does not model does not fail the update.
func applyDatabaseField(cur *databaseDef, path string, patch databaseDef) {
	switch path {
	case "*":
		// Apply the mutable fields only; the output-only fields (name, uid,
		// create/update/delete time, etag) stay under the server's control.
		if patch.LocationID != "" {
			cur.LocationID = patch.LocationID
		}
		if patch.Type != "" {
			cur.Type = patch.Type
		}
		if patch.ConcurrencyMode != "" {
			cur.ConcurrencyMode = patch.ConcurrencyMode
		}
		if patch.AppEngineIntegrationMode != "" {
			cur.AppEngineIntegrationMode = patch.AppEngineIntegrationMode
		}
		if patch.PointInTimeRecoveryEnablement != "" {
			cur.PointInTimeRecoveryEnablement = patch.PointInTimeRecoveryEnablement
		}
		if patch.DeleteProtectionState != "" {
			cur.DeleteProtectionState = patch.DeleteProtectionState
		}
		if patch.DatabaseEdition != "" {
			cur.DatabaseEdition = patch.DatabaseEdition
		}
		if patch.KeyPrefix != "" {
			cur.KeyPrefix = patch.KeyPrefix
		}
		if patch.VersionRetentionPeriod > 0 {
			cur.VersionRetentionPeriod = patch.VersionRetentionPeriod
		}
	case "concurrency_mode":
		cur.ConcurrencyMode = patch.ConcurrencyMode
	case "app_engine_integration_mode":
		cur.AppEngineIntegrationMode = patch.AppEngineIntegrationMode
	case "point_in_time_recovery_enablement":
		cur.PointInTimeRecoveryEnablement = patch.PointInTimeRecoveryEnablement
	case "delete_protection_state":
		cur.DeleteProtectionState = patch.DeleteProtectionState
	case "location_id":
		cur.LocationID = patch.LocationID
	case "type":
		cur.Type = patch.Type
	case "database_edition":
		cur.DatabaseEdition = patch.DatabaseEdition
	case "key_prefix":
		cur.KeyPrefix = patch.KeyPrefix
	case "version_retention_period":
		if patch.VersionRetentionPeriod > 0 {
			cur.VersionRetentionPeriod = patch.VersionRetentionPeriod
		}
	}
}

// DeleteDatabaseDef soft-deletes a database record (or tombstones the implicit
// default) and returns the terminal operation name.
func (s *Service) DeleteDatabaseDef(ctx context.Context, project, db, etag string) (string, error) {
	_, err := s.mutateDatabase(ctx, project, db, func(cur databaseDef) (databaseDef, error) {
		if etag != "" && etag != cur.Etag {
			return databaseDef{}, gcpAborted("etag mismatch")
		}
		cur.Deleted = true
		cur.DeleteTime = clock.Now()
		cur.UpdateTime = cur.DeleteTime
		cur.Etag = randomHex(16)
		return cur, nil
	})
	if err != nil {
		return "", err
	}
	return databaseOperationName(project, db, randomHex(12)), nil
}

// ─── Fields ───────────────────────────────────────────────────────────────────

// GetField returns a collection-group field record.
func (s *Service) GetField(ctx context.Context, project, db, cg, fieldPath string) (fieldDef, error) {
	if fieldPath == "" {
		return fieldDef{}, gcpInvalidArgument("field path is required")
	}
	var f fieldDef
	if err := s.getResource(ctx, project, rtField, fieldRel(db, cg, fieldPath), &f); err != nil {
		return fieldDef{}, err
	}
	return f, nil
}

// ListFields returns the configured collection-group fields for a database,
// optionally scoped to one collection group (cg == "-" is the wildcard) and
// filtered by a substring of the field name, honoring page_size/page_token.
func (s *Service) ListFields(ctx context.Context, project, db, cg, filter string, page pageParams) ([]fieldDef, string, error) {
	prefix := "databases/" + db + "/collectionGroups/"
	if cg != "" && cg != "-" {
		prefix = "databases/" + db + "/collectionGroups/" + cg + "/fields/"
	}
	fields, err := listResources[fieldDef](ctx, s, project, rtField, prefix)
	if err != nil {
		return nil, "", err
	}
	if filter != "" {
		kept := fields[:0]
		for _, f := range fields {
			if fieldMatchesFilter(f, filter) {
				kept = append(kept, f)
			}
		}
		fields = kept
	}
	pageFields, nextToken := paging.Page(fields, func(f fieldDef) string { return f.Name }, page.params())
	return pageFields, nextToken, nil
}

// fieldMatchesFilter evaluates the subset of the documented ListFields filter
// expressions the emulator models — conjunctive (" AND ") clauses over
// indexConfig / ttlConfig — and falls back to a substring match on the field
// name for an unrecognized clause.
func fieldMatchesFilter(f fieldDef, filter string) bool {
	for _, clause := range strings.Split(filter, " AND ") {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		switch {
		case strings.HasPrefix(clause, "ttlConfig"):
			if f.TtlConfig == nil {
				return false
			}
		case strings.Contains(clause, "usesAncestorConfig:false"):
			if f.IndexConfig == nil || f.IndexConfig.UsesAncestorConfig {
				return false
			}
		case strings.Contains(clause, "usesAncestorConfig:true"):
			if f.IndexConfig == nil || !f.IndexConfig.UsesAncestorConfig {
				return false
			}
		default:
			if !strings.Contains(f.Name, clause) {
				return false
			}
		}
	}
	return true
}

// UpdateFieldDef creates or updates a field's index/ttl configuration under
// mask, returning the stored field and the terminal operation name.
func (s *Service) UpdateFieldDef(ctx context.Context, project, db, cg, fieldPath string, patch fieldDef, mask []string) (fieldDef, string, error) {
	if fieldPath == "" {
		return fieldDef{}, "", gcpInvalidArgument("field path is required")
	}
	if len(mask) == 0 {
		return fieldDef{}, "", gcpInvalidArgument("update_mask is required")
	}
	cur := fieldDef{Name: fieldName(project, db, cg, fieldPath)}
	var existing fieldDef
	if err := s.getResource(ctx, project, rtField, fieldRel(db, cg, fieldPath), &existing); err == nil {
		cur = existing
	} else if !isNotFound(err) {
		return fieldDef{}, "", err
	}
	for _, path := range mask {
		applyFieldMask(&cur, path, patch)
	}
	cur.Name = fieldName(project, db, cg, fieldPath)
	if err := s.putResource(ctx, project, rtField, fieldRel(db, cg, fieldPath), cur, false); err != nil {
		return fieldDef{}, "", err
	}
	return cur, databaseOperationName(project, db, randomHex(12)), nil
}

// applyFieldMask copies one Field configuration by proto field path.
func applyFieldMask(cur *fieldDef, path string, patch fieldDef) {
	switch path {
	case "*", "index_config":
		if patch.IndexConfig != nil {
			cur.IndexConfig = patch.IndexConfig
		}
	case "ttl_config":
		if patch.TtlConfig != nil {
			cur.TtlConfig = patch.TtlConfig
		}
	case "index_config.indexes":
		if patch.IndexConfig != nil {
			cur.IndexConfig = &fieldIndexConfig{
				Indexes:            patch.IndexConfig.Indexes,
				UsesAncestorConfig: false,
				Reverting:          false,
			}
		}
	case "index_config.uses_ancestor_config":
		if patch.IndexConfig != nil {
			ensureIndexConfig(cur)
			cur.IndexConfig.UsesAncestorConfig = patch.IndexConfig.UsesAncestorConfig
		}
	case "index_config.ancestor_field":
		if patch.IndexConfig != nil {
			ensureIndexConfig(cur)
			cur.IndexConfig.AncestorField = patch.IndexConfig.AncestorField
		}
	case "index_config.reverting":
		if patch.IndexConfig != nil {
			ensureIndexConfig(cur)
			cur.IndexConfig.Reverting = patch.IndexConfig.Reverting
		}
	case "ttl_config.state":
		if patch.TtlConfig != nil {
			ensureTtlConfig(cur)
			cur.TtlConfig.State = patch.TtlConfig.State
		}
	case "ttl_config.expiration_offset":
		if patch.TtlConfig != nil {
			ensureTtlConfig(cur)
			cur.TtlConfig.ExpirationOffset = patch.TtlConfig.ExpirationOffset
		}
	}
}

func ensureIndexConfig(f *fieldDef) {
	if f.IndexConfig == nil {
		f.IndexConfig = &fieldIndexConfig{}
	}
}

func ensureTtlConfig(f *fieldDef) {
	if f.TtlConfig == nil {
		f.TtlConfig = &fieldTtlConfig{}
	}
}

// ─── User creds ───────────────────────────────────────────────────────────────

// CreateUserCredsDef creates a user creds record and returns it with a
// freshly-minted secret password.
func (s *Service) CreateUserCredsDef(ctx context.Context, project, db, id string, uc userCredsDef) (userCredsDef, error) {
	if id == "" {
		id = randomHex(20)
	}
	uc.Name = userCredsName(project, db, id)
	now := clock.Now()
	uc.CreateTime, uc.UpdateTime = now, now
	if uc.State == "" {
		uc.State = "ENABLED"
	}
	uc.Principal = id + "@" + project + ".iam.gserviceaccount.com"
	uc.SecretPassword = randomPassword()
	if err := s.putResource(ctx, project, rtUserCreds, userCredsRel(db, id), uc, true); err != nil {
		return userCredsDef{}, err
	}
	return uc, nil
}

// GetUserCreds returns a user creds record without its secret password.
func (s *Service) GetUserCreds(ctx context.Context, project, db, id string) (userCredsDef, error) {
	var uc userCredsDef
	if err := s.getResource(ctx, project, rtUserCreds, userCredsRel(db, id), &uc); err != nil {
		return userCredsDef{}, err
	}
	uc.SecretPassword = ""
	return uc, nil
}

// ListUserCreds returns every user creds record in a database, without secrets.
func (s *Service) ListUserCreds(ctx context.Context, project, db string) ([]userCredsDef, error) {
	ucs, err := listResources[userCredsDef](ctx, s, project, rtUserCreds, "databases/"+db+"/userCreds/")
	if err != nil {
		return nil, err
	}
	for i := range ucs {
		ucs[i].SecretPassword = ""
	}
	sort.Slice(ucs, func(i, j int) bool { return ucs[i].Name < ucs[j].Name })
	return ucs, nil
}

// SetUserCredsState enables or disables a user creds record.
func (s *Service) SetUserCredsState(ctx context.Context, project, db, id, state string) (userCredsDef, error) {
	var uc userCredsDef
	if err := s.getResource(ctx, project, rtUserCreds, userCredsRel(db, id), &uc); err != nil {
		return userCredsDef{}, err
	}
	uc.State = state
	uc.UpdateTime = clock.Now()
	if err := s.putResource(ctx, project, rtUserCreds, userCredsRel(db, id), uc, false); err != nil {
		return userCredsDef{}, err
	}
	uc.SecretPassword = ""
	return uc, nil
}

// ResetUserPassword rotates a user creds secret and returns it once.
func (s *Service) ResetUserPassword(ctx context.Context, project, db, id string) (userCredsDef, error) {
	var uc userCredsDef
	if err := s.getResource(ctx, project, rtUserCreds, userCredsRel(db, id), &uc); err != nil {
		return userCredsDef{}, err
	}
	uc.SecretPassword = randomPassword()
	uc.UpdateTime = clock.Now()
	if err := s.putResource(ctx, project, rtUserCreds, userCredsRel(db, id), uc, false); err != nil {
		return userCredsDef{}, err
	}
	return uc, nil
}

// DeleteUserCreds removes a user creds record.
func (s *Service) DeleteUserCreds(ctx context.Context, project, db, id string) error {
	if s.resources == nil {
		return nil
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtUserCreds, userCredsRel(db, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound("user creds")
		}
		return err
	}
	return nil
}

// ─── Backup schedules ─────────────────────────────────────────────────────────

// CreateBackupScheduleDef creates a backup schedule record.
func (s *Service) CreateBackupScheduleDef(ctx context.Context, project, db, id string, bs backupScheduleDef) (backupScheduleDef, error) {
	if id == "" {
		id = "1"
	}
	bs.Name = backupScheduleName(project, db, id)
	now := clock.Now()
	bs.CreateTime, bs.UpdateTime = now, now
	if err := s.putResource(ctx, project, rtBackupSchedule, backupScheduleRel(db, id), bs, true); err != nil {
		return backupScheduleDef{}, err
	}
	return bs, nil
}

// GetBackupSchedule returns a backup schedule record.
func (s *Service) GetBackupSchedule(ctx context.Context, project, db, id string) (backupScheduleDef, error) {
	var bs backupScheduleDef
	if err := s.getResource(ctx, project, rtBackupSchedule, backupScheduleRel(db, id), &bs); err != nil {
		return backupScheduleDef{}, err
	}
	return bs, nil
}

// ListBackupSchedules returns every backup schedule for a database.
func (s *Service) ListBackupSchedules(ctx context.Context, project, db string) ([]backupScheduleDef, error) {
	bss, err := listResources[backupScheduleDef](ctx, s, project, rtBackupSchedule, "databases/"+db+"/backupSchedules/")
	if err != nil {
		return nil, err
	}
	sort.Slice(bss, func(i, j int) bool { return bss[i].Name < bss[j].Name })
	return bss, nil
}

// UpdateBackupScheduleDef applies patch to a backup schedule under mask.
func (s *Service) UpdateBackupScheduleDef(ctx context.Context, project, db, id string, patch backupScheduleDef, mask []string) (backupScheduleDef, error) {
	if len(mask) == 0 {
		return backupScheduleDef{}, gcpInvalidArgument("update_mask is required")
	}
	cur, err := s.GetBackupSchedule(ctx, project, db, id)
	if err != nil {
		return backupScheduleDef{}, err
	}
	for _, path := range mask {
		switch path {
		case "*", "retention":
			if patch.Retention > 0 {
				cur.Retention = patch.Retention
			}
		case "daily_recurrence":
			if patch.Recurrence == "DAILY" {
				cur.Recurrence, cur.DayOfWeek = "DAILY", 0
			}
		case "weekly_recurrence":
			if patch.Recurrence == "WEEKLY" {
				cur.Recurrence, cur.DayOfWeek = "WEEKLY", patch.DayOfWeek
			}
		}
	}
	cur.UpdateTime = clock.Now()
	if err := s.putResource(ctx, project, rtBackupSchedule, backupScheduleRel(db, id), cur, false); err != nil {
		return backupScheduleDef{}, err
	}
	return cur, nil
}

// DeleteBackupSchedule removes a backup schedule record.
func (s *Service) DeleteBackupSchedule(ctx context.Context, project, db, id string) error {
	if s.resources == nil {
		return nil
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtBackupSchedule, backupScheduleRel(db, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound("backup schedule")
		}
		return err
	}
	return nil
}

// ─── Backups ──────────────────────────────────────────────────────────────────

// GetBackup returns a backup record.
func (s *Service) GetBackup(ctx context.Context, project, loc, id string) (backupDef, error) {
	var b backupDef
	if err := s.getResource(ctx, project, rtBackup, backupRel(loc, id), &b); err != nil {
		return backupDef{}, err
	}
	return b, nil
}

// ListBackups returns the backups in a location, filtered by a substring of the
// backup resource name.
func (s *Service) ListBackups(ctx context.Context, project, loc, filter string) ([]backupDef, error) {
	backups, err := listResources[backupDef](ctx, s, project, rtBackup, "locations/"+loc+"/backups/")
	if err != nil {
		return nil, err
	}
	if filter != "" {
		kept := backups[:0]
		for _, b := range backups {
			if strings.Contains(b.Name, filter) {
				kept = append(kept, b)
			}
		}
		backups = kept
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].Name < backups[j].Name })
	return backups, nil
}

// DeleteBackup removes a backup record.
func (s *Service) DeleteBackup(ctx context.Context, project, loc, id string) error {
	if s.resources == nil {
		return gcpNotFound("backup")
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtBackup, backupRel(loc, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound("backup")
		}
		return err
	}
	return nil
}

// isNotFound reports whether err is the provider NotFound error.
func isNotFound(err error) bool {
	var perr *model.ProviderError
	if errors.As(err, &perr) {
		return perr.Code == "NotFound" || perr.HTTPStatus == 404
	}
	return false
}
