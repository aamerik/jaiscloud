package hms

import hmsstore "jaiscloud/internal/gcp/store/hms"

// Field IDs and enum values derived from hive_metastore.thrift (Apache Hive
// branch-2.3, metastore/if/hive_metastore.thrift). Only the fields the serving
// plane actually reads/builds are named here; the codec itself is schema-free,
// so every other field round-trips generically.

// Database fields.
const (
	dbName        int16 = 1
	dbDescription int16 = 2
	dbLocationURI int16 = 3
	dbParameters  int16 = 4
	dbOwnerName   int16 = 6
	dbOwnerType   int16 = 7
)

// Table fields.
const (
	tblTableName int16 = 1
	tblDBName    int16 = 2
)

// LockComponent fields.
const (
	lcDBName    int16 = 3
	lcTableName int16 = 4
)

// LockRequest fields.
const (
	lrComponent int16 = 1
	lrUser      int16 = 3
	lrHostname  int16 = 4
)

// LockResponse fields.
const (
	lockRespLockID int16 = 1
	lockRespState  int16 = 2
)

// Notification structs.
const (
	notifEventID int16 = 1
	notifEvents  int16 = 1
)

// PrincipalType enum.
const (
	principalTypeUser  int32 = 1
	principalTypeRole  int32 = 2
	principalTypeGroup int32 = 3
)

// buildDatabaseStruct renders a store Database as a wire Database struct,
// emitting fields in ascending field-ID order (1,2,3,4,6,7). ownerType defaults
// to USER; privileges and catalogName are not persisted and are omitted.
func buildDatabaseStruct(db hmsstore.Database) *Struct {
	b := NewBuilder().Str(dbName, db.Name)
	if db.Description != "" {
		b.Str(dbDescription, db.Description)
	}
	if db.LocationURI != "" {
		b.Str(dbLocationURI, db.LocationURI)
	}
	b.MapStrStr(dbParameters, db.Parameters)
	if db.Owner != "" {
		b.Str(dbOwnerName, db.Owner)
		b.I32(dbOwnerType, principalTypeUser)
	}
	return b.Build()
}

// buildLockResponse renders (lockid, state) as a LockResponse struct.
func buildLockResponse(lockID int64, state hmsstore.LockState) *Struct {
	return NewBuilder().
		I64(lockRespLockID, lockID).
		I32(lockRespState, int32(state)).
		Build()
}
