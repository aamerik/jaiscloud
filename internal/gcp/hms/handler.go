package hms

import (
	"context"
	"regexp"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// methodHandler decodes a method's args struct and returns its result struct.
// A nil result with a nil error is a void success (encoded as an empty result
// struct). A non-nil *DeclaredError is encoded as an exception field in the
// result struct; a non-nil *AppError (or any other error) becomes a
// TApplicationException.
type methodHandler func(ctx context.Context, args *Struct) (*Struct, error)

func result0(v Value) *Struct { return &Struct{Fields: []Field{{ID: 0, V: v}}} }
func voidResult() *Struct     { return &Struct{} }

// declared builds a *DeclaredError at the given result-struct position.
func declared(fieldID int16, name, msg string) error {
	return &DeclaredError{FieldID: fieldID, Name: name, Message: msg}
}

// matchName reports whether name matches pattern, which Hive's
// get_databases/get_tables treat as a regular expression. Empty and "*"
// patterns match everything; a pattern that fails to compile falls back to a
// literal substring match.
func matchName(pattern, name string) bool {
	if pattern == "" || pattern == "*" || pattern == ".*" {
		return true
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return strings.Contains(name, pattern)
	}
	return re.MatchString(name)
}

// --- Databases ---

func (s *Server) createDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.Struct(1)
	name := db.String(dbName)
	if name == "" {
		return nil, declared(2, excInvalidObjectException, "database name cannot be empty")
	}
	if err := s.store.CreateDatabase(ctx, hmsstore.Database{
		Name:        name,
		Description: db.String(dbDescription),
		LocationURI: db.String(dbLocationURI),
		Parameters:  db.MapStrStr(dbParameters),
		Owner:       db.String(dbOwnerName),
	}); err != nil {
		switch {
		case err == hmsstore.ErrDatabaseExists:
			return nil, declared(1, excAlreadyExistsException, "database "+name+" already exists")
		default:
			return nil, declared(3, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

func (s *Server) getDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(1)
	db, err := s.store.GetDatabase(ctx, name)
	if err != nil {
		switch {
		case err == hmsstore.ErrDatabaseNotFound:
			return nil, declared(1, excNoSuchObjectException, "database "+name+" not found")
		default:
			return nil, declared(2, excMetaException, err.Error())
		}
	}
	return result0(StructV(buildDatabaseStruct(db))), nil
}

func (s *Server) dropDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(1)
	cascade := args.Bool(3)
	if err := s.store.DropDatabase(ctx, name, cascade); err != nil {
		switch {
		case err == hmsstore.ErrDatabaseNotFound:
			return nil, declared(1, excNoSuchObjectException, "database "+name+" not found")
		case err == hmsstore.ErrDatabaseNotEmpty:
			return nil, declared(2, excInvalidOperationException, "database "+name+" is not empty")
		default:
			return nil, declared(3, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

func (s *Server) listDatabases(ctx context.Context, args *Struct) (*Struct, error) {
	pattern := args.String(1)
	names, err := s.store.ListDatabases(ctx)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if matchName(pattern, n) {
			filtered = append(filtered, n)
		}
	}
	b := NewBuilder()
	b.ListStr(0, filtered)
	return b.Build(), nil
}

func (s *Server) alterDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(1)
	db := args.Struct(2)
	if err := s.store.AlterDatabase(ctx, name, hmsstore.Database{
		Name:        name,
		Description: db.String(dbDescription),
		LocationURI: db.String(dbLocationURI),
		Parameters:  db.MapStrStr(dbParameters),
		Owner:       db.String(dbOwnerName),
	}); err != nil {
		switch {
		case err == hmsstore.ErrDatabaseNotFound:
			return nil, declared(2, excNoSuchObjectException, "database "+name+" not found")
		default:
			return nil, declared(1, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

// --- Tables ---

// tableJSON encodes a decoded Table struct as canonical JSON and returns its
// (dbName, tableName, json) plus an error for an invalid object.
func tableJSON(t *Struct) (db, tbl string, raw []byte, err error) {
	if t == nil {
		return "", "", nil, declared(2, excInvalidObjectException, "table is null")
	}
	db = t.String(tblDBName)
	tbl = t.String(tblTableName)
	raw, err = MarshalStruct(t)
	if err != nil {
		return "", "", nil, declared(3, excMetaException, err.Error())
	}
	return db, tbl, raw, nil
}

func (s *Server) createTable(ctx context.Context, args *Struct) (*Struct, error) {
	db, tbl, raw, err := tableJSON(args.Struct(1))
	if err != nil {
		if de, ok := err.(*DeclaredError); ok {
			return nil, de
		}
		return nil, declared(3, excMetaException, err.Error())
	}
	if tbl == "" {
		return nil, declared(2, excInvalidObjectException, "table name cannot be empty")
	}
	if db == "" {
		db = "default"
	}
	if serr := s.store.CreateTable(ctx, db, tbl, hmsstore.Table{DBName: db, TableName: tbl, TableJSON: raw}); serr != nil {
		switch {
		case serr == hmsstore.ErrTableExists:
			return nil, declared(1, excAlreadyExistsException, "table "+db+"."+tbl+" already exists")
		case serr == hmsstore.ErrDatabaseNotFound:
			return nil, declared(4, excNoSuchObjectException, "database "+db+" not found")
		default:
			return nil, declared(3, excMetaException, serr.Error())
		}
	}
	return voidResult(), nil
}

func (s *Server) getTable(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	t, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		switch {
		case err == hmsstore.ErrTableNotFound:
			return nil, declared(2, excNoSuchObjectException, "table "+db+"."+tbl+" not found")
		default:
			return nil, declared(1, excMetaException, err.Error())
		}
	}
	st, err := UnmarshalStruct(t.TableJSON)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	return result0(StructV(st)), nil
}

func (s *Server) listTables(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	pattern := args.String(2)
	names, err := s.store.ListTables(ctx, db)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if matchName(pattern, n) {
			filtered = append(filtered, n)
		}
	}
	b := NewBuilder()
	b.ListStr(0, filtered)
	return b.Build(), nil
}

// getTableObjectsByName implements get_table_objects_by_name(dbname,
// list<string>) -> list<Table>. It is not in D3's enumerated subset but is on
// the real Iceberg HiveCatalog.listTables critical path (verified Iceberg
// 1.5.2 HiveCatalog.java:133), so it is implemented rather than stubbed.
func (s *Server) getTableObjectsByName(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	names := args.List(2)
	var tables []*Struct
	for _, n := range names {
		if n.T != thrift.STRING {
			continue
		}
		t, err := s.store.GetTable(ctx, db, n.Str)
		if err != nil {
			if err == hmsstore.ErrTableNotFound {
				continue // tolerate missing tables (the client filters them)
			}
			return nil, declared(1, excMetaException, err.Error())
		}
		st, err := UnmarshalStruct(t.TableJSON)
		if err != nil {
			return nil, declared(1, excMetaException, err.Error())
		}
		tables = append(tables, st)
	}
	b := NewBuilder()
	b.ListStruct(0, tables)
	return b.Build(), nil
}

// alterTable handles alter_table/alter_table_with_environment_context: a plain
// overwrite of the stored Table (D5). When the incoming Table's dbName or
// tableName differs from the addressed (db, tbl), it is a rename (the real Hive
// rename path — Iceberg's renameTable drives alter_table with a renamed Table).
func (s *Server) alterTable(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	newDB, newTbl, raw, err := tableJSON(args.Struct(3))
	if err != nil {
		if de, ok := err.(*DeclaredError); ok {
			return nil, de
		}
		return nil, declared(2, excMetaException, err.Error())
	}
	if newTbl == "" {
		newTbl = tbl
	}
	if newDB == "" {
		newDB = db
	}

	if newDB != db || newTbl != tbl {
		_, rerr := s.store.RenameTable(ctx, db, tbl, newDB, newTbl, hmsstore.Table{DBName: newDB, TableName: newTbl, TableJSON: raw})
		if rerr != nil {
			switch {
			case rerr == hmsstore.ErrTableNotFound:
				return nil, declared(1, excInvalidOperationException, "table "+db+"."+tbl+" not found")
			case rerr == hmsstore.ErrDatabaseNotFound:
				return nil, declared(1, excInvalidOperationException, "database "+newDB+" not found")
			case rerr == hmsstore.ErrTableExists:
				return nil, declared(1, excInvalidOperationException, "new table "+newDB+"."+newTbl+" already exists")
			default:
				return nil, declared(2, excMetaException, rerr.Error())
			}
		}
		return voidResult(), nil
	}

	_, aerr := s.store.AlterTable(ctx, db, tbl, func(current hmsstore.Table) (hmsstore.Table, error) {
		// Plain unconditional overwrite (D5): the lock is the only concurrency
		// control, matching real Hive alter_table.
		return hmsstore.Table{DBName: db, TableName: tbl, TableJSON: raw}, nil
	})
	if aerr != nil {
		switch {
		case aerr == hmsstore.ErrTableNotFound:
			return nil, declared(1, excInvalidOperationException, "table "+db+"."+tbl+" not found")
		default:
			return nil, declared(2, excMetaException, aerr.Error())
		}
	}
	return voidResult(), nil
}

func (s *Server) dropTable(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	if err := s.store.DropTable(ctx, db, tbl); err != nil {
		switch {
		case err == hmsstore.ErrTableNotFound:
			return nil, declared(1, excNoSuchObjectException, "table "+db+"."+tbl+" not found")
		default:
			return nil, declared(2, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

// --- Functions (stubbed: no function store) ---

func (s *Server) createFunction(_ context.Context, _ *Struct) (*Struct, error) {
	return voidResult(), nil
}

func (s *Server) dropFunction(_ context.Context, _ *Struct) (*Struct, error) {
	return voidResult(), nil
}

func (s *Server) getFunctions(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(ListV(thrift.STRING, nil)), nil
}

func (s *Server) getFunction(_ context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	fn := args.String(2)
	return nil, declared(2, excNoSuchObjectException, "function "+db+"."+fn+" not found")
}

func (s *Server) getAllFunctions(_ context.Context, _ *Struct) (*Struct, error) {
	// GetAllFunctionsResponse {1: optional list<Function> functions} — omit the
	// field so the client observes an empty registry.
	return result0(StructV(&Struct{})), nil
}

// --- Locks ---

func (s *Server) lock(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	l := hmsstore.Lock{User: req.String(lrUser), Hostname: req.String(lrHostname)}
	if comps := req.List(lrComponent); len(comps) > 0 {
		if c := comps[0].S; c != nil {
			l.DBName = c.String(lcDBName)
			l.TableName = c.String(lcTableName)
		}
	}
	id, err := s.store.Lock(ctx, l)
	if err != nil {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: err.Error()}
	}
	return result0(StructV(buildLockResponse(id, hmsstore.LockStateAcquired))), nil
}

func (s *Server) checkLock(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	id := req.I64(1)
	state, err := s.store.CheckLock(ctx, id)
	if err != nil {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: err.Error()}
	}
	return result0(StructV(buildLockResponse(id, state))), nil
}

func (s *Server) unlock(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	id := req.I64(1)
	if err := s.store.Unlock(ctx, id); err != nil {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: err.Error()}
	}
	return voidResult(), nil
}

// --- Notifications (stubbed: valid empty, never an error — F10) ---

func (s *Server) getCurrentNotificationEventID(_ context.Context, _ *Struct) (*Struct, error) {
	// CurrentNotificationEventId {1: required i64 eventId}.
	return result0(StructV(NewBuilder().I64(notifEventID, 0).Build())), nil
}

func (s *Server) getNextNotification(_ context.Context, _ *Struct) (*Struct, error) {
	// NotificationEventResponse {1: required list<NotificationEvent> events}.
	return result0(StructV(NewBuilder().Add(notifEvents, ListV(thrift.STRUCT, nil)).Build())), nil
}

// --- Partition stubs (D3: return empty / NoSuchObjectException, never crash) ---

// partitionNotFound is the shared NoSuchObjectException for single-partition
// get/add methods.
func partitionNotFound(msg string) error {
	return declared(2, excNoSuchObjectException, msg)
}

func (s *Server) stubPartitionNotFound(_ context.Context, args *Struct) (*Struct, error) {
	return nil, partitionNotFound("partition not found")
}

func (s *Server) stubPartitionList(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(ListV(thrift.STRUCT, nil)), nil
}

func (s *Server) stubPartitionNameList(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(ListV(thrift.STRING, nil)), nil
}

func (s *Server) stubPartitionBoolFalse(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(BoolV(false)), nil
}

func (s *Server) stubPartitionBoolTrue(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(BoolV(true)), nil
}

func (s *Server) stubPartitionI32Zero(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(I32V(0)), nil
}

func (s *Server) stubPartitionVoid(_ context.Context, _ *Struct) (*Struct, error) {
	return voidResult(), nil
}

func (s *Server) stubPartitionNameSpec(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(MapV(thrift.STRING, thrift.STRING, nil)), nil
}

// --- Unsupported (honest failure, not a silent ACK — F10) ---

// unsupportedMethod returns a TApplicationException(INTERNAL_ERROR) for methods
// that are out of scope (ACID txn, heartbeat, Hive-3.x table-meta, ...). A
// TApplicationException (msg type EXCEPTION) is used rather than a declared
// exception so the failure is unambiguous even for methods with no throws
// clause (get_open_txns, etc.).
func (s *Server) unsupportedMethod(_ context.Context, _ *Struct) (*Struct, error) {
	return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: "method not supported by the emulator"}
}

func (s *Server) heartbeat(_ context.Context, _ *Struct) (*Struct, error) {
	return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: "heartbeat (ACID transaction locks) is not supported"}
}
