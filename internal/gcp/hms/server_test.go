package hms

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// --- test server harness ---

func startServer(t *testing.T, store hmsstore.Store) (addr string, stop func()) {
	t.Helper()
	srv := NewServer("127.0.0.1:0", store)
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	return srv.Addr().String(), func() { srv.Stop(); <-done }
}

// --- raw Thrift client helper ---

type reply struct {
	name    string
	msgType thrift.TMessageType
	result  *Struct
}

// hmsCall dials addr, sends one CALL for method with args, and reads the reply.
func hmsCall(t *testing.T, addr, method string, args *Struct) reply {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	sock := thrift.NewTSocketFromConnTimeout(conn, 2*time.Second)
	proto := thrift.NewTBinaryProtocolTransport(sock)
	ctx := context.Background()

	if err := proto.WriteMessageBegin(ctx, method, thrift.CALL, 1); err != nil {
		t.Fatalf("write msg begin: %v", err)
	}
	if err := WriteStruct(ctx, proto, args); err != nil {
		t.Fatalf("write args: %v", err)
	}
	if err := proto.WriteMessageEnd(ctx); err != nil {
		t.Fatalf("write msg end: %v", err)
	}
	if err := proto.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	name, msgType, _, err := proto.ReadMessageBegin(ctx)
	if err != nil {
		t.Fatalf("read msg begin: %v", err)
	}
	result, err := ReadStruct(ctx, proto)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if err := proto.ReadMessageEnd(ctx); err != nil {
		t.Fatalf("read msg end: %v", err)
	}
	return reply{name: name, msgType: msgType, result: result}
}

func strList(vs []string) []Value {
	out := make([]Value, 0, len(vs))
	for _, v := range vs {
		out = append(out, StringV(v))
	}
	return out
}

// testTable builds a realistic Iceberg Hive Table struct.
func testTable(db, tbl, metadataLocation string) *Struct {
	serde := NewBuilder().
		Str(1, tbl).
		Str(2, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe").
		MapStrStr(3, map[string]string{"serialization.format": "1"}).
		Build()
	sd := NewBuilder().
		ListStruct(1, nil).
		Str(2, "gs://bucket/wh/"+db+"/"+tbl).
		Str(3, "org.apache.hadoop.mapred.FileInputFormat").
		Str(4, "org.apache.hadoop.mapred.FileOutputFormat").
		Bool(5, false).
		I32(6, 0).
		Struct(7, serde).
		ListStr(8, nil).
		ListStruct(9, nil).
		MapStrStr(10, nil).
		Bool(12, false).
		Build()
	return NewBuilder().
		Str(tblTableName, tbl).
		Str(tblDBName, db).
		Str(3, "spark").
		I32(4, 1700000000).
		I32(5, 0).
		I32(6, 0).
		Struct(7, sd).
		ListStruct(8, nil).
		MapStrStr(9, map[string]string{
			"metadata_location": metadataLocation,
			"table_type":        "ICEBERG",
		}).
		Str(10, "").
		Str(11, "").
		Str(12, "EXTERNAL_TABLE").
		Bool(14, false).
		Bool(15, false).
		Build()
}

func TestServerEndToEnd(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	// create_database
	createDB := NewBuilder().Struct(1, NewBuilder().
		Str(dbName, "default").
		Str(dbLocationURI, "gs://bucket/wh/default").
		Build()).Build()
	r := hmsCall(t, addr, "create_database", createDB)
	if r.msgType != thrift.REPLY {
		t.Fatalf("create_database msgType=%d result=%+v", r.msgType, r.result)
	}

	// create_database again -> AlreadyExistsException (field 1)
	r = hmsCall(t, addr, "create_database", createDB)
	if r.msgType != thrift.REPLY {
		t.Fatalf("create_database dup msgType=%d", r.msgType)
	}
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 1 || f[0].V.T != thrift.STRUCT {
		t.Fatalf("expected AlreadyExistsException field 1, got %+v", f)
	}

	// get_database
	getDB := NewBuilder().Str(1, "default").Build()
	r = hmsCall(t, addr, "get_database", getDB)
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_database msgType=%d", r.msgType)
	}
	dbStruct := r.result.Struct(0)
	if dbStruct == nil || dbStruct.String(dbName) != "default" || dbStruct.String(dbLocationURI) != "gs://bucket/wh/default" {
		t.Fatalf("get_database wrong: %+v", r.result)
	}

	// create_table
	ml1 := "gs://bucket/wh/default/t1/metadata/00001.metadata.json"
	createTbl := NewBuilder().Struct(1, testTable("default", "t1", ml1)).Build()
	r = hmsCall(t, addr, "create_table", createTbl)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("create_table failed: type=%d result=%+v", r.msgType, r.result)
	}

	// get_table
	getTbl := NewBuilder().Str(1, "default").Str(2, "t1").Build()
	r = hmsCall(t, addr, "get_table", getTbl)
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_table msgType=%d", r.msgType)
	}
	gotTbl := r.result.Struct(0)
	if gotTbl == nil {
		t.Fatal("get_table returned no table")
	}
	if gotTbl.String(tblTableName) != "t1" || gotTbl.String(tblDBName) != "default" {
		t.Fatalf("get_table identity wrong: %+v", gotTbl)
	}
	if m := gotTbl.MapStrStr(9); m["metadata_location"] != ml1 {
		t.Fatalf("get_table params lost metadata_location: %v", m)
	}
	// the round-tripped table must echo every field, incl. sd.serdeInfo.
	if sd := gotTbl.Struct(7); sd == nil || sd.String(3) != "org.apache.hadoop.mapred.FileInputFormat" {
		t.Fatalf("get_table sd lost: %+v", sd)
	}

	// get_all_tables
	r = hmsCall(t, addr, "get_all_tables", NewBuilder().Str(1, "default").Build())
	if names := r.result.List(0); len(names) != 1 || names[0].Str != "t1" {
		t.Fatalf("get_all_tables wrong: %+v", r.result)
	}

	// get_table_objects_by_name (Iceberg listTables critical path)
	r = hmsCall(t, addr, "get_table_objects_by_name", NewBuilder().Str(1, "default").Add(2, ListV(thrift.STRING, strList([]string{"t1"}))).Build())
	if tables := r.result.List(0); len(tables) != 1 || tables[0].S == nil || tables[0].S.String(tblTableName) != "t1" {
		t.Fatalf("get_table_objects_by_name wrong: %+v", r.result)
	}

	// alter_table — full-Table overwrite with a new metadata_location.
	ml2 := "gs://bucket/wh/default/t1/metadata/00002.metadata.json"
	alter := NewBuilder().Str(1, "default").Str(2, "t1").Struct(3, testTable("default", "t1", ml2)).Build()
	r = hmsCall(t, addr, "alter_table", alter)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("alter_table failed: type=%d result=%+v", r.msgType, r.result)
	}
	r = hmsCall(t, addr, "get_table", getTbl)
	if m := r.result.Struct(0).MapStrStr(9); m["metadata_location"] != ml2 {
		t.Fatalf("alter_table did not overwrite metadata_location: %v", m)
	}

	// rename via alter_table (new table name in the Table struct).
	renamed := testTable("default", "t1_renamed", ml2)
	alter = NewBuilder().Str(1, "default").Str(2, "t1").Struct(3, renamed).Build()
	r = hmsCall(t, addr, "alter_table", alter)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("rename via alter_table failed: type=%d result=%+v", r.msgType, r.result)
	}
	r = hmsCall(t, addr, "get_table", NewBuilder().Str(1, "default").Str(2, "t1_renamed").Build())
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("renamed table not found: %+v", r.result)
	}

	// drop_table
	dropTbl := NewBuilder().Str(1, "default").Str(2, "t1_renamed").Bool(3, false).Build()
	r = hmsCall(t, addr, "drop_table", dropTbl)
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("drop_table failed: %+v", r.result)
	}
	r = hmsCall(t, addr, "get_table", NewBuilder().Str(1, "default").Str(2, "t1_renamed").Build())
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 2 { // NoSuchObjectException is field 2
		t.Fatalf("expected NoSuchObjectException after drop, got %+v", f)
	}
}

func TestServerLocks(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	// lock
	comp := NewBuilder().I32(1, 3).I32(2, 2).Str(3, "default").Str(4, "tbl").Build()
	req := NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(comp)})).Str(3, "spark").Str(4, "host").Build()
	r := hmsCall(t, addr, "lock", NewBuilder().Struct(1, req).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("lock msgType=%d", r.msgType)
	}
	resp := r.result.Struct(0)
	if resp == nil {
		t.Fatal("lock returned no LockResponse")
	}
	lockID := resp.I64(lockRespLockID)
	if lockID == 0 {
		t.Fatal("lock returned lockid 0")
	}
	if resp.I32(lockRespState) != int32(hmsstore.LockStateAcquired) {
		t.Fatalf("lock state = %d, want ACQUIRED", resp.I32(lockRespState))
	}

	// check_lock -> ACQUIRED
	check := NewBuilder().I64(1, lockID).Build()
	r = hmsCall(t, addr, "check_lock", NewBuilder().Struct(1, check).Build())
	if resp := r.result.Struct(0); resp == nil || resp.I32(lockRespState) != int32(hmsstore.LockStateAcquired) {
		t.Fatalf("check_lock wrong: %+v", r.result)
	}

	// unlock
	unlock := NewBuilder().I64(1, lockID).Build()
	r = hmsCall(t, addr, "unlock", NewBuilder().Struct(1, unlock).Build())
	if r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("unlock failed: %+v", r.result)
	}

	// check_lock after unlock -> NOT_ACQUIRED (absent, not an error)
	r = hmsCall(t, addr, "check_lock", NewBuilder().Struct(1, check).Build())
	if resp := r.result.Struct(0); resp == nil || resp.I32(lockRespState) != int32(hmsstore.LockStateNotAcquired) {
		t.Fatalf("check_lock after unlock should be NOT_ACQUIRED, got %+v", r.result)
	}
}

func TestServerNotificationsAndFunctions(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	r := hmsCall(t, addr, "get_current_notificationEventId", &Struct{})
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_current_notificationEventId msgType=%d", r.msgType)
	}
	if resp := r.result.Struct(0); resp == nil || resp.I64(1) != 0 {
		t.Fatalf("get_current_notificationEventId wrong: %+v", r.result)
	}

	r = hmsCall(t, addr, "get_next_notification", &Struct{})
	if r.msgType != thrift.REPLY || r.result.Struct(0) == nil {
		t.Fatalf("get_next_notification wrong: %+v", r.result)
	}

	r = hmsCall(t, addr, "get_functions", NewBuilder().Str(1, "default").Str(2, "").Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_functions msgType=%d", r.msgType)
	}
	if l := r.result.List(0); len(l) != 0 {
		t.Fatalf("get_functions should be empty: %+v", l)
	}

	r = hmsCall(t, addr, "get_function", NewBuilder().Str(1, "default").Str(2, "fn").Build())
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 2 { // NoSuchObjectException is field 2
		t.Fatalf("get_function should throw NoSuchObjectException, got %+v", f)
	}
}

func TestServerUnknownMethod(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	r := hmsCall(t, addr, "definitely_not_a_method", &Struct{})
	if r.msgType != thrift.EXCEPTION {
		t.Fatalf("unknown method should be EXCEPTION, got %d", r.msgType)
	}
	if r.result == nil || r.result.String(1) == "" {
		t.Fatalf("unknown method should carry a TApplicationException message, got %+v", r.result)
	}
}

func TestServerPartitionStubs(t *testing.T) {
	store := hmsstore.NewMemoryStore()
	addr, stop := startServer(t, store)
	defer stop()

	r := hmsCall(t, addr, "get_partitions", NewBuilder().Str(1, "db").Str(2, "tbl").I16(3, -1).Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_partitions msgType=%d", r.msgType)
	}
	if l := r.result.List(0); l == nil || len(l) != 0 {
		t.Fatalf("get_partitions should be empty list, got %+v", r.result)
	}

	r = hmsCall(t, addr, "get_partition", NewBuilder().Str(1, "db").Str(2, "tbl").Add(3, ListV(thrift.STRING, nil)).Build())
	if f := r.result.Fields; len(f) != 1 || f[0].ID != 2 { // NoSuchObjectException
		t.Fatalf("get_partition should throw NoSuchObjectException, got %+v", f)
	}
}
