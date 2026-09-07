package bigquery

import (
	"bytes"
	"context"
	"testing"
)

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	if _, err := s.GetDataset(ctx, "proj", "nope"); err != ErrNoSuchDataset {
		t.Fatalf("expected ErrNoSuchDataset, got %v", err)
	}

	d := Dataset{DatasetID: "Sales", Config: []byte(`{"friendlyName":"Sales","description":"d"}`), Labels: map[string]string{"env": "dev"}}
	if err := s.CreateDataset(ctx, "proj", d); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if err := s.CreateDataset(ctx, "proj", d); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
	got, err := s.GetDataset(ctx, "proj", "Sales")
	if err != nil {
		t.Fatalf("get dataset: %v", err)
	}
	if got.Labels["env"] != "dev" {
		t.Fatalf("dataset labels lost: %+v", got)
	}
	if string(got.Config) != `{"friendlyName":"Sales","description":"d"}` {
		t.Fatalf("dataset config not verbatim: %s", got.Config)
	}

	// Tables
	if _, err := s.GetTable(ctx, "proj", "Sales", "nope"); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable, got %v", err)
	}
	tb := Table{
		TableID: "Orders",
		Config:  []byte(`{"friendlyName":"Orders Table"}`),
		Schema:  []byte(`{"fields":[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]}`),
		Labels:  map[string]string{"tier": "gold"},
	}
	if err := s.CreateTable(ctx, "proj", "Sales", tb); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := s.CreateTable(ctx, "proj", "Sales", tb); err != ErrAlreadyExists {
		t.Fatalf("expected table ErrAlreadyExists, got %v", err)
	}
	gotTable, err := s.GetTable(ctx, "proj", "Sales", "Orders")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if gotTable.Labels["tier"] != "gold" || string(gotTable.Schema) != string(tb.Schema) {
		t.Fatalf("table labels/schema lost: %+v", gotTable)
	}

	// Rows round-trip + numRows accounting.
	rows := []Row{
		{Data: []byte(`{"id":1,"name":"alice"}`)},
		{Data: []byte(`{"id":2,"name":"bob"}`)},
	}
	if err := s.InsertRows(ctx, "proj", "Sales", "Orders", rows); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
	gotTable, _ = s.GetTable(ctx, "proj", "Sales", "Orders")
	if gotTable.NumRows != 2 {
		t.Fatalf("expected numRows=2, got %d", gotTable.NumRows)
	}
	listRows, err := s.ListRows(ctx, "proj", "Sales", "Orders")
	if err != nil || len(listRows) != 2 {
		t.Fatalf("list rows: %v %d", err, len(listRows))
	}
	if listRows[0].Seq != 1 || listRows[1].Seq != 2 {
		t.Fatalf("row seq not monotonic: %+v", listRows)
	}
	if string(listRows[1].Data) != `{"id":2,"name":"bob"}` {
		t.Fatalf("row data lost: %s", listRows[1].Data)
	}
	if err := s.InsertRows(ctx, "proj", "Sales", "missing", rows); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable on row insert into missing table, got %v", err)
	}

	// Jobs
	if _, err := s.GetJob(ctx, "proj", "nope"); err != ErrNoSuchJob {
		t.Fatalf("expected ErrNoSuchJob, got %v", err)
	}
	job := Job{JobID: "j1", Config: []byte(`{"configuration":{"query":{"query":"SELECT 1"}}}`)}
	if err := s.CreateJob(ctx, "proj", job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := s.CreateJob(ctx, "proj", job); err != ErrAlreadyExists {
		t.Fatalf("expected job ErrAlreadyExists, got %v", err)
	}
	gotJob, err := s.GetJob(ctx, "proj", "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if string(gotJob.Config) != string(job.Config) {
		t.Fatalf("job config not verbatim: %s", gotJob.Config)
	}
	jobList, err := s.ListJobs(ctx, "proj")
	if err != nil || len(jobList) != 1 {
		t.Fatalf("list jobs: %v %d", err, len(jobList))
	}

	// Lists + deletes cascade.
	dsList, err := s.ListDatasets(ctx, "proj")
	if err != nil || len(dsList) != 1 {
		t.Fatalf("list datasets: %v %d", err, len(dsList))
	}
	tbList, err := s.ListTables(ctx, "proj", "Sales")
	if err != nil || len(tbList) != 1 {
		t.Fatalf("list tables: %v %d", err, len(tbList))
	}
	if err := s.DeleteJob(ctx, "proj", "j1"); err != nil {
		t.Fatalf("delete job: %v", err)
	}
	if err := s.DeleteTable(ctx, "proj", "Sales", "Orders"); err != nil {
		t.Fatalf("delete table: %v", err)
	}
	if _, err := s.GetTable(ctx, "proj", "Sales", "Orders"); err != ErrNoSuchTable {
		t.Fatalf("expected ErrNoSuchTable after delete, got %v", err)
	}
	if err := s.DeleteDataset(ctx, "proj", "Sales"); err != nil {
		t.Fatalf("delete dataset: %v", err)
	}
	if _, err := s.GetDataset(ctx, "proj", "Sales"); err != ErrNoSuchDataset {
		t.Fatalf("expected ErrNoSuchDataset after delete, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateDataset(ctx, "p", Dataset{DatasetID: "d", Labels: map[string]string{"k": "v"}, Config: []byte(`{"friendlyName":"F"}`)})
	_ = s.CreateTable(ctx, "p", "d", Table{TableID: "t", Schema: []byte(`{"fields":[{"name":"a","type":"STRING"}]}`), Labels: map[string]string{"x": "y"}})
	_ = s.InsertRows(ctx, "p", "d", "t", []Row{{Data: []byte(`{"a":"hi"}`)}})
	_ = s.CreateJob(ctx, "p", Job{JobID: "j", Config: []byte(`{"configuration":{"query":{"query":"SELECT 1"}}}`)})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := s2.GetDataset(ctx, "p", "d")
	if err != nil || got.Labels["k"] != "v" {
		t.Fatalf("dataset lost after restore: %v %+v", err, got)
	}
	gotTable, err := s2.GetTable(ctx, "p", "d", "t")
	if err != nil || gotTable.NumRows != 1 || string(gotTable.Schema) != `{"fields":[{"name":"a","type":"STRING"}]}` {
		t.Fatalf("table lost after restore: %v %+v", err, gotTable)
	}
	gotRows, err := s2.ListRows(ctx, "p", "d", "t")
	if err != nil || len(gotRows) != 1 || string(gotRows[0].Data) != `{"a":"hi"}` {
		t.Fatalf("rows lost after restore: %v %+v", err, gotRows)
	}
	gotJob, err := s2.GetJob(ctx, "p", "j")
	if err != nil || string(gotJob.Config) != `{"configuration":{"query":{"query":"SELECT 1"}}}` {
		t.Fatalf("job lost after restore: %v %+v", err, gotJob)
	}
}
