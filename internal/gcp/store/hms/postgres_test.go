//go:build gcp_persistence

package hms

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStore runs the shared store matrix against Postgres.
func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres store test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)
	runStoreTests(t, s)
}

// TestPostgresTableJSONVerbatim verifies the full Table JSON survives a
// Postgres Snapshot/Restore round trip byte-for-byte.
func TestPostgresTableJSONVerbatim(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const tblJSON = `["o",[[1,["s","t1"]],[2,["s","default"]],[9,["m",11,11,[[["s","metadata_location"],["s","gs://bucket/wh/default/t1/metadata/00001.metadata.json"]],["s","table_type"],["s","ICEBERG"]]]]]]`
	if err := s.CreateDatabase(ctx, Database{Name: "default", LocationURI: "gs://bucket/wh/default"}); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := s.CreateTable(ctx, "default", "t1", Table{DBName: "default", TableName: "t1", TableJSON: json.RawMessage(tblJSON)}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetTable(ctx, "default", "t1")
	if err != nil {
		t.Fatalf("get table after restore: %v", err)
	}
	if string(got.TableJSON) != tblJSON {
		t.Fatalf("table JSON not verbatim after restore:\n got  %s\n want %s", got.TableJSON, tblJSON)
	}
}
