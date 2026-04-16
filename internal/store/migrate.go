package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// RunMigrations creates the cloud schema if it does not exist, then applies
// all unapplied SQL migration files in sorted order. Each file is tracked in
// jc_schema_migrations (inside the cloud schema) by filename; already-applied
// files are skipped so the function is safe to call on every startup.
//
// cloud must be an allowlist-validated value ("aws", "azure", "gcp").
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, cloud string) error {
	// Create the cloud schema. pgx.Identifier quotes the name correctly.
	schemaIdent := pgx.Identifier{cloud}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaIdent); err != nil {
		return fmt.Errorf("create schema %s: %w", cloud, err)
	}

	// Ensure the tracking table exists inside the cloud schema.
	// search_path is already set to cloud on every pool connection, so the
	// unqualified name jc_schema_migrations resolves to the correct schema.
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jc_schema_migrations (
			filename   TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("create jc_schema_migrations: %w", err)
	}

	// Collect SQL file names in sorted order.
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		// Skip if already applied.
		var count int
		err = pool.QueryRow(ctx,
			`SELECT count(*) FROM jc_schema_migrations WHERE filename=$1`, name,
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if count > 0 {
			continue
		}

		// Read and execute.
		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO jc_schema_migrations (filename) VALUES ($1)`, name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}
