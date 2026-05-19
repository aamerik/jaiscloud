package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"jaiscloud/internal/persistence/version"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// RunMigrations creates the cloud schema if it does not exist, then applies
// all unapplied SQL migration files in sorted order. Each file is tracked in
// jc_schema_migrations (inside the cloud schema) by filename; already-applied
// files are skipped so the function is safe to call on every startup.
//
// cloud must be an allowlist-validated value ("aws", "azure", "gcp").
//
// After applying all migrations, RunMigrations verifies:
//  1. The number of applied migrations equals version.CodeDBSchemaVersion.
//  2. The stored cloud matches the running binary cloud.
//  3. Per-file SHA-256 checksums match the embedded SQL files.
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
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			checksum   TEXT        NOT NULL DEFAULT ''
		)
	`)
	if err != nil {
		return fmt.Errorf("create jc_schema_migrations: %w", err)
	}

	// Ensure the meta table exists for cloud identity.
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jc_meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("create jc_meta: %w", err)
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
		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		sum := checksumSQL(data)

		// Check if already applied.
		var storedSum string
		var count int
		err = pool.QueryRow(ctx,
			`SELECT count(*), coalesce(max(checksum),'') FROM jc_schema_migrations WHERE filename=$1`, name,
		).Scan(&count, &storedSum)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if count > 0 {
			// Verify checksum integrity. Empty storedSum means legacy row (no checksum column yet).
			if storedSum != "" && storedSum != sum {
				return fmt.Errorf("migration %s checksum mismatch: stored=%s computed=%s; "+
					"migration files must not be modified after deployment", name, storedSum, sum)
			}
			continue
		}

		// Apply the migration.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO jc_schema_migrations (filename, checksum) VALUES ($1, $2)`, name, sum,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}

	// DB schema version guard.
	var appliedCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jc_schema_migrations`).Scan(&appliedCount); err != nil {
		return fmt.Errorf("count applied migrations: %w", err)
	}
	if appliedCount > version.CodeDBSchemaVersion {
		return fmt.Errorf("stored DB schema version (%d migrations applied) is ahead of this binary (%d); "+
			"upgrade jaiscloud to the latest release", appliedCount, version.CodeDBSchemaVersion)
	}
	if appliedCount < version.CodeDBSchemaVersion {
		return fmt.Errorf("stored DB schema version (%d migrations applied) is behind this binary (%d); "+
			"run with --fresh-start to wipe and reinitialize the database", appliedCount, version.CodeDBSchemaVersion)
	}

	// Cloud identity guard: ensure the DB was initialized for the same cloud.
	var storedCloud string
	err = pool.QueryRow(ctx, `SELECT value FROM jc_meta WHERE key='cloud'`).Scan(&storedCloud)
	if err != nil {
		// First run: write the cloud identity.
		if _, err := pool.Exec(ctx,
			`INSERT INTO jc_meta (key, value) VALUES ('cloud', $1) ON CONFLICT (key) DO NOTHING`, cloud,
		); err != nil {
			return fmt.Errorf("write cloud identity: %w", err)
		}
	} else if storedCloud != cloud {
		return fmt.Errorf("database cloud mismatch: stored=%q running=%q; "+
			"use jaiscloud-%s with this database or run with --fresh-start to reinitialize", storedCloud, cloud, storedCloud)
	}

	return nil
}

// checksumSQL returns the hex-encoded SHA-256 of the SQL file content.
func checksumSQL(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
