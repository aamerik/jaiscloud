// PostgresResourceStore is a PostgreSQL-backed implementation of ResourceStore using pgx/v5.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresResourceStore implements ResourceStore against PostgreSQL.
type PostgresResourceStore struct {
	pool *pgxpool.Pool
}

// poolConfig returns a pgxpool.Config with sensible connection-pool defaults
// similar to HikariCP: bounded pool size, health checks, and fast connection
// acquisition timeouts.
//
// cloud sets the PostgreSQL search_path so every connection in the pool
// automatically resolves unqualified table names to the cloud-specific schema
// (e.g. "aws", "azure", "gcp"). The value is allowlist-validated by
// config.Load() before it reaches here, so no sanitisation is needed.
func poolConfig(dsn, cloud string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 40
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 10 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	if cfg.ConnConfig.Config.RuntimeParams == nil {
		cfg.ConnConfig.Config.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.Config.RuntimeParams["search_path"] = cloud
	return cfg, nil
}

// NewPostgresResourceStore opens a connection pool with startup retry, runs
// migrations, and returns a ready store.  It retries the initial ping up to
// maxAttempts times with exponential backoff so that the server can be started
// before the database is ready (e.g. docker-compose spin-up order).
func NewPostgresResourceStore(ctx context.Context, dsn, cloud string) (*PostgresResourceStore, error) {
	cfg, err := poolConfig(dsn, cloud)
	if err != nil {
		return nil, fmt.Errorf("pgxpool config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}

	const maxAttempts = 10
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pingErr := pool.Ping(ctx)
		if pingErr == nil {
			break
		}
		if attempt == maxAttempts {
			pool.Close()
			return nil, fmt.Errorf("postgres ping: %w", pingErr)
		}
		slog.Warn("postgres not ready, retrying",
			"attempt", attempt, "max", maxAttempts, "backoff", backoff, "err", pingErr)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}

	if err := RunMigrations(ctx, pool, cloud); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}
	return &PostgresResourceStore{pool: pool}, nil
}

// Pool exposes the underlying pool so service-specific stores can share it.
func (s *PostgresResourceStore) Pool() *pgxpool.Pool { return s.pool }

// Close shuts down the connection pool.
func (s *PostgresResourceStore) Close() { s.pool.Close() }

func (s *PostgresResourceStore) Create(ctx context.Context, entry ResourceEntry) error {
	now := time.Now()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_resources (resource_type, id, data, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`, entry.Type, entry.ID, json.RawMessage(entry.Data), now, now)
	return wrapPgError("Create", err)
}

func (s *PostgresResourceStore) Get(ctx context.Context, resourceType, id string) (ResourceEntry, error) {
	var e ResourceEntry
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT resource_type, id, data, created_at, updated_at
		FROM jc_resources
		WHERE resource_type=$1 AND id=$2
	`, resourceType, id).Scan(&e.Type, &e.ID, &data, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return ResourceEntry{}, wrapPgError("Get", err)
	}
	e.Data = json.RawMessage(data)
	return e, nil
}

func (s *PostgresResourceStore) Update(ctx context.Context, entry ResourceEntry) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_resources
		SET data=$1, updated_at=now()
		WHERE resource_type=$2 AND id=$3
	`, json.RawMessage(entry.Data), entry.Type, entry.ID)
	if err != nil {
		return wrapPgError("Update", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresResourceStore) Delete(ctx context.Context, resourceType, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_resources WHERE resource_type=$1 AND id=$2
	`, resourceType, id)
	if err != nil {
		return wrapPgError("Delete", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresResourceStore) List(ctx context.Context, resourceType, prefix string) ([]ResourceEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT resource_type, id, data, created_at, updated_at
		FROM jc_resources
		WHERE resource_type=$1 AND ($2 = '' OR id LIKE '%' || $2 || '%')
		ORDER BY created_at
	`, resourceType, prefix)
	if err != nil {
		return nil, wrapPgError("List", err)
	}
	defer rows.Close()

	var results []ResourceEntry
	for rows.Next() {
		var e ResourceEntry
		var data []byte
		if err := rows.Scan(&e.Type, &e.ID, &data, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, wrapPgError("List scan", err)
		}
		e.Data = json.RawMessage(data)
		results = append(results, e)
	}
	return results, wrapPgError("List rows", rows.Err())
}

func (s *PostgresResourceStore) Purge(ctx context.Context, resourceType string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_resources WHERE resource_type=$1`, resourceType)
	return wrapPgError("Purge", err)
}

func (s *PostgresResourceStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_resources`)
}

// wrapPgError classifies a pgx error:
//   - pgx.ErrNoRows          → ErrNotFound
//   - unique-violation (23505) → ErrAlreadyExists
//   - network / connectivity  → ErrStorageUnavailable (wraps original)
//   - anything else           → fmt.Errorf wrapping original
//
// Callers should use this instead of raw error returns so that providers can
// distinguish "not found" from "database is down" without importing pgx.
func wrapPgError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		// Class 08 = connection errors; class 57 = operator intervention
		if len(pgErr.Code) >= 2 && (pgErr.Code[:2] == "08" || pgErr.Code[:2] == "57") {
			return fmt.Errorf("%s: %w: %w", op, ErrStorageUnavailable, err)
		}
		return fmt.Errorf("%s: %w", op, err)
	}
	// pgxpool returns net.Error or context errors when the DB is unreachable
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w: %w", op, ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}
