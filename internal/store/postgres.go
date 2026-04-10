// PostgresResourceStore is a PostgreSQL-backed implementation of ResourceStore using pgx/v5.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
func poolConfig(dsn string) (*pgxpool.Config, error) {
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
	return cfg, nil
}

// NewPostgresResourceStore opens a connection pool with startup retry, runs
// migrations, and returns a ready store.  It retries the initial ping up to
// maxAttempts times with exponential backoff so that the server can be started
// before the database is ready (e.g. docker-compose spin-up order).
func NewPostgresResourceStore(ctx context.Context, dsn string) (*PostgresResourceStore, error) {
	cfg, err := poolConfig(dsn)
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

	if err := RunMigrations(ctx, pool); err != nil {
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
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return fmt.Errorf("Create: %w", err)
	}
	return nil
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
		if errors.Is(err, pgx.ErrNoRows) {
			return ResourceEntry{}, ErrNotFound
		}
		return ResourceEntry{}, fmt.Errorf("Get: %w", err)
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
		return fmt.Errorf("Update: %w", err)
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
		return fmt.Errorf("Delete: %w", err)
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
		return nil, fmt.Errorf("List: %w", err)
	}
	defer rows.Close()

	var results []ResourceEntry
	for rows.Next() {
		var e ResourceEntry
		var data []byte
		if err := rows.Scan(&e.Type, &e.ID, &data, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("List scan: %w", err)
		}
		e.Data = json.RawMessage(data)
		results = append(results, e)
	}
	return results, rows.Err()
}

func (s *PostgresResourceStore) Purge(ctx context.Context, resourceType string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_resources WHERE resource_type=$1`, resourceType)
	return err
}

func (s *PostgresResourceStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_resources`)
}
