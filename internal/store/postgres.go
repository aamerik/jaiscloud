// PostgresResourceStore is a PostgreSQL-backed implementation of ResourceStore using pgx/v5.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresResourceStore implements ResourceStore against PostgreSQL.
type PostgresResourceStore struct {
	pool *pgxpool.Pool
}

// NewPostgresResourceStore opens a connection pool, runs migrations, and returns a ready store.
func NewPostgresResourceStore(ctx context.Context, dsn string) (*PostgresResourceStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
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
